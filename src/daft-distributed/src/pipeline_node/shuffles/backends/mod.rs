use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef,
    ShuffleBackend as LocalShuffleBackend, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_partition_refs::FlightPartitionRef;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeContext, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod celeborn;
mod flight;
mod ray;

pub(crate) use celeborn::CelebornShuffleBackendConfig;
use celeborn::CelebornShuffleReadSpec;
pub(crate) use flight::FlightShuffleBackendConfig;

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

#[derive(Clone)]
pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightShuffleBackendConfig),
    Celeborn(CelebornShuffleBackendConfig),
}

#[derive(Clone)]
pub(crate) struct ShuffleBackend {
    backend: DistributedShuffleBackend,
    schema: SchemaRef,
    node_id: NodeID,
}

impl ShuffleBackend {
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        backend: DistributedShuffleBackend,
    ) -> Self {
        Self {
            schema,
            node_id: context.node_id,
            backend: match backend {
                DistributedShuffleBackend::Ray => DistributedShuffleBackend::Ray,
                DistributedShuffleBackend::Flight(backend) => {
                    DistributedShuffleBackend::Flight(FlightShuffleBackendConfig {
                        shuffle_id: make_shuffle_id(context),
                        shuffle_dirs: backend.shuffle_dirs,
                        compression: backend.compression,
                    })
                }
                DistributedShuffleBackend::Celeborn(backend) => {
                    DistributedShuffleBackend::Celeborn(CelebornShuffleBackendConfig {
                        shuffle_id: make_shuffle_id(context),
                        lm_host: backend.lm_host,
                        lm_port: backend.lm_port,
                        app_id: backend.app_id,
                        compression: backend.compression,
                        push_data_timeout_ms: backend.push_data_timeout_ms,
                        fetch_data_timeout_ms: backend.fetch_data_timeout_ms,
                    })
                }
            },
        }
    }

    pub(crate) fn backend(&self) -> &DistributedShuffleBackend {
        &self.backend
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn node_id(&self) -> NodeID {
        self.node_id
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            DistributedShuffleBackend::Ray => {}
            DistributedShuffleBackend::Flight(backend) => {
                flight::register_cleanup(backend, plan_context);
            }
            DistributedShuffleBackend::Celeborn(backend) => {
                celeborn::register_cleanup(backend, plan_context);
            }
        }
    }

    /// The local-plan `ShuffleBackend` matching this distributed shuffle
    /// backend, for use when building local ops like `IntoPartitions`,
    /// `GatherWrite`, or `RepartitionWrite`.
    pub(crate) fn local_shuffle_backend(&self) -> LocalShuffleBackend {
        match self.backend.clone() {
            DistributedShuffleBackend::Ray => LocalShuffleBackend::Ray,
            DistributedShuffleBackend::Flight(cfg) => LocalShuffleBackend::Flight {
                shuffle_id: cfg.shuffle_id,
                shuffle_dirs: cfg.shuffle_dirs,
                compression: cfg.compression,
            },
            DistributedShuffleBackend::Celeborn(cfg) => LocalShuffleBackend::Celeborn {
                shuffle_id: cfg.shuffle_id,
            },
        }
    }

    /// Build a `SwordfishTaskBuilder` whose plan reads from already-materialized
    /// partition refs (`in_memory_scan` for Ray, `shuffle_read(Flight)` for Flight)
    /// and then applies `wrap_plan` on top. The partition refs are attached to
    /// the task via the backend-appropriate API (`with_psets` /
    /// `with_flight_shuffle_reads`).
    pub(crate) fn build_refs_task_builder<F>(
        &self,
        partition_refs: Vec<PartitionRef>,
        node: &dyn PipelineNodeImpl,
        wrap_plan: F,
    ) -> SwordfishTaskBuilder
    where
        F: FnOnce(LocalPhysicalPlanRef) -> LocalPhysicalPlanRef,
    {
        let node_id = self.node_id;
        match &self.backend {
            DistributedShuffleBackend::Ray => {
                let total_size_bytes = partition_refs.iter().map(|p| p.size_bytes()).sum::<usize>();
                let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
                    node_id,
                    self.schema.clone(),
                    total_size_bytes,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                );
                let plan = wrap_plan(in_memory_scan);
                SwordfishTaskBuilder::new(plan, node, node_id).with_psets(node_id, partition_refs)
            }
            DistributedShuffleBackend::Flight(_) => {
                let flight_refs = partition_refs
                    .into_iter()
                    .map(|p| {
                        p.as_any()
                            .downcast_ref::<FlightPartitionRef>()
                            .expect("expected flight partition ref")
                            .clone()
                    })
                    .collect::<Vec<_>>();
                let shuffle_read = LocalPhysicalPlan::shuffle_read(
                    node_id,
                    self.schema.clone(),
                    ShuffleReadBackend::Flight {
                        shuffle_id: 0,
                        server_cache_mapping: Default::default(),
                    },
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                );
                let plan = wrap_plan(shuffle_read);
                SwordfishTaskBuilder::new(plan, node, node_id).with_flight_shuffle_reads(
                    node_id,
                    vec![FlightShuffleReadInput { refs: flight_refs }],
                )
            }
            DistributedShuffleBackend::Celeborn(_) => {
                unreachable!("Celeborn does not use produce_read_task; use emit_read_tasks instead")
            }
        }
    }

    pub(crate) async fn emit_read_tasks(
        &self,
        partition_groups: Vec<Vec<MaterializedOutput>>,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
        num_partitions: usize,
    ) -> DaftResult<()> {
        match &self.backend {
            DistributedShuffleBackend::Ray => {
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            DistributedShuffleBackend::Flight(_) => {
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            DistributedShuffleBackend::Celeborn(backend) => {
                let read_spec: CelebornShuffleReadSpec = celeborn::read_spec_from_backend(backend);
                celeborn::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    num_partitions,
                    backend,
                    read_spec,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }
}
