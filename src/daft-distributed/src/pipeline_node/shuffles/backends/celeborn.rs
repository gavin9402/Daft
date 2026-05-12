use common_error::DaftResult;
use daft_local_plan::{
    CelebornShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

/// Runtime configuration for the Celeborn shuffle backend.
///
/// One instance lives on the coordinator side per `RepartitionNode`. It is
/// cloned into both the write-stage `RepartitionWriteBackend::Celeborn` (which
/// is serialized to each map task) and the read-stage `ShuffleReadBackend::Celeborn`
/// (which is serialized to each reduce task).
#[derive(Clone)]
pub(crate) struct CelebornShuffleBackendConfig {
    pub(crate) shuffle_id: u64,
    pub(crate) lm_host: String,
    pub(crate) lm_port: i32,
    pub(crate) app_id: String,
    pub(crate) compression: String,
}

/// Read-stage spec carrying the metadata needed to construct reduce tasks.
///
/// For Celeborn this is intentionally lightweight: the Celeborn cluster itself
/// owns the partition location index, so reduce tasks only need to know the
/// shuffle id; the per-task `partition_idx` is
/// supplied separately through the task `Input::CelebornShuffle` channel (see
/// `emit_read_tasks`).
pub(crate) struct CelebornShuffleReadSpec {
    pub(crate) shuffle_id: u64,
}

/// Cleanup hook for Celeborn shuffles.
///
/// Celeborn shuffle data is owned by the Celeborn cluster, not by Daft worker
/// processes, so there are no local files for `PlanExecutionContext` to track.
/// Resource release happens via `CelebornClient::unregister_shuffle`, which is
/// invoked on the worker that runs the reduce side once all reduce tasks for
/// the shuffle have completed (see `ShuffleReadSource::celeborn` driver in
/// `daft-local-execution`); on top of that, the Celeborn cluster GCs orphan
/// shuffle data by application lifecycle.
///
/// This function therefore intentionally performs no work — it exists so the
/// `ShuffleBackend::register_cleanup` dispatch table has a uniform shape across
/// all backend variants.
pub(crate) fn register_cleanup(
    _backend: &CelebornShuffleBackendConfig,
    _plan_context: &mut PlanExecutionContext,
) {
}

/// Build a `CelebornShuffleReadSpec` from the on-coordinator backend config.
///
/// Unlike Flight which needs to aggregate per-mapper output locations, Celeborn
/// hides the location index inside its own cluster—so the spec is just the
/// shuffle id.
pub(crate) fn read_spec_from_backend(
    backend: &CelebornShuffleBackendConfig,
) -> CelebornShuffleReadSpec {
    CelebornShuffleReadSpec {
        shuffle_id: backend.shuffle_id,
    }
}

/// Emit one reduce task per partition. Each task runs a local
/// `LocalPhysicalPlan::shuffle_read` whose backend is
/// `ShuffleReadBackend::Celeborn`. The target `partition_idx` is attached as a
/// `CelebornShuffleReadInput` so the worker-side `ShuffleReadSource` knows which
/// reduce partition to call `CelebornClient::read_partition` for.
pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    num_partitions: usize,
    _backend: &CelebornShuffleBackendConfig,
    read_spec: CelebornShuffleReadSpec,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for partition_idx in 0..num_partitions {
        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Celeborn {
                shuffle_id: read_spec.shuffle_id,
            },
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let task = SwordfishTaskBuilder::new(shuffle_read_plan, node, node_id)
            .with_celeborn_shuffle_reads(node_id, vec![CelebornShuffleReadInput { partition_idx }]);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_backend() -> CelebornShuffleBackendConfig {
        CelebornShuffleBackendConfig {
            shuffle_id: 42,
            lm_host: "host1".to_string(),
            lm_port: 9097,
            app_id: "app-123".to_string(),
            compression: "lz4".to_string(),
        }
    }

    /// `read_spec_from_backend` is the bridge between the coordinator-side
    /// backend config and the per-task read spec. Today it forwards
    /// `shuffle_id`; this test pins that contract so future changes that
    /// accidentally drop or rename these fields are caught.
    #[test]
    fn read_spec_from_backend_forwards_shuffle_id() {
        let backend = sample_backend();
        let spec = read_spec_from_backend(&backend);
        assert_eq!(spec.shuffle_id, 42);
    }

    /// `read_spec_from_backend` must be cheap (no allocations beyond the
    /// returned struct) and must not mutate the source backend. We verify the
    /// "no mutation" half by re-reading the backend after the call.
    #[test]
    fn read_spec_from_backend_does_not_mutate_input() {
        let backend = sample_backend();
        let _ = read_spec_from_backend(&backend);
        assert_eq!(backend.shuffle_id, 42);
        assert_eq!(backend.lm_host, "host1");
        assert_eq!(backend.lm_port, 9097);
        assert_eq!(backend.app_id, "app-123");
    }

    /// `CelebornShuffleReadInput` is what `emit_read_tasks` attaches to each
    /// reduce task to convey the target partition index. The reducer
    /// (`CelebornShuffleReadSource`) reads this field directly to call
    /// `client.read_partition(_, partition_idx)`. This test pins the round-trip
    /// contract so any breaking rename/move of the field is caught.
    #[test]
    fn celeborn_shuffle_read_input_round_trip_via_serde() {
        let inputs: Vec<CelebornShuffleReadInput> = (0..5)
            .map(|i| CelebornShuffleReadInput { partition_idx: i })
            .collect();

        let json = serde_json::to_string(&inputs).expect("serializable");
        let decoded: Vec<CelebornShuffleReadInput> =
            serde_json::from_str(&json).expect("deserializable");

        assert_eq!(decoded.len(), 5);
        for (i, input) in decoded.iter().enumerate() {
            assert_eq!(input.partition_idx, i);
        }
    }
}
