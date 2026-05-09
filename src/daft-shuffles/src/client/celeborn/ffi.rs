//! FFI-backed Celeborn shuffle client implementation.
//!
//! This bridges our async [`CelebornClient`] trait to the synchronous
//! `celeborn_client::ShuffleClient` (C++ FFI). All FFI calls are dispatched
//! to a blocking thread via `tokio::task::spawn_blocking` so they never block
//! the async runtime.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use celeborn_client::{Config as CelebornConfig, ShuffleClient};
use common_error::DaftResult;
use futures::stream;

use super::client::{CelebornClient, CelebornClientConfig, PartitionDataStream};

/// Thread-safe wrapper around `celeborn_client::ShuffleClient`.
///
/// `ShuffleClient` contains a CXX `UniquePtr<ShuffleClientHandle>` whose raw
/// pointer (`*const cxx::void`) prevents auto `Send`/`Sync`. All access is
/// serialised through a `Mutex`, so concurrent use from multiple threads is safe.
struct CelebornShuffleClient(ShuffleClient);

// SAFETY: see doc comment above — all access goes through `Mutex`.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for CelebornShuffleClient {}

/// Celeborn shuffle client backed by the C++ FFI implementation.
///
/// Thread-safety: `ShuffleClient` requires `&mut self` for all operations, so
/// we wrap it in an `Arc<Mutex<_>>`. The `Mutex` is held only for the duration
/// of each synchronous FFI call inside `spawn_blocking`, keeping contention
/// minimal.
pub struct ShuffleCelebornClient {
    inner: Arc<Mutex<CelebornShuffleClient>>,
    /// Number of map tasks in this shuffle (required by FFI `push_data` and `mapper_end`).
    num_mappers: i32,
    /// Number of reduce partitions (required by FFI `push_data`).
    num_partitions: i32,
}

// SAFETY: All access to the FFI client goes through `Mutex<CelebornShuffleClient>`
// which serialises all operations. The struct only holds `Arc`, `i32` — all
// inherently `Send + Sync` once the inner type is `Send`.
unsafe impl Send for ShuffleCelebornClient {}
unsafe impl Sync for ShuffleCelebornClient {}

impl ShuffleCelebornClient {
    /// Connect to a running Celeborn LifecycleManager and return a new client.
    ///
    /// # Arguments
    /// * `config` - Application-level Celeborn configuration.
    /// * `lm_host` - LifecycleManager hostname or IP.
    /// * `lm_port` - LifecycleManager port.
    /// * `num_mappers` - Total number of map tasks in this shuffle.
    /// * `num_partitions` - Total number of reduce partitions.
    pub fn connect(
        config: &CelebornClientConfig,
        lm_host: &str,
        lm_port: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> DaftResult<Self> {
        let codec = config.compression.to_uppercase();
        let celeborn_config = CelebornConfig {
            app_id: config.app_id.clone(),
            push_buffer_max_size: 0, // use C++ default (64KB)
            shuffle_compression_codec: codec,
        };

        let client = ShuffleClient::connect(celeborn_config, lm_host, lm_port).map_err(|e| {
            common_error::DaftError::External(
                format!(
                    "Failed to connect to Celeborn LifecycleManager at {lm_host}:{lm_port}: {e}"
                )
                .into(),
            )
        })?;

        Ok(Self {
            inner: Arc::new(Mutex::new(CelebornShuffleClient(client))),
            num_mappers,
            num_partitions,
        })
    }
}

#[async_trait]
impl CelebornClient for ShuffleCelebornClient {
    async fn push_data(
        &self,
        shuffle_id: u64,
        map_id: u32,
        attempt_id: u32,
        partition_id: u32,
        data: &[u8],
    ) -> DaftResult<()> {
        let inner = Arc::clone(&self.inner);
        let shuffle_id = shuffle_id as i32;
        let map_id = map_id as i32;
        let attempt_id = attempt_id as i32;
        let partition_id = partition_id as i32;
        let num_mappers = self.num_mappers;
        let num_partitions = self.num_partitions;
        // Copy data to owned buffer so it can be moved into spawn_blocking.
        let data_owned = data.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|e| {
                common_error::DaftError::External(
                    format!("Celeborn client lock poisoned: {e}").into(),
                )
            })?;
            guard
                .0
                .push_data(
                    shuffle_id,
                    map_id,
                    attempt_id,
                    partition_id,
                    &data_owned,
                    num_mappers,
                    num_partitions,
                )
                .map_err(|e| {
                    common_error::DaftError::External(
                        format!("Celeborn push_data failed: {e}").into(),
                    )
                })
        })
        .await
        .map_err(|e| {
            common_error::DaftError::External(
                format!("Celeborn push_data task panicked: {e}").into(),
            )
        })?
    }

    async fn mapper_end(
        &self,
        shuffle_id: u64,
        map_id: u32,
        attempt_id: u32,
        num_mappers: u32,
    ) -> DaftResult<()> {
        let inner = Arc::clone(&self.inner);
        let shuffle_id = shuffle_id as i32;
        let map_id = map_id as i32;
        let attempt_id = attempt_id as i32;
        let num_mappers = num_mappers as i32;

        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|e| {
                common_error::DaftError::External(
                    format!("Celeborn client lock poisoned: {e}").into(),
                )
            })?;
            guard
                .0
                .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
                .map_err(|e| {
                    common_error::DaftError::External(
                        format!("Celeborn mapper_end failed: {e}").into(),
                    )
                })
        })
        .await
        .map_err(|e| {
            common_error::DaftError::External(
                format!("Celeborn mapper_end task panicked: {e}").into(),
            )
        })?
    }

    async fn read_partition(
        &self,
        shuffle_id: u64,
        partition_id: u32,
    ) -> DaftResult<PartitionDataStream> {
        let inner = Arc::clone(&self.inner);
        let shuffle_id_i32 = shuffle_id as i32;
        let partition_id_i32 = partition_id as i32;
        let num_mappers = self.num_mappers;

        let data = tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|e| {
                common_error::DaftError::External(
                    format!("Celeborn client lock poisoned: {e}").into(),
                )
            })?;
            // Must update reducer file group metadata before reading.
            guard
                .0
                .update_reducer_file_group(shuffle_id_i32)
                .map_err(|e| {
                    common_error::DaftError::External(
                        format!("Celeborn update_reducer_file_group failed: {e}").into(),
                    )
                })?;
            guard
                .0
                .read_partition_all(shuffle_id_i32, partition_id_i32, num_mappers)
                .map_err(|e| {
                    common_error::DaftError::External(
                        format!("Celeborn read_partition failed: {e}").into(),
                    )
                })
        })
        .await
        .map_err(|e| {
            common_error::DaftError::External(
                format!("Celeborn read_partition task panicked: {e}").into(),
            )
        })??;

        // Wrap the returned bytes as a single-chunk stream.
        let bytes = Bytes::from(data);
        Ok(Box::pin(stream::once(async move { Ok(bytes) })))
    }

    async fn unregister_shuffle(&self, shuffle_id: u64) -> DaftResult<()> {
        let inner = Arc::clone(&self.inner);
        let shuffle_id_i32 = shuffle_id as i32;

        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|e| {
                common_error::DaftError::External(
                    format!("Celeborn client lock poisoned: {e}").into(),
                )
            })?;
            // The FFI client doesn't expose an explicit unregister API.
            // For per-shuffle cleanup this is a no-op in the current FFI layer.
            let _ = shuffle_id_i32;
            drop(guard);
            Ok::<(), common_error::DaftError>(())
        })
        .await
        .map_err(|e| {
            common_error::DaftError::External(
                format!("Celeborn unregister_shuffle task panicked: {e}").into(),
            )
        })?
    }
}
