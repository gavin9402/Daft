//! FFI-backed Celeborn shuffle client implementation.
//!
//! This bridges our async [`CelebornClient`] trait to the synchronous
//! `celeborn_client::ShuffleClient` (C++ FFI). All FFI calls are dispatched
//! to a blocking thread via `tokio::task::spawn_blocking` so they never block
//! the async runtime.

use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;
use celeborn_client::{Config as CelebornConfig, ShuffleClient};
use common_error::{DaftError, DaftResult};
use futures::stream;

use super::client::{CelebornClient, CelebornClientConfig, PartitionDataStream};

/// Convert a value to `i32`, returning a descriptive error on overflow.
///
/// The Celeborn C++ FFI uses `i32` for all ID parameters while the Daft
/// trait uses wider unsigned types. This helper centralises the checked
/// conversion so callers don't repeat the same boilerplate.
fn to_ffi_i32(value: impl TryInto<i32> + std::fmt::Display + Copy, name: &str) -> DaftResult<i32> {
    value.try_into().map_err(|_| {
        DaftError::External(format!("{name} {value} overflows i32 (Celeborn FFI limit)").into())
    })
}

/// Acquire the FFI client lock, returning a descriptive error if poisoned.
fn lock_client(
    inner: &Mutex<CelebornShuffleClient>,
) -> DaftResult<MutexGuard<'_, CelebornShuffleClient>> {
    inner
        .lock()
        .map_err(|e| DaftError::External(format!("Celeborn client lock poisoned: {e}").into()))
}

/// Run a synchronous FFI closure on the tokio blocking thread pool and
/// map JoinError (panic) into a [`DaftError`].
async fn run_blocking<F, R>(op_name: &str, f: F) -> DaftResult<R>
where
    F: FnOnce() -> DaftResult<R> + Send + 'static,
    R: Send + 'static,
{
    let op = op_name.to_owned();
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| DaftError::External(format!("Celeborn {op} task panicked: {e}").into()))?
}

/// Thread-safe wrapper around `celeborn_client::ShuffleClient`.
///
/// `ShuffleClient` contains a CXX `UniquePtr<ShuffleClientHandle>` whose raw
/// pointer (`*const cxx::void`) prevents auto `Send`/`Sync`. All access is
/// serialised through a `Mutex`, so concurrent use from multiple threads is safe.
struct CelebornShuffleClient(ShuffleClient);

// SAFETY: `ShuffleClient` is !Send only because it contains a CXX
// `UniquePtr` with a raw pointer. The raw pointer is exclusively owned
// by this newtype and never shared. All access to the inner
// `ShuffleClient` is mediated through `Arc<Mutex<CelebornShuffleClient>>`
// in `ShuffleCelebornClient`, which guarantees:
//   1. Only one thread holds the lock at any time (mutual exclusion).
//   2. The `Mutex` provides a happens-before relationship between
//      lock/unlock pairs (memory ordering).
// Therefore it is safe to move the wrapper between threads.
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

// SAFETY: All fields are inherently `Send + Sync`:
//   - `Arc<Mutex<CelebornShuffleClient>>`: `Arc` is `Send + Sync` when the
//     inner type is `Send`, which we guarantee above via the manual `Send`
//     impl on `CelebornShuffleClient` + the `Mutex` serialisation.
//   - `i32` values: trivially `Send + Sync`.
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
            DaftError::External(
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
        let shuffle_id = to_ffi_i32(shuffle_id, "shuffle_id")?;
        let map_id = to_ffi_i32(map_id, "map_id")?;
        let attempt_id = to_ffi_i32(attempt_id, "attempt_id")?;
        let partition_id = to_ffi_i32(partition_id, "partition_id")?;
        let num_mappers = self.num_mappers;
        let num_partitions = self.num_partitions;
        // Copy data to owned buffer so it can be moved into spawn_blocking.
        let data_owned = data.to_vec();

        run_blocking("push_data", move || {
            let mut guard = lock_client(&inner)?;
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
                .map_err(|e| DaftError::External(format!("Celeborn push_data failed: {e}").into()))
        })
        .await
    }

    async fn mapper_end(
        &self,
        shuffle_id: u64,
        map_id: u32,
        attempt_id: u32,
        num_mappers: u32,
    ) -> DaftResult<()> {
        let inner = Arc::clone(&self.inner);
        let shuffle_id = to_ffi_i32(shuffle_id, "shuffle_id")?;
        let map_id = to_ffi_i32(map_id, "map_id")?;
        let attempt_id = to_ffi_i32(attempt_id, "attempt_id")?;
        let num_mappers = to_ffi_i32(num_mappers, "num_mappers")?;

        run_blocking("mapper_end", move || {
            let mut guard = lock_client(&inner)?;
            guard
                .0
                .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
                .map_err(|e| DaftError::External(format!("Celeborn mapper_end failed: {e}").into()))
        })
        .await
    }

    async fn read_partition(
        &self,
        shuffle_id: u64,
        partition_id: u32,
    ) -> DaftResult<PartitionDataStream> {
        let inner = Arc::clone(&self.inner);
        let shuffle_id = to_ffi_i32(shuffle_id, "shuffle_id")?;
        let partition_id = to_ffi_i32(partition_id, "partition_id")?;
        let num_mappers = self.num_mappers;

        let data = run_blocking("read_partition", move || {
            let mut guard = lock_client(&inner)?;
            // Must update reducer file group metadata before reading.
            guard.0.update_reducer_file_group(shuffle_id).map_err(|e| {
                DaftError::External(
                    format!("Celeborn update_reducer_file_group failed: {e}").into(),
                )
            })?;
            guard
                .0
                .read_partition_all(shuffle_id, partition_id, num_mappers)
                .map_err(|e| {
                    DaftError::External(format!("Celeborn read_partition failed: {e}").into())
                })
        })
        .await?;

        // Wrap the returned bytes as a single-chunk stream.
        let bytes = Bytes::from(data);
        Ok(Box::pin(stream::once(async move { Ok(bytes) })))
    }

    /// No-op in the current FFI layer.
    ///
    /// The underlying `celeborn_client::ShuffleClient` C++ FFI does not expose
    /// an explicit `unregister_shuffle` API. Shuffle data cleanup is handled
    /// by the Celeborn cluster's own garbage-collection mechanism
    /// (LifecycleManager timeout / application heartbeat expiry), so the
    /// client side does not need to take any action here.
    async fn unregister_shuffle(&self, _shuffle_id: u64) -> DaftResult<()> {
        Ok(())
    }
}
