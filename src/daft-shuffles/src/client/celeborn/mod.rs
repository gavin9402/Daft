//! Celeborn shuffle client abstraction and implementations.
//!
//! See [`client::CelebornClient`] for the trait definition and
//! [`mock::MockShuffleCelebornClient`] for the in-memory placeholder used during
//! Daft-side development.

use std::sync::Arc;

use common_error::DaftResult;

mod client;
#[cfg(feature = "celeborn")]
mod ffi;
#[cfg(all(feature = "celeborn", test))]
mod integration_tests;
mod mock;

pub use client::{CelebornClient, CelebornClientConfig, PartitionDataStream};
#[cfg(feature = "celeborn")]
pub use ffi::ShuffleCelebornClient;
pub use mock::MockShuffleCelebornClient;

/// Create a connected Celeborn client from connection-level configuration.
///
/// When the `celeborn` feature is enabled this returns a real FFI-backed
/// [`ShuffleCelebornClient`] that connects to the Celeborn LifecycleManager.
/// Without the feature the function returns an error — callers should only
/// reach this path when a distributed executor has selected
/// `shuffle_algorithm = "celeborn"`, which requires the feature.
///
/// # Arguments
/// * `config` - Connection-level Celeborn configuration (lm_host, lm_port,
///   app_id, compression).
pub fn connect_celeborn_client(
    config: &CelebornClientConfig,
) -> DaftResult<Arc<dyn CelebornClient>> {
    #[cfg(feature = "celeborn")]
    {
        let client = ShuffleCelebornClient::connect(config)?;
        Ok(Arc::new(client))
    }
    #[cfg(not(feature = "celeborn"))]
    {
        let _ = config;
        Err(common_error::DaftError::InternalError(
            "Celeborn shuffle backend requires the `celeborn` feature to be enabled. \
             Rebuild with `--features celeborn`."
                .to_string(),
        ))
    }
}
