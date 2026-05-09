//! Celeborn shuffle client abstraction and implementations.
//!
//! See [`client::CelebornClient`] for the trait definition and
//! [`mock::MockShuffleCelebornClient`] for the in-memory placeholder used during
//! Daft-side development.

mod client;
#[cfg(feature = "celeborn")]
mod ffi;
mod mock;

pub use client::{CelebornClient, CelebornClientConfig, PartitionDataStream};
#[cfg(feature = "celeborn")]
pub use ffi::ShuffleCelebornClient;
pub use mock::MockShuffleCelebornClient;
