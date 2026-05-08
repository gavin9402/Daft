//! Celeborn shuffle client abstraction and implementations.
//!
//! See [`client::CelebornClient`] for the trait definition and
//! [`mock::MockCelebornClient`] for the in-memory placeholder used during
//! Daft-side development.

mod client;
mod mock;

pub use client::{CelebornClient, CelebornClientConfig, PartitionDataStream};
pub use mock::MockCelebornClient;
