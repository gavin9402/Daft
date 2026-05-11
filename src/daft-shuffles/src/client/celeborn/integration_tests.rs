//! Integration tests for the Celeborn FFI shuffle client.
//!
//! These tests require a **running Celeborn cluster** and are gated behind
//! `#[ignore]`. Run them explicitly with:
//!
//! ```bash
//! CELEBORN_CPP_PREFIX=/path/to/celeborn/cpp/build/installed \
//!   cargo test -p daft-shuffles --features celeborn \
//!     --lib 'client::celeborn::integration_tests' \
//!     -- --ignored --nocapture
//! ```
//!
//! Connection defaults can be overridden via environment variables:
//! - `CELEBORN_LM_HOST` (default: `30.150.24.146`)
//! - `CELEBORN_LM_PORT` (default: `32393`)
//! - `CELEBORN_APP_ID`  (default: `my-rust-app-001`)

use std::sync::Arc;

use daft_core::{
    datatypes::{Float64Array, Int32Array, Utf8Array},
    series::IntoSeries,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::StreamExt;

use super::{
    client::{CelebornClient, CelebornClientConfig},
    ffi::ShuffleCelebornClient,
};

/// Helper: read Celeborn connection info from environment variables,
/// falling back to the default dev cluster.
fn celeborn_test_config() -> CelebornClientConfig {
    let lm_host = std::env::var("CELEBORN_LM_HOST").unwrap_or_else(|_| "30.150.24.146".to_string());
    let lm_port: i32 = std::env::var("CELEBORN_LM_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(32393);
    let app_id = std::env::var("CELEBORN_APP_ID").unwrap_or_else(|_| "my-rust-app-001".to_string());

    CelebornClientConfig {
        lm_host,
        lm_port,
        app_id,
        compression: "NONE".to_string(),
    }
}

/// Verify basic connectivity: connect to the real Celeborn LifecycleManager.
#[tokio::test]
#[ignore = "requires a running Celeborn cluster"]
async fn connect_to_real_celeborn() {
    let config = celeborn_test_config();
    println!(
        "connecting to Celeborn LM at {}:{} ...",
        config.lm_host, config.lm_port
    );

    let client = ShuffleCelebornClient::connect(&config)
        .expect("failed to connect to Celeborn LifecycleManager");

    println!("connected successfully, client created.");
    drop(client);
}

/// End-to-end: push raw bytes → mapper_end → read_partition → assert equal.
#[tokio::test]
#[ignore = "requires a running Celeborn cluster"]
async fn push_read_raw_bytes_roundtrip() {
    let config = celeborn_test_config();

    let num_mappers = 2;
    let num_partitions = 3;
    let shuffle_id: u64 = 1001;

    let client = ShuffleCelebornClient::connect(&config).expect("failed to connect");

    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await
        .expect("register_shuffle failed");

    // Mapper 0 pushes to partition 0.
    let payload_m0 = b"hello from mapper 0";
    client
        .push_data(shuffle_id, 0, 0, 0, payload_m0)
        .await
        .expect("push_data mapper 0 failed");
    println!("mapper 0: pushed {} bytes to partition 0", payload_m0.len());

    // Mapper 1 pushes to partition 0 (same partition, different mapper).
    let payload_m1 = b"hello from mapper 1";
    client
        .push_data(shuffle_id, 1, 0, 0, payload_m1)
        .await
        .expect("push_data mapper 1 failed");
    println!("mapper 1: pushed {} bytes to partition 0", payload_m1.len());

    // Both mappers signal end.
    client
        .mapper_end(shuffle_id, 0, 0)
        .await
        .expect("mapper_end(0) failed");
    client
        .mapper_end(shuffle_id, 1, 0)
        .await
        .expect("mapper_end(1) failed");
    println!("both mappers ended");

    // Read partition 0 — should contain data from both mappers.
    let mut stream = client
        .read_partition(shuffle_id, 0)
        .await
        .expect("read_partition failed");

    let mut all_bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("stream chunk error");
        all_bytes.extend_from_slice(&chunk);
    }

    println!("read {} total bytes from partition 0", all_bytes.len());

    // The combined data must contain both payloads.
    assert!(
        all_bytes.len() >= payload_m0.len() + payload_m1.len(),
        "expected at least {} bytes, got {}",
        payload_m0.len() + payload_m1.len(),
        all_bytes.len()
    );

    // Verify both payloads are present (order may vary by Celeborn internals).
    let combined = String::from_utf8_lossy(&all_bytes);
    assert!(
        combined.contains("hello from mapper 0"),
        "missing mapper 0 data in: {combined}"
    );
    assert!(
        combined.contains("hello from mapper 1"),
        "missing mapper 1 data in: {combined}"
    );
    println!("raw bytes roundtrip PASSED");
}

/// End-to-end Arrow IPC roundtrip through real Celeborn:
/// MicroPartition → IPC bytes → push_data → mapper_end → read_partition → deserialize → assert equal.
#[tokio::test]
#[ignore = "requires a running Celeborn cluster"]
async fn arrow_ipc_roundtrip_real_celeborn() {
    let config = celeborn_test_config();

    let num_mappers = 2;
    let num_partitions = 4;
    let shuffle_id: u64 = 2001;
    let target_partition: u32 = 2;

    let client = ShuffleCelebornClient::connect(&config).expect("failed to connect");

    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await
        .expect("register_shuffle failed");

    // --- Mapper 0: 3 rows ---
    let string_values_m0 = vec!["alpha", "beta", "gamma"];
    let batch_m0 = RecordBatch::from_nonempty_columns(vec![
        Int32Array::from_slice("id", &[10, 20, 30]).into_series(),
        Float64Array::from_slice("score", &[1.5, 2.5, 3.5]).into_series(),
        Utf8Array::from_slice("name", string_values_m0.as_slice()).into_series(),
    ])
    .expect("failed to build batch m0");

    let mp_m0 = MicroPartition::new_loaded(
        batch_m0.schema.clone(),
        Arc::new(vec![batch_m0.clone()]),
        None,
    );
    let ipc_m0 = mp_m0.write_to_ipc_stream().expect("failed to serialize m0");

    client
        .push_data(shuffle_id, 0, 0, target_partition, &ipc_m0)
        .await
        .expect("push_data mapper 0 failed");
    println!(
        "mapper 0: pushed {} IPC bytes to partition {target_partition}",
        ipc_m0.len()
    );

    // --- Mapper 1: 2 rows (same schema) ---
    let string_values_m1 = vec!["delta", "epsilon"];
    let batch_m1 = RecordBatch::from_nonempty_columns(vec![
        Int32Array::from_slice("id", &[40, 50]).into_series(),
        Float64Array::from_slice("score", &[4.5, 5.5]).into_series(),
        Utf8Array::from_slice("name", string_values_m1.as_slice()).into_series(),
    ])
    .expect("failed to build batch m1");

    let mp_m1 = MicroPartition::new_loaded(
        batch_m1.schema.clone(),
        Arc::new(vec![batch_m1.clone()]),
        None,
    );
    let ipc_m1 = mp_m1.write_to_ipc_stream().expect("failed to serialize m1");

    client
        .push_data(shuffle_id, 1, 0, target_partition, &ipc_m1)
        .await
        .expect("push_data mapper 1 failed");
    println!(
        "mapper 1: pushed {} IPC bytes to partition {target_partition}",
        ipc_m1.len()
    );

    // --- mapper_end for both ---
    client
        .mapper_end(shuffle_id, 0, 0)
        .await
        .expect("mapper_end(0) failed");
    client
        .mapper_end(shuffle_id, 1, 0)
        .await
        .expect("mapper_end(1) failed");
    println!("both mappers ended");

    // --- Read partition ---
    let mut stream = client
        .read_partition(shuffle_id, target_partition)
        .await
        .expect("read_partition failed");

    let mut all_bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("stream chunk error");
        all_bytes.extend_from_slice(&chunk);
    }
    println!(
        "read {} total bytes from partition {target_partition}",
        all_bytes.len()
    );

    // Celeborn concatenates push_data payloads, so the read result is
    // `ipc_m0 ++ ipc_m1` (or the reverse). We know the exact boundary
    // sizes, so we can split and deserialize each independently.
    assert_eq!(
        all_bytes.len(),
        ipc_m0.len() + ipc_m1.len(),
        "total bytes mismatch: expected {} + {} = {}, got {}",
        ipc_m0.len(),
        ipc_m1.len(),
        ipc_m0.len() + ipc_m1.len(),
        all_bytes.len()
    );

    // Split at the known boundary (mapper 0's IPC is first since map_id=0).
    let (part0, part1) = all_bytes.split_at(ipc_m0.len());
    let rt_m0 = MicroPartition::read_from_ipc_stream(part0).expect("deserialize m0 failed");
    let rt_m1 = MicroPartition::read_from_ipc_stream(part1).expect("deserialize m1 failed");

    // Verify mapper 0's data.
    assert_eq!(rt_m0.len(), 3, "mapper 0 should have 3 rows");
    assert_eq!(rt_m0.schema(), mp_m0.schema());
    assert_eq!(rt_m0.record_batches()[0], batch_m0);
    println!("mapper 0 data verified: 3 rows OK");

    // Verify mapper 1's data.
    assert_eq!(rt_m1.len(), 2, "mapper 1 should have 2 rows");
    assert_eq!(rt_m1.schema(), mp_m1.schema());
    assert_eq!(rt_m1.record_batches()[0], batch_m1);
    println!("mapper 1 data verified: 2 rows OK");

    println!("Arrow IPC roundtrip through real Celeborn PASSED");
}
