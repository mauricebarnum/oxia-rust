// Copyright 2025-2026 Maurice S. Barnum
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Parameterized integration tests that run against multiple configurations:
//! standalone (1 shard), standalone (4 shards), and cluster (3 nodes, 6 shards).

#![cfg(not(miri))]

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Barrier;
use tracing::info;

use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::config;

mod common;
use common::TestEnv;
use common::TestResultExt;

// ---------------------------------------------------------------------------
// Test bodies — each takes a `&dyn TestEnv` and returns `anyhow::Result<()>`.
// ---------------------------------------------------------------------------

async fn test_basic_crud(env: &dyn TestEnv) -> anyhow::Result<()> {
    let client = trace_err!(common::connect_env(env).await)?;

    // Put 50 keys
    let keys: Vec<String> = (0..50).map(|i| format!("crud-{i:04}")).collect();
    for key in &keys {
        trace_err!(client.put(key, format!("value-{key}")).await)?;
    }

    // Get all back
    for key in &keys {
        let result = trace_err!(client.get(key).await)?;
        assert!(result.is_some(), "key {key} should exist");
        let value = result.unwrap().value.unwrap();
        assert_eq!(value.as_ref(), format!("value-{key}").as_bytes());
    }

    // Delete the first 10
    for key in &keys[..10] {
        trace_err!(client.delete(key).await)?;
    }

    // Verify deleted
    for key in &keys[..10] {
        let result = trace_err!(client.get(key).await)?;
        assert!(result.is_none(), "key {key} should be deleted");
    }

    // Verify remaining still exist
    for key in &keys[10..] {
        let result = trace_err!(client.get(key).await)?;
        assert!(result.is_some(), "key {key} should still exist");
    }

    Ok(())
}

async fn test_list_keys(env: &dyn TestEnv) -> anyhow::Result<()> {
    let client = trace_err!(common::connect_env(env).await)?;

    let keys: Vec<String> = (0..100).map(|i| format!("list-{i:04}")).collect();
    for key in &keys {
        trace_err!(client.put(key, "v").await)?;
    }

    // List all keys (empty range = everything)
    let result = trace_err!(client.list("", "").await)?;

    // All of our keys should appear
    let result_set: std::collections::HashSet<&str> =
        result.keys.iter().map(String::as_str).collect();
    for key in &keys {
        assert!(
            result_set.contains(key.as_str()),
            "list should contain {key}"
        );
    }

    info!(total_keys = result.keys.len(), "listed keys");
    Ok(())
}

async fn test_range_scan(env: &dyn TestEnv) -> anyhow::Result<()> {
    let client = trace_err!(common::connect_env(env).await)?;

    // Insert keys with a shared prefix so we can range-scan a subset
    let all_keys: Vec<String> = (0..30).map(|i| format!("rscan-{i:04}")).collect();
    for key in &all_keys {
        trace_err!(client.put(key, format!("val-{key}")).await)?;
    }

    // Scan [rscan-0010, rscan-0020)
    let start_key = "rscan-0010";
    let end_key = "rscan-0020";
    let result = trace_err!(client.range_scan(start_key, end_key).await)?;

    let scanned_keys: Vec<&str> = result
        .records
        .iter()
        .map(|r| r.key.as_ref().unwrap().as_str())
        .collect();

    // Should contain keys 10..20
    assert_eq!(scanned_keys.len(), 10, "should scan exactly 10 keys");
    for i in 10..20 {
        let expected = format!("rscan-{i:04}");
        assert!(
            scanned_keys.contains(&expected.as_str()),
            "scan should contain {expected}"
        );
    }

    Ok(())
}

async fn test_delete_range(env: &dyn TestEnv) -> anyhow::Result<()> {
    let client = trace_err!(common::connect_env(env).await)?;

    let keys: Vec<String> = (0..20).map(|i| format!("drange-{i:04}")).collect();
    for key in &keys {
        trace_err!(client.put(key, "v").await)?;
    }

    // Delete [drange-0005, drange-0015)
    trace_err!(client.delete_range("drange-0005", "drange-0015").await)?;

    // Verify deleted range
    for key in &keys[5..15] {
        let result = trace_err!(client.get(key).await)?;
        assert!(result.is_none(), "key {key} should be deleted");
    }

    // Verify surviving keys
    for key in keys.iter().take(5).chain(keys.iter().skip(15)) {
        let result = trace_err!(client.get(key).await)?;
        assert!(result.is_some(), "key {key} should survive");
    }

    Ok(())
}

async fn test_batch_get(env: &dyn TestEnv) -> anyhow::Result<()> {
    let builder = config::Config::builder().max_parallel_requests(10);
    let client = trace_err!(common::connect_env_with(env, builder).await)?;

    let keys: Vec<String> = (0..40).map(|i| format!("batch-{i:04}")).collect();
    for key in &keys {
        trace_err!(client.put(key, format!("val-{key}")).await)?;
    }

    let req = client::batch_get::Request::builder()
        .add_keys(keys.clone())
        .build();

    let mut stream = trace_err!(client.batch_get(req))?;
    let mut results: HashMap<String, bool> = HashMap::new();

    while let Some(item) = stream.next().await {
        let response = trace_err!(item.response)?;
        results.insert(item.key.to_string(), response.is_some());
    }

    assert_eq!(results.len(), keys.len(), "should get all 40 keys back");
    for key in &keys {
        assert_eq!(
            results.get(key.as_str()),
            Some(&true),
            "batch_get should find {key}"
        );
    }

    Ok(())
}

async fn test_concurrent_ops(env: &dyn TestEnv) -> anyhow::Result<()> {
    const NUM_TASKS: usize = 20;

    let client = Arc::new(trace_err!(common::connect_env(env).await)?);

    let barrier = Arc::new(Barrier::new(NUM_TASKS));
    let mut handles = Vec::with_capacity(NUM_TASKS);

    for task_id in 0..NUM_TASKS {
        let c = Arc::clone(&client);
        let b = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let key = format!("conc-{task_id:04}");
            let expected = format!("value-{task_id}");
            c.put(&key, expected.clone()).await?;
            let result = c.get(&key).await?;
            assert!(
                result.is_some(),
                "task {task_id}: key should exist after put"
            );
            let got = result.unwrap().value.unwrap();
            assert_eq!(
                got.as_ref(),
                expected.as_bytes(),
                "task {task_id}: value mismatch"
            );
            Ok::<(), client::Error>(())
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok(), "task {i} failed: {:?}", result.err());
    }

    // Verify all keys exist from main context
    for task_id in 0..NUM_TASKS {
        let key = format!("conc-{task_id:04}");
        let result = trace_err!(client.get(&key).await)?;
        assert!(
            result.is_some(),
            "key {key} should exist after concurrent puts"
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Parameterized invocations
// ---------------------------------------------------------------------------

parameterized_test_standard!(test_basic_crud);
parameterized_test_standard!(test_list_keys);
parameterized_test_standard!(test_range_scan);
parameterized_test_standard!(test_delete_range);
parameterized_test_standard!(test_batch_get);
parameterized_test_standard!(test_concurrent_ops);
