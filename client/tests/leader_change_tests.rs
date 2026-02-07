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

//! Integration tests that kill or stop cluster nodes to trigger leader
//! re-elections and verify client operations recover transparently.
//!
//! All tests use 3 servers with RF=3 and 4 shards so that every shard is
//! replicated on every server and killing any single server is always safe.

#![cfg(not(miri))]

use std::time::Duration;

use futures::StreamExt;

use mauricebarnum_oxia_client::Client;
use mauricebarnum_oxia_client::batch_get;
use mauricebarnum_oxia_client::config;

mod common;
use common::TestCluster;
use common::TestResultExt;

async fn connect_cluster(cluster: &TestCluster) -> Client {
    let builder = config::Config::builder()
        .max_parallel_requests(4)
        .request_timeout(Duration::from_secs(30))
        .retry(config::RetryConfig::new(10, Duration::from_millis(200)))
        .retry_on_stale_shard_map(true);
    cluster
        .connect_with(builder)
        .await
        .expect("cluster connect failed")
}

/// Single-key put/get operations recover from SIGKILL.
///
/// Exercises the standard retry path: SIGKILL causes connection errors
/// detected by `is_retryable()`. Between retries the background shard
/// assignment task updates the `LeaderDirectory`.
#[test_log::test(tokio::test)]
async fn test_put_get_survives_kill() -> anyhow::Result<()> {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = TestCluster::start(3, 3, 4)?;
        let client = connect_cluster(&cluster).await;

        // Put 20 keys
        for i in 0..20 {
            let key = format!("lc-pk-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        // Kill server 1
        cluster.kill_server(1)?;
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Get all 20 keys
        for i in 0..20 {
            let key = format!("lc-pk-{i:04}");
            let result = trace_err!(client.get(&key).await)?;
            assert!(result.is_some(), "key {key} should still exist after kill");
        }

        // Put 10 more keys
        for i in 20..30 {
            let key = format!("lc-pk-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        Ok::<_, anyhow::Error>(())
    });
    timeout.await??;
    Ok(())
}

/// Single-key put/get operations recover from SIGTERM.
///
/// Exercises the stale shard map retry path if the Go server sends
/// wrong-leader errors before exiting, or falls back to the standard
/// retry path if the server exits immediately.
#[test_log::test(tokio::test)]
async fn test_put_get_survives_stop() -> anyhow::Result<()> {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = TestCluster::start(3, 3, 4)?;
        let client = connect_cluster(&cluster).await;

        // Put 20 keys
        for i in 0..20 {
            let key = format!("lc-ps-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        // SIGTERM server 1
        cluster.stop_server(1)?;

        // Immediately try operations — retries should handle any errors
        for i in 0..20 {
            let key = format!("lc-ps-{i:04}");
            let result = trace_err!(client.get(&key).await)?;
            assert!(result.is_some(), "key {key} should still exist after stop");
        }

        for i in 20..30 {
            let key = format!("lc-ps-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        Ok::<_, anyhow::Error>(())
    });
    timeout.await??;
    Ok(())
}

/// Fan-out operations (list, `range_scan`, `batch_get`, `delete_range`)
/// recover from a killed server.
///
/// These operations fan out to multiple shards via `buffer_unordered` and
/// merge results, so partial shard failures must be handled correctly.
#[test_log::test(tokio::test)]
async fn test_fan_out_ops_survive_leader_change() -> anyhow::Result<()> {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = TestCluster::start(3, 3, 4)?;
        let client = connect_cluster(&cluster).await;

        // Put 50 keys
        for i in 0..50 {
            let key = format!("lc-fo-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        // Kill server 2
        cluster.kill_server(2)?;
        tokio::time::sleep(Duration::from_secs(4)).await;

        // List
        let list_result = trace_err!(client.list("lc-fo-", "lc-fo-~").await)?;
        assert_eq!(list_result.keys.len(), 50, "list should return all 50 keys");

        // Range scan
        let scan_result = trace_err!(client.range_scan("lc-fo-", "lc-fo-~").await)?;
        assert_eq!(
            scan_result.records.len(),
            50,
            "range_scan should return all 50 records"
        );

        // Batch get
        let batch_keys: Vec<String> = (0..50).map(|i| format!("lc-fo-{i:04}")).collect();
        let req = batch_get::Request::builder()
            .add_keys(batch_keys.iter().map(String::as_str))
            .build();
        let stream = trace_err!(client.batch_get(req))?;
        let items: Vec<_> = stream.collect().await;
        assert_eq!(items.len(), 50, "batch_get should return 50 items");
        for item in &items {
            assert!(
                item.response.is_ok(),
                "batch_get item {:?} should succeed",
                item.key
            );
        }

        // Delete range (first 10 keys)
        trace_err!(client.delete_range("lc-fo-0000", "lc-fo-0010").await)?;

        // Verify deletion
        let list_after = trace_err!(client.list("lc-fo-", "lc-fo-~").await)?;
        assert_eq!(
            list_after.keys.len(),
            40,
            "list should return 40 keys after delete_range"
        );

        Ok::<_, anyhow::Error>(())
    });
    timeout.await??;
    Ok(())
}

/// Full lifecycle: kill -> recover -> restart -> kill another -> recover.
///
/// Verifies that a restarted server re-joins the cluster and that
/// subsequent kills of a different server are also handled.
#[test_log::test(tokio::test)]
async fn test_kill_restart_kill() -> anyhow::Result<()> {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = TestCluster::start(3, 3, 4)?;
        let client = connect_cluster(&cluster).await;

        // Put initial data
        for i in 0..20 {
            let key = format!("lc-kr-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        // Kill server 1
        cluster.kill_server(1)?;
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Verify reads
        for i in 0..20 {
            let key = format!("lc-kr-{i:04}");
            let result = trace_err!(client.get(&key).await)?;
            assert!(
                result.is_some(),
                "key {key} should exist after killing server 1"
            );
        }

        // Restart server 1
        cluster.restart_server(1)?;

        // Kill server 2
        cluster.kill_server(2)?;
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Verify reads + writes
        for i in 0..20 {
            let key = format!("lc-kr-{i:04}");
            let result = trace_err!(client.get(&key).await)?;
            assert!(
                result.is_some(),
                "key {key} should exist after killing server 2"
            );
        }
        for i in 20..30 {
            let key = format!("lc-kr-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        Ok::<_, anyhow::Error>(())
    });
    timeout.await??;
    Ok(())
}

/// Concurrent operations survive a leader change.
///
/// Spawns 5 tasks performing put/get loops and kills a server mid-flight.
/// All tasks should complete without error thanks to retries.
#[test_log::test(tokio::test)]
async fn test_concurrent_ops_during_kill() -> anyhow::Result<()> {
    let timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut cluster = TestCluster::start(3, 3, 4)?;
        let client = connect_cluster(&cluster).await;

        // Put initial data
        for i in 0..20 {
            let key = format!("lc-co-{i:04}");
            trace_err!(client.put(&key, format!("val-{i}")).await)?;
        }

        // Spawn 5 concurrent tasks doing put/get loops
        let mut handles = Vec::new();
        for task_id in 0..5u32 {
            let c = client.clone();
            let handle = tokio::spawn(async move {
                for round in 0..10u32 {
                    let key = format!("lc-co-t{task_id}-{round:04}");
                    c.put(&key, format!("v-{task_id}-{round}"))
                        .await
                        .unwrap_or_else(|e| panic!("task {task_id} put round {round} failed: {e}"));
                    let result = c
                        .get(&key)
                        .await
                        .unwrap_or_else(|e| panic!("task {task_id} get round {round} failed: {e}"));
                    assert!(
                        result.is_some(),
                        "task {task_id} round {round}: key {key} should exist"
                    );
                }
            });
            handles.push(handle);
        }

        // Let tasks start, then kill a server
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster.kill_server(1)?;

        // Wait for all tasks to complete
        for (i, handle) in handles.into_iter().enumerate() {
            handle
                .await
                .unwrap_or_else(|e| panic!("task {i} panicked: {e}"));
        }

        Ok::<_, anyhow::Error>(())
    });
    timeout.await??;
    Ok(())
}
