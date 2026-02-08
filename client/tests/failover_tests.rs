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

//! Tests for shard assignment stream failover when server 0 dies.

#![cfg(not(miri))]

use std::sync::Arc;
use std::time::Duration;

use mauricebarnum_oxia_client::Client;
use mauricebarnum_oxia_client::config;
use mauricebarnum_oxia_client::discovery::CoordinatorServiceDiscovery;
use mauricebarnum_oxia_client::discovery::StaticServiceDiscovery;

mod common;
use common::TestCluster;

/// Start a 3-server cluster, connect a client with `additional_service_addrs`
/// pointing to servers 1 and 2, write some keys, kill server 0, and verify
/// the client recovers by reconnecting the assignment stream to another server.
#[test_log::test(tokio::test)]
async fn test_failover_on_server0_kill() -> anyhow::Result<()> {
    let mut cluster = TestCluster::start(3, 3, 4)?;
    let all_addrs = cluster.server_addrs();

    // Connect with all server addresses so the client can fail over
    let cfg = config::Config::builder()
        .service_discovery(StaticServiceDiscovery::new(all_addrs))
        .build();

    let mut client = Client::new(Arc::clone(&cfg));
    client.connect().await?;

    // Write some keys before the kill
    for i in 0..20 {
        let key = format!("failover-key-{i}");
        client.put(&key, format!("value-{i}")).await?;
    }

    // Kill server 0 — the shard assignment stream was initially connected here
    tracing::info!("killing server 0");
    cluster.kill_server(0)?;

    // Brief pause to let the client notice the stream break
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify reads still work (client should have reconnected to server 1 or 2)
    let mut read_successes = 0u32;
    let mut read_failures = 0u32;
    for i in 0..20 {
        let key = format!("failover-key-{i}");
        let result = client.get(&key).await;
        match result {
            Ok(Some(r)) => {
                let v = r.value.expect("should have value");
                assert_eq!(v.as_ref(), format!("value-{i}").as_bytes());
                read_successes += 1;
            }
            Ok(None) => {
                // Key might have been on server 0's shard — acceptable
                tracing::warn!(key, "key not found after failover (may be on killed shard)");
                read_failures += 1;
            }
            Err(e) => {
                tracing::warn!(key, ?e, "error during failover read");
                read_failures += 1;
            }
        }
    }
    tracing::info!(read_successes, read_failures, "post-failover read results");
    assert!(
        read_successes > 0,
        "expected at least one successful read after failover"
    );

    // Verify writes work after failover
    let mut write_successes = 0u32;
    let mut write_failures = 0u32;
    for i in 20..30 {
        let key = format!("failover-key-{i}");
        let result = client.put(&key, format!("value-{i}")).await;
        match result {
            Ok(_) => {
                write_successes += 1;
            }
            Err(e) => {
                tracing::warn!(key, ?e, "error during failover write");
                write_failures += 1;
            }
        }
    }
    tracing::info!(
        write_successes,
        write_failures,
        "post-failover write results"
    );
    assert!(
        write_successes > 0,
        "expected at least one successful write after failover"
    );

    Ok(())
}

/// Start a 3-server cluster, connect a client with `coordinator_addr` so it
/// discovers server addresses via `ListNodes`, then verify failover works.
#[test_log::test(tokio::test)]
async fn test_coordinator_bootstrap() -> anyhow::Result<()> {
    let mut cluster = TestCluster::start(3, 3, 4)?;

    // Connect with coordinator_addr for address discovery
    let cfg = config::Config::builder()
        .service_discovery(CoordinatorServiceDiscovery::new(
            cluster.coordinator_admin_addr().to_owned(),
        ))
        .build();

    let mut client = Client::new(Arc::clone(&cfg));
    client.connect().await?;

    // Write keys
    for i in 0..20 {
        let key = format!("coord-key-{i}");
        client.put(&key, format!("value-{i}")).await?;
    }

    // Kill server 0
    tracing::info!("killing server 0");
    cluster.kill_server(0)?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify the client recovers via coordinator-discovered addresses
    let mut read_successes = 0u32;
    let mut read_failures = 0u32;
    for i in 0..20 {
        let key = format!("coord-key-{i}");
        let result = client.get(&key).await;
        match result {
            Ok(Some(r)) => {
                let v = r.value.expect("should have value");
                assert_eq!(v.as_ref(), format!("value-{i}").as_bytes());
                read_successes += 1;
            }
            Ok(None) => {
                tracing::warn!(key, "key not found after failover");
                read_failures += 1;
            }
            Err(e) => {
                tracing::warn!(key, ?e, "error during failover read");
                read_failures += 1;
            }
        }
    }
    tracing::info!(read_successes, read_failures, "post-failover read results");
    assert!(
        read_successes > 0,
        "expected at least one successful read after failover"
    );

    // Verify new writes work
    let mut write_successes = 0u32;
    let mut write_failures = 0u32;
    for i in 20..30 {
        let key = format!("coord-key-{i}");
        let result = client.put(&key, format!("value-{i}")).await;
        match result {
            Ok(_) => {
                write_successes += 1;
            }
            Err(e) => {
                tracing::warn!(key, ?e, "error during failover write");
                write_failures += 1;
            }
        }
    }
    tracing::info!(
        write_successes,
        write_failures,
        "post-failover write results"
    );
    assert!(
        write_successes > 0,
        "expected at least one successful write after failover"
    );

    Ok(())
}
