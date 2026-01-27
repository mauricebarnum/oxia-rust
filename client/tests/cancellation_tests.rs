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

//! Integration tests for cancellation and shutdown behavior.

#![cfg(not(miri))]

use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::config;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tracing::info;

mod common;
use common::TestResultExt;

/// Test that dropping a client completes without hanging.
/// This verifies the cancellation token properly stops background tasks.
#[test_log::test(tokio::test)]
async fn test_client_drop_completes() -> anyhow::Result<()> {
    let server = trace_err!(common::TestServer::start())?;
    let client = trace_err!(server.connect().await)?;

    // Do some operations to ensure background tasks are running
    trace_err!(client.put("test-key", "test-value").await)?;
    let _ = trace_err!(client.get("test-key").await)?;

    // Drop should complete without hanging
    let drop_result = tokio::time::timeout(Duration::from_secs(5), async {
        drop(client);
    })
    .await;

    assert!(drop_result.is_ok(), "client drop should not hang");
    Ok(())
}

/// Test that ephemeral keys expire after client is dropped.
/// This verifies that heartbeat tasks are cancelled when the client is dropped,
/// causing the session to expire and ephemeral keys to be deleted.
#[test_log::test(tokio::test)]
async fn test_ephemeral_key_expires_after_client_drop() -> anyhow::Result<()> {
    let session_timeout = Duration::from_secs(2);

    let server = trace_err!(common::TestServer::start())?;
    let builder = config::Config::builder().session_timeout(session_timeout);
    let client = trace_err!(server.connect_with(builder).await)?;

    // Create an ephemeral key - this starts a session with heartbeats
    let key = "ephemeral-cancel-test";
    trace_err!(
        client
            .put_with_options(
                key,
                "ephemeral-value",
                client::PutOptions::builder().ephemeral(true).build()
            )
            .await
    )?;

    // Verify key exists
    let result = trace_err!(client.get(key).await)?;
    assert!(result.is_some(), "ephemeral key should exist initially");

    // Drop the client - this should cancel the heartbeat task
    info!("dropping client to stop heartbeats");
    drop(client);

    // Wait for session to expire (heartbeats should have stopped)
    let wait_time = session_timeout.mul_f32(2.5);
    info!(?wait_time, "waiting for session expiration");
    tokio::time::sleep(wait_time).await;

    // Reconnect and verify key is gone
    let client2 = trace_err!(server.connect().await)?;
    let result = trace_err!(client2.get(key).await)?;
    assert!(
        result.is_none(),
        "ephemeral key should be deleted after session expires"
    );

    Ok(())
}

/// Test that reconnect() properly cleans up old resources.
/// The old shard manager should be dropped, cancelling its background tasks.
#[test_log::test(tokio::test)]
async fn test_reconnect_cleans_up_old_manager() -> anyhow::Result<()> {
    let session_timeout = Duration::from_secs(2);

    let server = trace_err!(common::TestServer::start())?;
    let builder = config::Config::builder().session_timeout(session_timeout);
    let mut client = trace_err!(server.connect_with(builder).await)?;

    // Create an ephemeral key with the first connection
    let key = "ephemeral-reconnect-test";
    trace_err!(
        client
            .put_with_options(
                key,
                "value1",
                client::PutOptions::builder().ephemeral(true).build()
            )
            .await
    )?;

    // Verify key exists
    assert!(trace_err!(client.get(key).await)?.is_some());

    // Reconnect - this should drop the old manager and cancel its tasks
    info!("reconnecting client");
    trace_err!(client.reconnect().await)?;

    // Wait for old session to expire
    let wait_time = session_timeout.mul_f32(2.5);
    info!(?wait_time, "waiting for old session expiration");
    tokio::time::sleep(wait_time).await;

    // The ephemeral key from the old session should be gone
    let result = trace_err!(client.get(key).await)?;
    assert!(
        result.is_none(),
        "ephemeral key from old session should be deleted"
    );

    Ok(())
}

/// Test that cloned clients share the same cancellation lifecycle.
/// Dropping one clone should not affect the other.
#[test_log::test(tokio::test)]
async fn test_cloned_clients_share_lifecycle() -> anyhow::Result<()> {
    let server = trace_err!(common::TestServer::start())?;
    let client1 = trace_err!(server.connect().await)?;
    let client2 = client1.clone();

    // Both clients should work
    trace_err!(client1.put("key1", "value1").await)?;
    trace_err!(client2.put("key2", "value2").await)?;

    // Drop one clone
    drop(client1);

    // The other clone should still work (shard manager is Arc-shared)
    let result = trace_err!(client2.get("key1").await)?;
    assert!(result.is_some(), "remaining clone should still work");

    trace_err!(client2.put("key3", "value3").await)?;
    let result = trace_err!(client2.get("key3").await)?;
    assert!(result.is_some());

    Ok(())
}

/// Test that concurrent operations complete gracefully when client is dropped.
#[test_log::test(tokio::test)]
async fn test_concurrent_ops_during_shutdown() -> anyhow::Result<()> {
    let server = trace_err!(common::TestServer::start())?;
    let client = trace_err!(server.connect().await)?;

    // Seed some data
    for i in 0..10 {
        trace_err!(
            client
                .put(format!("concurrent-{i}"), format!("value-{i}"))
                .await
        )?;
    }

    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    // Spawn tasks that will do operations
    for i in 0..3 {
        let c = client.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            // These operations may succeed or fail depending on timing
            for j in 0..5 {
                let _ = c.get(format!("concurrent-{}", (i * 5 + j) % 10)).await;
                let _ = c.put(format!("new-{i}-{j}"), "v").await;
            }
        }));
    }

    // Wait for tasks to start, then drop our reference
    barrier.wait().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(client);

    // All spawned tasks should complete (success or failure, no panic/hang)
    let results = tokio::time::timeout(Duration::from_secs(10), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await;

    assert!(
        results.is_ok(),
        "concurrent operations should complete during shutdown"
    );

    Ok(())
}

/// Test that dropping a notifications stream completes without hanging.
#[test_log::test(tokio::test)]
async fn test_notifications_stream_drop_completes() -> anyhow::Result<()> {
    use futures::StreamExt;

    let server = trace_err!(common::TestServer::start())?;
    let client = trace_err!(server.connect().await)?;

    // Create a notifications stream
    let mut stream = trace_err!(client.create_notifications_stream())?;

    // Generate some notifications
    for i in 0..3 {
        trace_err!(
            client
                .put(format!("notify-{i}"), format!("value-{i}"))
                .await
        )?;
    }

    // Read a few notifications
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;

    // Dropping the stream should complete without hanging
    let drop_result = tokio::time::timeout(Duration::from_secs(2), async {
        drop(stream);
    })
    .await;

    assert!(
        drop_result.is_ok(),
        "notifications stream drop should not hang"
    );

    Ok(())
}
