// Copyright 2025 Maurice S. Barnum
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

use chrono::Utc;
use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::config;
use std::fs;
use std::time::Duration;
use tracing::info;

mod common;
use common::TestResultExt;
use common::non_zero;
use common::trace_err;

#[test_log::test(tokio::test)]
async fn test_basic() -> anyhow::Result<()> {
    let session_timeout = Duration::from_secs(2);
    let server = trace_err!(common::TestServer::start_nshards(non_zero(4)))?;
    let builder = config::Builder::new()
        .retry(config::RetryConfig::new(3, Duration::from_millis(23)))
        .max_parallel_requests(3)
        .session_timeout(session_timeout);
    let client = trace_err!(server.connect(Some(builder)).await)?;

    let mut key = String::new();
    for i in 0..11 {
        key = format!("foo-{i}");
        for j in 0..7 {
            let result = trace_err!(client.put(&key, format!("value-{i}-{j}")).await)?;
            info!(op = "put", ?result);
        }
    }

    // And do a get
    let result = client.get(&key).await?;
    info!(op = "get", ?result);

    // Let's list all of the keys
    let result = trace_err!(client.list("", "").await)?;
    for (i, k) in result.keys.iter().enumerate() {
        info!(op = "list", i, key = ?k);
    }

    // Delete the last key inserted
    trace_err!(client.delete(&key).await)?;

    // Scan for all of the keys

    let result = trace_err!(client.range_scan("", "").await)?;
    for (i, v) in result.records.iter().enumerate() {
        info!(op = "scan", i, value = ?v);
    }

    // And do another get, this time expecting KeyNotFound
    if let Some(r) = client.get(&key).await? {
        return Err(anyhow::anyhow!("unexpected get success: {r:?}"));
    }

    let result = client
        .put_with_options(
            "ephemeral_key",
            Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            client::PutOptions::new().with(|x| x.ephemeral()),
        )
        .await?;
    info!(op = "put", ?result);

    let sleep_time = session_timeout.mul_f32(2.01);
    info!(?sleep_time, "sleepging to let session exkjjpire");
    tokio::time::sleep(sleep_time).await;
    drop(client);

    trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
    Ok(())
}

async fn do_test_disconnect(retry: bool) -> anyhow::Result<()> {
    let mut server = common::TestServer::start()?;
    let builder = config::Builder::new()
        .max_parallel_requests(3)
        .request_timeout(Duration::from_millis(300))
        .session_timeout(Duration::from_millis(2001));
    let builder = if retry {
        builder.retry(config::RetryConfig::new(3, Duration::from_millis(100)))
    } else {
        builder
    };

    let client = server.connect(Some(builder)).await?;
    assert_eq!(None, trace_err!(client.get("foo").await)?);

    trace_err!(client.put("foo", "bar").await)?;
    assert!(trace_err!(client.get("foo").await)?.is_some());

    info!("restarting test server");
    trace_err!(server.restart())?;

    let r = trace_err!(client.get("foo").await)?;
    info!(op = "get", ?r);
    assert!(r.is_some());

    trace_err!(fs::remove_dir_all(server.data_dir.path()))?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_disconnect() -> anyhow::Result<()> {
    do_test_disconnect(true).await
}

#[test_log::test(tokio::test)]
async fn test_disconnect_fail() {
    let r = do_test_disconnect(false).await;
    assert!(r.is_err());
}

#[test_log::test(tokio::test)]
async fn test_range_delete() -> anyhow::Result<()> {
    const N: usize = 8;

    let nshards = non_zero(3);
    let server = trace_err!(common::TestServer::start_nshards(nshards))?;

    let builder = config::Builder::new();
    let client = trace_err!(server.connect(Some(builder)).await)?;

    let keys: Vec<String> = (0..N * 2).map(|i| format!("k{i:04}")).collect();
    for key in &keys {
        let r = trace_err!(client.put(key, "").await)?;
        info!(key, ?r, "put");
    }

    let result = trace_err!(client.list("", "").await)?;
    for k in result.keys.iter().filter(|k| keys.contains(k)) {
        info!(op = "list -- BEFORE RANGE DELETE", key = ?k);
    }

    trace_err!(client.delete_range(&keys[0], &keys[N]).await)?;

    let result = trace_err!(client.list("", "").await)?;
    for k in result.keys.iter().filter(|k| keys.contains(k)) {
        info!(op = "list -- AFTER RANGE DELETE", key = ?k);
    }

    for key in &keys[0..N] {
        let r = trace_err!(client.get(key).await)?;
        info!(key, ?r, "get");
        assert!(r.is_none());
    }
    for key in &keys[N + 1..] {
        let r = trace_err!(client.get(key).await)?;
        info!(key, ?r, "get");
        assert!(r.is_some());
    }

    Ok(())
}
