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

#![cfg(not(miri))]

use futures::StreamExt;
use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::config;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::time::Duration;
use tokio::time::timeout;
use tracing::info;

mod common;
use common::TestResultExt;
use common::non_zero;
use common::trace_err;

const TEST_TIMEOUT_SECS: u64 = 5;

#[test_log::test(tokio::test)]
async fn test_batch_get_basic() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(3)))?;
        let builder = config::Builder::new().max_parallel_requests(5);
        let client = trace_err!(server.connect(Some(builder)).await)?;

        // Insert test data
        let keys = vec!["batch-key-1", "batch-key-2", "batch-key-3"];
        for key in &keys {
            trace_err!(client.put(*key, format!("value-{}", key)).await)?;
        }

        // Build batch_get request
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add_keys(keys.clone());
            })
            .build();

        // Execute batch_get
        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut results = HashMap::new();

        while let Some(item) = stream.next().await {
            info!(key = ?item.key, response = ?item.response, "batch_get result");
            let value = trace_err!(item.response)?;
            results.insert(item.key, value);
        }

        // Verify all keys were returned
        assert_eq!(results.len(), keys.len());
        for key in &keys {
            assert!(results.contains_key(*key));
            assert!(results[*key].is_some());
        }

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_missing_keys() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(2)))?;
        let client = trace_err!(server.connect(None).await)?;

        // Insert only some keys
        trace_err!(client.put("exists-1", "value-1").await)?;
        trace_err!(client.put("exists-3", "value-3").await)?;

        // Request both existing and non-existing keys
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add_keys(vec!["exists-1", "missing-2", "exists-3", "missing-4"]);
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut found = HashSet::new();
        let mut not_found = HashSet::new();

        while let Some(item) = stream.next().await {
            let result = trace_err!(item.response)?;
            if result.is_some() {
                found.insert(item.key);
            } else {
                not_found.insert(item.key);
            }
        }

        assert_eq!(found.len(), 2);
        assert!(found.contains("exists-1"));
        assert!(found.contains("exists-3"));

        assert_eq!(not_found.len(), 2);
        assert!(not_found.contains("missing-2"));
        assert!(not_found.contains("missing-4"));

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_with_options() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(2)))?;
        let client = trace_err!(server.connect(None).await)?;

        // Insert test data
        trace_err!(client.put("key-with-value", "some-value").await)?;
        trace_err!(client.put("key-no-value", "another-value").await)?;

        // Request with different options
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add("key-with-value");
                b.add_with_options(
                    "key-no-value",
                    client::GetOptions::new().with(|o| {
                        o.exclude_value();
                    }),
                );
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut results = HashMap::new();

        while let Some(item) = stream.next().await {
            let response = trace_err!(item.response)?;
            results.insert(item.key, response);
        }

        // Verify key-with-value has the value
        let with_value = results.get("key-with-value").unwrap().as_ref().unwrap();
        assert!(with_value.value.is_some());

        // Verify key-no-value excludes the value
        let no_value = results.get("key-no-value").unwrap().as_ref().unwrap();
        assert!(no_value.value.is_none());

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_with_batch_options() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(2)))?;
        let client = trace_err!(server.connect(None).await)?;

        // Insert test data
        trace_err!(client.put("key-with-value", "some-value").await)?;
        trace_err!(client.put("key-no-value", "another-value").await)?;

        // Request with different options
        let req =
            client::batch_get::Request::builder_with_options(client::GetOptions::new().with(|o| {
                o.exclude_value();
            }))
            .with(|b| {
                b.add_with_options(
                    "key-with-value",
                    client::GetOptions::new().with(|o| {
                        o.include_value();
                    }),
                );
                b.add("key-no-value");
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut results = HashMap::new();

        while let Some(item) = stream.next().await {
            let response = trace_err!(item.response)?;
            results.insert(item.key, response);
        }

        // Verify key-with-value has the value
        let with_value = results.get("key-with-value").unwrap().as_ref().unwrap();
        assert!(with_value.value.is_some());

        // Verify key-no-value excludes the value
        let no_value = results.get("key-no-value").unwrap().as_ref().unwrap();
        assert!(no_value.value.is_none());

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}
#[test_log::test(tokio::test)]
async fn test_batch_get_across_shards() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let nshards = non_zero(4);
        let server = trace_err!(common::TestServer::start_nshards(nshards))?;
        let client = trace_err!(server.connect(None).await)?;

        // Insert keys that will likely hash to different shards
        let keys: Vec<String> = (0..20).map(|i| format!("shard-key-{}", i)).collect();
        for key in &keys {
            trace_err!(client.put(key, format!("value-{}", key)).await)?;
        }

        // Request all keys via batch_get
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add_keys(keys.clone());
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut count = 0;

        while let Some(item) = stream.next().await {
            trace_err!(item.response)?;
            count += 1;
        }

        assert_eq!(count, keys.len());

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_empty_request() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start())?;
        let client = trace_err!(server.connect(None).await)?;

        // Empty request
        let req = client::batch_get::Request::builder().build();
        let stream = trace_err!(client.batch_get(req).await)?;

        // Should yield no items
        let count = stream.fold(0, |acc, _| async move { acc + 1 }).await;
        assert_eq!(count, 0);

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_with_partition_keys() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(3)))?;
        let client = trace_err!(server.connect(None).await)?;

        let partition_key = "my-partition";

        // Insert keys with partition key
        let opts = client::PutOptions::new().with(|o| {
            o.partition_key(partition_key);
        });

        trace_err!(
            client
                .put_with_options("pk-key-1", "value-1", opts.clone())
                .await
        )?;
        trace_err!(
            client
                .put_with_options("pk-key-2", "value-2", opts.clone())
                .await
        )?;

        // Request with partition key options
        let get_opts = client::GetOptions::new().with(|o| {
            o.partition_key(partition_key);
        });

        let req = client::batch_get::Request::builder_with_options(get_opts)
            .with(|b| {
                b.add_keys(vec!["pk-key-1", "pk-key-2"]);
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut results = Vec::new();

        while let Some(item) = stream.next().await {
            let response = trace_err!(item.response)?;
            results.push((item.key, response));
        }

        assert_eq!(results.len(), 2);

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_large_batch() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(4)))?;
        let builder = config::Builder::new()
            .max_parallel_requests(10)
            .request_timeout(Duration::from_secs(TEST_TIMEOUT_SECS));
        let client = trace_err!(server.connect(Some(builder)).await)?;

        // Insert many keys
        const N: usize = 100;
        let keys: Vec<String> = (0..N).map(|i| format!("large-batch-{:04}", i)).collect();

        for key in &keys {
            trace_err!(client.put(key, format!("value-{}", key)).await)?;
        }

        // Batch get all keys
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add_keys(keys.clone());
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut received_keys = HashSet::new();

        while let Some(item) = stream.next().await {
            trace_err!(item.response)?;
            received_keys.insert(item.key);
        }

        assert_eq!(received_keys.len(), N);
        for key in &keys {
            assert!(received_keys.contains(key.as_str()));
        }

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_duplicate_keys() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start())?;
        let client = trace_err!(server.connect(None).await)?;

        trace_err!(client.put("dup-key", "value").await)?;

        // Request same key multiple times
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add("dup-key");
                b.add("dup-key");
                b.add("dup-key");
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut count = 0;

        while let Some(item) = stream.next().await {
            assert_eq!(item.key.as_ref(), "dup-key");
            let response = trace_err!(item.response)?;
            assert!(response.is_some());
            count += 1;
        }

        // Should receive response for each request, even if duplicate
        assert_eq!(count, 3);

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_mixed_success_failure() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start_nshards(non_zero(2)))?;
        let client = trace_err!(server.connect(None).await)?;

        // Insert some keys
        trace_err!(client.put("success-1", "value-1").await)?;
        trace_err!(client.put("success-2", "value-2").await)?;

        // Mix of existing and non-existing keys
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add_keys(vec!["success-1", "missing-1", "success-2", "missing-2"]);
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;
        let mut successes = 0;
        let mut not_found = 0;

        while let Some(item) = stream.next().await {
            match trace_err!(item.response)? {
                Some(_) => successes += 1,
                None => not_found += 1,
            }
        }

        assert_eq!(successes, 2);
        assert_eq!(not_found, 2);

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}

#[test_log::test(tokio::test)]
async fn test_batch_get_version_tracking() -> anyhow::Result<()> {
    timeout(Duration::from_secs(TEST_TIMEOUT_SECS), async {
        let server = trace_err!(common::TestServer::start())?;
        let client = trace_err!(server.connect(None).await)?;

        // Insert and update a key multiple times
        let mut version = trace_err!(client.put("versioned-key", "v1").await)?;
        info!(?version, "initial put");

        version = trace_err!(client.put("versioned-key", "v2").await)?;
        info!(?version, "second put");

        // Batch get should return the latest version
        let req = client::batch_get::Request::builder()
            .with(|b| {
                b.add("versioned-key");
            })
            .build();

        let mut stream = trace_err!(client.batch_get(req).await)?;

        if let Some(item) = stream.next().await {
            let response = trace_err!(item.response)?.unwrap();
            info!(retrieved_version = ?response.version, "batch_get result");
            assert_eq!(response.version.version_id, version.version.version_id);
            assert_eq!(response.version.modifications_count, 1);
        } else {
            return Err(anyhow::anyhow!("Expected at least one result"));
        }

        trace_err!(fs::remove_dir_all(server.data_dir.path()))?;
        Ok(())
    })
    .await?
}
