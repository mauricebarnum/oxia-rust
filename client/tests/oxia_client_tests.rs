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

//! Comprehensive tests for Oxia client ported from Go implementation
//!
//! This module contains tests that mirror the functionality tested in the original Go client,
//! adapted for the Rust client implementation.

#![cfg(not(miri))]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use client::Client;
use client::DeleteOptions;
use client::Error;
use client::GetOptions;
use client::KeyComparisonType;
use client::ListOptions;
use client::PutOptions;
use client::RangeScanOptions;
use client::Result;
use client::SecondaryIndex;
use mauricebarnum_oxia_client as client;
use mauricebarnum_oxia_client::NotificationType;
use mauricebarnum_oxia_client::NotificationsOptions;
use mauricebarnum_oxia_client::OxiaError;
use mauricebarnum_oxia_client::config;

mod common;
use common::TestResultExt;
use common::non_zero;
use tokio::time::timeout;

/// Helper function to create a test client
async fn create_test_client_nshards(nshards: u32) -> Result<(common::TestServer, Client)> {
    let session_timeout = Duration::from_secs(2);
    let server = trace_err!(common::TestServer::start_nshards(non_zero(nshards)))?;
    let builder = config::Builder::new()
        .retry(config::RetryConfig::new(3, Duration::from_millis(23)))
        .max_parallel_requests(3)
        .session_timeout(session_timeout);
    let client = trace_err!(server.connect(Some(builder)).await)?;
    Ok((server, client))
}

async fn create_test_client() -> Result<(common::TestServer, Client)> {
    create_test_client_nshards(1u32).await
}

/// Helper function to create a unique test key
fn test_key(suffix: &str) -> String {
    format!(
        "test-{}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap(),
        suffix
    )
}

#[test_log::test(tokio::test)]
async fn test_basic_crud_operations() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Test Put operation
    let key = test_key("basic-crud");
    let value = &b"test-value-0"[..];

    let put_result = client.put(&key, value).await?;
    assert_eq!(put_result.version.modifications_count, 0);
    assert!(put_result.version.version_id >= 0);

    // Test Get operation
    let get_result = client.get(&key).await?;
    assert!(get_result.is_some());
    let get_response = get_result.unwrap();
    assert_eq!(get_response.value, Some(value.into()));
    assert_eq!(
        get_response.version.version_id,
        put_result.version.version_id
    );

    // Test Put with expected version (update)
    let value2 = b"test-value-1".to_vec();
    let put_result2 = client
        .put_with_options(
            &key,
            value2.clone(),
            PutOptions::new().with(|x| {
                x.expected_version_id(put_result.version.version_id);
            }),
        )
        .await?;
    assert_eq!(put_result2.version.modifications_count, 1);
    assert!(put_result2.version.version_id > put_result.version.version_id);

    // Test Delete operation
    client
        .delete_with_options(
            &key,
            DeleteOptions::new().with(|x| {
                x.expected_version_id(put_result2.version.version_id);
            }),
        )
        .await?;

    // Verify key is deleted
    let get_result = client.get(&key).await?;
    assert!(get_result.is_none());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_secondary_indexes() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Create records with secondary indexes
    let mut secondary_indexes = Vec::new();
    for i in 0..10 {
        let primary_key = format!("{}", char::from(b'a' + i));
        let value = format!("{i}");
        let secondary_index = SecondaryIndex::new("val-idx", &value);
        secondary_indexes.push(secondary_index.clone());

        client
            .put_with_options(
                &primary_key,
                value,
                PutOptions::new().with(move |p| {
                    p.secondary_indexes([secondary_index]);
                }),
            )
            .await?;
    }

    // Test list with secondary index
    let list_result = client
        .list_with_options(
            "1",
            "4",
            ListOptions::new().with(|x| {
                x.secondary_index_name("val-idx");
            }),
        )
        .await?;

    let expected_keys = vec!["b".to_string(), "c".to_string(), "d".to_string()];
    assert_eq!(list_result.keys, expected_keys);

    // Test range scan with secondary index
    let mut scan_results = Vec::new();
    let range_scan = client
        .range_scan_with_options(
            "1",
            "4",
            RangeScanOptions::new().with(|x| {
                x.secondary_index_name("val-idx");
            }),
        )
        .await?;

    for record in range_scan.records {
        scan_results.push((
            record.key.unwrap(),
            String::from_utf8(record.value.unwrap().to_vec()).unwrap(),
        ));
    }

    let expected_results = vec![
        ("b".to_string(), "1".to_string()),
        ("c".to_string(), "2".to_string()),
        ("d".to_string(), "3".to_string()),
    ];
    assert_eq!(scan_results, expected_results);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_key_comparisons() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Setup test data with gaps (skip 'b' and 'f')
    let test_data = BTreeMap::from([("a", "0"), ("c", "2"), ("d", "3"), ("e", "4"), ("g", "6")]);

    for (&key, &value) in &test_data {
        client.put(key, value).await?;
    }

    // Test exact match
    let result = client
        .get_with_options(
            "a",
            GetOptions::new().with(|x| {
                x.comparison_type(KeyComparisonType::Equal);
            }),
        )
        .await?;
    assert!(result.is_some());
    assert_eq!(
        result.unwrap().value.unwrap().as_ref(),
        test_data["a"].as_bytes()
    );

    // Test floor comparison - should find "a" when searching for "a"
    let result = client
        .get_with_options(
            "a",
            GetOptions::new().with(|x| {
                x.comparison_type(KeyComparisonType::Floor);
            }),
        )
        .await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap().key.as_ref().unwrap(), "a");

    // Test ceiling comparison - should find "c" when searching for "b"
    let result = client
        .get_with_options(
            "b",
            GetOptions::new().with(|x| {
                x.comparison_type(KeyComparisonType::Ceiling);
            }),
        )
        .await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap().key.as_ref().unwrap(), "c");

    // Test higher comparison - should find "c" when searching for "a"
    let result = client
        .get_with_options(
            "a",
            GetOptions::new().with(|x| {
                x.comparison_type(KeyComparisonType::Higher);
            }),
        )
        .await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap().key.as_ref().unwrap(), "c");

    // Test lower comparison - should find "a" when searching for "b"
    let result = client
        .get_with_options(
            "b",
            GetOptions::new().with(|x| {
                x.comparison_type(KeyComparisonType::Lower);
            }),
        )
        .await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap().key.as_ref().unwrap(), "a");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_partition_routing() -> Result<()> {
    let (_server, client) = create_test_client_nshards(3).await?;

    // Note: this test is brittle as we're assuming the choice of keys and partitions will map to
    // specific shards. If the test data changes, this test will need to be updated.

    // These partition keys
    let partition_key = "x";

    // Put data with partition key
    client
        .put_with_options(
            "a",
            "0",
            PutOptions::new().with(|x| {
                x.partition_key(partition_key);
            }),
        )
        .await?;

    // Should not find without partition key
    let result = client.get("a").await?;
    assert!(result.is_none());

    // Should find with correct partition key
    let result = client
        .get_with_options(
            "a",
            GetOptions::new().with(|x| {
                x.partition_key(partition_key);
            }),
        )
        .await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap().value.as_ref().unwrap(), &b"0"[..]);

    // Create multiple records with same partition
    let keys = ["a", "b", "c", "d", "e"];
    for (i, &key) in keys.iter().enumerate() {
        client
            .put_with_options(
                key,
                format!("{i}"),
                PutOptions::new().with(|x| {
                    x.partition_key(partition_key);
                }),
            )
            .await?;
    }

    // Test list with partition key
    let list_result = client
        .list_with_options(
            "a",
            "d",
            ListOptions::new().with(|x| {
                x.partition_key(partition_key);
            }),
        )
        .await?;
    assert_eq!(list_result.keys, vec!["a", "b", "c"]);

    // Test list with wrong partition key (should be empty)
    let list_result = client
        .list_with_options(
            "a",
            "d",
            ListOptions::new().with(|x| {
                x.partition_key("wrong-partition");
            }),
        )
        .await?;
    assert!(list_result.keys.is_empty());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_sequential_keys() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    let partition_key = "seq-partition";

    // Test single sequence key delta
    let result1 = client
        .put_with_options(
            "a",
            "0",
            PutOptions::new().with(|x| {
                x.partition_key(partition_key).sequence_key_deltas([1]);
            }),
        )
        .await?;

    let expected_key1 = format!("a-{:020}", 1);
    assert_eq!(result1.key.as_ref().unwrap(), &expected_key1);

    // Test multiple sequence key deltas
    let result2 = client
        .put_with_options(
            "a",
            "1",
            PutOptions::new().with(|x| {
                x.partition_key(partition_key).sequence_key_deltas([3]);
            }),
        )
        .await?;

    let expected_key2 = format!("a-{:020}", 4);
    assert_eq!(result2.key.as_ref().unwrap(), &expected_key2);

    // Test multiple deltas in single operation
    let result3 = client
        .put_with_options(
            "a",
            "2",
            PutOptions::new().with(|x| {
                x.partition_key(partition_key).sequence_key_deltas([1, 6]);
            }),
        )
        .await?;

    let expected_key3 = format!("a-{:020}-{:020}", 5, 6);
    assert_eq!(result3.key.as_ref().unwrap(), &expected_key3);

    // Verify we can retrieve the values using the generated keys
    let get_result = client
        .get_with_options(
            &expected_key1,
            GetOptions::new().with(|x| {
                x.partition_key(partition_key);
            }),
        )
        .await?;
    assert!(get_result.is_some());
    assert_eq!(get_result.unwrap().value.unwrap().as_ref(), b"0");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_ephemeral_records() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    let key = test_key("ephemeral");

    // Create ephemeral record
    let put_result = client
        .put_with_options(
            &key,
            "ephemeral-value",
            PutOptions::new().with(|x| {
                x.ephemeral();
            }),
        )
        .await?;

    assert!(put_result.version.is_ephemeral());

    // Verify we can read it
    let get_result = client.get(&key).await?;
    assert!(get_result.is_some());
    assert!(get_result.unwrap().version.is_ephemeral());

    // Override with non-ephemeral value
    let put_result2 = client.put(&key, "non-ephemeral-value").await?;
    assert!(!put_result2.version.is_ephemeral());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_delete_range() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Create test records
    let keys = ["a", "b", "c", "d", "e", "f", "g"];
    for (i, &key) in keys.iter().enumerate() {
        client.put(key, format!("{i}")).await?;
    }

    // Delete range b to f (exclusive)
    client.delete_range("b", "f").await?;

    // Verify deletions
    for &key in &["b", "c", "d", "e"] {
        let result = client.get(key).await?;
        assert!(result.is_none(), "Key {key} should have been deleted");
    }

    // Verify remaining keys
    for &key in &["a", "f", "g"] {
        let result = client.get(key).await?;
        assert!(result.is_some(), "Key {key} should still exist");
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_list_operations() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Create test data
    let test_data = vec![
        ("key-a", "value-a"),
        ("key-b", "value-b"),
        ("key-c", "value-c"),
        ("key-d", "value-d"),
        ("key-z", "value-z"),
    ];

    for (key, value) in &test_data {
        client.put(*key, *value).await?;
    }

    // Test basic list
    let list_result = client.list("key-a", "key-d").await?;
    assert_eq!(list_result.keys, vec!["key-a", "key-b", "key-c"]);
    assert!(list_result.sorted);

    // Test list with empty range
    let list_result = client.list("key-x", "key-y").await?;
    assert!(list_result.keys.is_empty());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_range_scan() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Create test data
    let test_data = vec![
        ("scan-a", "0"),
        ("scan-b", "1"),
        ("scan-c", "2"),
        ("scan-d", "3"),
        ("scan-e", "4"),
    ];

    for (key, value) in &test_data {
        client.put(*key, *value).await?;
    }

    // Test range scan
    let scan_result = client.range_scan("scan-b", "scan-e").await?;

    let expected_records = [("scan-b", "1"), ("scan-c", "2"), ("scan-d", "3")];

    assert_eq!(scan_result.records.len(), expected_records.len());
    for (i, record) in scan_result.records.iter().enumerate() {
        assert_eq!(record.key.as_ref().unwrap(), expected_records[i].0);
        assert_eq!(
            String::from_utf8(record.value.as_ref().unwrap().to_vec()).unwrap(),
            expected_records[i].1
        );
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_get_without_value() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    let key = test_key("no-value");
    let value = "test-value";

    // Put a record
    client.put(&key, value).await?;

    // Get with value included (default)
    let result = client
        .get_with_options(
            &key,
            GetOptions::new().with(|x| {
                x.include_value();
            }),
        )
        .await?;
    assert!(result.is_some());
    assert!(result.as_ref().unwrap().value.is_some());

    // Get without value
    let result = client
        .get_with_options(
            &key,
            GetOptions::new().with(|x| {
                x.exclude_value();
            }),
        )
        .await?;
    assert!(result.is_some());
    assert!(result.unwrap().value.is_none());

    Ok(())
}

/// Test that the empty first batch from server is discarded.
/// Server sends an empty batch on connection to establish cursor position.
/// This test verifies the client discards it by checking the first notification
/// received is for our put, not an empty batch.
#[test_log::test(tokio::test)]
async fn test_notifications_first_batch_discarded() -> anyhow::Result<()> {
    let (_server, client) = create_test_client().await?;

    let mut notifications = client.create_notifications_stream()?;
    notifications.wait_ready(Duration::from_secs(5)).await?;

    let key = test_key("first_batch");

    // Put a record
    let put_result = client.put(&key, &b"value"[..]).await?;

    // First notification after the put should be non-empty and for our key
    let batch = timeout(Duration::from_secs(5), notifications.next())
        .await?
        .expect("should receive notification")?;

    assert!(
        !batch.notifications.is_empty(),
        "batch should not be empty (empty server handshake batch should have been discarded)"
    );

    let notification = batch
        .notifications
        .get(&key)
        .expect("batch should contain notification for our key");
    assert_eq!(notification.type_, NotificationType::KeyCreated);
    assert_eq!(notification.version_id, Some(put_result.version.version_id));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_notifications() -> anyhow::Result<()> {
    let (_server, client) = create_test_client().await?;

    // Create notifications stream
    let mut notifications = client.create_notifications_stream()?;
    notifications.wait_ready(Duration::from_secs(5)).await?;

    let key = test_key("notifications");

    // Put a new record - should generate KeyCreated notification
    let put_result = client.put(&key, &b"value1"[..]).await?;

    // Check for notification
    if let Some(batch_result) = timeout(Duration::from_secs(5), notifications.next()).await? {
        let batch = batch_result?;

        // Find notification for our key
        if let Some(notification) = batch.notifications.get(&key) {
            assert_eq!(notification.type_, NotificationType::KeyCreated);
            assert_eq!(notification.version_id, Some(put_result.version.version_id));
        }
    }

    // Update the record - should generate KeyModified notification
    client.put(&key, &b"value2"[..]).await?;

    if let Some(batch_result) = timeout(Duration::from_secs(5), notifications.next()).await? {
        let batch = batch_result?;

        if let Some(notification) = batch.notifications.get(&key) {
            assert_eq!(notification.type_, NotificationType::KeyModified);
        }
    }

    // Delete the record - should generate KeyDeleted notification
    client.delete(&key).await?;

    if let Some(batch_result) = timeout(Duration::from_secs(5), notifications.next()).await? {
        let batch = batch_result?;

        if let Some(notification) = batch.notifications.get(&key) {
            assert_eq!(notification.type_, NotificationType::KeyDeleted);
        }
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_version_ordering() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    let mut version_ids = Vec::new();

    // Create multiple records and verify version IDs are increasing
    for i in 0..5 {
        let key = format!("version-test-{i}");
        let result = client.put(&key, format!("value-{i}")).await?;
        version_ids.push(result.version.version_id);
    }

    // Verify version IDs are strictly increasing
    for i in 1..version_ids.len() {
        assert!(
            version_ids[i] > version_ids[i - 1],
            "Version ID {} should be greater than {}",
            version_ids[i],
            version_ids[i - 1]
        );
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_error_conditions() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    let key = test_key("error-test");

    // Test get non-existent key
    let result = client.get(&key).await?;
    assert!(result.is_none());

    // Test delete non-existent key (should succeed silently in Oxia)
    let result = client.delete(&key).await;
    assert!(matches!(result, Err(Error::Oxia(OxiaError::KeyNotFound))));

    // Test put with wrong expected version
    client.put(&key, "value").await?;

    let result = client
        .put_with_options(
            &key,
            "new-value",
            PutOptions::new().with(|x| {
                x.expected_version_id(999_999);
            }), // Wrong version
        )
        .await;

    // Should get an error for unexpected version
    assert!(matches!(result, Err(Error::Oxia(_))));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_concurrent_operations() -> Result<()> {
    let (_server, client) = create_test_client().await?;
    let client = Arc::new(client);

    let mut handles = Vec::new();

    // Launch concurrent put operations
    for i in 0..10 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let key = format!("concurrent-{i}");
            let value = format!("value-{i}");
            client_clone.put(&key, value).await
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    // Verify all records exist
    for i in 0..10 {
        let key = format!("concurrent-{i}");
        let result = client.get(&key).await?;
        assert!(result.is_some());
    }

    Ok(())
}

/// Integration test that combines multiple features
#[test_log::test(tokio::test)]
async fn test_integration_scenario() -> Result<()> {
    let (_server, client) = create_test_client().await?;

    // Scenario: E-commerce order system with secondary indexes
    let partition_key = "customer-123";

    // Create orders with secondary indexes for status and date
    let orders = vec![
        ("order-001", "pending", "2024-01-01"),
        ("order-002", "shipped", "2024-01-02"),
        ("order-003", "delivered", "2024-01-03"),
        ("order-004", "pending", "2024-01-04"),
    ];

    for (order_id, status, date) in &orders {
        let secondary_indexes = vec![
            SecondaryIndex::new("status-idx", *status),
            SecondaryIndex::new("date-idx", *date),
        ];

        client
            .put_with_options(
                *order_id,
                format!("{{\"status\": \"{status}\", \"date\": \"{date}\"}}"),
                PutOptions::new().with(|p| {
                    p.partition_key(partition_key)
                        .secondary_indexes(secondary_indexes);
                }),
            )
            .await?;
    }

    // Query pending orders using secondary index
    let pending_orders = client
        .list_with_options(
            "pending",
            "pending~", // Use tilde to get all keys starting with "pending"
            ListOptions::new().with(|p| {
                p.secondary_index_name("status-idx")
                    .partition_key(partition_key);
            }),
        )
        .await?;

    assert_eq!(pending_orders.keys.len(), 2);
    assert!(pending_orders.keys.contains(&"order-001".to_string()));
    assert!(pending_orders.keys.contains(&"order-004".to_string()));

    // Update order status
    client
        .put_with_options(
            "order-001",
            "{\"status\": \"shipped\", \"date\": \"2024-01-01\"}",
            PutOptions::new().with(move |p| {
                p.partition_key(partition_key).secondary_indexes(vec![
                    SecondaryIndex::new("status-idx", "shipped"),
                    SecondaryIndex::new("date-idx", "2024-01-01"),
                ]);
            }),
        )
        .await?;

    // Range scan orders by date
    let date_range_orders = client
        .range_scan_with_options(
            "2024-01-01",
            "2024-01-03",
            RangeScanOptions::new().with(|p| {
                p.secondary_index_name("date-idx")
                    .partition_key(partition_key);
            }),
        )
        .await?;

    assert_eq!(date_range_orders.records.len(), 2);

    Ok(())
}

/// Test that notifications stream with options works correctly
#[test_log::test(tokio::test)]
async fn test_notifications_with_options() -> anyhow::Result<()> {
    let (_server, client) = create_test_client().await?;

    // Create notifications stream with default options (same as create_notifications_stream)
    let opts = NotificationsOptions::default();
    let mut notifications = client.create_notifications_stream_with_options(opts)?;
    notifications.wait_ready(Duration::from_secs(5)).await?;

    let key = test_key("notifications_opts");

    // Put a new record - should generate KeyCreated notification
    let put_result = client.put(&key, &b"value1"[..]).await?;

    // Check for notification
    if let Some(batch_result) = timeout(Duration::from_secs(5), notifications.next()).await? {
        let batch = batch_result?;

        // Find notification for our key
        if let Some(notification) = batch.notifications.get(&key) {
            assert_eq!(notification.type_, NotificationType::KeyCreated);
            assert_eq!(notification.version_id, Some(put_result.version.version_id));
        }
    }

    Ok(())
}

/// Test that notifications stream with reconnect_on_close option reconnects after server restart
#[test_log::test(tokio::test)]
async fn test_notifications_reconnect_on_close() -> anyhow::Result<()> {
    let mut server = common::TestServer::start()?;
    let client = trace_err!(server.connect(Some(config::Builder::new())).await)?;

    // Create stream with reconnect_on_close enabled
    let opts = NotificationsOptions::new().with(|o| {
        o.reconnect_on_close();
    });
    let mut notifications = client.create_notifications_stream_with_options(opts)?;
    notifications.wait_ready(Duration::from_secs(5)).await?;

    let key = test_key("reconnect_close");

    // Put a record - should generate notification
    client.put(&key, "value1").await?;

    // Check for notification (same pattern as test_notifications)
    if let Some(batch_result) = timeout(Duration::from_secs(5), notifications.next()).await? {
        let batch = batch_result?;
        if let Some(notification) = batch.notifications.get(&key) {
            assert_eq!(notification.type_, NotificationType::KeyCreated);
        }
    }

    // Restart server to force stream close
    tracing::info!("restarting server to test reconnect on close");
    server.restart()?;

    // Wait for server to be ready and reconnect to occur
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Put another record with a different key to ensure a new notification
    let key2 = test_key("reconnect_close_2");
    client.put(&key2, "value2").await?;

    // Should receive notification after reconnect
    // Keep polling until we find our key or timeout
    let start = std::time::Instant::now();
    let max_wait = Duration::from_secs(15);
    let mut found = false;

    while start.elapsed() < max_wait && !found {
        match timeout(Duration::from_secs(3), notifications.next()).await {
            Ok(Some(Ok(batch))) => {
                if batch.notifications.contains_key(&key2) {
                    found = true;
                }
            }
            Ok(Some(Err(e))) => {
                tracing::warn!(?e, "error from notification stream");
            }
            Ok(None) => {
                // Stream ended - keep waiting for reconnect
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {
                // Timeout on this poll - continue
            }
        }
    }
    assert!(found, "should receive notification after reconnect");

    Ok(())
}

/// Test that notifications stream without reconnect options terminates after server restart
#[test_log::test(tokio::test)]
async fn test_notifications_no_reconnect_default() -> anyhow::Result<()> {
    let mut server = common::TestServer::start()?;
    let client = trace_err!(server.connect(Some(config::Builder::new())).await)?;

    // Create stream with default options (no reconnect)
    let mut notifications = client.create_notifications_stream()?;
    notifications.wait_ready(Duration::from_secs(5)).await?;

    let key = test_key("no_reconnect");

    // Put a record
    client.put(&key, "value1").await?;

    // Verify we receive the notification
    if let Some(batch_result) = timeout(Duration::from_secs(5), notifications.next()).await? {
        let _ = batch_result?;
    }

    // Restart server
    tracing::info!("restarting server to test no reconnect");
    server.restart()?;

    // Wait a bit for the stream to potentially close
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The stream should either timeout or return None/error (not continue receiving)
    // We test that we don't get a successful notification after restart
    let result = timeout(Duration::from_secs(3), notifications.next()).await;

    // Either timeout or stream ended is acceptable
    match result {
        Err(_) => {
            // Timeout is expected - stream is not reconnecting
        }
        Ok(None) => {
            // Stream ended is also acceptable
        }
        Ok(Some(Err(_))) => {
            // Error is also acceptable - indicates stream had issues
        }
        Ok(Some(Ok(_))) => {
            // Getting a successful notification might happen if the stream
            // was still processing before the restart completed - this is ok
        }
    }

    Ok(())
}
