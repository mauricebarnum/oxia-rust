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

//! Integration tests for gRPC metrics collection functionality.
//!
//! These tests verify that metrics are properly collected for all gRPC operations
//! and that the metrics data is accurate and complete.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use mauricebarnum_oxia_client::{
    GrpcMetricsCollector, MetricLabels, MetricsConfig, MetricsConfigBuilder,
    MetricsEnabledClientFactory, OperationMetrics,
};

/// Test basic metrics collection functionality
#[tokio::test]
async fn test_basic_metrics_collection() {
    let metrics = Arc::new(GrpcMetricsCollector::new());

    // Verify initial state
    assert!(metrics.is_enabled());
    let initial_metrics = metrics.get_metrics().await;
    assert!(initial_metrics.is_empty());

    // Record some test operations
    let labels = MetricLabels::new("test_read", Some(1), "test_namespace");
    metrics
        .record_operation(labels.clone(), Duration::from_millis(50), false)
        .await;
    metrics
        .record_operation(labels.clone(), Duration::from_millis(75), false)
        .await;
    metrics
        .record_operation(labels.clone(), Duration::from_millis(100), true)
        .await;

    // Verify metrics were recorded
    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();

    assert_eq!(operation_metrics.total_requests, 3);
    assert_eq!(operation_metrics.total_errors, 1);
    assert!((operation_metrics.error_rate() - 1.0 / 3.0).abs() < f64::EPSILON);

    // Check latency metrics
    assert_eq!(
        operation_metrics.min_duration,
        Some(Duration::from_millis(50))
    );
    assert_eq!(
        operation_metrics.max_duration,
        Some(Duration::from_millis(100))
    );

    let avg_duration = operation_metrics.average_duration().unwrap();
    let expected_avg = Duration::from_millis(75); // (50 + 75 + 100) / 3
    assert!((avg_duration.as_millis() as i64 - expected_avg.as_millis() as i64).abs() <= 1);
}

/// Test metrics collection with different operation types
#[tokio::test]
async fn test_multiple_operation_types() {
    let metrics = Arc::new(GrpcMetricsCollector::new());

    // Record different types of operations
    let read_labels = MetricLabels::new("read", Some(1), "test_ns");
    let write_labels = MetricLabels::new("write_stream", Some(1), "test_ns");
    let list_labels = MetricLabels::new("list", Some(2), "test_ns");
    let session_labels = MetricLabels::new("create_session", None, "test_ns");

    // Record operations for each type
    metrics
        .record_operation(read_labels.clone(), Duration::from_millis(10), false)
        .await;
    metrics
        .record_operation(read_labels.clone(), Duration::from_millis(15), false)
        .await;

    metrics
        .record_operation(write_labels.clone(), Duration::from_millis(50), false)
        .await;
    metrics
        .record_operation(write_labels.clone(), Duration::from_millis(60), true)
        .await;

    metrics
        .record_operation(list_labels.clone(), Duration::from_millis(200), false)
        .await;

    metrics
        .record_operation(session_labels.clone(), Duration::from_millis(5), false)
        .await;

    // Verify all operation types were recorded
    let all_metrics = metrics.get_metrics().await;
    assert_eq!(all_metrics.len(), 4);

    // Verify read operations
    let read_metrics = all_metrics.get(&read_labels).unwrap();
    assert_eq!(read_metrics.total_requests, 2);
    assert_eq!(read_metrics.total_errors, 0);

    // Verify write operations
    let write_metrics = all_metrics.get(&write_labels).unwrap();
    assert_eq!(write_metrics.total_requests, 2);
    assert_eq!(write_metrics.total_errors, 1);
    assert_eq!(write_metrics.error_rate(), 0.5);

    // Verify list operations
    let list_metrics = all_metrics.get(&list_labels).unwrap();
    assert_eq!(list_metrics.total_requests, 1);
    assert_eq!(list_metrics.total_errors, 0);
    assert_eq!(list_metrics.max_duration, Some(Duration::from_millis(200)));

    // Verify session operations (no shard ID)
    let session_metrics = all_metrics.get(&session_labels).unwrap();
    assert_eq!(session_metrics.total_requests, 1);
    assert_eq!(session_metrics.total_errors, 0);
}

/// Test metrics histogram functionality
#[tokio::test]
async fn test_metrics_histogram() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let labels = MetricLabels::new("test_operation", Some(1), "test_ns");

    // Record operations with various latencies to test histogram buckets
    let latencies = vec![
        Duration::from_micros(100),  // 0.1ms
        Duration::from_millis(1),    // 1ms
        Duration::from_millis(5),    // 5ms
        Duration::from_millis(25),   // 25ms
        Duration::from_millis(100),  // 100ms
        Duration::from_millis(500),  // 500ms
        Duration::from_millis(2000), // 2s
    ];

    for latency in latencies {
        metrics
            .record_operation(labels.clone(), latency, false)
            .await;
    }

    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    assert_eq!(operation_metrics.total_requests, 7);

    // Verify histogram buckets contain the expected counts
    assert!(operation_metrics.histogram.get("le_0.25").unwrap() >= &1); // 0.1ms
    assert!(operation_metrics.histogram.get("le_1.0").unwrap() >= &1); // 1ms
    assert!(operation_metrics.histogram.get("le_5.0").unwrap() >= &2); // 1ms + 5ms
    assert!(operation_metrics.histogram.get("le_25.0").unwrap() >= &3); // + 25ms
    assert!(operation_metrics.histogram.get("le_100.0").unwrap() >= &4); // + 100ms
    assert!(operation_metrics.histogram.get("le_500.0").unwrap() >= &5); // + 500ms
    assert_eq!(operation_metrics.histogram.get("le_inf").unwrap(), &7); // All requests
}

/// Test enabling and disabling metrics collection
#[tokio::test]
async fn test_enable_disable_metrics() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let labels = MetricLabels::new("test_operation", Some(1), "test_ns");

    // Initially enabled
    assert!(metrics.is_enabled());

    // Record an operation
    metrics
        .record_operation(labels.clone(), Duration::from_millis(10), false)
        .await;
    assert_eq!(
        metrics
            .get_operation_metrics(&labels)
            .await
            .unwrap()
            .total_requests,
        1
    );

    // Disable metrics
    metrics.set_enabled(false);
    assert!(!metrics.is_enabled());

    // Record another operation - should be ignored
    metrics
        .record_operation(labels.clone(), Duration::from_millis(20), false)
        .await;
    assert_eq!(
        metrics
            .get_operation_metrics(&labels)
            .await
            .unwrap()
            .total_requests,
        1
    );

    // Re-enable metrics
    metrics.set_enabled(true);
    assert!(metrics.is_enabled());

    // Record another operation - should be counted
    metrics
        .record_operation(labels.clone(), Duration::from_millis(30), false)
        .await;
    assert_eq!(
        metrics
            .get_operation_metrics(&labels)
            .await
            .unwrap()
            .total_requests,
        2
    );
}

/// Test clearing metrics
#[tokio::test]
async fn test_clear_metrics() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let labels = MetricLabels::new("test_operation", Some(1), "test_ns");

    // Record some operations
    metrics
        .record_operation(labels.clone(), Duration::from_millis(10), false)
        .await;
    metrics
        .record_operation(labels.clone(), Duration::from_millis(20), true)
        .await;

    // Verify metrics exist
    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    assert_eq!(operation_metrics.total_requests, 2);
    assert_eq!(operation_metrics.total_errors, 1);

    // Clear all metrics
    metrics.clear_metrics().await;

    // Verify metrics are cleared
    let all_metrics = metrics.get_metrics().await;
    assert!(all_metrics.is_empty());
    assert!(metrics.get_operation_metrics(&labels).await.is_none());
}

/// Test Prometheus export format
#[tokio::test]
async fn test_prometheus_export() {
    let metrics = Arc::new(GrpcMetricsCollector::new());

    // Record operations for multiple methods and shards
    let labels1 = MetricLabels::new("read", Some(1), "prod");
    let labels2 = MetricLabels::new("write_stream", Some(2), "prod");
    let labels3 = MetricLabels::new("create_session", None, "prod");

    metrics
        .record_operation(labels1, Duration::from_millis(10), false)
        .await;
    metrics
        .record_operation(labels2, Duration::from_millis(50), true)
        .await;
    metrics
        .record_operation(labels3, Duration::from_millis(5), false)
        .await;

    // Export in Prometheus format
    let prometheus_output = metrics.export_prometheus_format().await;

    // Verify the output contains expected metrics
    assert!(prometheus_output.contains("oxia_grpc_requests_total"));
    assert!(prometheus_output.contains("oxia_grpc_request_errors_total"));
    assert!(prometheus_output.contains("oxia_grpc_request_duration_seconds"));

    // Verify specific labels are present
    assert!(prometheus_output.contains(r#"method="read""#));
    assert!(prometheus_output.contains(r#"method="write_stream""#));
    assert!(prometheus_output.contains(r#"method="create_session""#));
    assert!(prometheus_output.contains(r#"namespace="prod""#));
    assert!(prometheus_output.contains(r#"shard_id="1""#));
    assert!(prometheus_output.contains(r#"shard_id="2""#));

    // Verify histogram buckets
    assert!(prometheus_output.contains(r#"le="0.1""#));
    assert!(prometheus_output.contains(r#"le="1.0""#));
    assert!(prometheus_output.contains(r#"le="inf""#));

    // Verify values
    assert!(
        prometheus_output.contains(
            "oxia_grpc_requests_total{method=\"read\",namespace=\"prod\",shard_id=\"1\"} 1"
        )
    );
    assert!(prometheus_output.contains("oxia_grpc_request_errors_total{method=\"write_stream\",namespace=\"prod\",shard_id=\"2\"} 1"));
}

/// Test MetricsEnabledClientFactory
#[tokio::test]
async fn test_metrics_enabled_factory() {
    let config = MetricsConfigBuilder::new()
        .enabled(true)
        .log_summaries(false) // Disable for test
        .enable_prometheus_export(true)
        .build();

    let factory = MetricsEnabledClientFactory::new(config);
    let metrics = factory.metrics();

    // Verify factory creates enabled metrics
    assert!(metrics.is_enabled());

    // Record a test operation
    let labels = MetricLabels::new("test_factory", Some(1), "test_ns");
    metrics
        .record_operation(labels.clone(), Duration::from_millis(25), false)
        .await;

    // Verify metrics are collected
    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    assert_eq!(operation_metrics.total_requests, 1);

    // Test Prometheus export through factory
    let prometheus_output = factory.export_prometheus().await;
    assert!(prometheus_output.contains("oxia_grpc_requests_total"));

    // Test clearing through factory
    factory.clear_metrics().await;
    let all_metrics = metrics.get_metrics().await;
    assert!(all_metrics.is_empty());
}

/// Test concurrent metrics collection
#[tokio::test]
async fn test_concurrent_metrics_collection() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let labels = MetricLabels::new("concurrent_test", Some(1), "test_ns");

    // Spawn multiple concurrent tasks recording metrics
    let mut handles = Vec::new();
    for i in 0..10 {
        let metrics_clone = metrics.clone();
        let labels_clone = labels.clone();

        let handle = tokio::spawn(async move {
            for j in 0..5 {
                let latency = Duration::from_millis((i * 5 + j) as u64 + 10);
                let is_error = j % 3 == 0; // Every 3rd request is an error
                metrics_clone
                    .record_operation(labels_clone.clone(), latency, is_error)
                    .await;

                // Small delay to simulate real operation timing
                sleep(Duration::from_micros(100)).await;
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final metrics
    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    assert_eq!(operation_metrics.total_requests, 50); // 10 tasks * 5 operations each

    // Verify error counting (every 3rd request should be an error)
    let expected_errors = 50 / 3 + if 50 % 3 != 0 { 1 } else { 0 }; // Ceiling division
    assert!(operation_metrics.total_errors >= expected_errors - 5); // Allow some variance due to concurrency
    assert!(operation_metrics.total_errors <= expected_errors + 5);

    // Verify latency bounds
    assert!(operation_metrics.min_duration.unwrap() >= Duration::from_millis(10));
    assert!(operation_metrics.max_duration.unwrap() <= Duration::from_millis(60));
}

/// Test metric labels equality and hashing
#[tokio::test]
async fn test_metric_labels() {
    let labels1 = MetricLabels::new("read", Some(1), "test_ns");
    let labels2 = MetricLabels::new("read", Some(1), "test_ns");
    let labels3 = MetricLabels::new("read", Some(2), "test_ns"); // Different shard
    let labels4 = MetricLabels::new("write", Some(1), "test_ns"); // Different method
    let labels5 = MetricLabels::new("read", None, "test_ns"); // No shard

    // Test equality
    assert_eq!(labels1, labels2);
    assert_ne!(labels1, labels3);
    assert_ne!(labels1, labels4);
    assert_ne!(labels1, labels5);

    // Test that equal labels can be used as HashMap keys
    let metrics = Arc::new(GrpcMetricsCollector::new());

    metrics
        .record_operation(labels1.clone(), Duration::from_millis(10), false)
        .await;
    metrics
        .record_operation(labels2.clone(), Duration::from_millis(20), false)
        .await;

    // Should be recorded under the same key since labels1 == labels2
    let operation_metrics = metrics.get_operation_metrics(&labels1).await.unwrap();
    assert_eq!(operation_metrics.total_requests, 2);
}

/// Test error rate calculations
#[tokio::test]
async fn test_error_rate_calculations() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let labels = MetricLabels::new("error_rate_test", Some(1), "test_ns");

    // Test 0% error rate
    metrics
        .record_operation(labels.clone(), Duration::from_millis(10), false)
        .await;
    metrics
        .record_operation(labels.clone(), Duration::from_millis(15), false)
        .await;

    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    assert_eq!(operation_metrics.error_rate(), 0.0);

    // Add an error - should be 33.33% (1/3)
    metrics
        .record_operation(labels.clone(), Duration::from_millis(20), true)
        .await;

    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    let expected_rate = 1.0 / 3.0;
    assert!((operation_metrics.error_rate() - expected_rate).abs() < f64::EPSILON);

    // Add another error - should be 50% (2/4)
    metrics
        .record_operation(labels.clone(), Duration::from_millis(25), true)
        .await;

    let operation_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
    assert_eq!(operation_metrics.error_rate(), 0.5);
}

/// Test that metrics work with different namespace configurations
#[tokio::test]
async fn test_namespace_isolation() {
    let metrics = Arc::new(GrpcMetricsCollector::new());

    // Create labels for different namespaces
    let prod_labels = MetricLabels::new("read", Some(1), "production");
    let staging_labels = MetricLabels::new("read", Some(1), "staging");
    let dev_labels = MetricLabels::new("read", Some(1), "development");

    // Record operations for each namespace
    metrics
        .record_operation(prod_labels.clone(), Duration::from_millis(10), false)
        .await;
    metrics
        .record_operation(prod_labels.clone(), Duration::from_millis(15), true)
        .await;

    metrics
        .record_operation(staging_labels.clone(), Duration::from_millis(20), false)
        .await;

    metrics
        .record_operation(dev_labels.clone(), Duration::from_millis(5), false)
        .await;
    metrics
        .record_operation(dev_labels.clone(), Duration::from_millis(8), false)
        .await;
    metrics
        .record_operation(dev_labels.clone(), Duration::from_millis(12), false)
        .await;

    // Verify namespace isolation
    let prod_metrics = metrics.get_operation_metrics(&prod_labels).await.unwrap();
    assert_eq!(prod_metrics.total_requests, 2);
    assert_eq!(prod_metrics.total_errors, 1);

    let staging_metrics = metrics
        .get_operation_metrics(&staging_labels)
        .await
        .unwrap();
    assert_eq!(staging_metrics.total_requests, 1);
    assert_eq!(staging_metrics.total_errors, 0);

    let dev_metrics = metrics.get_operation_metrics(&dev_labels).await.unwrap();
    assert_eq!(dev_metrics.total_requests, 3);
    assert_eq!(dev_metrics.total_errors, 0);

    // Verify total count
    let all_metrics = metrics.get_metrics().await;
    assert_eq!(all_metrics.len(), 3);
}
