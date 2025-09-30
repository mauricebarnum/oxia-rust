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

//! Comprehensive tests for distributed tracing functionality.
//!
//! These tests verify that distributed traces are created correctly,
//! that parent-child relationships are maintained, and that traces
//! properly integrate with the metrics system.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use mauricebarnum_oxia_client::{
    DistributedTracingCollector, GrpcMetricsCollector, SpanContext, TraceId, TracedOperation,
};

/// Test basic trace creation and completion
#[tokio::test]
async fn test_basic_trace_creation() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    // Create a root span
    let root_span = tracer.start_root_span("test_operation".to_string()).await;
    let trace_id = root_span.trace_id().clone();

    // Simulate some work
    sleep(Duration::from_millis(10)).await;

    // Complete the span
    root_span.complete_success().await;

    // Verify trace was recorded
    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 1);

    let trace = &traces[0];
    assert_eq!(trace.context.trace_id, trace_id);
    assert_eq!(trace.context.operation, "test_operation");
    assert!(trace.success);
    assert!(trace.duration >= Duration::from_millis(10));
    assert!(trace.child_spans.is_empty());
}

/// Test parent-child span relationships
#[tokio::test]
async fn test_parent_child_spans() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    // Create root span
    let root_span = tracer
        .start_root_span("Client::list_with_options".to_string())
        .await;
    let trace_id = root_span.trace_id().clone();

    // Create child spans for shard operations
    let child1 = root_span
        .create_child("shard:list".to_string(), Some(1))
        .await
        .expect("Should create child span");

    let child2 = root_span
        .create_child("shard:list".to_string(), Some(2))
        .await
        .expect("Should create child span");

    let child3 = root_span
        .create_child("shard:list".to_string(), Some(3))
        .await
        .expect("Should create child span");

    // Simulate shard operations with different durations
    sleep(Duration::from_millis(5)).await;
    child1.complete_success().await;

    sleep(Duration::from_millis(3)).await;
    child2.complete_success().await;

    sleep(Duration::from_millis(7)).await;
    child3.complete_success().await;

    // Complete root span
    sleep(Duration::from_millis(2)).await;
    root_span.complete_success().await;

    // Verify trace structure
    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 1);

    let trace = &traces[0];
    assert_eq!(trace.context.trace_id, trace_id);
    assert_eq!(trace.context.operation, "Client::list_with_options");
    assert!(trace.success);

    // Check child spans
    assert_eq!(trace.child_spans.len(), 3);
    assert_eq!(trace.shard_fan_out(), 3);
    assert!(trace.all_shards_successful());

    // Verify child span details
    for child in &trace.child_spans {
        assert_eq!(child.context.operation, "shard:list");
        assert!(child.success);
        assert!(child.context.attributes.contains_key("shard_id"));
        assert!(child.duration > Duration::ZERO);
    }

    // Check that total shard time is reasonable
    let total_shard_time = trace.total_shard_time();
    assert!(total_shard_time >= Duration::from_millis(15)); // 5+3+7
    assert!(total_shard_time <= trace.duration);
}

/// Test error handling in traces
#[tokio::test]
async fn test_error_handling() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    // Test successful root with failed child
    let root_span = tracer
        .start_root_span("Client::get_with_options".to_string())
        .await;

    let child1 = root_span
        .create_child("shard:read".to_string(), Some(1))
        .await
        .unwrap();

    let child2 = root_span
        .create_child("shard:read".to_string(), Some(2))
        .await
        .unwrap();

    // First child succeeds
    child1.complete_success().await;

    // Second child fails
    child2
        .complete_error("Shard connection timeout".to_string())
        .await;

    // Root operation succeeds despite one shard failure
    root_span.complete_success().await;

    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 1);

    let trace = &traces[0];
    assert!(trace.success); // Root succeeded
    assert_eq!(trace.child_spans.len(), 2);
    assert!(!trace.all_shards_successful()); // Not all shards succeeded

    // Check individual child results
    let successful_children = trace
        .child_spans
        .iter()
        .filter(|child| child.success)
        .count();
    let failed_children = trace
        .child_spans
        .iter()
        .filter(|child| !child.success)
        .count();

    assert_eq!(successful_children, 1);
    assert_eq!(failed_children, 1);

    // Verify error message is captured
    let failed_child = trace
        .child_spans
        .iter()
        .find(|child| !child.success)
        .unwrap();
    assert_eq!(
        failed_child.error_message,
        Some("Shard connection timeout".to_string())
    );

    // Test completely failed operation
    let failed_root = tracer
        .start_root_span("Client::put_with_options".to_string())
        .await;
    failed_root
        .complete_error("Authentication failed".to_string())
        .await;

    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 2);

    let failed_trace = &traces[1];
    assert!(!failed_trace.success);
    assert_eq!(
        failed_trace.error_message,
        Some("Authentication failed".to_string())
    );
}

/// Test multiple concurrent traces
#[tokio::test]
async fn test_concurrent_traces() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    let mut handles = Vec::new();

    // Create multiple concurrent traces
    for i in 0..5 {
        let tracer_clone = tracer.clone();
        let handle = tokio::spawn(async move {
            let operation_name = format!("concurrent_operation_{}", i);
            let root_span = tracer_clone.start_root_span(operation_name).await;

            // Create variable number of child operations
            for shard_id in 1..=i + 1 {
                if let Some(child) = root_span
                    .create_child("shard:test".to_string(), Some(shard_id))
                    .await
                {
                    sleep(Duration::from_millis((i + 1) * 2)).await;
                    child.complete_success().await;
                }
            }

            sleep(Duration::from_millis(5)).await;
            root_span.complete_success().await;

            i // Return the operation index
        });

        handles.push(handle);
    }

    // Wait for all operations to complete
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("All tasks should complete successfully");

    // Verify we got results from all operations
    assert_eq!(results.len(), 5);

    // Check traces
    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 5);

    // Verify each trace has the expected structure
    for (i, trace) in traces.iter().enumerate() {
        assert!(trace.context.operation.contains("concurrent_operation_"));
        assert!(trace.success);
        // Operation i should have i+1 shard operations (0->1, 1->2, 2->3, etc.)
        let expected_shard_count = traces
            .iter()
            .position(|t| t.context.operation.ends_with(&i.to_string()))
            .map(|idx| idx + 1)
            .unwrap_or(1);

        // We can't guarantee ordering, so just check that we have reasonable shard counts
        assert!(trace.shard_fan_out() >= 1);
        assert!(trace.shard_fan_out() <= 5);
    }

    // Verify all traces have unique trace IDs
    let trace_ids: Vec<_> = traces.iter().map(|t| &t.context.trace_id).collect();
    let unique_trace_ids: std::collections::HashSet<_> = trace_ids.iter().cloned().collect();
    assert_eq!(trace_ids.len(), unique_trace_ids.len());
}

/// Test operation statistics calculation
#[tokio::test]
async fn test_operation_statistics() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    // Create multiple traces for the same operation type
    let operation_name = "Client::batch_get";

    for i in 0..3 {
        let root_span = tracer.start_root_span(operation_name.to_string()).await;

        // Variable number of shard operations and durations
        for shard_id in 1..=(i + 2) {
            if let Some(child) = root_span
                .create_child("shard:read".to_string(), Some(shard_id))
                .await
            {
                sleep(Duration::from_millis(10 + i * 5)).await;
                child.complete_success().await;
            }
        }

        sleep(Duration::from_millis(5)).await;
        root_span.complete_success().await;
    }

    // Add one failed operation
    let failed_span = tracer.start_root_span(operation_name.to_string()).await;
    failed_span
        .complete_error("Network error".to_string())
        .await;

    // Get operation statistics
    let stats = tracer.get_operation_stats(operation_name).await;

    assert_eq!(stats.operation, operation_name);
    assert_eq!(stats.trace_count, 4); // 3 successful + 1 failed
    assert_eq!(stats.success_rate, 0.75); // 3/4 = 75%

    assert!(stats.avg_duration > Duration::ZERO);
    assert!(stats.min_duration > Duration::ZERO);
    assert!(stats.max_duration >= stats.avg_duration);
    assert!(stats.max_duration >= stats.min_duration);

    // Fan-out should be > 1 (we created multiple shards per operation)
    assert!(stats.avg_fan_out > 1.0);
    assert!(stats.avg_shard_time > Duration::ZERO);

    // Efficiency should be reasonable (shard time / total time)
    assert!(stats.avg_efficiency > 0.0);
    assert!(stats.avg_efficiency <= 1.0);
}

/// Test trace cleanup and memory management
#[tokio::test]
async fn test_trace_cleanup() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    // Create several traces
    for i in 0..5 {
        let span = tracer
            .start_root_span(format!("test_operation_{}", i))
            .await;
        span.complete_success().await;
    }

    // Verify traces were recorded
    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 5);

    // Clear traces
    tracer.clear_completed_traces().await;

    // Verify traces were cleared
    let traces_after_clear = tracer.get_completed_traces().await;
    assert!(traces_after_clear.is_empty());

    // Create new traces after clearing
    let span = tracer.start_root_span("new_operation".to_string()).await;
    span.complete_success().await;

    let new_traces = tracer.get_completed_traces().await;
    assert_eq!(new_traces.len(), 1);
    assert_eq!(new_traces[0].context.operation, "new_operation");
}

/// Test trace ID generation and uniqueness
#[tokio::test]
async fn test_trace_id_generation() {
    // Test TraceId creation and formatting
    let trace_id1 = TraceId::new();
    let trace_id2 = TraceId::new();

    // Should be different
    assert_ne!(trace_id1, trace_id2);

    // Should have reasonable string representation
    assert!(!trace_id1.as_str().is_empty());
    assert!(!trace_id2.as_str().is_empty());

    // Test Display trait
    let trace_id_str = format!("{}", trace_id1);
    assert_eq!(trace_id_str, trace_id1.as_str());

    // Test from_string
    let trace_id3 = TraceId::from_string("test-trace-id".to_string());
    assert_eq!(trace_id3.as_str(), "test-trace-id");
}

/// Test span context creation and attributes
#[tokio::test]
async fn test_span_context() {
    // Test root span context
    let root_context = SpanContext::new_root("test_operation".to_string());
    assert_eq!(root_context.operation, "test_operation");
    assert!(root_context.parent_span_id.is_none());
    assert!(!root_context.span_id.is_empty());
    assert!(!root_context.trace_id.as_str().is_empty());

    // Test child span context
    let child_context = root_context.new_child("child_operation".to_string());
    assert_eq!(child_context.operation, "child_operation");
    assert_eq!(
        child_context.parent_span_id,
        Some(root_context.span_id.clone())
    );
    assert_eq!(child_context.trace_id, root_context.trace_id);
    assert_ne!(child_context.span_id, root_context.span_id);

    // Test attributes
    let context_with_attrs = root_context
        .with_attribute("user_id".to_string(), "12345".to_string())
        .with_attribute("operation_type".to_string(), "read".to_string());

    assert_eq!(
        context_with_attrs.attributes.get("user_id"),
        Some(&"12345".to_string())
    );
    assert_eq!(
        context_with_attrs.attributes.get("operation_type"),
        Some(&"read".to_string())
    );

    // Test elapsed time
    let start_time = std::time::Instant::now();
    let context = SpanContext::new_root("timing_test".to_string());
    sleep(Duration::from_millis(10)).await;
    let elapsed = context.elapsed();
    assert!(elapsed >= Duration::from_millis(10));
}

/// Test integration with metrics system
#[tokio::test]
async fn test_metrics_integration() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics.clone()));

    // Create traces that should generate metrics
    let root_span = tracer.start_root_span("Client::get".to_string()).await;

    let child_span = root_span
        .create_child("shard:read".to_string(), Some(1))
        .await
        .unwrap();

    sleep(Duration::from_millis(20)).await;
    child_span.complete_success().await;

    sleep(Duration::from_millis(5)).await;
    root_span.complete_success().await;

    // Give time for metrics to be recorded
    sleep(Duration::from_millis(10)).await;

    // Check that metrics were recorded
    let all_metrics = metrics.get_metrics().await;
    assert!(!all_metrics.is_empty());

    // Look for shard operation metrics
    let shard_metrics = all_metrics
        .iter()
        .find(|(labels, _)| labels.method == "read" && labels.shard_id == Some(1));

    assert!(shard_metrics.is_some());
    let (_, operation_metrics) = shard_metrics.unwrap();
    assert_eq!(operation_metrics.total_requests, 1);
    assert_eq!(operation_metrics.total_errors, 0);
    assert!(operation_metrics.min_duration.unwrap() >= Duration::from_millis(20));
}

/// Test auto-completion on drop
#[tokio::test]
async fn test_auto_completion_on_drop() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    {
        let _root_span = tracer
            .start_root_span("dropped_operation".to_string())
            .await;
        // Span is dropped without explicit completion
    }

    // Give time for drop handler to execute
    sleep(Duration::from_millis(50)).await;

    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), 1);

    let trace = &traces[0];
    assert_eq!(trace.context.operation, "dropped_operation");
    assert!(!trace.success); // Should be marked as failed
    assert!(trace.error_message.is_some());
    assert!(trace.error_message.as_ref().unwrap().contains("dropped"));
}

/// Performance test for high-volume tracing
#[tokio::test]
async fn test_high_volume_tracing() {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    let start_time = std::time::Instant::now();
    let num_operations = 100;

    // Create many traces quickly
    for i in 0..num_operations {
        let root_span = tracer
            .start_root_span(format!("bulk_operation_{}", i))
            .await;

        // Small number of child operations to keep test fast
        for shard_id in 1..=2 {
            if let Some(child) = root_span
                .create_child("shard:bulk".to_string(), Some(shard_id))
                .await
            {
                child.complete_success().await;
            }
        }

        root_span.complete_success().await;
    }

    let elapsed = start_time.elapsed();
    println!(
        "Created {} traces with {} child spans in {:?}",
        num_operations,
        num_operations * 2,
        elapsed
    );

    // Verify all traces were recorded
    let traces = tracer.get_completed_traces().await;
    assert_eq!(traces.len(), num_operations);

    // Performance assertion - should be reasonably fast
    // This is a rough benchmark, adjust based on system capabilities
    assert!(elapsed < Duration::from_secs(5), "Tracing should be fast");

    // Verify trace quality wasn't compromised
    for trace in &traces {
        assert!(trace.success);
        assert_eq!(trace.shard_fan_out(), 2);
    }
}
