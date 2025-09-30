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

//! Distributed tracing integration for Oxia client operations.
//!
//! This module provides distributed tracing capabilities that connect high-level
//! API operations with their constituent shard-level gRPC calls. It creates
//! hierarchical spans showing how operations like `Client::get_with_options()`
//! fan out to multiple shard requests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{Instrument, Level, Span, debug, info, instrument, span, warn};
use uuid::Uuid;

use crate::metrics::{GrpcMetricsCollector, MetricLabels, OperationMetrics};
use crate::{Error, Result};

/// Unique identifier for tracing an operation across multiple shards
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TraceId(String);

impl TraceId {
    /// Generate a new unique trace ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create from existing string
    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Span context for tracking operation hierarchy
#[derive(Debug, Clone)]
pub struct SpanContext {
    /// Unique trace identifier
    pub trace_id: TraceId,
    /// Parent span ID (if this is a child span)
    pub parent_span_id: Option<String>,
    /// This span's ID
    pub span_id: String,
    /// Operation name
    pub operation: String,
    /// Start time
    pub start_time: Instant,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

impl SpanContext {
    /// Create a new root span context
    pub fn new_root(operation: String) -> Self {
        Self {
            trace_id: TraceId::new(),
            parent_span_id: None,
            span_id: Uuid::new_v4().to_string(),
            operation,
            start_time: Instant::now(),
            attributes: HashMap::new(),
        }
    }

    /// Create a child span context
    pub fn new_child(&self, operation: String) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            parent_span_id: Some(self.span_id.clone()),
            span_id: Uuid::new_v4().to_string(),
            operation,
            start_time: Instant::now(),
            attributes: HashMap::new(),
        }
    }

    /// Add an attribute to the span
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes.insert(key, value);
        self
    }

    /// Get elapsed time since span started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Information about a completed span
#[derive(Debug, Clone)]
pub struct CompletedSpan {
    pub context: SpanContext,
    pub duration: Duration,
    pub success: bool,
    pub error_message: Option<String>,
    pub child_spans: Vec<CompletedSpan>,
}

impl CompletedSpan {
    /// Get all shard operations that were part of this trace
    pub fn get_shard_operations(&self) -> Vec<&CompletedSpan> {
        let mut operations = Vec::new();
        self.collect_shard_operations(&mut operations);
        operations
    }

    fn collect_shard_operations(&self, operations: &mut Vec<&CompletedSpan>) {
        if self.context.operation.contains("shard:") {
            operations.push(self);
        }
        for child in &self.child_spans {
            child.collect_shard_operations(operations);
        }
    }

    /// Calculate total time spent in shard operations
    pub fn total_shard_time(&self) -> Duration {
        self.get_shard_operations()
            .iter()
            .map(|span| span.duration)
            .sum()
    }

    /// Get the fan-out factor (number of shard operations)
    pub fn shard_fan_out(&self) -> usize {
        self.get_shard_operations().len()
    }

    /// Check if operation was successful across all shards
    pub fn all_shards_successful(&self) -> bool {
        self.get_shard_operations().iter().all(|span| span.success)
    }
}

/// Distributed tracing collector that integrates with metrics
#[derive(Debug)]
pub struct DistributedTracingCollector {
    /// Active spans indexed by trace ID
    active_spans: Arc<RwLock<HashMap<TraceId, SpanHierarchy>>>,
    /// Completed traces for analysis
    completed_traces: Arc<RwLock<Vec<CompletedSpan>>>,
    /// Integration with metrics collector
    metrics: Arc<GrpcMetricsCollector>,
    /// Maximum number of completed traces to keep
    max_completed_traces: usize,
}

#[derive(Debug)]
struct SpanHierarchy {
    root: SpanContext,
    children: HashMap<String, SpanContext>,
    completed_children: Vec<CompletedSpan>,
}

impl DistributedTracingCollector {
    /// Create a new tracing collector
    pub fn new(metrics: Arc<GrpcMetricsCollector>) -> Self {
        Self {
            active_spans: Arc::new(RwLock::new(HashMap::new())),
            completed_traces: Arc::new(RwLock::new(Vec::new())),
            metrics,
            max_completed_traces: 1000,
        }
    }

    /// Start a new root span for a top-level API operation
    pub async fn start_root_span(&self, operation: String) -> TracedOperation {
        let context = SpanContext::new_root(operation);
        let trace_id = context.trace_id.clone();

        // Record in active spans
        {
            let mut active = self.active_spans.write().await;
            active.insert(
                trace_id.clone(),
                SpanHierarchy {
                    root: context.clone(),
                    children: HashMap::new(),
                    completed_children: Vec::new(),
                },
            );
        }

        info!(
            trace_id = %trace_id,
            operation = %context.operation,
            "Started root span"
        );

        TracedOperation::new(context, self.clone())
    }

    /// Start a child span for a shard operation
    pub async fn start_child_span(
        &self,
        trace_id: &TraceId,
        operation: String,
        shard_id: Option<i64>,
    ) -> Option<TracedOperation> {
        let mut active = self.active_spans.write().await;

        if let Some(hierarchy) = active.get_mut(trace_id) {
            let mut context = hierarchy.root.new_child(operation);

            // Add shard-specific attributes
            if let Some(shard_id) = shard_id {
                context = context.with_attribute("shard_id".to_string(), shard_id.to_string());
            }

            let span_id = context.span_id.clone();
            hierarchy.children.insert(span_id, context.clone());

            debug!(
                trace_id = %trace_id,
                parent_span = %hierarchy.root.span_id,
                span_id = %context.span_id,
                operation = %context.operation,
                shard_id = ?shard_id,
                "Started child span"
            );

            Some(TracedOperation::new(context, self.clone()))
        } else {
            warn!(
                trace_id = %trace_id,
                operation = %operation,
                "Cannot create child span: parent trace not found"
            );
            None
        }
    }

    /// Complete a span and record its results
    pub async fn complete_span(
        &self,
        context: SpanContext,
        success: bool,
        error_message: Option<String>,
    ) {
        let duration = context.elapsed();
        let trace_id = &context.trace_id;

        debug!(
            trace_id = %trace_id,
            span_id = %context.span_id,
            operation = %context.operation,
            duration_ms = duration.as_millis(),
            success = success,
            "Completed span"
        );

        // Record metrics for this span
        self.record_span_metrics(&context, duration, !success).await;

        let mut active = self.active_spans.write().await;

        if let Some(hierarchy) = active.get_mut(trace_id) {
            let completed_span = CompletedSpan {
                context: context.clone(),
                duration,
                success,
                error_message,
                child_spans: Vec::new(), // Will be populated when root completes
            };

            if context.parent_span_id.is_none() {
                // This is the root span - complete the entire trace
                let mut root_span = CompletedSpan {
                    context: hierarchy.root.clone(),
                    duration,
                    success,
                    error_message,
                    child_spans: hierarchy.completed_children.clone(),
                };

                // Add any remaining active children
                for child_context in hierarchy.children.values() {
                    root_span.child_spans.push(CompletedSpan {
                        context: child_context.clone(),
                        duration: child_context.elapsed(),
                        success: false, // Assume failure if not explicitly completed
                        error_message: Some("Span not properly completed".to_string()),
                        child_spans: Vec::new(),
                    });
                }

                // Store completed trace
                {
                    let mut completed = self.completed_traces.write().await;
                    completed.push(root_span.clone());

                    // Limit memory usage
                    if completed.len() > self.max_completed_traces {
                        completed.remove(0);
                    }
                }

                // Remove from active spans
                active.remove(trace_id);

                info!(
                    trace_id = %trace_id,
                    duration_ms = duration.as_millis(),
                    child_spans = root_span.child_spans.len(),
                    success = success,
                    "Completed trace"
                );

                // Log trace summary
                self.log_trace_summary(&root_span);
            } else {
                // This is a child span
                hierarchy.completed_children.push(completed_span);
                if let Some(parent_span_id) = &context.parent_span_id {
                    hierarchy.children.remove(&context.span_id);
                }
            }
        }
    }

    /// Record metrics for a span
    async fn record_span_metrics(&self, context: &SpanContext, duration: Duration, is_error: bool) {
        // Extract method name from operation
        let method = if context.operation.starts_with("shard:") {
            context
                .operation
                .strip_prefix("shard:")
                .unwrap_or(&context.operation)
        } else {
            &context.operation
        };

        // Extract shard ID from attributes
        let shard_id = context
            .attributes
            .get("shard_id")
            .and_then(|s| s.parse().ok());

        // Get namespace from attributes or use default
        let namespace = context
            .attributes
            .get("namespace")
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        let labels = MetricLabels::new(method, shard_id, &namespace);
        self.metrics
            .record_operation(labels, duration, is_error)
            .await;
    }

    /// Get all completed traces
    pub async fn get_completed_traces(&self) -> Vec<CompletedSpan> {
        self.completed_traces.read().await.clone()
    }

    /// Get traces that match a specific pattern
    pub async fn get_traces_for_operation(&self, operation: &str) -> Vec<CompletedSpan> {
        let completed = self.completed_traces.read().await;
        completed
            .iter()
            .filter(|trace| trace.context.operation == operation)
            .cloned()
            .collect()
    }

    /// Get performance statistics for an operation
    pub async fn get_operation_stats(&self, operation: &str) -> OperationStats {
        let traces = self.get_traces_for_operation(operation).await;
        OperationStats::from_traces(operation, &traces)
    }

    /// Clear completed traces (for memory management)
    pub async fn clear_completed_traces(&self) {
        self.completed_traces.write().await.clear();
        info!("Cleared all completed traces");
    }

    /// Log a summary of trace performance
    fn log_trace_summary(&self, trace: &CompletedSpan) {
        let shard_ops = trace.get_shard_operations();
        let total_shard_time = trace.total_shard_time();
        let fan_out = trace.shard_fan_out();

        info!(
            trace_id = %trace.context.trace_id,
            operation = %trace.context.operation,
            total_duration_ms = trace.duration.as_millis(),
            shard_operations = fan_out,
            shard_time_ms = total_shard_time.as_millis(),
            shard_time_ratio = if trace.duration.as_millis() > 0 {
                total_shard_time.as_millis() as f64 / trace.duration.as_millis() as f64
            } else { 0.0 },
            success = trace.success,
            "Trace completed"
        );

        // Log individual shard operations if there are multiple
        if fan_out > 1 {
            for (i, shard_op) in shard_ops.iter().enumerate() {
                debug!(
                    trace_id = %trace.context.trace_id,
                    shard_operation = i + 1,
                    shard_id = ?shard_op.context.attributes.get("shard_id"),
                    duration_ms = shard_op.duration.as_millis(),
                    success = shard_op.success,
                    "Shard operation detail"
                );
            }
        }
    }

    /// Log performance analysis across all traces
    pub async fn log_performance_analysis(&self) {
        let traces = self.get_completed_traces().await;
        if traces.is_empty() {
            return;
        }

        // Group traces by operation
        let mut operations: HashMap<String, Vec<&CompletedSpan>> = HashMap::new();
        for trace in &traces {
            operations
                .entry(trace.context.operation.clone())
                .or_insert_with(Vec::new)
                .push(trace);
        }

        info!("=== Distributed Tracing Performance Analysis ===");

        for (operation, op_traces) in operations {
            let stats = OperationStats::from_traces(&operation, op_traces);

            info!("Operation: {} ({} traces)", operation, stats.trace_count);
            info!(
                "  End-to-end: avg={:.1}ms, min={:.1}ms, max={:.1}ms",
                stats.avg_duration.as_secs_f64() * 1000.0,
                stats.min_duration.as_secs_f64() * 1000.0,
                stats.max_duration.as_secs_f64() * 1000.0
            );
            info!(
                "  Shard ops: avg={:.1} shards, total_time={:.1}ms avg",
                stats.avg_fan_out,
                stats.avg_shard_time.as_secs_f64() * 1000.0
            );
            info!(
                "  Efficiency: {:.1}% (shard_time/total_time)",
                stats.avg_efficiency * 100.0
            );
            info!("  Success rate: {:.1}%", stats.success_rate * 100.0);
        }

        info!("=== End Analysis ===");
    }
}

impl Clone for DistributedTracingCollector {
    fn clone(&self) -> Self {
        Self {
            active_spans: self.active_spans.clone(),
            completed_traces: self.completed_traces.clone(),
            metrics: self.metrics.clone(),
            max_completed_traces: self.max_completed_traces,
        }
    }
}

/// Statistics for an operation across multiple traces
#[derive(Debug, Clone)]
pub struct OperationStats {
    pub operation: String,
    pub trace_count: usize,
    pub success_rate: f64,
    pub avg_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub avg_fan_out: f64,
    pub avg_shard_time: Duration,
    pub avg_efficiency: f64, // shard_time / total_time
}

impl OperationStats {
    fn from_traces(operation: &str, traces: &[&CompletedSpan]) -> Self {
        if traces.is_empty() {
            return Self {
                operation: operation.to_string(),
                trace_count: 0,
                success_rate: 0.0,
                avg_duration: Duration::ZERO,
                min_duration: Duration::ZERO,
                max_duration: Duration::ZERO,
                avg_fan_out: 0.0,
                avg_shard_time: Duration::ZERO,
                avg_efficiency: 0.0,
            };
        }

        let successful = traces.iter().filter(|t| t.success).count();
        let success_rate = successful as f64 / traces.len() as f64;

        let durations: Vec<Duration> = traces.iter().map(|t| t.duration).collect();
        let total_duration: Duration = durations.iter().sum();
        let avg_duration = total_duration / traces.len() as u32;
        let min_duration = durations.iter().min().unwrap().clone();
        let max_duration = durations.iter().max().unwrap().clone();

        let fan_outs: Vec<usize> = traces.iter().map(|t| t.shard_fan_out()).collect();
        let avg_fan_out = fan_outs.iter().sum::<usize>() as f64 / traces.len() as f64;

        let shard_times: Vec<Duration> = traces.iter().map(|t| t.total_shard_time()).collect();
        let total_shard_time: Duration = shard_times.iter().sum();
        let avg_shard_time = total_shard_time / traces.len() as u32;

        let avg_efficiency = if total_duration.as_nanos() > 0 {
            total_shard_time.as_nanos() as f64 / total_duration.as_nanos() as f64
        } else {
            0.0
        };

        Self {
            operation: operation.to_string(),
            trace_count: traces.len(),
            success_rate,
            avg_duration,
            min_duration,
            max_duration,
            avg_fan_out,
            avg_shard_time,
            avg_efficiency,
        }
    }
}

/// A traced operation that automatically records timing and integrates with spans
pub struct TracedOperation {
    context: SpanContext,
    collector: DistributedTracingCollector,
    completed: bool,
}

impl TracedOperation {
    fn new(context: SpanContext, collector: DistributedTracingCollector) -> Self {
        Self {
            context,
            collector,
            completed: false,
        }
    }

    /// Get the trace ID for this operation
    pub fn trace_id(&self) -> &TraceId {
        &self.context.trace_id
    }

    /// Get the span context
    pub fn context(&self) -> &SpanContext {
        &self.context
    }

    /// Create a child traced operation
    pub async fn create_child(
        &self,
        operation: String,
        shard_id: Option<i64>,
    ) -> Option<TracedOperation> {
        self.collector
            .start_child_span(&self.context.trace_id, operation, shard_id)
            .await
    }

    /// Complete this operation successfully
    pub async fn complete_success(mut self) {
        if !self.completed {
            self.collector
                .complete_span(self.context.clone(), true, None)
                .await;
            self.completed = true;
        }
    }

    /// Complete this operation with an error
    pub async fn complete_error(mut self, error_message: String) {
        if !self.completed {
            self.collector
                .complete_span(self.context.clone(), false, Some(error_message))
                .await;
            self.completed = true;
        }
    }

    /// Complete based on a Result
    pub async fn complete_with_result<T, E: std::fmt::Display>(mut self, result: &Result<T, E>) {
        if !self.completed {
            match result {
                Ok(_) => self.complete_success().await,
                Err(e) => self.complete_error(e.to_string()).await,
            }
        }
    }
}

impl Drop for TracedOperation {
    fn drop(&mut self) {
        if !self.completed {
            let collector = self.collector.clone();
            let context = self.context.clone();

            // Try to complete the span as failed if not explicitly completed
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    collector
                        .complete_span(
                            context,
                            false,
                            Some("Operation dropped without completion".to_string()),
                        )
                        .await;
                });
            }
        }
    }
}

/// Convenience macro for instrumenting API operations with distributed tracing
#[macro_export]
macro_rules! trace_api_operation {
    ($tracer:expr, $operation:expr, $block:expr) => {{
        let traced_op = $tracer.start_root_span($operation.to_string()).await;
        let trace_id = traced_op.trace_id().clone();

        let result = async move { $block }.await;

        traced_op.complete_with_result(&result).await;
        result
    }};
}

/// Convenience macro for instrumenting shard operations
#[macro_export]
macro_rules! trace_shard_operation {
    ($parent:expr, $operation:expr, $shard_id:expr, $block:expr) => {{
        if let Some(traced_op) = $parent
            .create_child(format!("shard:{}", $operation), $shard_id)
            .await
        {
            let result = async move { $block }.await;
            traced_op.complete_with_result(&result).await;
            result
        } else {
            $block.await
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_basic_tracing() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = DistributedTracingCollector::new(metrics);

        let root_op = tracer.start_root_span("test_get".to_string()).await;
        let trace_id = root_op.trace_id().clone();

        // Simulate some work
        sleep(Duration::from_millis(10)).await;
        root_op.complete_success().await;

        // Check that trace was recorded
        let traces = tracer.get_completed_traces().await;
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].context.trace_id, trace_id);
        assert_eq!(traces[0].context.operation, "test_get");
        assert!(traces[0].success);
    }

    #[tokio::test]
    async fn test_parent_child_tracing() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = DistributedTracingCollector::new(metrics);

        let root_op = tracer.start_root_span("test_list".to_string()).await;
        let trace_id = root_op.trace_id().clone();

        // Create child operations for different shards
        let child1 = root_op
            .create_child("shard:list".to_string(), Some(1))
            .await
            .unwrap();
        let child2 = root_op
            .create_child("shard:list".to_string(), Some(2))
            .await
            .unwrap();

        // Simulate shard work
        sleep(Duration::from_millis(5)).await;
        child1.complete_success().await;

        sleep(Duration::from_millis(8)).await;
        child2.complete_success().await;

        // Complete root operation
        sleep(Duration::from_millis(2)).await;
        root_op.complete_success().await;

        // Check trace structure
        let traces = tracer.get_completed_traces().await;
        assert_eq!(traces.len(), 1);

        let trace = &traces[0];
        assert_eq!(trace.context.trace_id, trace_id);
        assert_eq!(trace.child_spans.len(), 2);
        assert_eq!(trace.shard_fan_out(), 2);
        assert!(trace.all_shards_successful());
    }

    #[tokio::test]
    async fn test_operation_stats() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = DistributedTracingCollector::new(metrics);

        // Create multiple traces for the same operation
        for i in 0..3 {
            let root_op = tracer.start_root_span("test_operation".to_string()).await;

            // Variable number of shard operations
            for shard_id in 0..=i {
                if let Some(child) = root_op
                    .create_child("shard:test".to_string(), Some(shard_id))
                    .await
                {
                    sleep(Duration::from_millis(i as u64 + 1)).await;
                    child.complete_success().await;
                }
            }

            sleep(Duration::from_millis(1)).await;
            root_op.complete_success().await;
        }

        let stats = tracer.get_operation_stats("test_operation").await;
        assert_eq!(stats.trace_count, 3);
        assert_eq!(stats.success_rate, 1.0);
        assert!(stats.avg_fan_out > 0.0);
        assert!(stats.avg_duration > Duration::ZERO);
    }
}
