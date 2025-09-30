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

//! Metrics collection for gRPC operations in the Oxia client.
//!
//! This module provides instrumentation for collecting latency and other metrics
//! for all gRPC calls made by the client.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Histogram bucket boundaries for latency measurements (in milliseconds)
const LATENCY_BUCKETS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
];

/// Metrics for a specific gRPC operation
#[derive(Debug, Clone)]
pub struct OperationMetrics {
    pub total_requests: u64,
    pub total_errors: u64,
    pub total_duration: Duration,
    pub min_duration: Option<Duration>,
    pub max_duration: Option<Duration>,
    pub histogram: HashMap<String, u64>, // bucket -> count
}

impl Default for OperationMetrics {
    fn default() -> Self {
        let mut histogram = HashMap::new();
        for &bucket in LATENCY_BUCKETS {
            histogram.insert(format!("le_{}", bucket), 0);
        }
        histogram.insert("le_inf".to_string(), 0);

        Self {
            total_requests: 0,
            total_errors: 0,
            total_duration: Duration::ZERO,
            min_duration: None,
            max_duration: None,
            histogram,
        }
    }
}

impl OperationMetrics {
    fn record_request(&mut self, duration: Duration, is_error: bool) {
        self.total_requests += 1;
        if is_error {
            self.total_errors += 1;
        }

        self.total_duration += duration;

        // Update min/max
        match self.min_duration {
            None => self.min_duration = Some(duration),
            Some(min) if duration < min => self.min_duration = Some(duration),
            _ => {}
        }

        match self.max_duration {
            None => self.max_duration = Some(duration),
            Some(max) if duration > max => self.max_duration = Some(duration),
            _ => {}
        }

        // Update histogram
        let duration_ms = duration.as_secs_f64() * 1000.0;
        for &bucket in LATENCY_BUCKETS {
            if duration_ms <= bucket {
                *self.histogram.entry(format!("le_{}", bucket)).or_insert(0) += 1;
            }
        }
        *self.histogram.entry("le_inf".to_string()).or_insert(0) += 1;
    }

    pub fn average_duration(&self) -> Option<Duration> {
        if self.total_requests == 0 {
            None
        } else {
            Some(self.total_duration / self.total_requests as u32)
        }
    }

    pub fn error_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.total_errors as f64 / self.total_requests as f64
        }
    }
}

/// Labels for gRPC metrics
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricLabels {
    pub method: String,
    pub shard_id: Option<i64>,
    pub namespace: String,
}

impl MetricLabels {
    pub fn new(
        method: impl Into<String>,
        shard_id: Option<i64>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            method: method.into(),
            shard_id,
            namespace: namespace.into(),
        }
    }
}

/// Central metrics collector for gRPC operations
#[derive(Debug)]
pub struct GrpcMetricsCollector {
    metrics: Arc<RwLock<HashMap<MetricLabels, OperationMetrics>>>,
    enabled: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for GrpcMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl GrpcMetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            enabled: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    /// Enable or disable metrics collection
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if metrics collection is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record a gRPC operation
    pub async fn record_operation(&self, labels: MetricLabels, duration: Duration, is_error: bool) {
        if !self.is_enabled() {
            return;
        }

        let mut metrics = self.metrics.write().await;
        let operation_metrics = metrics.entry(labels.clone()).or_default();
        operation_metrics.record_request(duration, is_error);

        debug!(
            method = %labels.method,
            shard_id = ?labels.shard_id,
            namespace = %labels.namespace,
            duration_ms = duration.as_millis(),
            is_error = is_error,
            "Recorded gRPC operation metrics"
        );
    }

    /// Get a snapshot of all metrics
    pub async fn get_metrics(&self) -> HashMap<MetricLabels, OperationMetrics> {
        self.metrics.read().await.clone()
    }

    /// Get metrics for a specific operation
    pub async fn get_operation_metrics(&self, labels: &MetricLabels) -> Option<OperationMetrics> {
        self.metrics.read().await.get(labels).cloned()
    }

    /// Clear all metrics
    pub async fn clear_metrics(&self) {
        self.metrics.write().await.clear();
        info!("Cleared all gRPC metrics");
    }

    /// Print metrics summary to logs
    pub async fn log_metrics_summary(&self) {
        let metrics = self.get_metrics().await;

        if metrics.is_empty() {
            info!("No gRPC metrics collected yet");
            return;
        }

        info!("=== gRPC Metrics Summary ===");
        for (labels, metrics) in &metrics {
            let avg_duration = metrics
                .average_duration()
                .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "N/A".to_string());

            let min_duration = metrics
                .min_duration
                .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "N/A".to_string());

            let max_duration = metrics
                .max_duration
                .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "N/A".to_string());

            info!(
                "Method: {} | Shard: {:?} | Namespace: {} | Requests: {} | Errors: {} | Error Rate: {:.2}% | Avg: {} | Min: {} | Max: {}",
                labels.method,
                labels.shard_id,
                labels.namespace,
                metrics.total_requests,
                metrics.total_errors,
                metrics.error_rate() * 100.0,
                avg_duration,
                min_duration,
                max_duration
            );
        }
        info!("=== End Metrics Summary ===");
    }

    /// Export metrics in Prometheus format
    pub async fn export_prometheus_format(&self) -> String {
        let metrics = self.get_metrics().await;
        let mut output = String::new();

        // Help and type definitions
        output.push_str("# HELP oxia_grpc_requests_total Total number of gRPC requests\n");
        output.push_str("# TYPE oxia_grpc_requests_total counter\n");

        output.push_str(
            "# HELP oxia_grpc_request_errors_total Total number of gRPC request errors\n",
        );
        output.push_str("# TYPE oxia_grpc_request_errors_total counter\n");

        output.push_str("# HELP oxia_grpc_request_duration_seconds Request duration histogram\n");
        output.push_str("# TYPE oxia_grpc_request_duration_seconds histogram\n");

        for (labels, metrics) in &metrics {
            let shard_label = labels
                .shard_id
                .map(|id| format!(r#",shard_id="{}""#, id))
                .unwrap_or_default();

            let base_labels = format!(
                r#"method="{}",namespace="{}"{}"#,
                labels.method, labels.namespace, shard_label
            );

            // Total requests
            output.push_str(&format!(
                "oxia_grpc_requests_total{{{}}} {}\n",
                base_labels, metrics.total_requests
            ));

            // Total errors
            output.push_str(&format!(
                "oxia_grpc_request_errors_total{{{}}} {}\n",
                base_labels, metrics.total_errors
            ));

            // Histogram buckets
            for (bucket, count) in &metrics.histogram {
                output.push_str(&format!(
                    "oxia_grpc_request_duration_seconds_bucket{{{}},le=\"{}\"}} {}\n",
                    base_labels,
                    bucket.strip_prefix("le_").unwrap_or(bucket),
                    count
                ));
            }

            // Histogram sum and count
            output.push_str(&format!(
                "oxia_grpc_request_duration_seconds_sum{{{}}} {}\n",
                base_labels,
                metrics.total_duration.as_secs_f64()
            ));
            output.push_str(&format!(
                "oxia_grpc_request_duration_seconds_count{{{}}} {}\n",
                base_labels, metrics.total_requests
            ));
        }

        output
    }
}

/// Wrapper around a future that automatically records metrics
pub struct MetricsTimer {
    start: Instant,
    collector: Arc<GrpcMetricsCollector>,
    labels: MetricLabels,
}

impl MetricsTimer {
    /// Start timing a gRPC operation
    pub fn start(collector: Arc<GrpcMetricsCollector>, labels: MetricLabels) -> Self {
        Self {
            start: Instant::now(),
            collector,
            labels,
        }
    }

    /// Finish timing and record the result
    pub async fn finish(self, is_error: bool) {
        let duration = self.start.elapsed();
        self.collector
            .record_operation(self.labels, duration, is_error)
            .await;
    }

    /// Finish timing with a Result, automatically detecting errors
    pub async fn finish_with_result<T, E>(self, result: &Result<T, E>) {
        self.finish(result.is_err()).await;
    }
}

/// Global metrics collector instance
static GLOBAL_METRICS: Mutex<Option<Arc<GrpcMetricsCollector>>> = Mutex::new(None);

/// Initialize the global metrics collector
pub fn init_global_metrics() -> Arc<GrpcMetricsCollector> {
    let mut global = GLOBAL_METRICS.lock().unwrap();
    match &*global {
        Some(collector) => collector.clone(),
        None => {
            let collector = Arc::new(GrpcMetricsCollector::new());
            *global = Some(collector.clone());
            info!("Initialized global gRPC metrics collector");
            collector
        }
    }
}

/// Get the global metrics collector
pub fn global_metrics() -> Option<Arc<GrpcMetricsCollector>> {
    GLOBAL_METRICS.lock().unwrap().clone()
}

/// Convenience macro for timing gRPC operations
#[macro_export]
macro_rules! time_grpc_operation {
    ($collector:expr, $method:expr, $shard_id:expr, $namespace:expr, $operation:expr) => {{
        let labels = $crate::metrics::MetricLabels::new($method, $shard_id, $namespace);
        let timer = $crate::metrics::MetricsTimer::start($collector, labels);
        let result = $operation.await;
        timer.finish_with_result(&result).await;
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_operation_metrics() {
        let mut metrics = OperationMetrics::default();

        // Record some requests
        metrics.record_request(Duration::from_millis(100), false);
        metrics.record_request(Duration::from_millis(200), true);
        metrics.record_request(Duration::from_millis(50), false);

        assert_eq!(metrics.total_requests, 3);
        assert_eq!(metrics.total_errors, 1);
        assert_eq!(metrics.error_rate(), 1.0 / 3.0);
        assert_eq!(metrics.min_duration, Some(Duration::from_millis(50)));
        assert_eq!(metrics.max_duration, Some(Duration::from_millis(200)));
    }

    #[tokio::test]
    async fn test_metrics_collector() {
        let collector = GrpcMetricsCollector::new();
        let labels = MetricLabels::new("test_method", Some(1), "test_namespace");

        collector
            .record_operation(labels.clone(), Duration::from_millis(100), false)
            .await;
        collector
            .record_operation(labels.clone(), Duration::from_millis(200), true)
            .await;

        let metrics = collector.get_operation_metrics(&labels).await.unwrap();
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.total_errors, 1);
    }

    #[tokio::test]
    async fn test_metrics_timer() {
        let collector = Arc::new(GrpcMetricsCollector::new());
        let labels = MetricLabels::new("test_method", Some(1), "test_namespace");

        let timer = MetricsTimer::start(collector.clone(), labels.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;
        timer.finish(false).await;

        let metrics = collector.get_operation_metrics(&labels).await.unwrap();
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.total_errors, 0);
        assert!(metrics.min_duration.unwrap() >= Duration::from_millis(10));
    }

    #[test]
    fn test_global_metrics() {
        let collector1 = init_global_metrics();
        let collector2 = global_metrics().unwrap();

        // Should be the same instance
        assert!(Arc::ptr_eq(&collector1, &collector2));
    }
}
