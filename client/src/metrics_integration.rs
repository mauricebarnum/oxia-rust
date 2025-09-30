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

//! Utilities for integrating gRPC metrics into existing Oxia client code.
//!
//! This module provides convenience functions and utilities to easily add
//! metrics collection to existing gRPC operations without major code changes.

use std::sync::Arc;
use std::time::Instant;

use tonic::transport::Channel;
use tracing::{info, warn};

use crate::instrumented_client::{InstrumentedClientFactory, InstrumentedGrpcClient};
use crate::metrics::{GrpcMetricsCollector, MetricLabels, MetricsTimer};
use crate::pool::ChannelPool;
use crate::{GrpcClient, Result};

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Whether metrics collection is enabled
    pub enabled: bool,
    /// Whether to log metrics summaries periodically
    pub log_summaries: bool,
    /// Interval for logging metrics summaries (in seconds)
    pub summary_interval_secs: u64,
    /// Whether to export Prometheus format metrics
    pub enable_prometheus_export: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_summaries: true,
            summary_interval_secs: 300, // 5 minutes
            enable_prometheus_export: false,
        }
    }
}

/// Enhanced gRPC client factory that creates instrumented clients
pub struct MetricsEnabledClientFactory {
    metrics: Arc<GrpcMetricsCollector>,
    config: MetricsConfig,
    factory: InstrumentedClientFactory,
}

impl MetricsEnabledClientFactory {
    /// Create a new metrics-enabled client factory
    pub fn new(config: MetricsConfig) -> Self {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        metrics.set_enabled(config.enabled);

        let factory = InstrumentedClientFactory::new(metrics.clone());

        let instance = Self {
            metrics: metrics.clone(),
            config,
            factory,
        };

        // Start periodic metrics logging if enabled
        if config.log_summaries {
            instance.start_periodic_logging();
        }

        instance
    }

    /// Get the metrics collector
    pub fn metrics(&self) -> Arc<GrpcMetricsCollector> {
        self.metrics.clone()
    }

    /// Create an instrumented gRPC client
    pub fn create_grpc_client(
        &self,
        channel: Channel,
        namespace: String,
    ) -> InstrumentedGrpcClient {
        self.factory.create_client(channel, namespace)
    }

    /// Start periodic metrics logging
    fn start_periodic_logging(&self) {
        let metrics = self.metrics.clone();
        let interval = self.config.summary_interval_secs;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval));
            loop {
                interval.tick().await;
                metrics.log_metrics_summary().await;
            }
        });
    }

    /// Export metrics in Prometheus format
    pub async fn export_prometheus(&self) -> String {
        self.metrics.export_prometheus_format().await
    }

    /// Clear all collected metrics
    pub async fn clear_metrics(&self) {
        self.metrics.clear_metrics().await;
    }
}

/// Drop-in replacement for the existing create_grpc_client function with metrics
pub async fn create_grpc_client_with_metrics(
    dest: &str,
    channel_pool: &ChannelPool,
    metrics: Arc<GrpcMetricsCollector>,
    namespace: String,
) -> Result<InstrumentedGrpcClient> {
    let url = {
        const SCHEME: &str = "http://";
        let mut url = String::with_capacity(SCHEME.len() + dest.len());
        url.push_str(SCHEME);
        url.push_str(dest);
        url
    };

    let channel = channel_pool.get(&url).await?;
    let inner = crate::GrpcClient::new(channel);

    Ok(InstrumentedGrpcClient::new(inner, metrics, namespace))
}

/// Metrics middleware for timing individual operations
pub struct OperationTimer {
    timer: Option<MetricsTimer>,
}

impl OperationTimer {
    /// Create a new operation timer
    pub fn new(
        metrics: Arc<GrpcMetricsCollector>,
        method: &str,
        shard_id: Option<i64>,
        namespace: &str,
    ) -> Self {
        let labels = MetricLabels::new(method, shard_id, namespace);
        let timer = MetricsTimer::start(metrics, labels);
        Self { timer: Some(timer) }
    }

    /// Create a no-op timer (when metrics are disabled)
    pub fn noop() -> Self {
        Self { timer: None }
    }

    /// Finish timing the operation
    pub async fn finish(mut self, is_error: bool) {
        if let Some(timer) = self.timer.take() {
            timer.finish(is_error).await;
        }
    }

    /// Finish timing with automatic error detection
    pub async fn finish_with_result<T, E>(mut self, result: &Result<T, E>) {
        if let Some(timer) = self.timer.take() {
            timer.finish_with_result(result).await;
        }
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            let rt = tokio::runtime::Handle::try_current();
            if let Ok(handle) = rt {
                handle.spawn(async move {
                    // Mark as error since we're dropping without explicit finish
                    timer.finish(true).await;
                });
            } else {
                warn!("Failed to finish metrics timer - no tokio runtime available");
            }
        }
    }
}

/// Helper macro for easily timing gRPC operations in existing code
#[macro_export]
macro_rules! with_grpc_metrics {
    ($metrics:expr, $method:expr, $shard_id:expr, $namespace:expr, $operation:block) => {{
        let timer = if let Some(metrics) = $metrics.as_ref() {
            $crate::metrics_integration::OperationTimer::new(
                metrics.clone(),
                $method,
                $shard_id,
                $namespace,
            )
        } else {
            $crate::metrics_integration::OperationTimer::noop()
        };

        let result = $operation;
        timer.finish_with_result(&result).await;
        result
    }};
}

/// Utility for getting global metrics instance
pub fn get_or_init_global_metrics() -> Arc<GrpcMetricsCollector> {
    crate::metrics::init_global_metrics()
}

/// Simple builder for metrics configuration
pub struct MetricsConfigBuilder {
    config: MetricsConfig,
}

impl MetricsConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: MetricsConfig::default(),
        }
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    pub fn log_summaries(mut self, log_summaries: bool) -> Self {
        self.config.log_summaries = log_summaries;
        self
    }

    pub fn summary_interval_secs(mut self, interval: u64) -> Self {
        self.config.summary_interval_secs = interval;
        self
    }

    pub fn enable_prometheus_export(mut self, enable: bool) -> Self {
        self.config.enable_prometheus_export = enable;
        self
    }

    pub fn build(self) -> MetricsConfig {
        self.config
    }
}

impl Default for MetricsConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP endpoint handler for Prometheus metrics export
#[cfg(feature = "prometheus")]
pub async fn prometheus_metrics_handler(
    metrics: Arc<GrpcMetricsCollector>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    Ok(metrics.export_prometheus_format().await)
}

/// Convenience function to setup metrics collection for the entire client
pub fn setup_client_metrics(config: MetricsConfig) -> Arc<GrpcMetricsCollector> {
    let collector = Arc::new(GrpcMetricsCollector::new());
    collector.set_enabled(config.enabled);

    if config.log_summaries {
        let metrics_clone = collector.clone();
        let interval = config.summary_interval_secs;
        tokio::spawn(async move {
            let mut interval_timer =
                tokio::time::interval(std::time::Duration::from_secs(interval));
            loop {
                interval_timer.tick().await;
                metrics_clone.log_metrics_summary().await;
            }
        });
    }

    info!(
        enabled = config.enabled,
        log_summaries = config.log_summaries,
        summary_interval_secs = config.summary_interval_secs,
        prometheus_export = config.enable_prometheus_export,
        "gRPC metrics collection configured"
    );

    collector
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics_config_builder() {
        let config = MetricsConfigBuilder::new()
            .enabled(true)
            .log_summaries(false)
            .summary_interval_secs(60)
            .enable_prometheus_export(true)
            .build();

        assert!(config.enabled);
        assert!(!config.log_summaries);
        assert_eq!(config.summary_interval_secs, 60);
        assert!(config.enable_prometheus_export);
    }

    #[test]
    fn test_default_metrics_config() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert!(config.log_summaries);
        assert_eq!(config.summary_interval_secs, 300);
        assert!(!config.enable_prometheus_export);
    }

    #[tokio::test]
    async fn test_operation_timer_noop() {
        let timer = OperationTimer::noop();
        timer.finish(false).await; // Should not panic
    }

    #[tokio::test]
    async fn test_operation_timer_with_metrics() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let timer = OperationTimer::new(metrics.clone(), "test_method", Some(1), "test_ns");

        tokio::time::sleep(Duration::from_millis(10)).await;
        timer.finish(false).await;

        let labels = MetricLabels::new("test_method", Some(1), "test_ns");
        let op_metrics = metrics.get_operation_metrics(&labels).await.unwrap();
        assert_eq!(op_metrics.total_requests, 1);
        assert_eq!(op_metrics.total_errors, 0);
    }

    #[test]
    fn test_metrics_enabled_factory() {
        let config = MetricsConfig::default();
        let factory = MetricsEnabledClientFactory::new(config);

        assert!(factory.metrics().is_enabled());
    }
}
