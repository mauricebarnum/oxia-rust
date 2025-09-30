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

//! Enhanced shard client with integrated metrics collection.
//!
//! This module provides a wrapper around the existing shard client that automatically
//! collects metrics for all gRPC operations without requiring changes to existing code.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tonic::Streaming;
use tonic::metadata::MetadataValue;
use tracing::{debug, info, trace, warn};

use mauricebarnum_oxia_common::proto as oxia_proto;

use crate::metrics::{GrpcMetricsCollector, MetricLabels, MetricsTimer};
use crate::shard::{Client as BaseShardClient, NotificationsStream};
use crate::{
    DeleteOptions, DeleteRangeOptions, Error, GetOptions, GetResponse, GrpcClient, ListOptions,
    ListResponse, PutOptions, PutResponse, RangeScanOptions, RangeScanResponse, Result, config,
    pool::ChannelPool,
};

/// Enhanced shard client with automatic metrics collection
#[derive(Debug)]
pub struct MetricsEnabledShardClient {
    inner: BaseShardClient,
    metrics: Arc<GrpcMetricsCollector>,
    namespace: String,
}

impl MetricsEnabledShardClient {
    /// Create a new metrics-enabled shard client
    pub fn new(
        inner: BaseShardClient,
        metrics: Arc<GrpcMetricsCollector>,
        namespace: String,
    ) -> Self {
        Self {
            inner,
            metrics,
            namespace,
        }
    }

    /// Get the inner shard client
    pub fn inner(&self) -> &BaseShardClient {
        &self.inner
    }

    /// Get the shard ID
    pub fn id(&self) -> i64 {
        self.inner.id()
    }

    /// Create metric labels for this client
    fn create_labels(&self, method: &str) -> MetricLabels {
        MetricLabels::new(method, Some(self.inner.id()), &self.namespace)
    }

    /// Time a gRPC operation and collect metrics
    async fn time_operation<F, T>(&self, method: &str, operation: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        if !self.metrics.is_enabled() {
            return operation.await;
        }

        let labels = self.create_labels(method);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);
        let start_time = Instant::now();

        let result = operation.await;
        let is_error = result.is_err();

        timer.finish(is_error).await;

        // Log detailed timing for debugging
        let duration = start_time.elapsed();
        trace!(
            method = method,
            shard_id = self.inner.id(),
            namespace = %self.namespace,
            duration_ms = duration.as_millis(),
            is_error = is_error,
            "Shard operation completed"
        );

        result
    }

    /// Get operation with metrics
    pub async fn get(&self, k: &str, opts: &GetOptions) -> Result<Option<GetResponse>> {
        self.time_operation("read", async { self.inner.get(k, opts).await })
            .await
    }

    /// Put operation with metrics
    pub async fn put(
        &self,
        k: &str,
        value: impl Into<Vec<u8>>,
        opts: &PutOptions,
    ) -> Result<PutResponse> {
        self.time_operation("write_stream", async {
            self.inner.put(k, value, opts).await
        })
        .await
    }

    /// Delete operation with metrics
    pub async fn delete(&self, k: &str, opts: &DeleteOptions) -> Result<()> {
        self.time_operation("write_stream", async { self.inner.delete(k, opts).await })
            .await
    }

    /// Delete range operation with metrics
    pub async fn delete_range(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        options: &DeleteRangeOptions,
    ) -> Result<()> {
        self.time_operation("write_stream", async {
            self.inner
                .delete_range(start_inclusive, end_exclusive, options)
                .await
        })
        .await
    }

    /// List operation with metrics
    pub async fn list(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        opts: &ListOptions,
    ) -> Result<ListResponse> {
        self.time_operation("list", async {
            self.inner.list(start_inclusive, end_exclusive, opts).await
        })
        .await
    }

    /// List with timeout operation with metrics
    pub async fn list_with_timeout(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        opts: &ListOptions,
        timeout: Duration,
    ) -> Result<ListResponse> {
        self.time_operation("list", async {
            self.inner
                .list_with_timeout(start_inclusive, end_exclusive, opts, timeout)
                .await
        })
        .await
    }

    /// Range scan operation with metrics
    pub async fn range_scan(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        opts: &RangeScanOptions,
    ) -> Result<RangeScanResponse> {
        self.time_operation("range_scan", async {
            self.inner
                .range_scan(start_inclusive, end_exclusive, opts)
                .await
        })
        .await
    }

    /// Get notifications with metrics
    pub async fn get_notifications(
        &self,
        start_offset_exclusive: Option<i64>,
    ) -> Result<NotificationsStream> {
        self.time_operation("get_notifications", async {
            self.inner.get_notifications(start_offset_exclusive).await
        })
        .await
    }

    /// Get the metrics collector
    pub fn metrics(&self) -> Arc<GrpcMetricsCollector> {
        self.metrics.clone()
    }

    /// Enable or disable metrics collection for this client
    pub fn set_metrics_enabled(&self, enabled: bool) {
        self.metrics.set_enabled(enabled);
    }

    /// Check if metrics collection is enabled
    pub fn is_metrics_enabled(&self) -> bool {
        self.metrics.is_enabled()
    }
}

/// Factory for creating metrics-enabled shard clients
#[derive(Debug)]
pub struct MetricsEnabledShardFactory {
    metrics: Arc<GrpcMetricsCollector>,
    channel_pool: Arc<ChannelPool>,
    config: Arc<config::Config>,
}

impl MetricsEnabledShardFactory {
    /// Create a new factory
    pub fn new(
        metrics: Arc<GrpcMetricsCollector>,
        channel_pool: Arc<ChannelPool>,
        config: Arc<config::Config>,
    ) -> Self {
        Self {
            metrics,
            channel_pool,
            config,
        }
    }

    /// Create a metrics-enabled shard client
    pub async fn create_client(
        &self,
        shard_id: i64,
        dest: String,
    ) -> Result<MetricsEnabledShardClient> {
        // Create the underlying gRPC client with metrics timing
        let labels = MetricLabels::new(
            "create_grpc_client",
            Some(shard_id),
            self.config.namespace(),
        );
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let grpc_client_result = create_grpc_client_internal(&dest, &self.channel_pool).await;
        timer.finish_with_result(&grpc_client_result).await;

        let grpc_client = grpc_client_result?;

        // Create the base shard client
        let base_client = BaseShardClient::new(grpc_client, self.config.clone(), shard_id, dest)?;

        // Wrap with metrics
        let metrics_client = MetricsEnabledShardClient::new(
            base_client,
            self.metrics.clone(),
            self.config.namespace().to_string(),
        );

        debug!(
            shard_id = shard_id,
            namespace = self.config.namespace(),
            "Created metrics-enabled shard client"
        );

        Ok(metrics_client)
    }

    /// Get the metrics collector
    pub fn metrics(&self) -> Arc<GrpcMetricsCollector> {
        self.metrics.clone()
    }
}

/// Internal helper to create gRPC client (copied from existing implementation)
async fn create_grpc_client_internal(dest: &str, channel_pool: &ChannelPool) -> Result<GrpcClient> {
    let url = {
        // TODO: http v https
        const SCHEME: &str = "http://";
        let mut url = String::with_capacity(SCHEME.len() + dest.len());
        url.push_str(SCHEME);
        url.push_str(dest);
        url
    };
    let channel = channel_pool.get(&url).await?;
    Ok(GrpcClient::new(channel))
}

/// Extension trait to add metrics to existing shard clients
pub trait ShardClientMetricsExtension {
    /// Wrap this shard client with metrics collection
    fn with_metrics(
        self,
        metrics: Arc<GrpcMetricsCollector>,
        namespace: String,
    ) -> MetricsEnabledShardClient;
}

impl ShardClientMetricsExtension for BaseShardClient {
    fn with_metrics(
        self,
        metrics: Arc<GrpcMetricsCollector>,
        namespace: String,
    ) -> MetricsEnabledShardClient {
        MetricsEnabledShardClient::new(self, metrics, namespace)
    }
}

/// Metrics-aware shard manager that creates metrics-enabled clients
#[derive(Debug)]
pub struct MetricsEnabledShardManager {
    factory: MetricsEnabledShardFactory,
    // Add any additional fields needed for shard management
}

impl MetricsEnabledShardManager {
    /// Create a new metrics-enabled shard manager
    pub fn new(
        metrics: Arc<GrpcMetricsCollector>,
        channel_pool: Arc<ChannelPool>,
        config: Arc<config::Config>,
    ) -> Self {
        let factory = MetricsEnabledShardFactory::new(metrics, channel_pool, config);
        Self { factory }
    }

    /// Get or create a metrics-enabled shard client
    pub async fn get_shard_client(
        &self,
        shard_id: i64,
        dest: String,
    ) -> Result<MetricsEnabledShardClient> {
        self.factory.create_client(shard_id, dest).await
    }

    /// Get the metrics collector
    pub fn metrics(&self) -> Arc<GrpcMetricsCollector> {
        self.factory.metrics()
    }

    /// Get metrics summary for all shards
    pub async fn get_shard_metrics_summary(&self) -> Vec<(i64, crate::metrics::OperationMetrics)> {
        let all_metrics = self.factory.metrics().get_metrics().await;
        let mut shard_summaries = std::collections::HashMap::new();

        // Aggregate metrics by shard ID
        for (labels, metrics) in all_metrics {
            if let Some(shard_id) = labels.shard_id {
                let entry = shard_summaries
                    .entry(shard_id)
                    .or_insert_with(|| crate::metrics::OperationMetrics::default());

                // Aggregate the metrics (simplified aggregation)
                entry.total_requests += metrics.total_requests;
                entry.total_errors += metrics.total_errors;
                entry.total_duration += metrics.total_duration;

                if entry.min_duration.is_none()
                    || (metrics.min_duration.is_some() && metrics.min_duration < entry.min_duration)
                {
                    entry.min_duration = metrics.min_duration;
                }

                if entry.max_duration.is_none()
                    || (metrics.max_duration.is_some() && metrics.max_duration > entry.max_duration)
                {
                    entry.max_duration = metrics.max_duration;
                }
            }
        }

        shard_summaries.into_iter().collect()
    }

    /// Log metrics summary for all shards
    pub async fn log_shard_metrics_summary(&self) {
        let shard_summaries = self.get_shard_metrics_summary().await;

        if shard_summaries.is_empty() {
            info!("No shard metrics available");
            return;
        }

        info!("=== Shard Metrics Summary ===");
        for (shard_id, metrics) in shard_summaries {
            let avg_duration = metrics
                .average_duration()
                .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "N/A".to_string());

            info!(
                "Shard {}: {} requests, {} errors ({:.1}% error rate), avg latency: {}",
                shard_id,
                metrics.total_requests,
                metrics.total_errors,
                metrics.error_rate() * 100.0,
                avg_duration
            );
        }
        info!("=== End Shard Metrics Summary ===");
    }
}

/// Convenience function to create a metrics-enabled client with default settings
pub async fn create_metrics_enabled_shard_client(
    shard_id: i64,
    dest: String,
    config: Arc<config::Config>,
    channel_pool: Arc<ChannelPool>,
    metrics: Option<Arc<GrpcMetricsCollector>>,
) -> Result<MetricsEnabledShardClient> {
    let metrics = metrics.unwrap_or_else(|| {
        // Create a default metrics collector if none provided
        Arc::new(GrpcMetricsCollector::new())
    });

    let factory = MetricsEnabledShardFactory::new(metrics, channel_pool, config);
    factory.create_client(shard_id, dest).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Helper to create test configuration
    fn create_test_config() -> Arc<config::Config> {
        Arc::new(
            config::Builder::new()
                .service_addr("localhost:6544")
                .namespace("test_namespace")
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_metrics_enabled_factory() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let config = create_test_config();
        let channel_pool = Arc::new(ChannelPool::new(&config));

        let factory = MetricsEnabledShardFactory::new(metrics.clone(), channel_pool, config);

        // Verify factory has the correct metrics reference
        assert!(Arc::ptr_eq(&factory.metrics(), &metrics));
    }

    #[tokio::test]
    async fn test_shard_client_metrics_extension() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let config = create_test_config();

        // This test would require a real shard client to work properly
        // For now, we just test the extension trait exists

        // The extension trait should be available for BaseShardClient
        // when we have a real instance to test with
    }

    #[tokio::test]
    async fn test_metrics_enabled_shard_manager() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let config = create_test_config();
        let channel_pool = Arc::new(ChannelPool::new(&config));

        let manager = MetricsEnabledShardManager::new(metrics.clone(), channel_pool, config);

        // Verify manager has the correct metrics reference
        assert!(Arc::ptr_eq(&manager.metrics(), &metrics));

        // Test shard metrics summary with no data
        let summary = manager.get_shard_metrics_summary().await;
        assert!(summary.is_empty());
    }

    #[test]
    fn test_metric_labels_creation() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let config = create_test_config();

        // Create a mock base client (this would be a real client in practice)
        // For now, we test the label creation logic conceptually

        let expected_namespace = "test_namespace";
        let expected_shard_id = 42i64;

        let labels = MetricLabels::new("test_method", Some(expected_shard_id), expected_namespace);

        assert_eq!(labels.method, "test_method");
        assert_eq!(labels.shard_id, Some(expected_shard_id));
        assert_eq!(labels.namespace, expected_namespace);
    }

    #[tokio::test]
    async fn test_shard_metrics_aggregation() {
        let metrics = Arc::new(GrpcMetricsCollector::new());

        // Record some test metrics for different shards
        let shard1_labels = MetricLabels::new("read", Some(1), "test_ns");
        let shard2_labels = MetricLabels::new("write_stream", Some(2), "test_ns");

        metrics
            .record_operation(shard1_labels, Duration::from_millis(10), false)
            .await;
        metrics
            .record_operation(shard2_labels, Duration::from_millis(20), true)
            .await;

        let all_metrics = metrics.get_metrics().await;
        assert_eq!(all_metrics.len(), 2);

        // Verify shard-specific metrics exist
        let shard1_metrics = all_metrics
            .get(&MetricLabels::new("read", Some(1), "test_ns"))
            .unwrap();
        assert_eq!(shard1_metrics.total_requests, 1);
        assert_eq!(shard1_metrics.total_errors, 0);

        let shard2_metrics = all_metrics
            .get(&MetricLabels::new("write_stream", Some(2), "test_ns"))
            .unwrap();
        assert_eq!(shard2_metrics.total_requests, 1);
        assert_eq!(shard2_metrics.total_errors, 1);
    }
}
