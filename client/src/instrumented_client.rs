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

//! Instrumented gRPC client wrapper that automatically collects latency metrics.
//!
//! This module provides a wrapper around the generated gRPC client that transparently
//! collects metrics for all gRPC operations without requiring changes to existing code.

use std::sync::Arc;
use std::time::Instant;

use futures::Stream;
use tonic::transport::Channel;
use tonic::{Request, Response, Streaming};

use mauricebarnum_oxia_common::proto as oxia_proto;

use crate::metrics::{GrpcMetricsCollector, MetricLabels, MetricsTimer};
use crate::{Error, Result};

/// Instrumented wrapper around the gRPC client that collects metrics
#[derive(Debug, Clone)]
pub struct InstrumentedGrpcClient {
    inner: oxia_proto::OxiaClientClient<Channel>,
    metrics: Arc<GrpcMetricsCollector>,
    namespace: String,
}

impl InstrumentedGrpcClient {
    /// Create a new instrumented client
    pub fn new(
        inner: oxia_proto::OxiaClientClient<Channel>,
        metrics: Arc<GrpcMetricsCollector>,
        namespace: String,
    ) -> Self {
        Self {
            inner,
            metrics,
            namespace,
        }
    }

    /// Get the inner client (for cases where direct access is needed)
    pub fn inner(&self) -> &oxia_proto::OxiaClientClient<Channel> {
        &self.inner
    }

    /// Extract shard ID from metadata in a request
    fn extract_shard_id<T>(request: &Request<T>) -> Option<i64> {
        request
            .metadata()
            .get("shard-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    /// Create metric labels for a gRPC operation
    fn create_labels(&self, method: &str, shard_id: Option<i64>) -> MetricLabels {
        MetricLabels::new(method, shard_id, &self.namespace)
    }

    /// Time a gRPC operation and collect metrics
    async fn time_operation<F, T>(
        &self,
        method: &str,
        shard_id: Option<i64>,
        operation: F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T, tonic::Status>>,
    {
        let labels = self.create_labels(method, shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);
        let start = Instant::now();

        let result = operation.await;
        let is_error = result.is_err();

        // Convert tonic::Status to our Error type if needed
        let converted_result = result.map_err(|e| Error::from(e));

        timer.finish(is_error).await;

        // Log detailed timing for debugging
        let duration = start.elapsed();
        tracing::debug!(
            method = method,
            shard_id = ?shard_id,
            duration_ms = duration.as_millis(),
            is_error = is_error,
            "gRPC operation completed"
        );

        converted_result
    }

    /// Instrumented create_session call
    pub async fn create_session(
        &mut self,
        request: Request<oxia_proto::CreateSessionRequest>,
    ) -> Result<Response<oxia_proto::CreateSessionResponse>, tonic::Status> {
        let shard_id = Self::extract_shard_id(&request);
        let labels = self.create_labels("create_session", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let result = self.inner.create_session(request).await;
        timer.finish_with_result(&result).await;

        result
    }

    /// Instrumented keep_alive call
    pub async fn keep_alive(
        &mut self,
        request: Request<oxia_proto::SessionHeartbeat>,
    ) -> Result<Response<oxia_proto::SessionHeartbeatResponse>, tonic::Status> {
        // Extract shard from the request payload
        let shard_id = Some(request.get_ref().shard);
        let labels = self.create_labels("keep_alive", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let result = self.inner.keep_alive(request).await;
        timer.finish_with_result(&result).await;

        result
    }

    /// Instrumented read call
    pub async fn read(
        &mut self,
        request: Request<oxia_proto::ReadRequest>,
    ) -> Result<Response<Streaming<oxia_proto::ReadResponse>>, tonic::Status> {
        let shard_id = request.get_ref().shard;
        let labels = self.create_labels("read", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let result = self.inner.read(request).await;
        timer.finish_with_result(&result).await;

        result
    }

    /// Instrumented write_stream call
    pub async fn write_stream<S>(
        &mut self,
        request: Request<S>,
    ) -> Result<Response<Streaming<oxia_proto::WriteResponse>>, tonic::Status>
    where
        S: Stream<Item = oxia_proto::WriteRequest> + Send + 'static,
    {
        // For streaming requests, we'll use None for shard_id initially
        // as we can't easily inspect the stream without consuming it
        let shard_id = None;
        let labels = self.create_labels("write_stream", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let result = self.inner.write_stream(request).await;
        timer.finish_with_result(&result).await;

        result
    }

    /// Instrumented list call
    pub async fn list(
        &mut self,
        request: Request<oxia_proto::ListRequest>,
    ) -> Result<Response<Streaming<oxia_proto::ListResponse>>, tonic::Status> {
        let shard_id = request.get_ref().shard;
        let labels = self.create_labels("list", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let result = self.inner.list(request).await;
        timer.finish_with_result(&result).await;

        result
    }

    /// Instrumented range_scan call
    pub async fn range_scan(
        &mut self,
        request: Request<oxia_proto::RangeScanRequest>,
    ) -> Result<Response<Streaming<oxia_proto::RangeScanResponse>>, tonic::Status> {
        let shard_id = request.get_ref().shard;
        let labels = self.create_labels("range_scan", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let result = self.inner.range_scan(request).await;
        timer.finish_with_result(&result).await;

        result
    }

    /// Instrumented get_notifications call
    pub async fn get_notifications(
        &mut self,
        request: oxia_proto::NotificationsRequest,
    ) -> Result<Response<Streaming<oxia_proto::NotificationBatch>>, tonic::Status> {
        let shard_id = Some(request.shard);
        let labels = self.create_labels("get_notifications", shard_id);
        let timer = MetricsTimer::start(self.metrics.clone(), labels);

        let request = Request::new(request);
        let result = self.inner.get_notifications(request).await;
        timer.finish_with_result(&result).await;

        result
    }
}

/// Factory for creating instrumented gRPC clients
pub struct InstrumentedClientFactory {
    metrics: Arc<GrpcMetricsCollector>,
}

impl InstrumentedClientFactory {
    /// Create a new factory with the given metrics collector
    pub fn new(metrics: Arc<GrpcMetricsCollector>) -> Self {
        Self { metrics }
    }

    /// Create an instrumented client from a channel
    pub fn create_client(&self, channel: Channel, namespace: String) -> InstrumentedGrpcClient {
        let inner = oxia_proto::OxiaClientClient::new(channel);
        InstrumentedGrpcClient::new(inner, self.metrics.clone(), namespace)
    }
}

/// Extension trait to add instrumentation to existing gRPC clients
pub trait GrpcClientInstrumentation {
    /// Wrap this client with instrumentation
    fn with_metrics(
        self,
        metrics: Arc<GrpcMetricsCollector>,
        namespace: String,
    ) -> InstrumentedGrpcClient;
}

impl GrpcClientInstrumentation for oxia_proto::OxiaClientClient<Channel> {
    fn with_metrics(
        self,
        metrics: Arc<GrpcMetricsCollector>,
        namespace: String,
    ) -> InstrumentedGrpcClient {
        InstrumentedGrpcClient::new(self, metrics, namespace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_stream::wrappers::ReceiverStream;

    // Mock helper for testing
    async fn create_test_client() -> InstrumentedGrpcClient {
        let metrics = Arc::new(GrpcMetricsCollector::new());

        // This would normally be created from a real channel
        // For testing, we'll create a mock scenario
        let channel = Channel::from_static("http://localhost:50051");
        let inner = oxia_proto::OxiaClientClient::new(channel);

        InstrumentedGrpcClient::new(inner, metrics, "test_namespace".to_string())
    }

    #[test]
    fn test_create_labels() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let channel = Channel::from_static("http://localhost:50051");
        let inner = oxia_proto::OxiaClientClient::new(channel);
        let client = InstrumentedGrpcClient::new(inner, metrics, "test_ns".to_string());

        let labels = client.create_labels("test_method", Some(42));
        assert_eq!(labels.method, "test_method");
        assert_eq!(labels.shard_id, Some(42));
        assert_eq!(labels.namespace, "test_ns");
    }

    #[test]
    fn test_extract_shard_id() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("shard-id", "123".parse().unwrap());

        let shard_id = InstrumentedGrpcClient::extract_shard_id(&request);
        assert_eq!(shard_id, Some(123));
    }

    #[test]
    fn test_extract_shard_id_invalid() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("shard-id", "invalid".parse().unwrap());

        let shard_id = InstrumentedGrpcClient::extract_shard_id(&request);
        assert_eq!(shard_id, None);
    }

    #[test]
    fn test_factory() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let factory = InstrumentedClientFactory::new(metrics.clone());

        let channel = Channel::from_static("http://localhost:50051");
        let client = factory.create_client(channel, "test_ns".to_string());

        assert_eq!(client.namespace, "test_ns");
        assert!(Arc::ptr_eq(&client.metrics, &metrics));
    }

    #[test]
    fn test_extension_trait() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let channel = Channel::from_static("http://localhost:50051");
        let inner = oxia_proto::OxiaClientClient::new(channel);

        let instrumented = inner.with_metrics(metrics.clone(), "test_ns".to_string());

        assert_eq!(instrumented.namespace, "test_ns");
        assert!(Arc::ptr_eq(&instrumented.metrics, &metrics));
    }
}
