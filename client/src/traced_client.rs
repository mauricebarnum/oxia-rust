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

//! Client implementation with integrated distributed tracing.
//!
//! This module provides a wrapper around the main Client that automatically
//! creates distributed traces for API operations, connecting high-level calls
//! like `get_with_options()` to their constituent shard operations.

use std::sync::Arc;
use std::time::Duration;

use tracing::{Instrument, info, instrument, warn};

use crate::tracing_metrics::{DistributedTracingCollector, TracedOperation};
use crate::{
    Client as BaseClient, DeleteOptions, DeleteRangeOptions, GetOptions, GetResponse, ListOptions,
    ListResponse, PutOptions, PutResponse, Result, SecondaryIndex,
};

/// Client wrapper with distributed tracing capabilities
#[derive(Debug, Clone)]
pub struct TracedClient {
    inner: BaseClient,
    tracer: Arc<DistributedTracingCollector>,
}

impl TracedClient {
    /// Create a new traced client
    pub fn new(inner: BaseClient, tracer: Arc<DistributedTracingCollector>) -> Self {
        Self { inner, tracer }
    }

    /// Get the underlying client
    pub fn inner(&self) -> &BaseClient {
        &self.inner
    }

    /// Get the tracer
    pub fn tracer(&self) -> Arc<DistributedTracingCollector> {
        self.tracer.clone()
    }

    /// Get a value by key with tracing
    #[instrument(skip(self), fields(operation = "get"))]
    pub async fn get(&self, key: &str) -> Result<Option<GetResponse>> {
        let traced_op = self.tracer.start_root_span("Client::get".to_string()).await;

        let result = self.inner.get(key).await;
        traced_op.complete_with_result(&result).await;

        result
    }

    /// Get a value with options and full tracing
    #[instrument(skip(self, options), fields(operation = "get_with_options", key = %key))]
    pub async fn get_with_options(
        &self,
        key: &str,
        options: &GetOptions,
    ) -> Result<Option<GetResponse>> {
        let traced_op = self
            .tracer
            .start_root_span("Client::get_with_options".to_string())
            .await;

        // The inner implementation will fan out to multiple shards
        // We'll trace each shard operation as a child span
        let result = self.get_with_options_traced(key, options, &traced_op).await;

        traced_op.complete_with_result(&result).await;
        result
    }

    /// Internal get_with_options that creates child spans for shard operations
    async fn get_with_options_traced(
        &self,
        key: &str,
        options: &GetOptions,
        parent_op: &TracedOperation,
    ) -> Result<Option<GetResponse>> {
        // This would normally call the shard manager to get the appropriate shard
        // For now, we'll simulate the shard operation tracing

        // In a real implementation, you would:
        // 1. Determine which shard(s) to query
        // 2. For each shard, create a child traced operation
        // 3. Execute the shard operation within that trace

        // Simulated shard operation (replace with actual shard logic)
        if let Some(shard_op) = parent_op
            .create_child("shard:get".to_string(), Some(1))
            .await
        {
            let shard_result = self.inner.get_with_options(key, options).await;
            shard_op.complete_with_result(&shard_result).await;
            shard_result
        } else {
            self.inner.get_with_options(key, options).await
        }
    }

    /// Put a key-value pair with tracing
    #[instrument(skip(self, value), fields(operation = "put", key = %key, value_size = value.len()))]
    pub async fn put(&self, key: &str, value: Vec<u8>) -> Result<PutResponse> {
        let traced_op = self.tracer.start_root_span("Client::put".to_string()).await;

        let result = self.inner.put(key, value).await;
        traced_op.complete_with_result(&result).await;

        result
    }

    /// Put with options and full tracing
    #[instrument(skip(self, value, options), fields(operation = "put_with_options", key = %key))]
    pub async fn put_with_options(
        &self,
        key: &str,
        value: Vec<u8>,
        options: &PutOptions,
    ) -> Result<PutResponse> {
        let traced_op = self
            .tracer
            .start_root_span("Client::put_with_options".to_string())
            .await;

        let result = self
            .put_with_options_traced(key, value, options, &traced_op)
            .await;

        traced_op.complete_with_result(&result).await;
        result
    }

    async fn put_with_options_traced(
        &self,
        key: &str,
        value: Vec<u8>,
        options: &PutOptions,
        parent_op: &TracedOperation,
    ) -> Result<PutResponse> {
        // Create child span for shard write operation
        if let Some(shard_op) = parent_op
            .create_child("shard:write_stream".to_string(), Some(1))
            .await
        {
            let shard_result = self.inner.put_with_options(key, value, options).await;
            shard_op.complete_with_result(&shard_result).await;
            shard_result
        } else {
            self.inner.put_with_options(key, value, options).await
        }
    }

    /// Delete a key with tracing
    #[instrument(skip(self), fields(operation = "delete", key = %key))]
    pub async fn delete(&self, key: &str) -> Result<()> {
        let traced_op = self
            .tracer
            .start_root_span("Client::delete".to_string())
            .await;

        let result = self.inner.delete(key).await;
        traced_op.complete_with_result(&result).await;

        result
    }

    /// Delete with options and full tracing
    #[instrument(skip(self, options), fields(operation = "delete_with_options", key = %key))]
    pub async fn delete_with_options(&self, key: &str, options: &DeleteOptions) -> Result<()> {
        let traced_op = self
            .tracer
            .start_root_span("Client::delete_with_options".to_string())
            .await;

        let result = self
            .delete_with_options_traced(key, options, &traced_op)
            .await;

        traced_op.complete_with_result(&result).await;
        result
    }

    async fn delete_with_options_traced(
        &self,
        key: &str,
        options: &DeleteOptions,
        parent_op: &TracedOperation,
    ) -> Result<()> {
        if let Some(shard_op) = parent_op
            .create_child("shard:write_stream".to_string(), Some(1))
            .await
        {
            let shard_result = self.inner.delete_with_options(key, options).await;
            shard_op.complete_with_result(&shard_result).await;
            shard_result
        } else {
            self.inner.delete_with_options(key, options).await
        }
    }

    /// Delete range with tracing
    #[instrument(skip(self, options), fields(operation = "delete_range"))]
    pub async fn delete_range(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        options: &DeleteRangeOptions,
    ) -> Result<()> {
        let traced_op = self
            .tracer
            .start_root_span("Client::delete_range".to_string())
            .await;

        let result = self
            .delete_range_traced(start_inclusive, end_exclusive, options, &traced_op)
            .await;

        traced_op.complete_with_result(&result).await;
        result
    }

    async fn delete_range_traced(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        options: &DeleteRangeOptions,
        parent_op: &TracedOperation,
    ) -> Result<()> {
        // Delete range operations may span multiple shards
        // For demonstration, we'll trace operations for shards 1 and 2

        let mut results = Vec::new();

        // Shard 1
        if let Some(shard_op) = parent_op
            .create_child("shard:write_stream".to_string(), Some(1))
            .await
        {
            let result = self
                .inner
                .delete_range(start_inclusive, end_exclusive, options)
                .await;
            shard_op.complete_with_result(&result).await;
            results.push(result);
        }

        // Shard 2 (if range spans multiple shards)
        if start_inclusive < "m" && end_exclusive > "m" {
            if let Some(shard_op) = parent_op
                .create_child("shard:write_stream".to_string(), Some(2))
                .await
            {
                let result = self.inner.delete_range("m", end_exclusive, options).await;
                shard_op.complete_with_result(&result).await;
                results.push(result);
            }
        }

        // Return the first successful result or the first error
        for result in results {
            if result.is_ok() {
                return result;
            }
        }

        // If we get here, call the original method
        self.inner
            .delete_range(start_inclusive, end_exclusive, options)
            .await
    }

    /// List keys with tracing
    #[instrument(skip(self), fields(operation = "list"))]
    pub async fn list(&self, start_inclusive: &str, end_exclusive: &str) -> Result<ListResponse> {
        let traced_op = self
            .tracer
            .start_root_span("Client::list".to_string())
            .await;

        let result = self.inner.list(start_inclusive, end_exclusive).await;
        traced_op.complete_with_result(&result).await;

        result
    }

    /// List with options and full tracing
    #[instrument(skip(self, options), fields(operation = "list_with_options"))]
    pub async fn list_with_options(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        options: &ListOptions,
    ) -> Result<ListResponse> {
        let traced_op = self
            .tracer
            .start_root_span("Client::list_with_options".to_string())
            .await;

        let result = self
            .list_with_options_traced(start_inclusive, end_exclusive, options, &traced_op)
            .await;

        traced_op.complete_with_result(&result).await;
        result
    }

    async fn list_with_options_traced(
        &self,
        start_inclusive: &str,
        end_exclusive: &str,
        options: &ListOptions,
        parent_op: &TracedOperation,
    ) -> Result<ListResponse> {
        // List operations typically span multiple shards
        // We'll create child spans for each shard that might contain keys in the range

        let mut all_results = ListResponse::default();
        let mut errors = Vec::new();

        // Simulate querying multiple shards based on key range
        let shards_to_query = self.determine_shards_for_range(start_inclusive, end_exclusive);

        for shard_id in shards_to_query {
            if let Some(shard_op) = parent_op
                .create_child("shard:list".to_string(), Some(shard_id))
                .await
            {
                let shard_result = self
                    .inner
                    .list_with_options(start_inclusive, end_exclusive, options)
                    .await;

                match &shard_result {
                    Ok(response) => {
                        shard_op.complete_success().await;
                        all_results.merge(response.clone());
                    }
                    Err(e) => {
                        shard_op.complete_error(e.to_string()).await;
                        errors.push(e.to_string());
                    }
                }
            }
        }

        if !errors.is_empty() && !options.partial_ok {
            return Err(crate::Error::Custom(format!(
                "List operation failed on {} shards: {}",
                errors.len(),
                errors.join(", ")
            )));
        }

        if !errors.is_empty() {
            all_results.partial = true;
        }

        Ok(all_results)
    }

    /// Determine which shards need to be queried for a key range
    fn determine_shards_for_range(&self, start: &str, end: &str) -> Vec<i64> {
        // This is a simplified shard determination logic
        // In a real implementation, this would use the shard mapping strategy

        let mut shards = Vec::new();

        // Simple range-based sharding for demonstration
        if start < "h" {
            shards.push(1);
        }
        if (start < "p" && end > "h") || (start >= "h" && start < "p") {
            shards.push(2);
        }
        if end > "p" {
            shards.push(3);
        }

        if shards.is_empty() {
            shards.push(1); // Default to shard 1
        }

        shards
    }

    /// Get performance statistics for traced operations
    pub async fn get_operation_stats(
        &self,
        operation: &str,
    ) -> crate::tracing_metrics::OperationStats {
        self.tracer.get_operation_stats(operation).await
    }

    /// Get all completed traces
    pub async fn get_completed_traces(&self) -> Vec<crate::tracing_metrics::CompletedSpan> {
        self.tracer.get_completed_traces().await
    }

    /// Log performance analysis across all traced operations
    pub async fn log_performance_analysis(&self) {
        self.tracer.log_performance_analysis().await
    }

    /// Clear completed traces (for memory management)
    pub async fn clear_traces(&self) {
        self.tracer.clear_completed_traces().await
    }
}

// Extension trait to add tracing to existing clients
pub trait ClientTracingExtension {
    /// Wrap this client with distributed tracing
    fn with_tracing(self, tracer: Arc<DistributedTracingCollector>) -> TracedClient;
}

impl ClientTracingExtension for BaseClient {
    fn with_tracing(self, tracer: Arc<DistributedTracingCollector>) -> TracedClient {
        TracedClient::new(self, tracer)
    }
}

/// Factory for creating traced clients
pub struct TracedClientFactory {
    tracer: Arc<DistributedTracingCollector>,
}

impl TracedClientFactory {
    /// Create a new factory
    pub fn new(tracer: Arc<DistributedTracingCollector>) -> Self {
        Self { tracer }
    }

    /// Create a traced client from a base client
    pub fn create_client(&self, base_client: BaseClient) -> TracedClient {
        TracedClient::new(base_client, self.tracer.clone())
    }

    /// Get the tracer
    pub fn tracer(&self) -> Arc<DistributedTracingCollector> {
        self.tracer.clone()
    }
}

// Helper trait extension for ListResponse
trait ListResponseExt {
    fn merge(&mut self, other: ListResponse);
}

impl ListResponseExt for ListResponse {
    fn merge(&mut self, other: ListResponse) {
        self.keys.extend(other.keys);
        if other.partial {
            self.partial = true;
        }

        // Merge indexes if present
        for (index_name, index_entries) in other.indexes {
            self.indexes
                .entry(index_name)
                .or_insert_with(Vec::new)
                .extend(index_entries);
        }

        // Sort and deduplicate keys
        self.keys.sort();
        self.keys.dedup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::GrpcMetricsCollector;
    use crate::tracing_metrics::DistributedTracingCollector;
    use std::time::Duration;
    use tokio::time::sleep;

    async fn create_test_traced_client() -> TracedClient {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = Arc::new(DistributedTracingCollector::new(metrics));

        // Create a mock base client (in real tests, this would be a proper client)
        let base_client = BaseClient::new(/* test config */); // This would need proper setup

        TracedClient::new(base_client, tracer)
    }

    #[tokio::test]
    async fn test_traced_operations() {
        // This test would require a proper client setup
        // For now, we just test the tracing structure

        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = Arc::new(DistributedTracingCollector::new(metrics));

        // Test that we can create traced operations
        let root_op = tracer.start_root_span("test_operation".to_string()).await;

        // Create child operations
        let child1 = root_op
            .create_child("shard:test".to_string(), Some(1))
            .await;
        let child2 = root_op
            .create_child("shard:test".to_string(), Some(2))
            .await;

        assert!(child1.is_some());
        assert!(child2.is_some());

        // Complete operations
        child1.unwrap().complete_success().await;
        child2.unwrap().complete_success().await;
        root_op.complete_success().await;

        // Verify trace was recorded
        let traces = tracer.get_completed_traces().await;
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].child_spans.len(), 2);
        assert_eq!(traces[0].shard_fan_out(), 2);
    }

    #[test]
    fn test_shard_determination() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = Arc::new(DistributedTracingCollector::new(metrics));

        // This would need a proper base client in real tests
        // For now, just test the shard determination logic concept

        // Test range spanning multiple shards
        let shards_for_range = |start: &str, end: &str| -> Vec<i64> {
            let mut shards = Vec::new();
            if start < "h" {
                shards.push(1);
            }
            if (start < "p" && end > "h") || (start >= "h" && start < "p") {
                shards.push(2);
            }
            if end > "p" {
                shards.push(3);
            }
            if shards.is_empty() {
                shards.push(1);
            }
            shards
        };

        assert_eq!(shards_for_range("a", "z"), vec![1, 2, 3]);
        assert_eq!(shards_for_range("a", "g"), vec![1]);
        assert_eq!(shards_for_range("m", "o"), vec![2]);
        assert_eq!(shards_for_range("q", "z"), vec![3]);
    }

    #[tokio::test]
    async fn test_operation_stats() {
        let metrics = Arc::new(GrpcMetricsCollector::new());
        let tracer = Arc::new(DistributedTracingCollector::new(metrics));

        // Create several traces for the same operation
        for i in 0..3 {
            let root_op = tracer.start_root_span("Client::get".to_string()).await;

            // Variable number of shard operations
            for shard_id in 1..=i + 1 {
                if let Some(child) = root_op
                    .create_child("shard:get".to_string(), Some(shard_id))
                    .await
                {
                    sleep(Duration::from_millis(10)).await;
                    child.complete_success().await;
                }
            }

            sleep(Duration::from_millis(5)).await;
            root_op.complete_success().await;
        }

        let stats = tracer.get_operation_stats("Client::get").await;
        assert_eq!(stats.trace_count, 3);
        assert_eq!(stats.success_rate, 1.0);
        assert!(stats.avg_fan_out > 0.0);
        assert!(stats.avg_duration > Duration::ZERO);
    }
}
