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

//! Comprehensive example demonstrating distributed tracing for Oxia client operations.
//!
//! This example shows how high-level API operations like `Client::get_with_options()`
//! are traced and connected to their constituent shard-level gRPC calls, providing
//! complete visibility into operation fan-out and performance characteristics.
//!
//! Run with: cargo run --example distributed_tracing_example

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, sleep};
use tracing::{Level, debug, info, warn};
use tracing_subscriber::{self, EnvFilter, fmt, prelude::*};

use mauricebarnum_oxia_client::{
    Client, ClientTracingExtension, DistributedTracingCollector, GrpcMetricsCollector, Result,
    TracedClient, TracedClientFactory, config,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging with tracing support
    init_tracing()?;

    info!("🚀 Starting Distributed Tracing Example");

    // Setup distributed tracing with metrics integration
    let (traced_client, tracer) = setup_traced_client().await?;

    // Run demonstration scenarios
    run_tracing_scenarios(&traced_client).await?;

    // Analyze collected traces
    analyze_traces(&tracer).await;

    // Demonstrate real-time trace monitoring
    start_trace_monitoring(&tracer).await;

    info!("✅ Distributed tracing example completed");
    Ok(())
}

/// Initialize tracing with appropriate levels and formatting
fn init_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,mauricebarnum_oxia_client=debug"))
        .map_err(|e| {
            mauricebarnum_oxia_client::Error::Custom(format!("Failed to initialize tracing: {}", e))
        })?;

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_span_events(fmt::format::FmtSpan::ENTER | fmt::format::FmtSpan::CLOSE),
        )
        .with(filter)
        .init();

    Ok(())
}

/// Setup traced client with metrics and distributed tracing
async fn setup_traced_client() -> Result<(TracedClient, Arc<DistributedTracingCollector>)> {
    // Create metrics collector
    let metrics = Arc::new(GrpcMetricsCollector::new());

    // Create distributed tracing collector
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));

    // Create Oxia client configuration
    let client_config = config::Builder::new()
        .service_addr("localhost:6544")
        .namespace("tracing_demo")
        .session_timeout(Duration::from_secs(30))
        .request_timeout(Some(Duration::from_secs(10)))
        .build()?;

    // Create base client
    let base_client = Client::with_config(client_config)?;

    // Wrap with distributed tracing
    let traced_client = base_client.with_tracing(tracer.clone());

    info!("🔧 Traced client setup complete");
    Ok((traced_client, tracer))
}

/// Run various tracing scenarios to demonstrate different patterns
async fn run_tracing_scenarios(client: &TracedClient) -> Result<()> {
    info!("🎯 Running tracing scenarios...");

    // Scenario 1: Single shard operations
    single_shard_operations(client).await?;

    // Scenario 2: Multi-shard operations (fan-out)
    multi_shard_operations(client).await?;

    // Scenario 3: Mixed operation types
    mixed_operations(client).await?;

    // Scenario 4: Error scenarios with tracing
    error_scenarios(client).await;

    // Scenario 5: Concurrent operations
    concurrent_operations(client).await?;

    Ok(())
}

/// Demonstrate single shard operations with tracing
async fn single_shard_operations(client: &TracedClient) -> Result<()> {
    info!("📍 Scenario 1: Single shard operations");

    // Simple get operation - should create root span + 1 child shard span
    match client.get("single_shard_key").await {
        Ok(Some(response)) => info!("✅ Retrieved value for single_shard_key"),
        Ok(None) => info!("ℹ️ Key single_shard_key not found"),
        Err(e) => warn!("❌ Get operation failed: {}", e),
    }

    // Put operation - root span + shard write span
    let value = b"single_shard_value".to_vec();
    match client.put("single_shard_key", value).await {
        Ok(_) => info!("✅ Put operation successful"),
        Err(e) => warn!("❌ Put operation failed: {}", e),
    }

    // Verify with another get
    match client.get("single_shard_key").await {
        Ok(Some(response)) => info!("✅ Verified put operation"),
        Ok(None) => warn!("⚠️ Key not found after put"),
        Err(e) => warn!("❌ Verification get failed: {}", e),
    }

    sleep(Duration::from_millis(100)).await;
    Ok(())
}

/// Demonstrate multi-shard operations (fan-out pattern)
async fn multi_shard_operations(client: &TracedClient) -> Result<()> {
    info!("🌐 Scenario 2: Multi-shard operations (fan-out)");

    // List operation that spans multiple shards
    // This should create: root span + multiple shard:list child spans
    match client.list("a", "z").await {
        Ok(response) => {
            info!("✅ List operation returned {} keys", response.keys.len());
            info!(
                "🔍 Keys found: {:?}",
                response.keys.iter().take(5).collect::<Vec<_>>()
            );
        }
        Err(e) => warn!("❌ List operation failed: {}", e),
    }

    // Range delete operation across shards
    match client
        .delete_range("temp_", "temp_z", &Default::default())
        .await
    {
        Ok(_) => info!("✅ Range delete operation successful"),
        Err(e) => warn!("❌ Range delete failed: {}", e),
    }

    // Create multiple keys across different shard ranges
    let keys_and_values = vec![
        ("apple", "fruit"),
        ("banana", "fruit"),
        ("carrot", "vegetable"),
        ("dog", "animal"),
        ("elephant", "animal"),
        ("fish", "animal"),
        ("grape", "fruit"),
        ("house", "building"),
        ("igloo", "building"),
        ("jungle", "place"),
        ("kite", "toy"),
        ("lion", "animal"),
        ("moon", "celestial"),
        ("night", "time"),
        ("ocean", "place"),
        ("planet", "celestial"),
        ("queen", "person"),
        ("river", "place"),
        ("sun", "celestial"),
        ("tree", "plant"),
        ("universe", "concept"),
        ("volcano", "geological"),
        ("water", "element"),
        ("xray", "technology"),
        ("yacht", "vehicle"),
        ("zebra", "animal"),
    ];

    info!("📝 Creating test data across multiple shards...");
    for (key, value) in &keys_and_values {
        if let Err(e) = client.put(key, value.as_bytes().to_vec()).await {
            warn!("⚠️ Failed to put {}: {}", key, e);
        }
    }

    // Now list again to see the fan-out across shards
    match client.list("a", "z").await {
        Ok(response) => {
            info!("✅ Multi-shard list returned {} keys", response.keys.len());
            info!("📊 This should show fan-out to multiple shards in traces");
        }
        Err(e) => warn!("❌ Multi-shard list failed: {}", e),
    }

    sleep(Duration::from_millis(200)).await;
    Ok(())
}

/// Demonstrate mixed operation types in sequence
async fn mixed_operations(client: &TracedClient) -> Result<()> {
    info!("🔄 Scenario 3: Mixed operation types");

    let operations = vec![
        ("get", "mixed_key_1"),
        ("put", "mixed_key_2"),
        ("get", "mixed_key_2"),
        ("delete", "mixed_key_2"),
        ("get", "mixed_key_2"), // Should be not found
    ];

    for (operation, key) in operations {
        match operation {
            "get" => match client.get(key).await {
                Ok(Some(_)) => info!("✅ GET {}: found", key),
                Ok(None) => info!("ℹ️ GET {}: not found", key),
                Err(e) => warn!("❌ GET {}: error - {}", key, e),
            },
            "put" => {
                let value = format!("value_for_{}", key);
                match client.put(key, value.into_bytes()).await {
                    Ok(_) => info!("✅ PUT {}: success", key),
                    Err(e) => warn!("❌ PUT {}: error - {}", key, e),
                }
            }
            "delete" => match client.delete(key).await {
                Ok(_) => info!("✅ DELETE {}: success", key),
                Err(e) => warn!("❌ DELETE {}: error - {}", key, e),
            },
            _ => unreachable!(),
        }

        sleep(Duration::from_millis(50)).await;
    }

    Ok(())
}

/// Demonstrate error scenarios and how they're traced
async fn error_scenarios(client: &TracedClient) {
    info!("⚠️ Scenario 4: Error scenarios with tracing");

    // These operations might fail, but should still be properly traced

    // Attempt to get from non-existent namespace or with invalid options
    if let Err(e) = client.get("definitely_nonexistent_key_12345").await {
        info!("Expected error traced: {}", e);
    }

    // Attempt operations that might timeout or fail
    if let Err(e) = client.list("invalid_range_start", "").await {
        info!("Expected list error traced: {}", e);
    }

    // These errors should appear in traces with proper error information
    sleep(Duration::from_millis(100)).await;
}

/// Demonstrate concurrent operations and trace correlation
async fn concurrent_operations(client: &TracedClient) -> Result<()> {
    info!("⚡ Scenario 5: Concurrent operations");

    let mut handles = Vec::new();

    // Launch multiple concurrent operations
    for i in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let key = format!("concurrent_key_{}", i);
            let value = format!("concurrent_value_{}", i);

            // Each of these will create separate distributed traces
            let _ = client_clone.put(&key, value.into_bytes()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = client_clone.get(&key).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            let _ = client_clone.delete(&key).await;

            info!("Completed concurrent operations for key: {}", key);
        });

        handles.push(handle);
    }

    // Wait for all concurrent operations to complete
    for handle in handles {
        if let Err(e) = handle.await {
            warn!("Concurrent operation failed: {:?}", e);
        }
    }

    // Give time for traces to be recorded
    sleep(Duration::from_millis(200)).await;
    Ok(())
}

/// Analyze collected traces and show insights
async fn analyze_traces(tracer: &Arc<DistributedTracingCollector>) {
    info!("📊 Analyzing collected traces...");

    let traces = tracer.get_completed_traces().await;
    info!("📈 Total traces collected: {}", traces.len());

    if traces.is_empty() {
        warn!("No traces found - operations may have failed to complete");
        return;
    }

    // Group traces by operation type
    let mut operations: HashMap<String, Vec<_>> = HashMap::new();
    for trace in &traces {
        operations
            .entry(trace.context.operation.clone())
            .or_insert_with(Vec::new)
            .push(trace);
    }

    info!("🔍 Trace Analysis by Operation Type:");
    info!(
        "┌─────────────────────────┬───────┬─────────┬─────────────┬──────────────┬─────────────┐"
    );
    info!(
        "│ Operation               │ Count │ Avg Fan │ Avg Duration│ Avg Shard T. │ Efficiency  │"
    );
    info!(
        "├─────────────────────────┼───────┼─────────┼─────────────┼──────────────┼─────────────┤"
    );

    for (operation, op_traces) in &operations {
        let count = op_traces.len();
        let avg_fan_out =
            op_traces.iter().map(|t| t.shard_fan_out()).sum::<usize>() as f64 / count as f64;
        let avg_duration = op_traces.iter().map(|t| t.duration).sum::<Duration>() / count as u32;
        let avg_shard_time = op_traces
            .iter()
            .map(|t| t.total_shard_time())
            .sum::<Duration>()
            / count as u32;
        let efficiency = if avg_duration.as_nanos() > 0 {
            avg_shard_time.as_nanos() as f64 / avg_duration.as_nanos() as f64
        } else {
            0.0
        };

        info!(
            "│ {:<23} │ {:>5} │ {:>7.1} │ {:>9.1}ms │ {:>10.1}ms │ {:>9.1}% │",
            truncate_string(operation, 23),
            count,
            avg_fan_out,
            avg_duration.as_secs_f64() * 1000.0,
            avg_shard_time.as_secs_f64() * 1000.0,
            efficiency * 100.0
        );
    }
    info!(
        "└─────────────────────────┴───────┴─────────┴─────────────┴──────────────┴─────────────┘"
    );

    // Show detailed trace examples
    show_detailed_trace_examples(&traces);

    // Show performance insights
    show_performance_insights(&traces);
}

/// Show detailed examples of specific traces
fn show_detailed_trace_examples(
    traces: &[mauricebarnum_oxia_client::tracing_metrics::CompletedSpan],
) {
    info!("🔍 Detailed Trace Examples:");

    // Find a trace with multiple shard operations
    if let Some(multi_shard_trace) = traces.iter().find(|t| t.shard_fan_out() > 1) {
        info!("📋 Multi-Shard Operation Example:");
        info!("  Trace ID: {}", multi_shard_trace.context.trace_id);
        info!("  Operation: {}", multi_shard_trace.context.operation);
        info!(
            "  Total Duration: {:.2}ms",
            multi_shard_trace.duration.as_secs_f64() * 1000.0
        );
        info!("  Shard Operations: {}", multi_shard_trace.shard_fan_out());
        info!("  Success: {}", multi_shard_trace.success);

        info!("  Shard Breakdown:");
        for (i, shard_op) in multi_shard_trace.get_shard_operations().iter().enumerate() {
            let shard_id = shard_op
                .context
                .attributes
                .get("shard_id")
                .unwrap_or(&"unknown".to_string());
            info!(
                "    Shard {}: {} ({}ms) {}",
                shard_id,
                shard_op.context.operation.replace("shard:", ""),
                (shard_op.duration.as_secs_f64() * 1000.0) as u64,
                if shard_op.success { "✅" } else { "❌" }
            );
        }
    }

    // Find traces with different patterns
    let single_shard_count = traces.iter().filter(|t| t.shard_fan_out() == 1).count();
    let multi_shard_count = traces.iter().filter(|t| t.shard_fan_out() > 1).count();
    let no_shard_count = traces.iter().filter(|t| t.shard_fan_out() == 0).count();

    info!("📊 Shard Fan-out Distribution:");
    info!("  Single shard operations: {}", single_shard_count);
    info!("  Multi-shard operations: {}", multi_shard_count);
    info!("  No shard operations: {}", no_shard_count);
}

/// Show performance insights from traces
fn show_performance_insights(traces: &[mauricebarnum_oxia_client::tracing_metrics::CompletedSpan]) {
    info!("🚀 Performance Insights:");

    // Find slowest operations
    let mut sorted_traces = traces.iter().collect::<Vec<_>>();
    sorted_traces.sort_by_key(|t| std::cmp::Reverse(t.duration));

    if let Some(slowest) = sorted_traces.first() {
        info!(
            "🐌 Slowest Operation: {} ({:.2}ms, {} shards)",
            slowest.context.operation,
            slowest.duration.as_secs_f64() * 1000.0,
            slowest.shard_fan_out()
        );
    }

    // Find most efficient operations (high shard time ratio)
    let mut efficiency_traces: Vec<_> = traces
        .iter()
        .map(|t| {
            let efficiency = if t.duration.as_nanos() > 0 {
                t.total_shard_time().as_nanos() as f64 / t.duration.as_nanos() as f64
            } else {
                0.0
            };
            (t, efficiency)
        })
        .filter(|(_, eff)| *eff > 0.0)
        .collect();

    efficiency_traces.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    if let Some((most_efficient, efficiency)) = efficiency_traces.first() {
        info!(
            "⚡ Most Efficient Operation: {} ({:.1}% efficiency)",
            most_efficient.context.operation,
            efficiency * 100.0
        );
    }

    // Error rate analysis
    let total_operations = traces.len();
    let failed_operations = traces.iter().filter(|t| !t.success).count();
    let error_rate = if total_operations > 0 {
        failed_operations as f64 / total_operations as f64
    } else {
        0.0
    };

    info!(
        "📉 Error Rate: {:.1}% ({}/{})",
        error_rate * 100.0,
        failed_operations,
        total_operations
    );

    // Shard operation distribution
    let total_shard_ops: usize = traces.iter().map(|t| t.shard_fan_out()).sum();
    let avg_fan_out = if total_operations > 0 {
        total_shard_ops as f64 / total_operations as f64
    } else {
        0.0
    };

    info!(
        "🌐 Average Fan-out: {:.1} shards per operation",
        avg_fan_out
    );
}

/// Start real-time trace monitoring
async fn start_trace_monitoring(tracer: &Arc<DistributedTracingCollector>) {
    info!("👁️ Starting real-time trace monitoring (5 seconds)...");

    let monitoring_duration = Duration::from_secs(5);
    let start_time = tokio::time::Instant::now();
    let mut interval = interval(Duration::from_secs(1));

    while start_time.elapsed() < monitoring_duration {
        interval.tick().await;

        let traces = tracer.get_completed_traces().await;
        let recent_traces: Vec<_> = traces
            .iter()
            .filter(|t| t.context.start_time.elapsed() < Duration::from_secs(10))
            .collect();

        if !recent_traces.is_empty() {
            info!(
                "📊 Live Stats: {} recent traces, avg duration: {:.1}ms",
                recent_traces.len(),
                if recent_traces.len() > 0 {
                    recent_traces
                        .iter()
                        .map(|t| t.duration)
                        .sum::<Duration>()
                        .as_secs_f64()
                        * 1000.0
                        / recent_traces.len() as f64
                } else {
                    0.0
                }
            );
        }
    }

    // Final analysis
    info!("📊 Final Trace Analysis:");
    tracer.log_performance_analysis().await;
}

/// Utility function to truncate strings for display
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Additional utility functions for demonstration

/// Simulate network delay (for more realistic traces)
async fn simulate_network_delay() {
    sleep(Duration::from_millis(fastrand::u32(5..=25) as u64)).await;
}

/// Create test data with predictable shard distribution
async fn create_test_data(client: &TracedClient, prefix: &str, count: usize) -> Result<()> {
    info!(
        "📝 Creating {} test records with prefix '{}'",
        count, prefix
    );

    for i in 0..count {
        let key = format!("{}_{:04}", prefix, i);
        let value = format!("test_data_{}", i);

        if let Err(e) = client.put(&key, value.into_bytes()).await {
            warn!("⚠️ Failed to create test data {}: {}", key, e);
        }

        // Small delay to create more realistic timing patterns
        if i % 10 == 0 {
            simulate_network_delay().await;
        }
    }

    Ok(())
}

/// Clean up test data
async fn cleanup_test_data(client: &TracedClient, prefix: &str, count: usize) {
    info!("🧹 Cleaning up test data with prefix '{}'", prefix);

    for i in 0..count {
        let key = format!("{}_{:04}", prefix, i);
        if let Err(e) = client.delete(&key).await {
            debug!("Note: Failed to delete {}: {} (may not exist)", key, e);
        }
    }
}

/// Demonstrate advanced tracing patterns
async fn advanced_tracing_patterns(client: &TracedClient) -> Result<()> {
    info!("🔬 Advanced Tracing Patterns:");

    // Pattern 1: Batch operations with tracing
    let batch_keys: Vec<String> = (0..10).map(|i| format!("batch_key_{}", i)).collect();

    for key in &batch_keys {
        let value = format!("batch_value_{}", key);
        client.put(key, value.into_bytes()).await?;
    }

    // Pattern 2: Transaction-like operations (multiple related operations)
    let transaction_id = uuid::Uuid::new_v4();
    info!(
        "🔄 Transaction {}: Starting related operations",
        transaction_id
    );

    // These operations would be part of a logical transaction
    client
        .put("tx_key_1", b"transaction_data_1".to_vec())
        .await?;
    client
        .put("tx_key_2", b"transaction_data_2".to_vec())
        .await?;
    client.list("tx_", "tx_z").await?;

    info!("✅ Transaction {}: Completed", transaction_id);

    // Pattern 3: Conditional operations based on previous results
    if let Ok(Some(_)) = client.get("tx_key_1").await {
        client
            .put("tx_key_derived", b"derived_from_key_1".to_vec())
            .await?;
        info!("🔗 Created derived key based on existing data");
    }

    Ok(())
}
