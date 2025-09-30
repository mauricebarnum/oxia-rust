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

//! Comprehensive example demonstrating gRPC metrics integration in the Oxia client.
//!
//! This example shows various ways to collect and export latency metrics for all gRPC operations:
//! 1. Basic metrics collection with automatic logging
//! 2. Manual metrics inspection and reporting
//! 3. Prometheus format export
//! 4. Custom metrics analysis
//!
//! Run with: cargo run --example grpc_metrics_example

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, sleep};
use tracing::{Level, info, warn};
use tracing_subscriber;

use mauricebarnum_oxia_client::{
    Client, Error, GrpcMetricsCollector, MetricLabels, MetricsConfig, MetricsConfigBuilder,
    MetricsEnabledClientFactory, OperationMetrics, Result, config,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Oxia gRPC Metrics Example");

    // Example 1: Basic setup with automatic metrics collection
    basic_metrics_example().await?;

    // Example 2: Advanced metrics configuration
    advanced_metrics_example().await?;

    // Example 3: Prometheus export
    prometheus_export_example().await?;

    // Example 4: Custom metrics analysis
    custom_analysis_example().await?;

    info!("All examples completed successfully");
    Ok(())
}

/// Example 1: Basic metrics collection with default settings
async fn basic_metrics_example() -> Result<()> {
    info!("=== Basic Metrics Example ===");

    // Create a basic metrics configuration
    let metrics_config = MetricsConfig::default();

    // Setup the metrics-enabled client factory
    let factory = MetricsEnabledClientFactory::new(metrics_config);

    // Create a client configuration
    let client_config = config::Builder::new()
        .service_addr("localhost:6544") // Default Oxia port
        .namespace("metrics_example")
        .build()?;

    // Create the client - this will automatically use instrumented gRPC calls
    let client = Client::with_config(client_config)?;

    // Perform some operations to generate metrics
    simulate_client_operations(&client).await;

    // Get the metrics collector and print a summary
    let metrics = factory.metrics();
    metrics.log_metrics_summary().await;

    Ok(())
}

/// Example 2: Advanced metrics configuration with custom settings
async fn advanced_metrics_example() -> Result<()> {
    info!("=== Advanced Metrics Configuration Example ===");

    // Create custom metrics configuration
    let metrics_config = MetricsConfigBuilder::new()
        .enabled(true)
        .log_summaries(false) // We'll handle logging manually
        .summary_interval_secs(60) // 1 minute intervals
        .enable_prometheus_export(true)
        .build();

    let factory = MetricsEnabledClientFactory::new(metrics_config);
    let metrics = factory.metrics();

    // Create client with the metrics factory
    let client_config = config::Builder::new()
        .service_addr("localhost:6544")
        .namespace("advanced_metrics_example")
        .build()?;

    let client = Client::with_config(client_config)?;

    // Simulate operations with custom monitoring
    let start_time = std::time::Instant::now();
    simulate_client_operations(&client).await;
    let elapsed = start_time.elapsed();

    // Get detailed metrics breakdown
    let all_metrics = metrics.get_metrics().await;

    info!("Operations completed in {:?}", elapsed);
    info!(
        "Collected metrics for {} operation types",
        all_metrics.len()
    );

    // Print detailed breakdown for each operation type
    for (labels, operation_metrics) in all_metrics {
        print_detailed_metrics(&labels, &operation_metrics);
    }

    Ok(())
}

/// Example 3: Prometheus format export
async fn prometheus_export_example() -> Result<()> {
    info!("=== Prometheus Export Example ===");

    let metrics_config = MetricsConfigBuilder::new()
        .enable_prometheus_export(true)
        .log_summaries(false)
        .build();

    let factory = MetricsEnabledClientFactory::new(metrics_config);
    let metrics = factory.metrics();

    // Create client and generate some metrics
    let client_config = config::Builder::new()
        .service_addr("localhost:6544")
        .namespace("prometheus_example")
        .build()?;

    let client = Client::with_config(client_config)?;
    simulate_client_operations(&client).await;

    // Export metrics in Prometheus format
    let prometheus_output = factory.export_prometheus().await;

    info!("Prometheus format metrics:");
    println!("{}", prometheus_output);

    // You could serve this on an HTTP endpoint for Prometheus scraping
    // Example: serve_prometheus_metrics(metrics).await;

    Ok(())
}

/// Example 4: Custom metrics analysis and alerting
async fn custom_analysis_example() -> Result<()> {
    info!("=== Custom Metrics Analysis Example ===");

    let metrics = Arc::new(GrpcMetricsCollector::new());

    // Create client with manual metrics integration
    let client_config = config::Builder::new()
        .service_addr("localhost:6544")
        .namespace("analysis_example")
        .build()?;

    let client = Client::with_config(client_config)?;

    // Start background metrics analysis
    start_metrics_analysis_task(metrics.clone());

    // Simulate operations
    simulate_client_operations(&client).await;

    // Wait a bit for analysis to run
    sleep(Duration::from_secs(2)).await;

    Ok(())
}

/// Simulate various client operations to generate metrics
async fn simulate_client_operations(client: &Client) {
    info!("Simulating client operations...");

    // Simulate various operations that would generate gRPC calls
    let operations = vec![
        ("get_operation", simulate_get_operations(client)),
        ("put_operation", simulate_put_operations(client)),
        ("list_operation", simulate_list_operations(client)),
        ("delete_operation", simulate_delete_operations(client)),
    ];

    for (name, operation) in operations {
        info!("Running {}", name);
        match operation.await {
            Ok(_) => info!("{} completed successfully", name),
            Err(e) => warn!("{} failed: {}", name, e),
        }

        // Small delay between operation types
        sleep(Duration::from_millis(100)).await;
    }
}

async fn simulate_get_operations(client: &Client) -> Result<()> {
    for i in 0..5 {
        let key = format!("test_key_{}", i);
        match client.get(&key).await {
            Ok(Some(response)) => info!("Retrieved key: {}", key),
            Ok(None) => info!("Key not found: {}", key),
            Err(e) => warn!("Get failed for key {}: {}", key, e),
        }
        sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn simulate_put_operations(client: &Client) -> Result<()> {
    for i in 0..3 {
        let key = format!("test_key_{}", i);
        let value = format!("test_value_{}", i).into_bytes();

        match client.put(&key, value).await {
            Ok(_) => info!("Put successful for key: {}", key),
            Err(e) => warn!("Put failed for key {}: {}", key, e),
        }
        sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn simulate_list_operations(client: &Client) -> Result<()> {
    match client.list("test_", "test_z").await {
        Ok(response) => info!("List returned {} keys", response.keys.len()),
        Err(e) => warn!("List operation failed: {}", e),
    }
    Ok(())
}

async fn simulate_delete_operations(client: &Client) -> Result<()> {
    for i in 0..2 {
        let key = format!("test_key_{}", i);
        match client.delete(&key).await {
            Ok(_) => info!("Delete successful for key: {}", key),
            Err(e) => warn!("Delete failed for key {}: {}", key, e),
        }
        sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// Print detailed metrics for a specific operation
fn print_detailed_metrics(labels: &MetricLabels, metrics: &OperationMetrics) {
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

    info!("--- Operation Metrics ---");
    info!("  Method: {}", labels.method);
    info!("  Shard ID: {:?}", labels.shard_id);
    info!("  Namespace: {}", labels.namespace);
    info!("  Total Requests: {}", metrics.total_requests);
    info!("  Total Errors: {}", metrics.total_errors);
    info!("  Error Rate: {:.2}%", metrics.error_rate() * 100.0);
    info!("  Average Latency: {}", avg_duration);
    info!("  Min Latency: {}", min_duration);
    info!("  Max Latency: {}", max_duration);
    info!(
        "  Total Duration: {:.2}ms",
        metrics.total_duration.as_secs_f64() * 1000.0
    );

    // Print histogram data for latency distribution
    info!("  Latency Distribution:");
    let mut histogram_vec: Vec<_> = metrics.histogram.iter().collect();
    histogram_vec.sort_by_key(|(bucket, _)| {
        if *bucket == "le_inf" {
            f64::INFINITY
        } else {
            bucket
                .strip_prefix("le_")
                .unwrap_or("0")
                .parse::<f64>()
                .unwrap_or(0.0)
        }
    });

    for (bucket, count) in histogram_vec {
        if *count > 0 {
            let bucket_name = if *bucket == "le_inf" {
                "+∞".to_string()
            } else {
                format!("≤{}ms", bucket.strip_prefix("le_").unwrap_or(bucket))
            };
            info!("    {}: {}", bucket_name, count);
        }
    }
    info!("------------------------");
}

/// Start a background task for continuous metrics analysis
fn start_metrics_analysis_task(metrics: Arc<GrpcMetricsCollector>) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let current_metrics = metrics.get_metrics().await;

            // Example analysis: detect high error rates
            for (labels, operation_metrics) in current_metrics {
                if operation_metrics.total_requests > 0 {
                    let error_rate = operation_metrics.error_rate();

                    if error_rate > 0.1 {
                        // Alert if error rate > 10%
                        warn!(
                            "⚠️  HIGH ERROR RATE DETECTED: {} has {:.1}% error rate ({}/{} requests)",
                            labels.method,
                            error_rate * 100.0,
                            operation_metrics.total_errors,
                            operation_metrics.total_requests
                        );
                    }

                    // Example analysis: detect high latency
                    if let Some(avg_duration) = operation_metrics.average_duration() {
                        if avg_duration > Duration::from_millis(1000) {
                            // Alert if avg > 1s
                            warn!(
                                "🐌 HIGH LATENCY DETECTED: {} average latency is {:.2}ms",
                                labels.method,
                                avg_duration.as_secs_f64() * 1000.0
                            );
                        }
                    }
                }
            }
        }
    });
}

/// Example function to serve Prometheus metrics over HTTP
/// (Commented out since it requires additional dependencies)
/*
async fn serve_prometheus_metrics(metrics: Arc<GrpcMetricsCollector>) -> Result<()> {
    use warp::Filter;

    let metrics_route = warp::path("metrics")
        .and(warp::get())
        .and_then(move || {
            let metrics = metrics.clone();
            async move {
                let output = metrics.export_prometheus_format().await;
                Ok::<_, warp::Rejection>(warp::reply::with_header(
                    output,
                    "content-type",
                    "text/plain; version=0.0.4; charset=utf-8",
                ))
            }
        });

    info!("Serving Prometheus metrics on http://localhost:3030/metrics");
    warp::serve(metrics_route)
        .run(([127, 0, 0, 1], 3030))
        .await;

    Ok(())
}
*/

/// Helper function to analyze metrics trends over time
fn analyze_metrics_trends(
    current: &HashMap<MetricLabels, OperationMetrics>,
    previous: &HashMap<MetricLabels, OperationMetrics>,
) {
    for (labels, current_metrics) in current {
        if let Some(previous_metrics) = previous.get(labels) {
            let request_diff = current_metrics
                .total_requests
                .saturating_sub(previous_metrics.total_requests);
            let error_diff = current_metrics
                .total_errors
                .saturating_sub(previous_metrics.total_errors);

            if request_diff > 0 {
                let current_avg = current_metrics.average_duration();
                let previous_avg = previous_metrics.average_duration();

                match (current_avg, previous_avg) {
                    (Some(curr), Some(prev)) => {
                        let latency_change = curr.as_secs_f64() - prev.as_secs_f64();
                        let latency_change_percent = (latency_change / prev.as_secs_f64()) * 100.0;

                        if latency_change_percent.abs() > 20.0 {
                            let trend = if latency_change > 0.0 {
                                "📈 INCREASED"
                            } else {
                                "📉 DECREASED"
                            };
                            info!(
                                "{} latency for {}: {:.1}% change ({:.2}ms → {:.2}ms)",
                                trend,
                                labels.method,
                                latency_change_percent,
                                prev.as_secs_f64() * 1000.0,
                                curr.as_secs_f64() * 1000.0
                            );
                        }
                    }
                    _ => {}
                }

                info!(
                    "Activity for {}: {} new requests, {} new errors",
                    labels.method, request_diff, error_diff
                );
            }
        }
    }
}

/// Example of creating custom metric labels for specific analysis
fn create_custom_analysis_labels() -> Vec<MetricLabels> {
    vec![
        MetricLabels::new("read", Some(1), "production"),
        MetricLabels::new("write_stream", Some(1), "production"),
        MetricLabels::new("list", Some(2), "production"),
        MetricLabels::new("create_session", None, "production"),
        MetricLabels::new("keep_alive", Some(1), "production"),
        MetricLabels::new("get_notifications", Some(1), "production"),
    ]
}

/// Print a summary table of all operations
fn print_metrics_table(metrics: &HashMap<MetricLabels, OperationMetrics>) {
    info!("📊 Metrics Summary Table");
    info!("┌─────────────────┬─────────┬─────────┬─────────────┬─────────────┬─────────────┐");
    info!("│ Method          │ Shard   │ Requests│ Errors      │ Avg Latency │ Error Rate  │");
    info!("├─────────────────┼─────────┼─────────┼─────────────┼─────────────┼─────────────┤");

    for (labels, metrics) in metrics {
        let shard_str = labels
            .shard_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| "N/A".to_string());
        let avg_latency = metrics
            .average_duration()
            .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| "N/A".to_string());
        let error_rate = format!("{:.1}%", metrics.error_rate() * 100.0);

        info!(
            "│ {:<15} │ {:<7} │ {:<7} │ {:<11} │ {:<11} │ {:<11} │",
            truncate_string(&labels.method, 15),
            truncate_string(&shard_str, 7),
            metrics.total_requests,
            metrics.total_errors,
            truncate_string(&avg_latency, 11),
            truncate_string(&error_rate, 11)
        );
    }

    info!("└─────────────────┴─────────┴─────────┴─────────────┴─────────────┴─────────────┘");
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}
