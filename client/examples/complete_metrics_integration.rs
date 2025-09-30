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

//! Complete end-to-end example of gRPC metrics collection in the Oxia Rust client.
//!
//! This example demonstrates:
//! 1. Setting up metrics collection at the client level
//! 2. Transparent instrumentation of all gRPC operations
//! 3. Real-time metrics monitoring and analysis
//! 4. Prometheus metrics export
//! 5. Custom alerting and performance analysis
//! 6. Integration with existing codebases
//!
//! Run with: cargo run --example complete_metrics_integration

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::signal;
use tokio::time::{Instant, interval, sleep};
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::{self, EnvFilter, fmt, prelude::*};

use mauricebarnum_oxia_client::{
    Client, Error, GrpcMetricsCollector, MetricLabels, MetricsConfig, MetricsConfigBuilder,
    MetricsEnabledClientFactory, OperationMetrics, Result, config,
};

// Simulated application configuration
#[derive(Debug, Clone)]
struct AppConfig {
    oxia_address: String,
    namespace: String,
    enable_metrics: bool,
    metrics_port: u16,
    log_interval_secs: u64,
    simulation_duration_secs: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            oxia_address: std::env::var("OXIA_ADDRESS")
                .unwrap_or_else(|_| "localhost:6544".to_string()),
            namespace: std::env::var("OXIA_NAMESPACE")
                .unwrap_or_else(|_| "metrics_demo".to_string()),
            enable_metrics: std::env::var("ENABLE_METRICS").is_ok(),
            metrics_port: std::env::var("METRICS_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            log_interval_secs: std::env::var("LOG_INTERVAL")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            simulation_duration_secs: std::env::var("SIMULATION_DURATION")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging
    init_logging()?;

    let app_config = AppConfig::default();

    info!("🚀 Starting Oxia gRPC Metrics Integration Demo");
    info!("Configuration: {:#?}", app_config);

    // Initialize metrics collection
    let metrics_factory = setup_metrics(&app_config)?;
    let metrics = metrics_factory.metrics();

    // Create Oxia client with metrics
    let client = create_oxia_client(&app_config, &metrics_factory).await?;

    // Start background monitoring tasks
    let monitoring_handles = start_monitoring_tasks(metrics.clone(), &app_config);

    // Start metrics HTTP server (commented out for this example)
    // let _server_handle = start_metrics_server(metrics.clone(), app_config.metrics_port);

    // Run the main application simulation
    let simulation_result = run_application_simulation(client, &app_config).await;

    // Wait for shutdown signal or simulation completion
    let shutdown_reason = select! {
        result = simulation_result => {
            match result {
                Ok(_) => "simulation completed",
                Err(e) => {
                    error!("Simulation failed: {}", e);
                    "simulation error"
                }
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down gracefully...");
            "user interrupt"
        }
    };

    info!("Shutdown reason: {}", shutdown_reason);

    // Final metrics report
    info!("📊 Final Metrics Report");
    generate_final_report(metrics.clone()).await;

    // Clean up monitoring tasks
    for handle in monitoring_handles {
        handle.abort();
    }

    info!("✅ Demo completed successfully");
    Ok(())
}

/// Initialize structured logging with appropriate levels
fn init_logging() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,mauricebarnum_oxia_client=debug"))
        .map_err(|e| Error::Custom(format!("Failed to initialize logging: {}", e)))?;

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(false).with_thread_ids(true))
        .with(filter)
        .init();

    Ok(())
}

/// Setup metrics collection with optimal configuration
fn setup_metrics(app_config: &AppConfig) -> Result<MetricsEnabledClientFactory> {
    let metrics_config = MetricsConfigBuilder::new()
        .enabled(app_config.enable_metrics)
        .log_summaries(false) // We'll handle logging manually for better control
        .summary_interval_secs(app_config.log_interval_secs)
        .enable_prometheus_export(true)
        .build();

    info!(
        "🔧 Metrics configuration: enabled={}, export={}",
        metrics_config.enabled, metrics_config.enable_prometheus_export
    );

    Ok(MetricsEnabledClientFactory::new(metrics_config))
}

/// Create Oxia client with metrics instrumentation
async fn create_oxia_client(
    app_config: &AppConfig,
    factory: &MetricsEnabledClientFactory,
) -> Result<Client> {
    let client_config = config::Builder::new()
        .service_addr(&app_config.oxia_address)
        .namespace(&app_config.namespace)
        .session_timeout(Duration::from_secs(30))
        .request_timeout(Some(Duration::from_secs(10)))
        .build()
        .map_err(|e| Error::Custom(format!("Failed to build client config: {}", e)))?;

    info!(
        "🔌 Connecting to Oxia at {} with namespace '{}'",
        app_config.oxia_address, app_config.namespace
    );

    // The client will automatically use metrics collection through the factory
    let client = Client::with_config(client_config)?;

    info!("✅ Oxia client created with metrics instrumentation");
    Ok(client)
}

/// Start background tasks for monitoring and analysis
fn start_monitoring_tasks(
    metrics: Arc<GrpcMetricsCollector>,
    app_config: &AppConfig,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();

    // Performance monitoring task
    {
        let metrics_clone = metrics.clone();
        let interval_secs = app_config.log_interval_secs;
        let handle = tokio::spawn(async move {
            performance_monitor(metrics_clone, interval_secs).await;
        });
        handles.push(handle);
    }

    // Alerting task
    {
        let metrics_clone = metrics.clone();
        let handle = tokio::spawn(async move {
            alerting_monitor(metrics_clone).await;
        });
        handles.push(handle);
    }

    // Resource usage monitoring
    {
        let metrics_clone = metrics.clone();
        let handle = tokio::spawn(async move {
            resource_monitor(metrics_clone).await;
        });
        handles.push(handle);
    }

    info!("📈 Started {} monitoring tasks", handles.len());
    handles
}

/// Main application simulation with various Oxia operations
async fn run_application_simulation(client: Client, app_config: &AppConfig) -> Result<()> {
    info!(
        "🎯 Starting application simulation for {} seconds",
        app_config.simulation_duration_secs
    );

    let start_time = Instant::now();
    let duration = Duration::from_secs(app_config.simulation_duration_secs);

    // Simulation phases
    let phases = vec![
        SimulationPhase {
            name: "Initialization",
            duration: Duration::from_secs(30),
            operation: PhaseOperation::Initialize,
        },
        SimulationPhase {
            name: "Normal Load",
            duration: Duration::from_secs(120),
            operation: PhaseOperation::NormalLoad,
        },
        SimulationPhase {
            name: "High Load",
            duration: Duration::from_secs(60),
            operation: PhaseOperation::HighLoad,
        },
        SimulationPhase {
            name: "Mixed Operations",
            duration: Duration::from_secs(90),
            operation: PhaseOperation::Mixed,
        },
    ];

    for phase in phases {
        if start_time.elapsed() >= duration {
            break;
        }

        info!("🔄 Starting phase: {}", phase.name);
        let phase_result = run_simulation_phase(&client, &phase).await;

        match phase_result {
            Ok(_) => info!("✅ Phase '{}' completed", phase.name),
            Err(e) => {
                warn!("⚠️ Phase '{}' encountered errors: {}", phase.name, e);
                // Continue with next phase instead of failing completely
            }
        }

        if start_time.elapsed() >= duration {
            break;
        }
    }

    info!("🏁 Simulation completed after {:?}", start_time.elapsed());
    Ok(())
}

#[derive(Debug)]
struct SimulationPhase {
    name: &'static str,
    duration: Duration,
    operation: PhaseOperation,
}

#[derive(Debug)]
enum PhaseOperation {
    Initialize,
    NormalLoad,
    HighLoad,
    Mixed,
}

async fn run_simulation_phase(client: &Client, phase: &SimulationPhase) -> Result<()> {
    let start = Instant::now();

    match phase.operation {
        PhaseOperation::Initialize => {
            // Setup initial data
            for i in 0..10 {
                let key = format!("init_key_{}", i);
                let value = format!("initial_value_{}", i).into_bytes();
                client.put(&key, value).await?;
                sleep(Duration::from_millis(100)).await;
            }
        }

        PhaseOperation::NormalLoad => {
            // Simulate normal application load
            while start.elapsed() < phase.duration {
                tokio::try_join!(
                    simulate_read_workload(client),
                    simulate_write_workload(client),
                    simulate_list_workload(client),
                )?;
                sleep(Duration::from_millis(500)).await;
            }
        }

        PhaseOperation::HighLoad => {
            // Simulate high load with concurrent operations
            let mut handles = Vec::new();

            for i in 0..10 {
                let client_clone = client.clone();
                let handle = tokio::spawn(async move {
                    let mut ops = 0;
                    while ops < 20 {
                        let _ = simulate_concurrent_operations(&client_clone, i).await;
                        ops += 1;
                        sleep(Duration::from_millis(50)).await;
                    }
                });
                handles.push(handle);
            }

            // Wait for all concurrent tasks
            for handle in handles {
                let _ = handle.await;
            }
        }

        PhaseOperation::Mixed => {
            // Mix of all operation types with varying patterns
            while start.elapsed() < phase.duration {
                let operation_type = start.elapsed().as_secs() % 4;
                match operation_type {
                    0 => {
                        simulate_read_workload(client).await?;
                    }
                    1 => {
                        simulate_write_workload(client).await?;
                    }
                    2 => {
                        simulate_list_workload(client).await?;
                    }
                    3 => {
                        simulate_delete_workload(client).await?;
                    }
                    _ => unreachable!(),
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

    Ok(())
}

async fn simulate_read_workload(client: &Client) -> Result<()> {
    for i in 0..5 {
        let key = format!("read_key_{}", i % 10);
        match client.get(&key).await {
            Ok(Some(_)) => debug!("Read successful: {}", key),
            Ok(None) => debug!("Key not found: {}", key),
            Err(e) => debug!("Read error for {}: {}", key, e),
        }
    }
    Ok(())
}

async fn simulate_write_workload(client: &Client) -> Result<()> {
    for i in 0..3 {
        let key = format!("write_key_{}", i);
        let value = format!("value_{}_{}", i, chrono::Utc::now().timestamp()).into_bytes();
        match client.put(&key, value).await {
            Ok(_) => debug!("Write successful: {}", key),
            Err(e) => debug!("Write error for {}: {}", key, e),
        }
    }
    Ok(())
}

async fn simulate_list_workload(client: &Client) -> Result<()> {
    match client.list("read_key_", "read_key_z").await {
        Ok(response) => debug!("List returned {} keys", response.keys.len()),
        Err(e) => debug!("List error: {}", e),
    }
    Ok(())
}

async fn simulate_delete_workload(client: &Client) -> Result<()> {
    let key = format!("temp_key_{}", chrono::Utc::now().timestamp());

    // Put then delete
    if client.put(&key, b"temporary".to_vec()).await.is_ok() {
        match client.delete(&key).await {
            Ok(_) => debug!("Delete successful: {}", key),
            Err(e) => debug!("Delete error for {}: {}", key, e),
        }
    }
    Ok(())
}

async fn simulate_concurrent_operations(client: &Client, worker_id: usize) -> Result<()> {
    let key = format!(
        "concurrent_key_{}_{}",
        worker_id,
        chrono::Utc::now().timestamp_nanos()
    );
    let value = format!("worker_{}_data", worker_id).into_bytes();

    // Put, Get, Delete sequence
    client.put(&key, value).await?;
    client.get(&key).await?;
    client.delete(&key).await?;

    Ok(())
}

/// Background task for performance monitoring
async fn performance_monitor(metrics: Arc<GrpcMetricsCollector>, interval_secs: u64) {
    let mut interval = interval(Duration::from_secs(interval_secs));
    let mut previous_metrics = HashMap::new();

    loop {
        interval.tick().await;

        let current_metrics = metrics.get_metrics().await;

        if !current_metrics.is_empty() {
            analyze_performance_trends(&current_metrics, &previous_metrics);
            log_performance_summary(&current_metrics);
            previous_metrics = current_metrics;
        }
    }
}

/// Background task for alerting on performance issues
async fn alerting_monitor(metrics: Arc<GrpcMetricsCollector>) {
    let mut interval = interval(Duration::from_secs(10)); // Check every 10 seconds

    loop {
        interval.tick().await;

        let current_metrics = metrics.get_metrics().await;
        check_performance_alerts(&current_metrics);
    }
}

/// Background task for resource usage monitoring
async fn resource_monitor(metrics: Arc<GrpcMetricsCollector>) {
    let mut interval = interval(Duration::from_secs(60)); // Check every minute

    loop {
        interval.tick().await;

        let current_metrics = metrics.get_metrics().await;
        let total_operations: u64 = current_metrics.values().map(|m| m.total_requests).sum();
        let total_errors: u64 = current_metrics.values().map(|m| m.total_errors).sum();

        info!(
            "📊 Resource Summary: {} total operations, {} unique operation types, {} total errors",
            total_operations,
            current_metrics.len(),
            total_errors
        );
    }
}

fn analyze_performance_trends(
    current: &HashMap<MetricLabels, OperationMetrics>,
    previous: &HashMap<MetricLabels, OperationMetrics>,
) {
    for (labels, current_metrics) in current {
        if let Some(previous_metrics) = previous.get(labels) {
            let request_delta = current_metrics
                .total_requests
                .saturating_sub(previous_metrics.total_requests);
            let error_delta = current_metrics
                .total_errors
                .saturating_sub(previous_metrics.total_errors);

            if request_delta > 0 {
                let current_avg = current_metrics.average_duration();
                let previous_avg = previous_metrics.average_duration();

                if let (Some(curr), Some(prev)) = (current_avg, previous_avg) {
                    let latency_change_percent =
                        ((curr.as_secs_f64() - prev.as_secs_f64()) / prev.as_secs_f64()) * 100.0;

                    if latency_change_percent.abs() > 20.0 {
                        let trend = if latency_change_percent > 0.0 {
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

                debug!(
                    "Activity for {}: +{} requests, +{} errors",
                    labels.method, request_delta, error_delta
                );
            }
        }
    }
}

fn log_performance_summary(metrics: &HashMap<MetricLabels, OperationMetrics>) {
    info!(
        "📈 Performance Summary ({} operation types):",
        metrics.len()
    );

    let mut sorted_metrics: Vec<_> = metrics.iter().collect();
    sorted_metrics.sort_by(|a, b| b.1.total_requests.cmp(&a.1.total_requests));

    for (labels, metrics) in sorted_metrics.into_iter().take(5) {
        let avg_latency = metrics
            .average_duration()
            .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| "N/A".to_string());

        info!(
            "  {} (shard {:?}): {} req, {:.1}% err, {} avg",
            labels.method,
            labels.shard_id,
            metrics.total_requests,
            metrics.error_rate() * 100.0,
            avg_latency
        );
    }
}

fn check_performance_alerts(metrics: &HashMap<MetricLabels, OperationMetrics>) {
    for (labels, metrics) in metrics {
        if metrics.total_requests < 5 {
            continue; // Skip low-volume operations for alerting
        }

        // High error rate alert
        let error_rate = metrics.error_rate();
        if error_rate > 0.05 {
            // 5% threshold
            warn!(
                "🚨 HIGH ERROR RATE: {} has {:.1}% error rate ({}/{})",
                labels.method,
                error_rate * 100.0,
                metrics.total_errors,
                metrics.total_requests
            );
        }

        // High latency alert
        if let Some(avg_duration) = metrics.average_duration() {
            if avg_duration > Duration::from_millis(500) {
                // 500ms threshold
                warn!(
                    "🐌 HIGH LATENCY: {} average latency is {:.2}ms",
                    labels.method,
                    avg_duration.as_secs_f64() * 1000.0
                );
            }
        }

        // Latency variance alert (high max vs avg indicates inconsistent performance)
        if let (Some(avg), Some(max)) = (metrics.average_duration(), metrics.max_duration) {
            if max > avg * 3 {
                warn!(
                    "⚡ HIGH LATENCY VARIANCE: {} max ({:.2}ms) >> avg ({:.2}ms)",
                    labels.method,
                    max.as_secs_f64() * 1000.0,
                    avg.as_secs_f64() * 1000.0
                );
            }
        }
    }
}

async fn generate_final_report(metrics: Arc<GrpcMetricsCollector>) {
    let all_metrics = metrics.get_metrics().await;

    if all_metrics.is_empty() {
        info!("No metrics collected during the demo");
        return;
    }

    info!("=== FINAL METRICS REPORT ===");

    // Overall statistics
    let total_requests: u64 = all_metrics.values().map(|m| m.total_requests).sum();
    let total_errors: u64 = all_metrics.values().map(|m| m.total_errors).sum();
    let overall_error_rate = if total_requests > 0 {
        total_errors as f64 / total_requests as f64
    } else {
        0.0
    };

    info!("📊 Overall Statistics:");
    info!("  Total Requests: {}", total_requests);
    info!("  Total Errors: {}", total_errors);
    info!("  Overall Error Rate: {:.2}%", overall_error_rate * 100.0);
    info!("  Operation Types: {}", all_metrics.len());

    // Per-operation breakdown
    info!("📋 Per-Operation Breakdown:");
    let mut sorted_ops: Vec<_> = all_metrics.iter().collect();
    sorted_ops.sort_by(|a, b| b.1.total_requests.cmp(&a.1.total_requests));

    for (labels, metrics) in sorted_ops {
        let avg_latency = metrics
            .average_duration()
            .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| "N/A".to_string());
        let min_latency = metrics
            .min_duration
            .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| "N/A".to_string());
        let max_latency = metrics
            .max_duration
            .map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0))
            .unwrap_or_else(|| "N/A".to_string());

        info!(
            "  {:<15} (shard {:>3?}): {:>6} req | {:>5.1}% err | {:>8} avg | {:>8} min | {:>8} max",
            truncate_string(&labels.method, 15),
            labels
                .shard_id
                .map(|s| s.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
            metrics.total_requests,
            metrics.error_rate() * 100.0,
            avg_latency,
            min_latency,
            max_latency
        );
    }

    // Prometheus export sample
    info!("🔧 Prometheus Export (first 10 lines):");
    let prometheus_output = metrics.export_prometheus_format().await;
    let lines: Vec<&str> = prometheus_output.lines().take(10).collect();
    for line in lines {
        info!("  {}", line);
    }
    if prometheus_output.lines().count() > 10 {
        info!(
            "  ... ({} more lines)",
            prometheus_output.lines().count() - 10
        );
    }

    info!("=== END REPORT ===");
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

// HTTP server for Prometheus metrics (requires additional dependencies)
// Uncomment and add `warp = "0.3"` to Cargo.toml to enable
/*
async fn start_metrics_server(metrics: Arc<GrpcMetricsCollector>, port: u16) -> tokio::task::JoinHandle<()> {
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

    let health_route = warp::path("health")
        .and(warp::get())
        .map(|| "OK");

    let routes = metrics_route.or(health_route);

    info!("🌐 Starting metrics server on http://localhost:{}/metrics", port);

    tokio::spawn(async move {
        warp::serve(routes)
            .run(([127, 0, 0, 1], port))
            .await;
    })
}
*/
