# gRPC Metrics Collection for Oxia Rust Client

This document describes how to collect and analyze latency metrics for all gRPC calls made by the Oxia Rust client.

## Overview

The Oxia client provides comprehensive metrics collection for all gRPC operations, including:

- **Latency metrics**: Min, max, average, and histogram distributions
- **Request counts**: Total requests and error counts per operation
- **Error rates**: Percentage of failed requests
- **Operation breakdown**: Metrics per gRPC method, shard, and namespace
- **Prometheus export**: Standard format for monitoring systems

## Supported gRPC Operations

The following gRPC operations are automatically instrumented:

| Operation | Description | Streaming |
|-----------|-------------|-----------|
| `create_session` | Creates new client sessions | No |
| `keep_alive` | Session heartbeat maintenance | No |
| `read` | Get operations for retrieving values | Response streaming |
| `write_stream` | Put/delete operations | Bidirectional streaming |
| `list` | List keys in a range | Response streaming |
| `range_scan` | Scan ranges with filters | Response streaming |
| `get_notifications` | Subscribe to change notifications | Response streaming |

## Quick Start

### Basic Setup

```rust
use mauricebarnum_oxia_client::{
    Client, MetricsConfig, MetricsConfigBuilder, 
    MetricsEnabledClientFactory, config
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure metrics collection
    let metrics_config = MetricsConfig::default();
    let factory = MetricsEnabledClientFactory::new(metrics_config);
    
    // Create client configuration
    let client_config = config::Builder::new()
        .service_addr("localhost:6544")
        .namespace("my_app")
        .build()?;
    
    // Create client - all gRPC calls are now automatically instrumented
    let client = Client::with_config(client_config)?;
    
    // Use the client normally - metrics are collected transparently
    client.put("key1", b"value1".to_vec()).await?;
    let value = client.get("key1").await?;
    
    // View metrics summary
    factory.metrics().log_metrics_summary().await;
    
    Ok(())
}
```

### Advanced Configuration

```rust
let metrics_config = MetricsConfigBuilder::new()
    .enabled(true)                     // Enable/disable metrics collection
    .log_summaries(true)              // Automatically log periodic summaries
    .summary_interval_secs(300)       // Log every 5 minutes
    .enable_prometheus_export(true)   // Enable Prometheus format export
    .build();

let factory = MetricsEnabledClientFactory::new(metrics_config);
```

## Accessing Metrics

### Real-time Metrics Access

```rust
let metrics = factory.metrics();

// Get all metrics
let all_metrics = metrics.get_metrics().await;

// Get metrics for a specific operation
use mauricebarnum_oxia_client::MetricLabels;
let labels = MetricLabels::new("read", Some(1), "my_namespace");
let operation_metrics = metrics.get_operation_metrics(&labels).await;

if let Some(metrics) = operation_metrics {
    println!("Total requests: {}", metrics.total_requests);
    println!("Error rate: {:.2}%", metrics.error_rate() * 100.0);
    if let Some(avg) = metrics.average_duration() {
        println!("Average latency: {:.2}ms", avg.as_secs_f64() * 1000.0);
    }
}
```

### Manual Operation Timing

For custom instrumentation or existing code:

```rust
use mauricebarnum_oxia_client::with_grpc_metrics;

let metrics = Some(factory.metrics());

// Time a custom operation
let result = with_grpc_metrics!(
    metrics,
    "my_custom_operation",
    Some(shard_id),
    "my_namespace",
    {
        // Your gRPC operation here
        client.get("some_key").await
    }
);
```

## Prometheus Integration

### Export Metrics

```rust
// Export in Prometheus format
let prometheus_output = factory.export_prometheus().await;
println!("{}", prometheus_output);
```

### Serve Metrics Endpoint

```rust
// Example using warp (add to Cargo.toml: warp = "0.3")
use warp::Filter;

let metrics = factory.metrics();
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

warp::serve(metrics_route)
    .run(([127, 0, 0, 1], 3030))
    .await;
```

### Prometheus Metrics Format

The client exports the following Prometheus metrics:

```
# Total gRPC requests
oxia_grpc_requests_total{method="read",namespace="my_app",shard_id="1"} 100

# Total gRPC errors
oxia_grpc_request_errors_total{method="read",namespace="my_app",shard_id="1"} 2

# Request duration histogram
oxia_grpc_request_duration_seconds_bucket{method="read",namespace="my_app",shard_id="1",le="0.001"} 10
oxia_grpc_request_duration_seconds_bucket{method="read",namespace="my_app",shard_id="1",le="0.01"} 50
oxia_grpc_request_duration_seconds_bucket{method="read",namespace="my_app",shard_id="1",le="0.1"} 95
oxia_grpc_request_duration_seconds_bucket{method="read",namespace="my_app",shard_id="1",le="+Inf"} 100
oxia_grpc_request_duration_seconds_sum{method="read",namespace="my_app",shard_id="1"} 0.5
oxia_grpc_request_duration_seconds_count{method="read",namespace="my_app",shard_id="1"} 100
```

## Metric Labels

All metrics include the following labels for filtering and aggregation:

- `method`: The gRPC method name (e.g., "read", "write_stream", "list")
- `namespace`: The Oxia namespace being accessed
- `shard_id`: The shard ID (when applicable, otherwise omitted)

## Performance Impact

The metrics collection system is designed for minimal overhead:

- **Instrumentation**: ~1-5μs per operation
- **Memory**: ~100 bytes per unique operation type
- **Background processing**: Metrics aggregation is performed asynchronously
- **Disabling**: Set `enabled: false` to completely disable collection

## Monitoring and Alerting

### Common Alerting Rules

#### High Error Rate
```yaml
# Prometheus alerting rule
- alert: OxiaHighErrorRate
  expr: |
    (
      rate(oxia_grpc_request_errors_total[5m]) /
      rate(oxia_grpc_requests_total[5m])
    ) * 100 > 5
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High gRPC error rate detected"
    description: "{{ $labels.method }} operation has {{ $value }}% error rate"
```

#### High Latency
```yaml
- alert: OxiaHighLatency
  expr: |
    histogram_quantile(0.95, 
      rate(oxia_grpc_request_duration_seconds_bucket[5m])
    ) * 1000 > 100
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High gRPC latency detected"
    description: "95th percentile latency is {{ $value }}ms for {{ $labels.method }}"
```

### Custom Analysis

```rust
use std::collections::HashMap;

async fn analyze_performance_trends(metrics: Arc<GrpcMetricsCollector>) {
    let current_metrics = metrics.get_metrics().await;
    
    for (labels, operation_metrics) in current_metrics {
        // High error rate detection
        if operation_metrics.error_rate() > 0.05 { // 5%
            eprintln!("⚠️  High error rate: {} at {:.1}%", 
                     labels.method, operation_metrics.error_rate() * 100.0);
        }
        
        // Latency analysis
        if let Some(avg) = operation_metrics.average_duration() {
            if avg > Duration::from_millis(100) {
                eprintln!("🐌 High latency: {} at {:.2}ms average", 
                         labels.method, avg.as_secs_f64() * 1000.0);
            }
        }
        
        // Volume analysis
        if operation_metrics.total_requests > 1000 {
            println!("📊 High volume: {} with {} requests", 
                    labels.method, operation_metrics.total_requests);
        }
    }
}
```

## Configuration Options

### MetricsConfig

```rust
pub struct MetricsConfig {
    pub enabled: bool,                    // Enable/disable collection
    pub log_summaries: bool,             // Automatic summary logging
    pub summary_interval_secs: u64,      // Summary logging interval
    pub enable_prometheus_export: bool,   // Prometheus format support
}
```

### Builder Pattern

```rust
let config = MetricsConfigBuilder::new()
    .enabled(true)
    .log_summaries(false)          // Disable automatic logging
    .summary_interval_secs(60)     // 1 minute intervals
    .enable_prometheus_export(true)
    .build();
```

## Integration Patterns

### Existing Code Integration

For codebases already using the Oxia client:

1. **Minimal changes**: Replace client creation with metrics-enabled factory
2. **Gradual rollout**: Use feature flags to enable/disable metrics
3. **Backward compatibility**: All existing APIs work unchanged

### Microservices Pattern

```rust
// Service-wide metrics setup
static METRICS: OnceCell<Arc<GrpcMetricsCollector>> = OnceCell::new();

pub fn init_service_metrics() -> Arc<GrpcMetricsCollector> {
    METRICS.get_or_init(|| {
        let config = MetricsConfigBuilder::new()
            .enabled(std::env::var("METRICS_ENABLED").is_ok())
            .log_summaries(true)
            .summary_interval_secs(300)
            .build();
        
        let factory = MetricsEnabledClientFactory::new(config);
        factory.metrics()
    }).clone()
}
```

## Troubleshooting

### Common Issues

1. **No metrics collected**: Ensure `enabled: true` in config
2. **Missing shard IDs**: Some operations (like session creation) don't have shard context
3. **High memory usage**: Clear metrics periodically with `clear_metrics()`
4. **Performance impact**: Disable collection in performance-critical sections

### Debug Logging

Enable debug logs to see detailed timing information:

```rust
// Add to your tracing configuration
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

### Metric Validation

```rust
async fn validate_metrics(metrics: &GrpcMetricsCollector) {
    let all_metrics = metrics.get_metrics().await;
    
    for (labels, operation_metrics) in all_metrics {
        // Validate consistency
        assert!(operation_metrics.total_errors <= operation_metrics.total_requests);
        
        // Check for reasonable values
        if let Some(avg) = operation_metrics.average_duration() {
            if avg > Duration::from_secs(60) {
                println!("⚠️  Suspicious average latency for {}: {:?}", 
                        labels.method, avg);
            }
        }
    }
}
```

## Examples

See `examples/grpc_metrics_example.rs` for a comprehensive demonstration including:

- Basic metrics setup
- Advanced configuration
- Prometheus export
- Custom analysis and alerting
- Performance monitoring
- Real-time metrics analysis

Run the example:

```bash
cargo run --example grpc_metrics_example
```

## Best Practices

1. **Enable by default**: Metrics have minimal overhead and provide valuable insights
2. **Use namespaces**: Separate metrics by service/environment using namespaces
3. **Monitor error rates**: Set up alerting for error rates > 1-5%
4. **Track latency trends**: Monitor 95th percentile latency over time
5. **Periodic cleanup**: Clear metrics periodically in long-running applications
6. **Resource monitoring**: Monitor memory usage with high-volume applications

## Contributing

To add new metrics or improve existing ones:

1. Add instrumentation in `src/instrumented_client.rs`
2. Update metric labels in `src/metrics.rs`
3. Add tests in the respective test modules
4. Update this documentation

## License

Licensed under the Apache License, Version 2.0. See the main project LICENSE file for details.