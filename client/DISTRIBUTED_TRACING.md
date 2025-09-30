# Distributed Tracing for Oxia Rust Client

This document provides a comprehensive guide to implementing distributed tracing for Oxia client operations, showing how high-level API calls connect to their constituent shard-level gRPC operations.

## 🎯 Overview

Distributed tracing in the Oxia client provides:

- **End-to-end visibility** - See how `Client::get_with_options()` fans out to multiple shard calls
- **Performance analysis** - Understand shard operation timing and efficiency
- **Hierarchical spans** - Root spans for API operations with child spans for each shard operation
- **Metrics integration** - Traces automatically feed into the metrics system
- **Error correlation** - Track errors across the entire operation tree

## 🏗️ Architecture

```
Client::get_with_options("key")
├─ Root Span: "Client::get_with_options" (trace_id: abc123)
├─ Child Span: "shard:read" (shard_id: 1, parent: abc123)
├─ Child Span: "shard:read" (shard_id: 2, parent: abc123) 
└─ Child Span: "shard:read" (shard_id: 3, parent: abc123)
```

Each span captures:
- Start/end timestamps
- Success/failure status
- Error messages (if applicable)
- Shard ID and operation type
- Hierarchical relationships

## 🚀 Quick Start

### Basic Setup

```rust
use mauricebarnum_oxia_client::{
    Client, ClientTracingExtension, DistributedTracingCollector,
    GrpcMetricsCollector, config
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create metrics collector
    let metrics = Arc::new(GrpcMetricsCollector::new());
    
    // 2. Create distributed tracing collector
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));
    
    // 3. Create and configure Oxia client
    let config = config::Builder::new()
        .service_addr("localhost:6544")
        .namespace("my_app")
        .build()?;
        
    let base_client = Client::with_config(config)?;
    
    // 4. Wrap with distributed tracing
    let traced_client = base_client.with_tracing(tracer.clone());
    
    // 5. Use normally - traces created automatically
    traced_client.get("my_key").await?;
    traced_client.put("my_key", b"my_value".to_vec()).await?;
    traced_client.list("prefix_", "prefix_z").await?;
    
    // 6. View trace analysis
    tracer.log_performance_analysis().await;
    
    Ok(())
}
```

### Factory Pattern

```rust
use mauricebarnum_oxia_client::TracedClientFactory;

// Create factory for multiple clients
let tracer = Arc::new(DistributedTracingCollector::new(metrics));
let factory = TracedClientFactory::new(tracer.clone());

// Create traced clients
let client1 = factory.create_client(base_client1);
let client2 = factory.create_client(base_client2);

// All operations automatically traced
client1.get("key1").await?;
client2.put("key2", value).await?;

// Analyze traces across all clients  
factory.tracer().log_performance_analysis().await;
```

## 📊 Understanding Traces

### Trace Structure

Each trace contains:

```rust
pub struct CompletedSpan {
    pub context: SpanContext,      // Trace ID, span ID, operation name
    pub duration: Duration,        // Total operation time
    pub success: bool,             // Success/failure status
    pub error_message: Option<String>, // Error details if failed
    pub child_spans: Vec<CompletedSpan>, // Shard operations
}
```

### Analyzing Traces

```rust
// Get all completed traces
let traces = tracer.get_completed_traces().await;

for trace in traces {
    println!("Operation: {}", trace.context.operation);
    println!("Duration: {}ms", trace.duration.as_millis());
    println!("Shard operations: {}", trace.shard_fan_out());
    println!("Success: {}", trace.success);
    
    // Analyze shard breakdown
    for shard_op in trace.get_shard_operations() {
        let shard_id = shard_op.context.attributes.get("shard_id");
        println!("  Shard {}: {}ms", 
                shard_id.unwrap_or(&"?".to_string()), 
                shard_op.duration.as_millis());
    }
}
```

### Operation Statistics

```rust
// Get stats for specific operation
let stats = tracer.get_operation_stats("Client::list_with_options").await;

println!("Operation: {}", stats.operation);
println!("Total traces: {}", stats.trace_count);
println!("Success rate: {:.1}%", stats.success_rate * 100.0);
println!("Average duration: {}ms", stats.avg_duration.as_millis());
println!("Average fan-out: {:.1} shards", stats.avg_fan_out);
println!("Average shard time: {}ms", stats.avg_shard_time.as_millis());
println!("Efficiency: {:.1}%", stats.avg_efficiency * 100.0);
```

## 🔧 Configuration

### Trace Collector Configuration

```rust
let tracer = DistributedTracingCollector::new(metrics);

// Configure maximum traces to keep in memory
let mut tracer = DistributedTracingCollector::new(metrics);
tracer.set_max_completed_traces(5000);

// Clear traces periodically for memory management  
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(300));
    loop {
        interval.tick().await;
        tracer.clear_completed_traces().await;
    }
});
```

### Integration with Structured Logging

```rust
use tracing::{info, instrument};

// Operations automatically include trace context in logs
#[instrument(skip(client))]
async fn business_operation(client: &TracedClient) -> Result<()> {
    info!("Starting business operation");
    
    let result = client.get("important_key").await?;
    info!("Retrieved data: {} bytes", result.map(|r| r.value.len()).unwrap_or(0));
    
    client.put("derived_key", b"processed_data".to_vec()).await?;
    info!("Stored processed data");
    
    Ok(())
}
```

## 📈 Advanced Usage

### Manual Span Creation

```rust
// Create custom spans for complex operations
let root_span = tracer.start_root_span("complex_business_operation").await;

// Child operations inherit trace context
if let Some(child_span) = root_span.create_child("data_validation", None).await {
    // Validate data
    let validation_result = validate_input().await;
    child_span.complete_with_result(&validation_result).await;
}

if let Some(child_span) = root_span.create_child("data_transformation", None).await {
    // Transform data
    let transform_result = transform_data().await;
    child_span.complete_with_result(&transform_result).await;
}

// Complete root span
root_span.complete_success().await;
```

### Custom Span Attributes

```rust
let mut context = SpanContext::new_root("custom_operation".to_string());
context = context
    .with_attribute("user_id".to_string(), "12345".to_string())
    .with_attribute("operation_type".to_string(), "batch_process".to_string())
    .with_attribute("batch_size".to_string(), "100".to_string());
```

### Trace Correlation with External Systems

```rust
// Extract trace ID for external system correlation
let traced_op = tracer.start_root_span("external_api_call").await;
let trace_id = traced_op.trace_id().as_str();

// Include trace ID in external API calls
let response = external_api_client
    .post("/api/endpoint")
    .header("X-Trace-ID", trace_id)
    .json(&request_data)
    .send()
    .await?;

traced_op.complete_success().await;
```

## 🔍 Monitoring and Analysis

### Real-time Monitoring

```rust
// Monitor traces in real-time
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        
        let traces = tracer.get_completed_traces().await;
        let recent_traces: Vec<_> = traces
            .iter()
            .filter(|t| t.context.start_time.elapsed() < Duration::from_secs(60))
            .collect();
            
        if !recent_traces.is_empty() {
            let avg_duration = recent_traces
                .iter()
                .map(|t| t.duration)
                .sum::<Duration>() / recent_traces.len() as u32;
                
            let error_rate = recent_traces.iter()
                .filter(|t| !t.success)
                .count() as f64 / recent_traces.len() as f64;
                
            info!(
                "📊 Last minute: {} ops, avg {}ms, {:.1}% errors",
                recent_traces.len(),
                avg_duration.as_millis(),
                error_rate * 100.0
            );
        }
    }
});
```

### Performance Alerts

```rust
async fn check_performance_alerts(tracer: &DistributedTracingCollector) {
    let traces = tracer.get_completed_traces().await;
    
    for trace in traces {
        // Alert on high latency
        if trace.duration > Duration::from_millis(1000) {
            warn!(
                "🐌 High latency detected: {} took {}ms with {} shards",
                trace.context.operation,
                trace.duration.as_millis(),
                trace.shard_fan_out()
            );
        }
        
        // Alert on high fan-out
        if trace.shard_fan_out() > 10 {
            warn!(
                "🌐 High fan-out: {} touched {} shards",
                trace.context.operation,
                trace.shard_fan_out()
            );
        }
        
        // Alert on low efficiency (high overhead)
        let efficiency = if trace.duration.as_nanos() > 0 {
            trace.total_shard_time().as_nanos() as f64 / trace.duration.as_nanos() as f64
        } else { 0.0 };
        
        if efficiency < 0.5 && trace.shard_fan_out() > 1 {
            warn!(
                "⚡ Low efficiency: {} only {:.1}% efficient",
                trace.context.operation,
                efficiency * 100.0
            );
        }
    }
}
```

### Export to External Tracing Systems

```rust
// Export traces to Jaeger/Zipkin format
async fn export_to_jaeger(traces: &[CompletedSpan]) -> Result<()> {
    for trace in traces {
        let jaeger_span = JaegerSpan {
            trace_id: trace.context.trace_id.as_str(),
            span_id: &trace.context.span_id,
            operation_name: &trace.context.operation,
            start_time: trace.context.start_time,
            duration: trace.duration,
            tags: trace.context.attributes.clone(),
            logs: if let Some(error) = &trace.error_message {
                vec![("error".to_string(), error.clone())]
            } else {
                vec![]
            },
        };
        
        jaeger_client.send_span(jaeger_span).await?;
    }
    
    Ok(())
}
```

## 🛠️ Integration Patterns

### Pattern 1: Service-Level Tracing

```rust
// Initialize tracing at service startup
#[tokio::main]
async fn main() -> Result<()> {
    let tracer = setup_distributed_tracing().await?;
    let client = create_traced_client(tracer.clone()).await?;
    
    // Start background monitoring
    start_trace_monitoring(tracer.clone());
    
    // Run application
    run_service(client).await?;
    
    Ok(())
}

async fn setup_distributed_tracing() -> Result<Arc<DistributedTracingCollector>> {
    let metrics = Arc::new(GrpcMetricsCollector::new());
    let tracer = Arc::new(DistributedTracingCollector::new(metrics));
    
    // Configure based on environment
    if std::env::var("ENABLE_TRACE_EXPORT").is_ok() {
        setup_trace_export(tracer.clone()).await?;
    }
    
    Ok(tracer)
}
```

### Pattern 2: Request-Scoped Tracing

```rust
// Create traces per request/transaction
async fn handle_request(
    client: &TracedClient, 
    request_id: &str
) -> Result<Response> {
    let tracer = client.tracer();
    let request_span = tracer
        .start_root_span(format!("handle_request_{}", request_id))
        .await;
    
    // All client operations within this scope inherit the trace
    let result = async {
        client.get("user_data").await?;
        client.put("session_data", session.to_bytes()).await?;
        client.list("user_items", "user_items_z").await?;
        Ok(build_response())
    }.await;
    
    request_span.complete_with_result(&result).await;
    result
}
```

### Pattern 3: Multi-Client Correlation

```rust
// Correlate operations across multiple clients
struct ServiceContext {
    user_client: TracedClient,
    session_client: TracedClient,
    tracer: Arc<DistributedTracingCollector>,
}

impl ServiceContext {
    async fn cross_client_operation(&self, user_id: &str) -> Result<()> {
        let operation_span = self.tracer
            .start_root_span("cross_client_operation".to_string())
            .await;
            
        // Operations across different clients share the same trace
        let user_data = self.user_client.get(&format!("user:{}", user_id)).await?;
        let session_data = self.session_client.get(&format!("session:{}", user_id)).await?;
        
        operation_span.complete_success().await;
        Ok(())
    }
}
```

## 📊 Metrics Integration

Distributed tracing automatically integrates with the metrics system:

```rust
// Traces feed into metrics automatically
let metrics = tracer.metrics(); // Gets the underlying metrics collector

// View combined metrics and trace data
let operation_metrics = metrics.get_operation_metrics(
    &MetricLabels::new("Client::get", Some(1), "production")
).await;

let operation_traces = tracer.get_traces_for_operation("Client::get").await;

println!("Metrics: {} requests, {:.1}% errors", 
         operation_metrics.total_requests,
         operation_metrics.error_rate() * 100.0);

println!("Traces: {} traces, avg {:.1} shard fan-out",
         operation_traces.len(),
         operation_traces.iter()
            .map(|t| t.shard_fan_out() as f64)
            .sum::<f64>() / operation_traces.len() as f64);
```

## 🔧 Troubleshooting

### Common Issues

**Memory Usage Growing**
```rust
// Implement periodic cleanup
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(300));
    loop {
        interval.tick().await;
        tracer.clear_completed_traces().await;
        info!("Cleared completed traces");
    }
});
```

**Traces Not Appearing**
```rust
// Check that operations are completing properly
let traced_op = tracer.start_root_span("test".to_string()).await;
// ⚠️ Make sure to complete the operation!
traced_op.complete_success().await; // Without this, trace won't be recorded
```

**High Memory Usage**
```rust
// Limit trace retention
let mut tracer = DistributedTracingCollector::new(metrics);
tracer.set_max_completed_traces(1000); // Keep only last 1000 traces
```

### Debug Information

Enable detailed logging:
```rust
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ENTER 
                     | tracing_subscriber::fmt::format::FmtSpan::CLOSE)
    .init();
```

## 🎯 Best Practices

1. **Always complete spans** - Use `complete_success()` or `complete_error()` 
2. **Use meaningful operation names** - Include context like "Client::batch_get" 
3. **Include relevant attributes** - Add user_id, batch_size, etc. for filtering
4. **Monitor trace volume** - Implement cleanup to prevent memory issues
5. **Correlate with external systems** - Export trace IDs to logs and external APIs
6. **Set appropriate retention** - Keep traces long enough for analysis but not too long
7. **Use structured logging** - Combine tracing with structured logs for full visibility

## 📚 Examples

Run the complete examples:

```bash
# Distributed tracing demonstration
cargo run --example distributed_tracing_example

# Integration with metrics
cargo run --example complete_metrics_integration
```

## 🔮 Advanced Topics

### Custom Trace Exporters

```rust
trait TraceExporter {
    async fn export(&self, traces: &[CompletedSpan]) -> Result<()>;
}

struct JaegerExporter { /* ... */ }
struct ZipkinExporter { /* ... */ }
struct DatadogExporter { /* ... */ }
```

### Sampling Strategies

```rust
// Implement sampling to reduce overhead in high-volume scenarios
struct SamplingTracer {
    inner: DistributedTracingCollector,
    sample_rate: f64,
}

impl SamplingTracer {
    async fn maybe_start_span(&self, operation: String) -> Option<TracedOperation> {
        if fastrand::f64() < self.sample_rate {
            Some(self.inner.start_root_span(operation).await)
        } else {
            None
        }
    }
}
```

### Trace Aggregation

```rust
// Aggregate traces for analysis
struct TraceAggregator {
    traces_by_operation: HashMap<String, Vec<CompletedSpan>>,
    traces_by_time: BTreeMap<SystemTime, Vec<CompletedSpan>>,
}

impl TraceAggregator {
    fn analyze_trends(&self) -> TrendAnalysis {
        // Implement trend analysis across time periods
        // - Latency trends
        // - Error rate changes  
        // - Fan-out pattern changes
        // - Performance regression detection
    }
}
```

---

**Distributed tracing provides complete visibility into Oxia client operations, from high-level API calls down to individual shard-level gRPC operations, enabling comprehensive performance analysis and debugging.**