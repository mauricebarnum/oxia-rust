# OpenTelemetry Instrumentation Design: Rust Oxia Client

This document provides a detailed design and implementation specification for adding OpenTelemetry (OTel) instrumentation (spans, metrics, and structured logs) to the Rust Oxia client library (`client` crate).

---

# Part A: Design Narrative (For Human Review)

## 1. Executive Summary

- **Proposed:** This design defines a standard, idiomatic telemetry layer for the Rust Oxia client library (`client` crate), aligned with OpenTelemetry Semantic Conventions version `v1.41.0`.
- **Proposed:** Telemetry will include:
  - **Traces (Spans):** High-level client operation boundaries (e.g., `get`, `put`, `delete`, etc.), client connection, background task events (discovery refreshes, heartbeat loops), and lower-level gRPC RPC calls.
  - **Metrics:** Client operation latencies, batching latencies, value/payload sizes, and request counts, ensuring behavioral and namespacing consistency with the Go reference library (`ext/oxia`).
  - **Logs:** Structured log correlation linking existing logging calls to active spans.
- **Proposed:** The following are out of scope:
  - Configuring exporters, tracing subscribers, or SDK pipelines within the library. The client library will only depend on telemetry APIs/facades.
  - System-level resource monitoring (CPU, memory), which is the responsibility of the server or the consumer application.

### 1.1 Current Implementation Summary

- **Implemented:** Direct OpenTelemetry metrics are compiled behind the optional `metrics` Cargo feature. The default build has no `opentelemetry` dependency and no metric timing, duration math, size extraction, or metric recording work.
- **Implemented:** Public operation metrics are centralized in `client/src/metrics.rs` through `operation_start`, `record_operation_result`, `record_operation_result_with_size`, and `record_operation_sync`.
- **Implemented:** Shard batch metrics are centralized in `client/src/metrics.rs` through `batch_start`, `batch_exec_duration`, and `record_batch`, preserving total duration, execution duration, payload size, and request count.
- **Implemented:** Operation spans use `#[tracing::instrument]` where practical. `Client::connect` and `Client::batch_get` keep manual spans because their setup flow is more awkward to express with the attribute macro.
- **Implemented:** `error.type` recording remains unconditional through `metrics::record_error_type`, including default builds where the `metrics` feature is disabled.
- **Implemented:** `go-metrics-compat` implies `metrics` and selects Go-compatible metric names and timer instrument shapes at compile time.

---

## 2. Repository Findings

### 2.1 Rust Crate Inventory

- **Observed:** **Public API Surface** (`client/src/lib.rs`):
  - `Client::new(config: Arc<config::Config>) -> Self` (line 869)
  - `Client::connect(&mut self) -> Result<()>` (line 886)
  - `Client::reconnect(&mut self) -> impl Future<Output = Result<()>>` (line 895)
  - `Client::get(&self, k: impl Into<String>) -> impl Future<Output = Result<Option<GetResponse>>>` (line 960)
  - `Client::get_with_options(&self, key: impl Into<String>, options: GetOptions) -> impl Future<Output = Result<Option<GetResponse>>>` (line 965)
  - `Client::batch_get(&self, req: batch_get::Request) -> Result<impl Stream<Item = batch_get::ResponseItem>>` (line 1031)
  - `Client::put(&self, key: impl Into<String>, value: impl Into<Bytes>) -> impl Future<Output = Result<PutResponse>>` (line 1087)
  - `Client::put_with_options(&self, key: impl Into<String>, value: impl Into<Bytes>, options: PutOptions) -> impl Future<Output = Result<PutResponse>>` (line 1096)
  - `Client::delete(&self, key: impl Into<String>) -> impl Future<Output = Result<()>>` (line 1124)
  - `Client::delete_with_options(&self, key: impl Into<String>, options: DeleteOptions) -> impl Future<Output = Result<()>>` (line 1128)
  - `Client::delete_range(&self, start_inclusive: impl Into<String>, end_exclusive: impl Into<String>) -> impl Future<Output = Result<()>>` (line 1150)
  - `Client::delete_range_with_options(&self, start_inclusive: impl Into<String>, end_exclusive: impl Into<String>, options: DeleteRangeOptions) -> impl Future<Output = Result<()>>` (line 1163)
  - `Client::list(&self, start_inclusive: impl Into<String>, end_exclusive: impl Into<String>) -> impl Future<Output = Result<ListResponse>>` (line 1242)
  - `Client::list_with_options(&self, start_inclusive: impl Into<String>, end_exclusive: impl Into<String>, options: ListOptions) -> impl Future<Output = Result<ListResponse>>` (line 1251)
  - `Client::range_scan(&self, start_inclusive: impl Into<String>, end_exclusive: impl Into<String>) -> impl Future<Output = Result<RangeScanResponse>>` (line 1325)
  - `Client::range_scan_with_options(&self, start_inclusive: impl Into<String>, end_exclusive: impl Into<String>, options: RangeScanOptions) -> impl Future<Output = Result<RangeScanResponse>>` (line 1334)
  - `Client::create_notifications_stream(&self) -> Result<NotificationsStream>` (line 1408)
  - `Client::create_notifications_stream_with_options(&self, options: NotificationsOptions) -> Result<NotificationsStream>` (line 1413)
- **Observed:** **Async Entry Points:**
  - `Client::connect` (file `client/src/lib.rs`, line 886)
  - `Client::get_internal` (file `client/src/lib.rs`, line 973)
  - `Client::put_internal` (file `client/src/lib.rs`, line 1105)
  - `Client::delete_internal` (file `client/src/lib.rs`, line 1137)
  - `Client::delete_range_internal` (file `client/src/lib.rs`, line 1176)
  - `Client::list_internal` (file `client/src/lib.rs`, line 1264)
  - `Client::range_scan_internal` (file `client/src/lib.rs`, line 1347)
  - `shard::Client::get` (file `client/src/shard.rs`, line 796)
  - `shard::Client::put` (file `client/src/shard.rs`, line 958)
  - `shard::Client::delete` (file `client/src/shard.rs`, line 984)
  - `shard::Client::delete_range` (file `client/src/shard.rs`, line 996)
  - `shard::Client::list` (file `client/src/shard.rs`, line 1009)
  - `shard::Client::range_scan` (file `client/src/shard.rs`, line 1059)
  - `shard::Client::get_notifications` (file `client/src/shard.rs`, line 1091)
  - `shard::Client::create_session` (file `client/src/shard.rs`, line 579)
- **Observed:** **Background Tasks:**
  - Shard assignment syncing loop `ShardAssignmentTask::run` (file `client/src/shard.rs`, line 1642)
  - Session heartbeat loop task spawned in `Client::start_heartbeat` (file `client/src/shard.rs`, line 483)
  - Notification stream change listener task spawned in `NotificationsStream::new` (file `client/src/notification.rs`, line 163)
- **Observed:** **Retry Loops:**
  - Transient/timeout retries in `execute_with_retry` (file `client/src/lib.rs`, line 741)
  - Wrong-leader routing retries in `stale_shard_map_retry` (file `client/src/lib.rs`, line 803)
  - Shard assignment stream reconnection loop in `ShardAssignmentTask::get_stream` (file `client/src/shard.rs`, line 1611)
- **Observed:** **Network Operations:**
  - Connecting gRPC channel in `ChannelPool::get` (file `client/src/pool.rs`, line 177)
  - Tonic gRPC calls:
    - `get_shard_assignments` (file `client/src/shard.rs`, line 1596)
    - `create_session` (file `client/src/shard.rs`, line 587)
    - `keep_alive` (file `client/src/shard.rs`, line 524)
    - `write_stream` (file `client/src/shard.rs`, line 767)
    - `read` (file `client/src/shard.rs`, line 799)
    - `list` (file `client/src/shard.rs`, line 1020)
    - `range_scan` (file `client/src/shard.rs`, line 1071)
    - `get_notifications` (file `client/src/shard.rs`, line 1101)
    - `list_nodes` (file `client/src/discovery.rs`, line 109)
- **Observed:** **Streaming Operations:**
  - Shard assignment updates stream `Streaming<oxia_proto::ShardAssignments>` (file `client/src/shard.rs`, line 1559)
  - Notifications stream `Streaming<oxia_proto::NotificationBatch>` (file `client/src/shard.rs`, line 1445)
  - `batch_get` result stream demuxing `demux_stream` (file `client/src/shard.rs`, line 834)
- **Observed:** **Batching Operations:**
  - Multi-key read batching in `Client::batch_get` (file `client/src/lib.rs`, line 1031)
- **Observed:** **Error Types** (`client/src/errors.rs`):
  - `Error` (line 320), `OxiaError` (line 31), `OxiaRpcError` (line 61), `ClientError` (line 233), `ServerError` (line 278), `ShardError` (line 304).
- **Observed:** **Concurrency Boundaries:**
  - `ArcSwap` for lock-free state swaps (file `client/src/pool.rs`, line 69; file `client/src/shard.rs`, line 1665, 1666)
  - `Mutex` for synchronizing channel creations (file `client/src/pool.rs`, line 70)
  - `OnceCell` for lazy session creation (file `client/src/shard.rs`, line 332, 598)
  - `watch::Sender` / `watch::Receiver` for notifying updates (file `client/src/shard.rs`, line 1669, 1808)
  - `tokio::sync::Notify` for refresh signals (file `client/src/shard.rs`, line 1670, 1819)
- **Observed:** **Task Spawning Boundaries:**
  - Spawning heartbeat loops (file `client/src/shard.rs`, line 483)
  - Spawning assignment sync loops (file `client/src/shard.rs`, line 1821)
  - Spawning configuration watcher tasks (file `client/src/notification.rs`, line 163)
- **Observed:** **Cancellation Semantics:**
  - Background loops are monitored by `CancellationToken` (file `client/src/shard.rs`, line 298, 1667; file `client/src/notification.rs`, line 150) and handles are aborted on drop.
- **Observed:** **Existing Telemetry Hooks:**
  - Crate depends on `tracing` (file `client/Cargo.toml`, line 15) and logs extensively via `info!`, `warn!`, `debug!`, `error!`, and `trace!`.

### 2.2 Go Reference Instrumentation Inventory

- **Observed:** **Metrics** (`ext/oxia/oxia/internal/metrics/metrics.go`):
  - `oxia_client_op` (Timer: sum counter and count counter): records call latency.
  - `oxia_client_op_value` (Histogram, unit: `"By"`): records KV size for `put` and `get`.
  - `oxia_client_batch_total` (Timer: sum counter and count counter): records time from request queueing to response.
  - `oxia_client_batch_exec` (Timer: sum counter and count counter): records time elapsed during batch execution.
  - `oxia_client_batch_value` (Histogram, unit: `"By"`): records total bytes processed in a batch.
  - `oxia_client_batch_request` (Histogram): records count of requests inside a batch.
  - Attributes registered per metric: `"type"` (operation name, e.g., `"get"`, `"put"`) and `"result"` (`"success"` or `"failure"`).
- **Observed:** **Traces (Spans):**
  - The Go client has no tracing instrumentation. `go.opentelemetry.io/otel/trace` is only listed as an `// indirect` dependency in `ext/oxia/oxia/go.mod` (line 40).
- **Observed:** **Structured Log Events:**
  - Go client uses structured logging (`log/slog`) for system state:
    - retry warnings (file `ext/oxia/oxia/async_client_impl.go`, line 433, 531)
    - notification events (file `ext/oxia/oxia/cache.go`, line 156)
    - batch rerouting notices (file `ext/oxia/oxia/internal/batch/read_batch.go`, line 96; file `ext/oxia/oxia/internal/batch/write_batch.go`, line 117)
    - connection warning/errors (file `ext/oxia/oxia/internal/executor.go`, line 153)

### 2.3 Cross-Language Mapping and Differences

- **Observed:** **Go-to-Rust Equivalency mapping:**
  - `DecoratePut` / `DecorateGet` / `DecorateDelete` -> Map directly to wrapping `Client::put_internal`, `Client::get_internal`, `Client::delete_internal` and `Client::batch_get` operations in `client/src/lib.rs`.
  - `WriteCallback` / `ReadCallback` -> Maps to wrappers around `shard::Client::process_write` and `shard::Client::get`/`batch_get` (file `client/src/shard.rs`).
- **Observed:** **Rust operations with no Go equivalent:**
  - `Client::connect` (file `client/src/lib.rs`, line 886) - Go client performs lazy connection at executor level.
  - `Client::reconnect` (file `client/src/lib.rs`, line 895).
  - `stale_shard_map_retry` (file `client/src/lib.rs`, line 803) - explicit stale-map wait loop in Rust client.
- **Observed:** **Go instrumentation with no Rust equivalent:**
  - Go's custom `Timer` metric type (represented in Rust compat mode as a sum `Float64Counter` and a count `Counter<u64>`).
  - Go's unstructured/slog logger. Rust utilizes `tracing` exclusively.

---

## 3. Goals, Constraints, and Assumptions

### 3.1 Telemetry Framework Choice

- **Implemented:** The library uses the **`tracing` crate** as its tracing and logging facade, and the optional **`opentelemetry` API crate** with its `metrics` feature as its metrics facade.
- **Justification:**
  1. The `tracing` crate is the de facto standard in Rust. Using it ensures all spans and logs integrate out-of-the-box with downstream subscriber ecosystems (such as `tracing-opentelemetry`).
  2. Direct instrumentation using the `opentelemetry` tracing API inside a Rust library requires handling a heavy API layer, which is less idiomatic and more difficult to format and correlate than Rust's native `tracing::Span` structures.
  3. Spans can be bridged downstream using the standard `tracing-opentelemetry` subscriber.
- **Implemented:** The library interacts only with the facade/API layers (`tracing` and `opentelemetry::metrics`). It does not instantiate or configure exporters, tracing subscribers, SDK pipelines, or metric exporters internally, but it supports user-supplied `MeterProvider` configurations.
- **Assumption:** If no `tracing` subscriber is registered by the parent application, tracing calls are handled by the normal `tracing` no-subscriber fast path.
- **Implemented:** To ensure zero OTel metric overhead when metrics are disabled, direct OTel metrics logic is conditionally compiled behind the `metrics` feature flag (see Section 8).

---

## 4. Instrumentation Boundaries

### 4.1 Boundary Scope

- **Implemented:** **Instrumented operations:**
  - `Client::connect` (to track cluster connectivity).
  - Main KV API entry points (`get`, `put`, `delete`, `delete_range`, `list`, `range_scan`, `batch_get`).
  - Shard read/write batch paths.
- **Planned:** **Additional instrumentation boundaries:**
  - `Client::reconnect`.
  - Coordinator discovery requests (`discovery::CoordinatorServiceDiscovery::addresses`).
  - Heartbeat loop cycles (to monitor session health).
  - Background shard assignment sync loop runs.
- **Proposed:** **Explicitly non-instrumented operations:**
  - Lower-level internal utilities (`util::with_timeout`, `util::with_retry`) to avoid duplicate spans. Span details will instead be captured as attributes or retry events on the parent operation span.

### 4.2 Architectural Design Decisions

- **Decision 1: Spans representation**
  - **Selected Approach:** Maintain spans using `tracing::Span` and rely on downstream `tracing-opentelemetry` for OTel exporting.
  - **Rejected Alternative:** Directly use the `opentelemetry::trace::Tracer` API in-crate.
  - **Reason for Rejection:** Direct OTel trace API makes logs harder to correlate and diverges from the existing `tracing` macros already used in the codebase.
- **Decision 2: Latency metrics implementation**
  - **Selected Approach (default / OTel mode):** Use OTel `Histogram` for operation latencies. A histogram provides percentiles, count, and sum natively without duplicating instruments.
  - **Selected Approach (`go-metrics-compat` mode):** Register a `Float64Counter` (`_sum`) and a `Counter<u64>` (`_count`) as separate instruments to exactly replicate the Go wire format. This is intentional: dashboards built against the Go client expect two distinct series.
  - **Rejected Alternative for default mode:** Translate Go's dual-counter Timer approach literally. Rejected because it is not idiomatic in modern OpenTelemetry.

---

## 5. Tracing and Logging Design

### 5.1 Naming and Scope

- **Implemented:** The tracing and metrics scope name is `"mauricebarnum_oxia_client"`.
- **Implemented:** Spans map to operations:
  - `"client.connect"` (SpanKind::Client)
  - `"db.operation.get"` (SpanKind::Client)
  - `"db.operation.put"` (SpanKind::Client)
  - `"db.operation.delete"` (SpanKind::Client)
  - `"db.operation.delete_range"` (SpanKind::Client)
  - `"db.operation.list"` (SpanKind::Client)
  - `"db.operation.range_scan"` (SpanKind::Client)
  - `"db.operation.batch_get"` (SpanKind::Client)
  - `"shard.heartbeat"` (SpanKind::Internal, planned)
  - `"shard.assignment_sync"` (SpanKind::Internal, planned)

### 5.2 Attributes (OpenTelemetry Semantic Conventions v1.41.0)

- **Implemented:** Spans representing database operations carry:
  - `db.system`: `"oxia"` (static string)
  - `db.name`: Namespace name from configuration (e.g., `"default"`)
  - `db.operation`: Operation identifier (`"get"`, `"put"`, `"delete"`, etc.)
  - `db.oxia.shard_id`: The ID of the shard targeted by the routing key, when known
  - `error.type`: The error string code on failure (e.g., `"KeyNotFound"`, `"TransportError"`)

`server.address` and `server.port` are not currently recorded on public operation spans.

### 5.3 Error and Status Rules

- **Implemented:** `error.type` is recorded on the current `tracing` span for `Err` results through `metrics::record_error_type`, regardless of whether the `metrics` feature is enabled.
- **Implemented:** `get` treats `Err(Error::Oxia(OxiaError::KeyNotFound))` as a metrics success while still preserving the returned error and `error.type` span field.
- **Not Implemented:** Span status mutation and `exception.message` events are not currently recorded by the client instrumentation.
- **Not Implemented:** Operation wrappers do not catch panics. Panics propagate normally.

### 5.4 Sampling and Logging Integration

- **Implemented:** Existing logs inherit active `tracing` span context when the caller installs a compatible subscriber.

---

## 6. Metrics Design

### 6.1 Metrics Specifications

- **Implemented:** In default mode, the client registers the following metrics when the `metrics` feature is enabled:
  - `"db.client.operation.duration"` (Histogram, unit: `"ms"`): Measures operation execution duration.
  - `"db.client.operation.size"` (Histogram, unit: `"By"`): Measures payload sizes of `put` values and `get`/`range_scan` returned values.
  - `"db.client.batch.duration"` (Histogram, unit: `"ms"`): Measures total duration of batched requests (queueing + execution).
  - `"db.client.batch.exec_duration"` (Histogram, unit: `"ms"`): Measures actual network execution duration of batches.
  - `"db.client.batch.size"` (Histogram, unit: `"By"`): Measures total payload size in a batch.
  - `"db.client.batch.request_count"` (Histogram, unit: `"1"`): Measures count of operations packed in a batch.

### 6.2 Attributes and Bounded Cardinality

- **Implemented:** All metrics use the following dimensions:
  - `db.system`: Bounded (Value: `"oxia"`)
  - `db.name` (Namespace): Bounded (Value from config)
  - `db.operation`: Bounded (Values: `"get"`, `"put"`, `"delete"`, etc.)
  - `result`: Bounded (Values: `"success"`, `"failure"`)
- **Implemented:** Shard IDs are intentionally not metric attributes. This keeps the public metric dimension set small and bounded.
- **Proposed:** **Cardinality Risk Flag:** No dynamic, user-provided data (such as keys, values, or raw error messages containing user parameters) may be used as metric dimensions. Recording arbitrary keys as dimensions is strictly prohibited due to infinite cardinality risks.

---

## 7. Context Propagation and Concurrency Design

### 7.1 Context over Boundaries

- **Proposed:** Context enters the client via the implicit thread-local/async `tracing::Span::current()`.
- **Implemented:** Public operation spans enter through the implicit thread-local/async `tracing::Span::current()` context and are exported only if the caller installs a compatible subscriber.
- **Proposed:** Since `client` operations are async and span boundaries, downstream applications should bridge via `tracing-opentelemetry`:
  - **Bridge via `tracing-opentelemetry`**: This handles propagating trace headers over spawned task boundaries (like the heartbeat and discovery tasks) using `tracing` parent span attachment.
- **Proposed:** For background tasks (`ShardAssignmentTask` and heartbeats), we will construct detached root spans `"shard.assignment_sync"` and `"shard.heartbeat"`.
- **Not Implemented:** Caller cancellation does not currently record a `"cancelled"` event. The span closes when the instrumented future is dropped.

---

## 8. Public API and Internal Structure Impact

### 8.1 Feature Gating Decision

- **Implemented:** Option B is used: direct OpenTelemetry metrics instrumentation is feature-gated and opt-in. The `opentelemetry` dependency is compiled only when downstream crates enable the `metrics` feature flag.
- **Rejected Alternative A (Always compiled):** Forces all users to download and compile `opentelemetry` API crates, increasing compile time and dependency trees for minimal-size environments.
- **Rejected Alternative C (Opt-out):** Violates Rust best practices by enabling a large transitive dependency tree by default.

### 8.2 Dependency and API Impact

- **Implemented:** `opentelemetry` is an optional dependency under `[dependencies]` in `client/Cargo.toml`.
- **Implemented:** The `metrics` feature enables the optional dependency.
- **Implemented:** `Config` and `ConfigBuilder` expose a `meter_provider` field and setter under the `metrics` feature flag. This allows downstream consumers to explicitly inject custom OTel `MeterProvider` implementations, mapped as `Arc<dyn opentelemetry::metrics::MeterProvider + Send + Sync>`. If not set, metrics fallback to `opentelemetry::global::meter_provider()`.
- **Proposed:** MSRV impact is negligible, as `opentelemetry` supports modern stable Rust compilers matching the workspace's version.

### 8.3 Metric Naming Feature Flag: `go-metrics-compat`

- **Resolved:** Add a second Cargo feature, `go-metrics-compat`, that selects the Go reference client's legacy metric name strings **and instrument types** at **compile time** rather than at runtime.
- **Rationale:** The two naming schemes (`db.client.operation.duration` vs `oxia_client_op`) differ in both string constants and instrument shape. Selecting between them with `#[cfg(feature = "go-metrics-compat")]` eliminates all branching at runtime — the unused variant is never materialized in the binary. This satisfies the requirement of zero runtime overhead.
- **Resolved:** `go-metrics-compat` implies `metrics` in the Cargo feature graph. This prevents silent no-ops and makes the dependency relationship explicit.
- **Resolved:** `go-metrics-compat` mode registers **two separate instruments** per timer concept (a `Float64Counter` for `_sum` and a `Counter<u64>` for `_count`) to exactly replicate Go wire format. Default (OTel) mode uses a single `Histogram`.
- **Implemented:** The Cargo feature declaration:

  ```toml
  # client/Cargo.toml
  [features]
  metrics           = ["dep:opentelemetry"]
  go-metrics-compat = ["metrics"]   # implies metrics
  ```

- **Implemented:** Inside `client/src/metrics.rs`, the instrument types and name strings are both selected by feature flags:

  ```rust
    // ── Default (OTel) mode ──────────────────────────────────────────────────
  #[cfg(not(feature = "go-metrics-compat"))]
  mod names {
      pub const OP_DURATION:         &str = "db.client.operation.duration";
      pub const OP_SIZE:             &str = "db.client.operation.size";  // proposed OTel extension
      pub const BATCH_TOTAL:         &str = "db.client.batch.duration";
      pub const BATCH_EXEC:          &str = "db.client.batch.exec_duration";
      pub const BATCH_SIZE:          &str = "db.client.batch.size";
      pub const BATCH_REQUEST_COUNT: &str = "db.client.batch.request_count";
  }

  // ── go-metrics-compat mode ───────────────────────────────────────────────
  // Timer concepts split into _sum (Float64Counter) + _count (Counter<u64>)
  // matching the Go client's wire format exactly.
  #[cfg(feature = "go-metrics-compat")]
  mod names {
      // sum counters
      pub const OP_DURATION_SUM:         &str = "oxia_client_op_sum";
      pub const BATCH_TOTAL_SUM:         &str = "oxia_client_batch_total_sum";
      pub const BATCH_EXEC_SUM:          &str = "oxia_client_batch_exec_sum";
      // count counters
      pub const OP_DURATION_COUNT:       &str = "oxia_client_op_count";
      pub const BATCH_TOTAL_COUNT:       &str = "oxia_client_batch_total_count";
      pub const BATCH_EXEC_COUNT:        &str = "oxia_client_batch_exec_count";
      // histograms (same in both modes)
      pub const OP_SIZE:                 &str = "oxia_client_op_value";
      pub const BATCH_SIZE:              &str = "oxia_client_batch_value";
      pub const BATCH_REQUEST_COUNT:     &str = "oxia_client_batch_request";
  }
  ```

  In default mode, all registrations use `Histogram`. In `go-metrics-compat` mode, timer-concept registrations create one `Float64Counter` (sum) and one `Counter<u64>` (count); size/request-count concepts remain `Histogram`.

- **Data migration:** Explicitly out of scope. Users switching feature sets will see a gap or duplicate series in their observability backend. The library documentation should note this.

---

## 9. Semantic Conventions and Cross-Language Consistency

- **Proposed:** Align strictly with OTel Semantic Conventions version `v1.41.0` (database client namespace) by default.
- **Proposed:** The `go-metrics-compat` Cargo feature (Section 8.3) is the **only supported mechanism** for selecting Go-compatible metric names. No runtime configuration knob is exposed for this choice — the selection is baked in at compile time.

### 9.1 Technology-Agnostic Naming Rationale

- **Proposed:** **Handling Technology-Agnostic Naming:** To support setups where Oxia serves as an internal coordinator/metadata engine inside a larger database system (similar to etcd/zookeeper configurations):
  - **Database system isolation:** Differentiating nested telemetry calls (e.g. outer database system client vs inner coordinator/metadata database client) is done purely via attributes. The inner Oxia calls will carry `db.system = "oxia"`, whereas the outer system client calls will carry `db.system = "my_custom_db"`.
  - **Unified dashboard querying:** Using the standard semantic convention name `db.client.operation.duration` across both clients enables operators to aggregate database latency metrics globally or slice them by `db.system` to analyze the exact overhead contribution of the coordination layer.

### 9.2 Name Mapping Reference

The table below is the authoritative mapping between the two naming regimes. The **`go-metrics-compat` name** column lists the exact string constants the Go reference client records metrics under; see Section 2.2 for observation source references.

| Concept                           | Default (OTel v1.41.0) name                       | `go-metrics-compat` instrument(s)                                            | Notes                                                                          |
| --------------------------------- | ------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Operation latency (sum)           | `db.client.operation.duration` (Histogram)        | `oxia_client_op_sum` (Float64Counter)                                        | Default: single Histogram. Compat: separate sum + count counters               |
| Operation latency (count)         | _(provided by Histogram internally)_              | `oxia_client_op_count` (Counter<u64>)                                        | Compat-mode only; not a separate instrument in default mode                    |
| Operation payload size            | `db.client.operation.size` _(proposed extension)_ (Histogram) | `oxia_client_op_value` (Histogram)                              | Histogram in both modes                                                        |
| Batch total duration (sum)        | `db.client.batch.duration` (Histogram)            | `oxia_client_batch_total_sum` (Float64Counter)                               | Default: single Histogram. Compat: separate sum + count counters               |
| Batch total duration (count)      | _(provided by Histogram internally)_              | `oxia_client_batch_total_count` (Counter<u64>)                               | Compat-mode only                                                               |
| Batch execution duration (sum)    | `db.client.batch.exec_duration` (Histogram)       | `oxia_client_batch_exec_sum` (Float64Counter)                                | Default: single Histogram. Compat: separate sum + count counters               |
| Batch execution duration (count)  | _(provided by Histogram internally)_              | `oxia_client_batch_exec_count` (Counter<u64>)                                | Compat-mode only                                                               |
| Batch payload size                | `db.client.batch.size` (Histogram)                | `oxia_client_batch_value` (Histogram)                                        | Histogram in both modes                                                        |
| Batch request count               | `db.client.batch.request_count` (Histogram)       | `oxia_client_batch_request` (Histogram)                                      | Histogram in both modes                                                        |

> [!NOTE]
> In **default mode** a single OTel `Histogram` covers each timer concept; count and sum are derived from the histogram's built-in aggregation. In **`go-metrics-compat` mode** each timer concept is split into a `Float64Counter` (`_sum`) and a `Counter<u64>` (`_count`) to exactly replicate the Go client's wire format. Size and request-count concepts use `Histogram` in both modes.

### 9.3 Switching Overhead Analysis

- **Zero runtime overhead:** Because the name strings are `const &str` values selected via `#[cfg]` attributes, the compiler eliminates the unused alternative entirely. There is no branch, no match expression, and no `if` check in the hot path.
- **Binary size:** Each name variant compiles to a single null-terminated UTF-8 string in the read-only data segment. Switching variants changes which string is present; it does not add any code.
- **No dynamic dispatch:** `MeterProvider::meter()` is called once during `Metrics::new()` (initialization, cold path). The name constant is passed at that point and never read again on the hot path.

---

## 10. Performance and Risk Analysis

- **Proposed:** **Operation Path Classification:**
  - **Hot path:** `Client::get`, `Client::put`, `Client::delete`, `Client::batch_get`.
    - _Mitigation:_ Ensure attributes are constructed using static slices or pre-allocated elements. Do not format strings (e.g. `format!("shard-{}", id)`) on these paths.
  - **Warm path:** `Client::delete_range`, `Client::list`, `Client::range_scan`, `Client::connect`, heartbeats.
  - **Cold path:** Background task startups, stale map retries.

---

## 11. Rollout and Migration Plan

- **Implemented:**
  - **Phase 1:** Added optional `metrics` Cargo feature, introduced the `client/src/metrics.rs` module, and configured the disabled-feature ZST metrics interface.
  - **Phase 2:** Instrumented public KV operation spans with `#[tracing::instrument]` where practical. `connect` and `batch_get` keep manual spans.
  - **Phase 3:** Centralized public operation metrics and shard read/write batch metrics. Exposed the `meter_provider` setter on `Config` when the `metrics` feature flag is active.
- **Planned:**
  - **Phase 4:** Instrument background loops such as heartbeat and assignment sync.

---

## 12. Testing and Validation Strategy

- **Implemented validation commands:**
  - `just fmt`
  - `cargo check --package mauricebarnum-oxia-client`
  - `cargo check --package mauricebarnum-oxia-client --features metrics`
  - `cargo check --package mauricebarnum-oxia-client --features go-metrics-compat`
  - `just build-all`
  - `just test-all`
  - `just lint`
- **Implemented tests:** Unit tests cover Go-compatible histogram boundaries and `KeyNotFound` result classification.
- **Remaining test opportunities:** A tracing-subscriber mock layer could verify span field propagation and a custom OTel test meter could verify exact emitted measurements.

---

## 13. Open Questions

- ~~Do downstream dashboards expect exact Go-compatible metric names (`oxia_client_op`) or should we migrate to standard OTel `v1.41.0` names (`db.client.operation.duration`)?~~ **Resolved:** Both naming schemes are supported via the `go-metrics-compat` compile-time feature flag (see Section 8.3). The default build uses OTel `v1.41.0` names. Consumers with existing Go-compatible dashboards enable `go-metrics-compat` at compile time. Data migration is explicitly out of scope.

- ~~Should `go-metrics-compat` imply `metrics` in the Cargo feature graph (recommended, see Section 8.3) or remain an independent flag that produces a `compile_error!` when `metrics` is not also enabled?~~ **Resolved**: `go-metrics-compat` feature implies `metrics` in the feature graph.

- ~~The Go client emits `oxia_client_op_sum` and `oxia_client_op_count` as separate instruments. Should the `go-metrics-compat` Rust mode register two instruments to exactly replicate that wire format, or is the Histogram approach (with its built-in count) an acceptable deviation?~~ **Resolved**: `go-metrics-compat` will register and use separate `_sum/_count` instruments.

---

# Part B: Implementation Handoff Spec

## B.1 Required Decisions

- [x] Utilize the `tracing` crate facade with a target dependency on `tracing-opentelemetry` compatibility; do not take a direct dependency on `opentelemetry-sdk` in the library.
- [x] Use Option B (Feature-gated, opt-in) via the `metrics` Cargo feature.
- [x] Use OTel `Histogram` for operation latencies instead of Go's counter-based timer approach.
- [x] Prohibit dynamic user keys or values in metric attributes.
- [x] Expose `meter_provider` setter on the `Config` builder when the `metrics` feature flag is active.
- [x] Add `go-metrics-compat` Cargo feature to select Go reference client metric name strings and instrument types at compile time (zero runtime overhead). Default build uses OTel `v1.41.0` semantic convention names.
- [x] `go-metrics-compat` implies `metrics` in the Cargo feature graph.
- [x] `go-metrics-compat` mode registers separate `Float64Counter` (`_sum`) and `Counter<u64>` (`_count`) instruments per timer concept to exactly replicate Go wire format. Default (OTel) mode uses a single `Histogram` per concept.

## B.2 Assumptions

- **MSRV:** Target client's current edition (2021).
- **Target OTel Version:** `v1.41.0` Semantic Conventions.
- **Runtime:** Assumes `tokio` multi-threaded executor downstream.

## B.3 Per-Operation Instrumentation Spec

### Summary Mapping Table

| Rust API Entry Point   | Span Name                     | Type (Span Kind)   | Metrics Emitted                                                           | Performance Classification |
| ---------------------- | ----------------------------- | ------------------ | ------------------------------------------------------------------------- | -------------------------- |
| `Client::connect`      | `"client.connect"`            | `SpanKind::Client` | —                                                                         | Warm path                  |
| `Client::get`          | `"db.operation.get"`          | `SpanKind::Client` | `db.client.operation.duration`, `db.client.operation.size`                         | Hot path                   |
| `Client::put`          | `"db.operation.put"`          | `SpanKind::Client` | `db.client.operation.duration`, `db.client.operation.size`                         | Hot path                   |
| `Client::delete`       | `"db.operation.delete"`       | `SpanKind::Client` | `db.client.operation.duration`                                                    | Hot path                   |
| `Client::delete_range` | `"db.operation.delete_range"` | `SpanKind::Client` | `db.client.operation.duration`                                                    | Warm path                  |
| `Client::list`         | `"db.operation.list"`         | `SpanKind::Client` | `db.client.operation.duration`                                                    | Warm path                  |
| `Client::range_scan`   | `"db.operation.range_scan"`   | `SpanKind::Client` | `db.client.operation.duration`, `db.client.operation.size`                         | Warm path                  |
| `Client::batch_get`    | `"db.operation.batch_get"`    | `SpanKind::Client` | `db.client.operation.duration` for setup; shard layer emits batch metrics per read | Hot path                   |

---

### Detailed Operation Specifications

##### Operation: `Client::connect`

- **Purpose:** Tracks connection and setup of the client's shard manager.
- **Span Name:** `"client.connect"`
- **Span Kind:** `SpanKind::Client`
- **Parent Context Behavior:** Attaches to implicit parent context.
- **Attributes:**
  - `db.system`: `"oxia"`
  - `db.name`: Namespace name (e.g. `"default"`)
- **Error / Status Behavior:** Records `error.type` if connection to coordinator fails.
- **Performance Notes:** Warm path, allocates client metadata during connection.

##### Operation: `Client::get`

- **Purpose:** Fetches value for a key.
- **Span Name:** `"db.operation.get"`
- **Span Kind:** `SpanKind::Client`
- **Parent Context Behavior:** Attaches to implicit parent context.
- **Attributes:**
  - `db.system`: `"oxia"`
  - `db.name`: Namespace name
  - `db.operation`: `"get"`
  - `db.oxia.shard_id`: Targeted shard ID
  - `error.type`: Set on failure.
- **Error / Status Behavior:** `KeyNotFound` is classified as metrics success. All `Err` results record `error.type` on the current span.
- **Performance Notes:** Hot path. Minimize dynamic allocations in attributes.

##### Operation: `Client::put`

- **Purpose:** Puts a key-value record.
- **Span Name:** `"db.operation.put"`
- **Span Kind:** `SpanKind::Client`
- **Parent Context Behavior:** Attaches to implicit parent context.
- **Attributes:**
  - `db.system`: `"oxia"`
  - `db.name`: Namespace name
  - `db.operation`: `"put"`
  - `db.oxia.shard_id`: Targeted shard ID
  - `error.type`: Set on failure.
- **Error / Status Behavior:** Version mismatches or network failures record `error.type` on the current span and classify the metrics result as failure.
- **Performance Notes:** Hot path. Do not clone payload bytes for attribute logging.

---

## B.4 Metric Inventory

> [!IMPORTANT]
> Metric names **and instrument types** are selected at compile time. The **Default** columns apply when `go-metrics-compat` is **not** enabled. The **`go-metrics-compat`** columns apply when it is. In compat mode, timer concepts emit two instruments; size/count concepts remain Histograms.

**Default mode (OTel v1.41.0)**

| Concept                | Instrument name                  | Type      | Unit   | Attributes                                       | Cardinality | Lifecycle                   |
| ---------------------- | -------------------------------- | --------- | ------ | ------------------------------------------------ | ----------- | --------------------------- |
| Operation latency      | `db.client.operation.duration`   | Histogram | `"ms"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Operation payload size | `db.client.operation.size`       | Histogram | `"By"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch total duration   | `db.client.batch.duration`       | Histogram | `"ms"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch exec duration    | `db.client.batch.exec_duration`  | Histogram | `"ms"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch payload size     | `db.client.batch.size`           | Histogram | `"By"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch request count    | `db.client.batch.request_count`  | Histogram | `"1"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |

**`go-metrics-compat` mode**

| Concept                          | Instrument name                      | Type             | Unit    | Attributes                                       | Cardinality | Lifecycle                   |
| -------------------------------- | ------------------------------------ | ---------------- | ------- | ------------------------------------------------ | ----------- | --------------------------- |
| Operation latency — sum          | `oxia_client_op_sum`                 | Float64Counter   | `"ms"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Operation latency — count        | `oxia_client_op_count`               | Counter<u64>     | `"1"`   | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Operation payload size           | `oxia_client_op_value`               | Histogram        | `"By"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch total duration — sum       | `oxia_client_batch_total_sum`        | Float64Counter   | `"ms"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch total duration — count     | `oxia_client_batch_total_count`      | Counter<u64>     | `"1"`   | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch exec duration — sum        | `oxia_client_batch_exec_sum`         | Float64Counter   | `"ms"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch exec duration — count      | `oxia_client_batch_exec_count`       | Counter<u64>     | `"1"`   | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch payload size               | `oxia_client_batch_value`            | Histogram        | `"By"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |
| Batch request count              | `oxia_client_batch_request`          | Histogram        | `"1"`   | `db.system`, `db.name`, `db.operation`, `result` | Bounded     | `metrics::Metrics::new()` |

---

## B.5 Module and File Plan

- **[IMPLEMENTED] `client/src/metrics.rs`**: Contains all metrics wrappers, declarations, result classifiers, size extractors, and feature-conditional definitions.
  - When `metrics` feature is off: compiles as a ZST metrics interface. Metric wrappers still record tracing span `error.type` for `Err` results, but avoid `Instant::now()`, duration math, result classifier calls, size extraction, OTel calls, and the `opentelemetry` dependency.
  - When `metrics` is on and `go-metrics-compat` is **off**: defines `pub(crate) mod names` with `const &str` name constants for OTel instruments; all timer concepts use `Histogram`.
  - When `metrics` is on and `go-metrics-compat` is **on**: defines `pub(crate) mod names` with separate `_sum`/`_count` name constants; timer concepts register a `Float64Counter` (sum) and a `Counter<u64>` (count) each. Size/count concepts remain `Histogram`. No `if` branches or runtime dispatch in either path.
  - Public operation helpers include `operation_start`, `record_operation_result`, `record_operation_result_with_size`, and `record_operation_sync`.
  - Shard batch helpers include `batch_start`, `batch_exec_duration`, and `record_batch`.
  - Result classifiers include `result_kind` and `get_result_kind`; size extractors include `get_response_size` and `range_scan_response_size`.
- **[IMPLEMENTED] `client/Cargo.toml`**:
  - Added optional dependency `opentelemetry = { version = "0.29.1", features = ["metrics"] }`.
  - Added feature `metrics = ["dep:opentelemetry"]`.
  - Added feature `go-metrics-compat = ["metrics"]` (implies `metrics`).
  - Preserved the external type allowlist entry for `opentelemetry::metrics::meter::MeterProvider`.
- **[IMPLEMENTED] `client/src/lib.rs`**:
  - Added `mod metrics;`.
  - Added `metrics: metrics::Metrics` to `Client`.
  - `Client::new` creates `Metrics::new(config.meter_provider())` when the `metrics` feature is enabled and `Metrics::new(None)` otherwise.
  - `get_internal`, `put_internal`, `delete_internal`, `delete_range_internal`, `list_internal`, and `range_scan_internal` use `#[tracing::instrument(...)]` with `db.system`, `db.name`, `db.operation`, `db.oxia.shard_id`, and `error.type` fields.
  - `connect` and `batch_get` retain manual spans and call centralized helpers for `error.type` and operation metrics.
  - Public operation metrics are recorded once after the operation future resolves, including early shard selection and setup errors.
- **[IMPLEMENTED] `client/src/shard.rs`**:
  - Shard client data carries `metrics::Metrics`.
  - Write batch handling builds one result and calls `metrics::record_batch` once.
  - Read batch demux setup classifies result, value size, and request count once and calls `metrics::record_batch` once.
  - Retry paths measure retry execution duration separately and preserve total duration, execution duration, value size, and request count.
- **[IMPLEMENTED] `client/src/config.rs`**:
  - Exposes `meter_provider` under `#[cfg(feature = "metrics")]` using `Option<Arc<dyn opentelemetry::metrics::MeterProvider + Send + Sync>>` with a setter in the `ConfigBuilder`.
- **[IMPLEMENTED] `Justfile`**:
  - Added `build-all` to build with metrics off, `metrics`, and `go-metrics-compat`.
  - Added `test-all` to run nextest with metrics off, `metrics`, and `go-metrics-compat`.
