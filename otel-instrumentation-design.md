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
  - Go's custom `Timer` metric type (split into a sum Float64Counter and a count Int64Counter under the same name).
  - Go's unstructured/slog logger. Rust utilizes `tracing` exclusively.

---

## 3. Goals, Constraints, and Assumptions

### 3.1 Telemetry Framework Choice

- **Proposed:** The library will use the **`tracing` crate** (already present in `client/Cargo.toml#L15`) as its tracing and logging facade, and the **`opentelemetry` API crate** (with the `metric` feature) as its metrics facade.
- **Justification:**
  1. The `tracing` crate is the de facto standard in Rust. Using it ensures all spans and logs integrate out-of-the-box with downstream subscriber ecosystems (such as `tracing-opentelemetry`).
  2. Direct instrumentation using the `opentelemetry` tracing API inside a Rust library requires handling a heavy API layer, which is less idiomatic and more difficult to format and correlate than Rust's native `tracing::Span` structures.
  3. Spans can be bridged downstream using the standard `tracing-opentelemetry` subscriber.
- **Proposed:** The library will interact only with the _facade/API layers_ (`tracing` and `opentelemetry::metric`). It will not instantiate or configure exporters, tracer providers, or meter providers internally, but it _will_ support user-supplied `MeterProvider` configurations.
- **Assumption:** If no OTel SDK or `tracing` subscriber is registered by the parent application, all instrumentation calls compile down to no-ops with negligible runtime overhead.
- **Proposed:** To ensure zero overhead when telemetry is disabled, the OTel metrics logic will be conditionally compiled behind a feature flag (see Section 8).

---

## 4. Instrumentation Boundaries

### 4.1 Boundary Scope

- **Proposed:** **Instrumented operations:**
  - `Client::connect` and `Client::reconnect` (to track cluster connectivity).
  - Main KV API entry points (`get`, `put`, `delete`, `delete_range`, `list`, `range_scan`, `batch_get`).
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
  - **Selected Approach:** Use OTel `Histogram` for operation latencies.
  - **Rejected Alternative:** Translate Go's dual-counter Timer approach literally (making a count counter and a latency sum counter).
  - **Reason for Rejection:** A dual-counter Timer is not idiomatic in modern OpenTelemetry and is rejected by the OTel API in favor of histograms, which natively provide percentiles, counts, and sums.

---

## 5. Tracing and Logging Design

### 5.1 Naming and Scope

- **Proposed:** The tracer/telemetry scope name will be `"mauricebarnum_oxia_client"`.
- **Proposed:** Spans will map to operations:
  - `"client.connect"` (SpanKind::Client)
  - `"db.operation.get"` (SpanKind::Client)
  - `"db.operation.put"` (SpanKind::Client)
  - `"db.operation.delete"` (SpanKind::Client)
  - `"db.operation.delete_range"` (SpanKind::Client)
  - `"db.operation.list"` (SpanKind::Client)
  - `"db.operation.range_scan"` (SpanKind::Client)
  - `"db.operation.batch_get"` (SpanKind::Client)
  - `"shard.heartbeat"` (SpanKind::Internal)
  - `"shard.assignment_sync"` (SpanKind::Internal)

### 5.2 Attributes (OpenTelemetry Semantic Conventions v1.41.0)

- **Proposed:** All spans representing database operations must carry:
  - `db.system`: `"oxia"` (static string)
  - `db.name`: Namespace name from configuration (e.g., `"default"`)
  - `db.operation`: Operation identifier (`"get"`, `"put"`, `"delete"`, etc.)
  - `db.oxia.shard_id`: The ID of the shard targeted by the routing key
  - `server.address`: Leader endpoint host (if resolved)
  - `server.port`: Leader endpoint port
  - `error.type`: The error string code on failure (e.g., `"KeyNotFound"`, `"TransportError"`)

### 5.3 Error and Status Rules

- **Proposed:** If an operation returns an `Err(Error::Oxia(OxiaError::KeyNotFound))` on a `get` operation, the span status remains `Ok` (this is a standard lookup result, not a system failure).
- **Proposed:** For all other `Err` results, the span status must be set to `Error`, and the error description must be recorded in the `exception.message` event.
- **Proposed:** Panics must be caught using `std::panic::AssertUnwindSafe` to set the span status to `Error` before resuming panic propagation.

### 5.4 Sampling and Logging Integration

- **Proposed:** Attributes requiring allocations (like formatting the server address) must be lazily computed only if the span is active (`Span::is_disabled` checks).
- **Proposed:** Existing logs will automatically inherit `trace_id` and `span_id` context when executed within the scope of active tracing spans.

---

## 6. Metrics Design

### 6.1 Metrics Specifications

- **Proposed:** The client will register the following metrics:
  
  - `"oxia.client.op.size"` (Histogram, unit: `"By"`): Measures payload sizes of `put` values and `get` returned values.
  - `"oxia.client.batch.total_duration"` (Histogram, unit: `"s"`): Measures total duration of batched requests (queueing + execution).
  - `"oxia.client.batch.exec_duration"` (Histogram, unit: `"s"`): Measures actual network execution duration of batches.
  - `"oxia.client.batch.size"` (Histogram, unit: `"By"`): Measures total payload size in a batch.
  - `"oxia.client.batch.request_count"` (Histogram, unit: `"1"`): Measures count of operations packed in a batch.

### 6.2 Attributes and Bounded Cardinality

- **Proposed:** All metrics will utilize the following dimensions:
  - `db.system`: Bounded (Value: `"oxia"`)
  - `db.name` (Namespace): Bounded (Value from config)
  - `db.operation`: Bounded (Values: `"get"`, `"put"`, `"delete"`, etc.)
  - `db.oxia.shard_id`: Bounded (Values: integers [0 - maximum sharding limit])
  - `result`: Bounded (Values: `"success"`, `"failure"`)
- **Proposed:** **Cardinality Risk Flag:** No dynamic, user-provided data (such as keys, values, or raw error messages containing user parameters) may be used as metric dimensions. Recording arbitrary keys as dimensions is strictly prohibited due to infinite cardinality risks.

---

## 7. Context Propagation and Concurrency Design

### 7.1 Context over Boundaries

- **Proposed:** Context enters the client via the implicit thread-local/async `tracing::Span::current()`.
- **Proposed:** Since `client` operations are async and span boundaries, we must recommend:
  - **Bridge via `tracing-opentelemetry`**: This handles propagating trace headers over spawned task boundaries (like the heartbeat and discovery tasks) using `tracing` parent span attachment.
- **Proposed:** For background tasks (`ShardAssignmentTask` and heartbeats), we will construct detached root spans `"shard.assignment_sync"` and `"shard.heartbeat"`.
- **Proposed:** If a caller cancels an operation future, the corresponding span will record a `"cancelled"` event and close immediately.

---

## 8. Public API and Internal Structure Impact

### 8.1 Feature Gating Decision

- **Proposed:** Recommend **Option B: Feature-gated, opt-in**. Telemetry instrumentation logic (specifically the direct `opentelemetry` metrics API dependency and context-propagation functions) will compile only when downstream crates enable the `telemetry` feature flag.
- **Rejected Alternative A (Always compiled):** Forces all users to download and compile `opentelemetry` API crates, increasing compile time and dependency trees for minimal-size environments.
- **Rejected Alternative C (Opt-out):** Violates Rust best practices by enabling a large transitive dependency tree by default.

### 8.2 Dependency and API Impact

- **Proposed:** Add `opentelemetry` (API crate) as an optional dependency under `[dependencies]` in `client/Cargo.toml`.
- **Proposed:** Add `telemetry` feature to `client/Cargo.toml` enabling the optional dependency.
- **Proposed:** Expose `meter_provider` setter field on `Config` and `ConfigBuilder` under the `telemetry` feature flag. This allows downstream consumers to explicitly inject custom OTel `MeterProvider` implementations (mapped as `Arc<dyn opentelemetry::metric::MeterProvider + Send + Sync>`). If not set, metrics fallback to `opentelemetry::global::meter_provider()`.
- **Proposed:** MSRV impact is negligible, as `opentelemetry` supports modern stable Rust compilers matching the workspace's version.

---

## 9. Semantic Conventions and Cross-Language Consistency

- **Proposed:** Align strictly with OTel Semantic Conventions version `v1.41.0` (database client namespace).
- **Proposed:** Go uses custom counter names `oxia_client_op`. We will export standard semantic convention metric names (`db.client.operation.duration`) by default, but allow custom mappings if required for legacy dashboard compatibility.
- **Proposed:** **Handling Technology-Agnostic Naming:** To support setups where Oxia serves as an internal coordinator/metadata engine inside a larger database system (similar to etcd/zookeeper configurations):
  - **Database system isolation:** Differentiating nested telemetry calls (e.g. outer database system client vs inner coordinator/metadata database client) is done purely via attributes. The inner Oxia calls will carry `db.system = "oxia"`, whereas the outer system client calls will carry `db.system = "my_custom_db"`.
  - **Unified dashboard querying:** Using the standard semantic convention name `db.client.operation.duration` across both clients enables operators to aggregate database latency metrics globally or slice them by `db.system` to analyze the exact overhead contribution of the coordination layer.

---

## 10. Performance and Risk Analysis

- **Proposed:** **Operation Path Classification:**
  - **Hot path:** `Client::get`, `Client::put`, `Client::delete`, `Client::batch_get`.
    - _Mitigation:_ Ensure attributes are constructed using static slices or pre-allocated elements. Do not format strings (e.g. `format!("shard-{}", id)`) on these paths.
  - **Warm path:** `Client::delete_range`, `Client::list`, `Client::range_scan`, `Client::connect`, heartbeats.
  - **Cold path:** Background task startups, stale map retries.

---

## 11. Rollout and Migration Plan

- **Proposed:** **Phased Approach:**
  - **Phase 1:** Add optional `telemetry` Cargo feature, introduce the `client/src/telemetry.rs` module, and configure the ZST metrics interface.
  - **Phase 2:** Instrument tracing spans using `tracing` macros across the KV operations in `lib.rs` and `shard.rs`.
  - **Phase 3:** Instrument metrics recorders inside the write batch and read batch streams. Expose the `meter_provider` setter on `Config` when the `telemetry` feature flag is active.
  - **Phase 4:** Instrument background loops (heartbeat and assignment task).

---

## 12. Testing and Validation Strategy

- **Proposed:** Introduce unit tests under `client/tests/` using `otel-test` or `tracing-subscriber` mock layer to verify:
  - Metric recordings match expected values (counts, sums) on success and failure.
  - Spans have correct attributes and parent/child relationship.
  - Custom injected `MeterProvider` receives the expected measurements.
  - When `telemetry` feature is disabled, tests verify compilation succeeds and no metrics are registered.

---

## 13. Open Questions

- **Reviewer Input Required:**
  1. Do downstream dashboards expect exact Go-compatible metric names (`oxia_client_op`) or should we migrate to standard OTel `v1.41.0` names (`db.client.operation.duration`)?

---

# Part B: Implementation Handoff Spec

## B.1 Required Decisions

- [x] Utilize the `tracing` crate facade with a target dependency on `tracing-opentelemetry` compatibility; do not take a direct dependency on `opentelemetry-sdk` in the library.
- [x] Use Option B (Feature-gated, opt-in) via the `telemetry` Cargo feature.
- [x] Use OTel `Histogram` for operation latencies instead of Go's counter-based timer approach.
- [x] Prohibit dynamic user keys or values in metric attributes.
- [x] Expose `meter_provider` setter on the `Config` builder when the `telemetry` feature flag is active.

## B.2 Assumptions

- **MSRV:** Target client's current edition (2021).
- **Target OTel Version:** `v1.41.0` Semantic Conventions.
- **Runtime:** Assumes `tokio` multi-threaded executor downstream.

## B.3 Per-Operation Instrumentation Spec

### Summary Mapping Table

| Rust API Entry Point   | Span Name                     | Type (Span Kind)   | Metrics Emitted                                                           | Performance Classification |
| ---------------------- | ----------------------------- | ------------------ | ------------------------------------------------------------------------- | -------------------------- |
| `Client::connect`      | `"client.connect"`            | `SpanKind::Client` | —                                                                         | Warm path                  |
| `Client::get`          | `"db.operation.get"`          | `SpanKind::Client` | `"oxia.client.op.duration"`, `"oxia.client.op.size"`                      | Hot path                   |
| `Client::put`          | `"db.operation.put"`          | `SpanKind::Client` | `"oxia.client.op.duration"`, `"oxia.client.op.size"`                      | Hot path                   |
| `Client::delete`       | `"db.operation.delete"`       | `SpanKind::Client` | `"oxia.client.op.duration"`                                               | Hot path                   |
| `Client::delete_range` | `"db.operation.delete_range"` | `SpanKind::Client` | `"oxia.client.op.duration"`                                               | Warm path                  |
| `Client::list`         | `"db.operation.list"`         | `SpanKind::Client` | `"oxia.client.op.duration"`                                               | Warm path                  |
| `Client::range_scan`   | `"db.operation.range_scan"`   | `SpanKind::Client` | `"oxia.client.op.duration"`, `"oxia.client.op.size"`                      | Warm path                  |
| `Client::batch_get`    | `"db.operation.batch_get"`    | `SpanKind::Client` | `"oxia.client.batch.total_duration"`, `"oxia.client.batch.exec_duration"` | Hot path                   |

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
- **Error / Status Behavior:** Sets status to `Error` if connection to coordinator fails.
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
- **Error / Status Behavior:** `KeyNotFound` is marked `Ok`. Other failures set status to `Error`.
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
- **Error / Status Behavior:** Fails with `Error` status on version mismatches or network failures.
- **Performance Notes:** Hot path. Do not clone payload bytes for attribute logging.

---

## B.4 Metric Inventory

| Metric Name                          | Instrument Type | Unit   | Attributes                                       | Attribute Cardinality | Semantic Convention Version | Lifecycle (init location)   | Cardinality Warnings |
| ------------------------------------ | --------------- | ------ | ------------------------------------------------ | --------------------- | --------------------------- | --------------------------- | -------------------- |
| `"oxia.client.op.duration"`          | Histogram       | `"s"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded               | v1.41.0                     | `telemetry::Metrics::new()` | None                 |
| `"oxia.client.op.size"`              | Histogram       | `"By"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded               | v1.41.0                     | `telemetry::Metrics::new()` | None                 |
| `"oxia.client.batch.total_duration"` | Histogram       | `"s"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded               | v1.41.0                     | `telemetry::Metrics::new()` | None                 |
| `"oxia.client.batch.exec_duration"`  | Histogram       | `"s"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded               | v1.41.0                     | `telemetry::Metrics::new()` | None                 |
| `"oxia.client.batch.size"`           | Histogram       | `"By"` | `db.system`, `db.name`, `db.operation`, `result` | Bounded               | v1.41.0                     | `telemetry::Metrics::new()` | None                 |
| `"oxia.client.batch.request_count"`  | Histogram       | `"1"`  | `db.system`, `db.name`, `db.operation`, `result` | Bounded               | v1.41.0                     | `telemetry::Metrics::new()` | None                 |

---

## B.5 Module and File Plan

- **[NEW] `client/src/telemetry.rs`**: Contains all metrics wrappers, declarations, and feature-conditional definitions. When `telemetry` feature is off, compile as a zero-overhead ZST.
- **[MODIFY] `client/Cargo.toml`**:
  - Add optional dependency `opentelemetry = { version = "0.27.0", features = ["metric"] }` (or matching latest stable version).
  - Add feature `telemetry = ["dep:opentelemetry"]`.
- **[MODIFY] `client/src/lib.rs`**:
  - Integrate operations tracing using `tracing::info_span!` macros inside `put_internal`, `get_internal`, `delete_internal`, etc.
  - Record execution metrics inside completion handlers.
- **[MODIFY] `client/src/config.rs`**:
  - Expose `meter_provider` field under `#[cfg(feature = "telemetry")]` using `Option<Arc<dyn opentelemetry::metric::MeterProvider + Send + Sync>>` with a setter in the `ConfigBuilder`.
