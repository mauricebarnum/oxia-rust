# Client Instrumentation

The Oxia client emits tracing spans unconditionally. OpenTelemetry metrics are
compiled in with the `metrics` feature; `go-metrics-compat` enables `metrics`
and adds counters compatible with the Go client's timer-style metric names.

Both spans and metrics use the instrumentation scope
`mauricebarnum_oxia_client`. The client creates instruments from the
`MeterProvider` supplied to `Config`, or from the OpenTelemetry global meter
provider when none is supplied. The application owns provider configuration,
readers, exporters, views, flushing, and shutdown. In particular, applications
that require exponential histograms must install views for the histogram
instruments. The CLI setup in `cmd/src/metrics.rs` is an example.

## Spans

All spans are emitted at `INFO` level. Keys, values, and range bounds are not
recorded.

| Span name | Description |
| --- | --- |
| `client.connect` | Connects the client and initializes shard management. |
| `db.operation.get` | Executes a logical get, including shard selection, retries, and any multi-shard fan-out. |
| `db.operation.batch_get` | Produces and consumes a logical batch-get stream across its shard requests. |
| `db.operation.put` | Executes a logical put, including shard selection and retries. |
| `db.operation.delete` | Executes a logical single-key delete, including shard selection and retries. |
| `db.operation.delete_range` | Executes a logical range delete, including any multi-shard fan-out. |
| `db.operation.list` | Executes a logical key listing, including any multi-shard fan-out. |
| `db.operation.range_scan` | Executes a logical record scan, including any multi-shard fan-out. |

Operation spans have the following fields:

| Field | Description |
| --- | --- |
| `db.system` | Always `oxia`. |
| `db.name` | Configured Oxia namespace. |
| `db.operation` | One of `get`, `batch_get`, `put`, `delete`, `delete_range`, `list`, or `range_scan`. |
| `db.oxia.shard_id` | Selected shard ID for a single-shard operation; absent for multi-shard operations and `batch_get`. |
| `error.type` | Display form of the returned error; absent on success. |

`client.connect` records `db.system`, `db.name`, and `error.type`, but does not
record `db.operation` or a shard ID. A `batch_get` span remains attached while
the returned stream is polled. Its operation-duration metric is successful
only when the stream reaches completion without an item error; an item error
or dropping the stream before completion records failure. Stream item errors
affect that metric classification but do not populate the span's `error.type`;
only an error returned while constructing the stream does so.

## Metrics

Every metric data point has these attributes:

| Attribute | Values |
| --- | --- |
| `db.system` | `oxia` |
| `db.name` | Configured Oxia namespace |
| `db.operation` | `get`, `batch_get`, `put`, `delete`, `delete_range`, `list`, or `range_scan` |
| `result` | `success` or `failure` |

| Metric | Instrument | Unit | Description |
| --- | --- | --- | --- |
| `db.client.operation.duration` | `f64` histogram | `s` | End-to-end logical client operation duration, including routing, retries, fan-out, and result processing. |
| `db.client.operation.size` | `u64` histogram | `By` | Value bytes for logical `get`, `put`, and `range_scan` operations. |
| `db.client.batch.duration` | `f64` histogram | `s` | Total per-shard batch duration from request preparation through response validation. |
| `db.client.batch.exec_duration` | `f64` histogram | `s` | Per-shard RPC execution duration from dispatch through receipt of the first streamed response. |
| `db.client.batch.size` | `u64` histogram | `By` | Sum of value bytes in a per-shard batch. |
| `db.client.batch.request_count` | `u64` histogram | `1` | Number of requests represented by a per-shard batch. |

Operation duration is recorded for every logical database operation.
Operation size is recorded only for `get`, `put`, and `range_scan`: `get` uses
the returned value length, `put` uses the submitted value length even when the
operation fails, and `range_scan` sums returned value lengths. Empty and
missing values contribute zero bytes.

Batch metrics describe the internal per-shard read or write batch, not the
logical top-level operation. Batch reads are emitted by `batch_get` shard
requests with `db.operation=get`; batch writes are emitted for `put`, `delete`,
and `delete_range`. Write batch size counts put value bytes, so delete batches
record zero bytes. A get result of key-not-found is considered successful. A
batch read is successful only when the response count matches the requested
key count and every per-key response is successful, including key-not-found.

## Go Compatibility Metrics

The `go-metrics-compat` feature records timer sums in milliseconds and event
counts alongside the semantic histograms. These counters have the same four
attributes as the semantic metrics.

| Metric | Instrument | Unit | Description |
| --- | --- | --- | --- |
| `oxia_client_op_sum` | `f64` counter | `ms` | Sum of logical operation durations. |
| `oxia_client_op_count` | `u64` counter | `1` | Number of logical operation duration samples. |
| `oxia_client_batch_total_sum` | `f64` counter | `ms` | Sum of total per-shard batch durations. |
| `oxia_client_batch_total_count` | `u64` counter | `1` | Number of total per-shard batch duration samples. |
| `oxia_client_batch_exec_sum` | `f64` counter | `ms` | Sum of per-shard batch execution durations. |
| `oxia_client_batch_exec_count` | `u64` counter | `1` | Number of per-shard batch execution duration samples. |

The compatibility feature does not add equivalents for operation size, batch
size, or batch request count.
