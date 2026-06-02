// Copyright 2025-2026 Maurice S. Barnum
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

#![allow(clippy::redundant_pub_crate)]

#[cfg(feature = "metrics")]
use std::time::Instant;

use crate::Error;
use crate::GetResponse;
use crate::RangeScanResponse;
use crate::errors::OxiaError;

pub(crate) const SCOPE_NAME: &str = "mauricebarnum_oxia_client";
pub(crate) const DB_SYSTEM: &str = "oxia";

#[cfg(feature = "metrics")]
pub(crate) type BatchStart = Instant;
#[cfg(not(feature = "metrics"))]
pub(crate) type BatchStart = ();
#[cfg(feature = "metrics")]
pub(crate) type BatchExecDuration = std::time::Duration;
#[cfg(not(feature = "metrics"))]
pub(crate) type BatchExecDuration = ();
#[cfg(feature = "metrics")]
pub(crate) type OperationStart = Instant;
#[cfg(not(feature = "metrics"))]
pub(crate) type OperationStart = ();

#[cfg(any(all(feature = "metrics", not(feature = "go-metrics-compat")), test))]
pub(crate) const LATENCY_BUCKETS_MILLIS: &[f64] = &[
    0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1_000.0, 2_000.0, 5_000.0,
    10_000.0, 20_000.0, 50_000.0,
];
#[cfg(any(feature = "metrics", test))]
pub(crate) const SIZE_BUCKETS_BYTES: &[f64] = &[
    0x10 as f64,
    0x20 as f64,
    0x40 as f64,
    0x80 as f64,
    0x100 as f64,
    0x200 as f64,
    0x400 as f64,
    0x800 as f64,
    0x1000 as f64,
    0x2000 as f64,
    0x4000 as f64,
    0x8000 as f64,
    0x10000 as f64,
    0x20000 as f64,
    0x40000 as f64,
    0x80000 as f64,
    0x10_0000 as f64,
    0x20_0000 as f64,
    0x40_0000 as f64,
    0x80_0000 as f64,
];
#[cfg(any(feature = "metrics", test))]
pub(crate) const COUNT_BUCKETS: &[f64] = &[
    1.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    200.0,
    500.0,
    1_000.0,
    10_000.0,
    20_000.0,
    50_000.0,
    100_000.0,
    1_000_000.0,
];

#[derive(Clone, Copy, Debug)]
pub(crate) enum Operation {
    Get,
    Put,
    Delete,
    DeleteRange,
    List,
    RangeScan,
    BatchGet,
}

impl Operation {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::DeleteRange => "delete_range",
            Self::List => "list",
            Self::RangeScan => "range_scan",
            Self::BatchGet => "batch_get",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultKind {
    Success,
    Failure,
}

impl ResultKind {
    #[cfg(feature = "metrics")]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

pub(crate) const fn result_kind<T>(result: &crate::Result<T>) -> ResultKind {
    match result {
        Ok(_) => ResultKind::Success,
        Err(_) => ResultKind::Failure,
    }
}

pub(crate) const fn get_result_kind(result: &crate::Result<Option<GetResponse>>) -> ResultKind {
    match result {
        Ok(_) | Err(Error::Oxia(OxiaError::KeyNotFound)) => ResultKind::Success,
        Err(_) => ResultKind::Failure,
    }
}

pub(crate) fn get_response_size(result: &crate::Result<Option<GetResponse>>) -> u64 {
    result
        .as_ref()
        .ok()
        .and_then(Option::as_ref)
        .and_then(|response| response.value.as_ref())
        .map_or(0, |value| value.len() as u64)
}

pub(crate) fn range_scan_response_size(result: &crate::Result<RangeScanResponse>) -> u64 {
    result.as_ref().map_or(0, |response| {
        response
            .records
            .iter()
            .map(|record| record.value.as_ref().map_or(0, bytes::Bytes::len) as u64)
            .sum()
    })
}

pub(crate) fn record_error_type<T>(result: &crate::Result<T>) {
    if let Err(error) = result {
        tracing::Span::current().record("error.type", error.to_string());
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn record_operation_sync<T, C, O>(
    metrics: &Metrics,
    namespace: &str,
    operation: Operation,
    classify: C,
    op: O,
) -> crate::Result<T>
where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
    O: FnOnce() -> crate::Result<T>,
{
    let start = Instant::now();
    let result = op();
    metrics.record_operation(namespace, operation, classify(&result), start.elapsed());
    record_error_type(&result);
    result
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_operation_sync<T, C, O>(
    _metrics: &Metrics,
    _namespace: &str,
    _operation: Operation,
    _classify: C,
    op: O,
) -> crate::Result<T>
where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
    O: FnOnce() -> crate::Result<T>,
{
    let result = op();
    record_error_type(&result);
    result
}

#[cfg(feature = "metrics")]
pub(crate) fn operation_start() -> OperationStart {
    Instant::now()
}

#[cfg(not(feature = "metrics"))]
pub(crate) const fn operation_start() {}

#[cfg(feature = "metrics")]
pub(crate) fn record_operation_result<T, C>(
    metrics: &Metrics,
    namespace: &str,
    operation: Operation,
    classify: C,
    start: OperationStart,
    result: &crate::Result<T>,
) where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
{
    metrics.record_operation(namespace, operation, classify(result), start.elapsed());
    record_error_type(result);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_operation_result<T, C>(
    _metrics: &Metrics,
    _namespace: &str,
    _operation: Operation,
    _classify: C,
    _start: OperationStart,
    result: &crate::Result<T>,
) where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
{
    record_error_type(result);
}

#[cfg(feature = "metrics")]
pub(crate) fn record_operation_result_with_size<T, C, S>(
    metrics: &Metrics,
    namespace: &str,
    operation: Operation,
    classify: C,
    size: S,
    start: OperationStart,
    result: &crate::Result<T>,
) where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
    S: FnOnce(&crate::Result<T>) -> u64,
{
    let result_kind = classify(result);
    metrics.record_operation(namespace, operation, result_kind, start.elapsed());
    metrics.record_operation_size(namespace, operation, result_kind, size(result));
    record_error_type(result);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_operation_result_with_size<T, C, S>(
    _metrics: &Metrics,
    _namespace: &str,
    _operation: Operation,
    _classify: C,
    _size: S,
    _start: OperationStart,
    result: &crate::Result<T>,
) where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
    S: FnOnce(&crate::Result<T>) -> u64,
{
    record_error_type(result);
}

#[cfg(feature = "metrics")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_batch(
    metrics: &Metrics,
    namespace: &str,
    operation: Operation,
    result: ResultKind,
    total_start: BatchStart,
    exec_duration: BatchExecDuration,
    value_size: u64,
    request_count: u64,
) {
    metrics.record_batch(
        namespace,
        operation,
        result,
        total_start.elapsed(),
        exec_duration,
        value_size,
        request_count,
    );
}

#[cfg(not(feature = "metrics"))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_batch(
    _metrics: &Metrics,
    _namespace: &str,
    _operation: Operation,
    _result: ResultKind,
    _total_start: BatchStart,
    _exec_duration: BatchExecDuration,
    _value_size: u64,
    _request_count: u64,
) {
}

#[cfg(feature = "metrics")]
pub(crate) fn batch_start() -> Instant {
    Instant::now()
}

#[cfg(feature = "metrics")]
pub(crate) fn batch_exec_duration(start: Instant) -> std::time::Duration {
    start.elapsed()
}

#[cfg(not(feature = "metrics"))]
pub(crate) const fn batch_start() {}

#[cfg(not(feature = "metrics"))]
pub(crate) const fn batch_exec_duration(_: ()) {}

#[cfg(feature = "metrics")]
mod imp {
    use opentelemetry::KeyValue;
    use opentelemetry::global;
    #[cfg(feature = "go-metrics-compat")]
    use opentelemetry::metrics::Counter;
    use opentelemetry::metrics::Histogram;

    use super::COUNT_BUCKETS;
    use super::DB_SYSTEM;
    #[cfg(not(feature = "go-metrics-compat"))]
    use super::LATENCY_BUCKETS_MILLIS;
    use super::Operation;
    use super::ResultKind;
    use super::SCOPE_NAME;
    use super::SIZE_BUCKETS_BYTES;

    #[cfg(not(feature = "go-metrics-compat"))]
    mod names {
        pub(super) const OP_DURATION: &str = "db.client.operation.duration";
        pub(super) const OP_SIZE: &str = "db.client.operation.size";
        pub(super) const BATCH_TOTAL: &str = "db.client.batch.duration";
        pub(super) const BATCH_EXEC: &str = "db.client.batch.exec_duration";
        pub(super) const BATCH_SIZE: &str = "db.client.batch.size";
        pub(super) const BATCH_REQUEST_COUNT: &str = "db.client.batch.request_count";
    }

    #[cfg(feature = "go-metrics-compat")]
    mod names {
        pub(super) const OP_DURATION_SUM: &str = "oxia_client_op_sum";
        pub(super) const OP_DURATION_COUNT: &str = "oxia_client_op_count";
        pub(super) const OP_SIZE: &str = "oxia_client_op_value";
        pub(super) const BATCH_TOTAL_SUM: &str = "oxia_client_batch_total_sum";
        pub(super) const BATCH_TOTAL_COUNT: &str = "oxia_client_batch_total_count";
        pub(super) const BATCH_EXEC_SUM: &str = "oxia_client_batch_exec_sum";
        pub(super) const BATCH_EXEC_COUNT: &str = "oxia_client_batch_exec_count";
        pub(super) const BATCH_SIZE: &str = "oxia_client_batch_value";
        pub(super) const BATCH_REQUEST_COUNT: &str = "oxia_client_batch_request";
    }

    pub(crate) type MeterProviderHandle = crate::config::MeterProviderHandle;

    #[derive(Clone)]
    pub(crate) struct Metrics {
        #[cfg(not(feature = "go-metrics-compat"))]
        op_duration: Histogram<f64>,
        #[cfg(feature = "go-metrics-compat")]
        op_duration: Timer,
        op_size: Histogram<u64>,
        #[cfg(not(feature = "go-metrics-compat"))]
        batch_total_duration: Histogram<f64>,
        #[cfg(feature = "go-metrics-compat")]
        batch_total_duration: Timer,
        #[cfg(not(feature = "go-metrics-compat"))]
        batch_exec_duration: Histogram<f64>,
        #[cfg(feature = "go-metrics-compat")]
        batch_exec_duration: Timer,
        batch_size: Histogram<u64>,
        batch_request_count: Histogram<u64>,
    }

    impl std::fmt::Debug for Metrics {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Metrics").finish_non_exhaustive()
        }
    }

    #[cfg(feature = "go-metrics-compat")]
    #[derive(Clone, Debug)]
    struct Timer {
        sum: Counter<f64>,
        count: Counter<u64>,
    }

    #[cfg(feature = "go-metrics-compat")]
    impl Timer {
        fn new(
            meter: &opentelemetry::metrics::Meter,
            sum: &'static str,
            count: &'static str,
        ) -> Self {
            Self {
                sum: meter.f64_counter(sum).with_unit("ms").build(),
                count: meter.u64_counter(count).with_unit("1").build(),
            }
        }

        fn record(&self, duration: std::time::Duration, attrs: &[KeyValue]) {
            self.sum.add(duration.as_secs_f64() * 1_000.0, attrs);
            self.count.add(1, attrs);
        }
    }

    impl Metrics {
        pub(crate) fn new(provider: Option<&MeterProviderHandle>) -> Self {
            let provider = provider.cloned().unwrap_or_else(global::meter_provider);
            let meter = provider.meter(SCOPE_NAME);

            Self {
                #[cfg(not(feature = "go-metrics-compat"))]
                op_duration: meter
                    .f64_histogram(names::OP_DURATION)
                    .with_unit("ms")
                    .with_boundaries(LATENCY_BUCKETS_MILLIS.to_vec())
                    .build(),
                #[cfg(feature = "go-metrics-compat")]
                op_duration: Timer::new(&meter, names::OP_DURATION_SUM, names::OP_DURATION_COUNT),
                op_size: meter
                    .u64_histogram(names::OP_SIZE)
                    .with_unit("By")
                    .with_boundaries(SIZE_BUCKETS_BYTES.to_vec())
                    .build(),
                #[cfg(not(feature = "go-metrics-compat"))]
                batch_total_duration: meter
                    .f64_histogram(names::BATCH_TOTAL)
                    .with_unit("ms")
                    .with_boundaries(LATENCY_BUCKETS_MILLIS.to_vec())
                    .build(),
                #[cfg(feature = "go-metrics-compat")]
                batch_total_duration: Timer::new(
                    &meter,
                    names::BATCH_TOTAL_SUM,
                    names::BATCH_TOTAL_COUNT,
                ),
                #[cfg(not(feature = "go-metrics-compat"))]
                batch_exec_duration: meter
                    .f64_histogram(names::BATCH_EXEC)
                    .with_unit("ms")
                    .with_boundaries(LATENCY_BUCKETS_MILLIS.to_vec())
                    .build(),
                #[cfg(feature = "go-metrics-compat")]
                batch_exec_duration: Timer::new(
                    &meter,
                    names::BATCH_EXEC_SUM,
                    names::BATCH_EXEC_COUNT,
                ),
                batch_size: meter
                    .u64_histogram(names::BATCH_SIZE)
                    .with_unit("By")
                    .with_boundaries(SIZE_BUCKETS_BYTES.to_vec())
                    .build(),
                batch_request_count: meter
                    .u64_histogram(names::BATCH_REQUEST_COUNT)
                    .with_unit("1")
                    .with_boundaries(COUNT_BUCKETS.to_vec())
                    .build(),
            }
        }

        pub(crate) fn record_operation(
            &self,
            namespace: &str,
            operation: Operation,
            result: ResultKind,
            duration: std::time::Duration,
        ) {
            let attrs = attrs(namespace, operation, result);
            record_timer(&self.op_duration, duration, &attrs);
        }

        pub(crate) fn record_operation_size(
            &self,
            namespace: &str,
            operation: Operation,
            result: ResultKind,
            size: u64,
        ) {
            self.op_size
                .record(size, &attrs(namespace, operation, result));
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) fn record_batch(
            &self,
            namespace: &str,
            operation: Operation,
            result: ResultKind,
            total_duration: std::time::Duration,
            exec_duration: std::time::Duration,
            value_size: u64,
            request_count: u64,
        ) {
            let attrs = attrs(namespace, operation, result);
            record_timer(&self.batch_total_duration, total_duration, &attrs);
            record_timer(&self.batch_exec_duration, exec_duration, &attrs);
            self.batch_size.record(value_size, &attrs);
            self.batch_request_count.record(request_count, &attrs);
        }
    }

    fn attrs(namespace: &str, operation: Operation, result: ResultKind) -> [KeyValue; 4] {
        [
            KeyValue::new("db.system", DB_SYSTEM),
            KeyValue::new("db.name", namespace.to_string()),
            KeyValue::new("db.operation", operation.as_str()),
            KeyValue::new("result", result.as_str()),
        ]
    }

    #[cfg(not(feature = "go-metrics-compat"))]
    fn record_timer(timer: &Histogram<f64>, duration: std::time::Duration, attrs: &[KeyValue]) {
        timer.record(duration.as_secs_f64() * 1_000.0, attrs);
    }

    #[cfg(feature = "go-metrics-compat")]
    fn record_timer(timer: &Timer, duration: std::time::Duration, attrs: &[KeyValue]) {
        timer.record(duration, attrs);
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    pub(crate) type MeterProviderHandle = ();

    #[derive(Clone, Debug)]
    pub(crate) struct Metrics;

    impl Metrics {
        pub(crate) const fn new(_provider: Option<&MeterProviderHandle>) -> Self {
            Self
        }
    }
}

pub(crate) use imp::Metrics;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_histogram_boundaries_match_reference() {
        assert_eq!(
            LATENCY_BUCKETS_MILLIS,
            &[
                0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1_000.0,
                2_000.0, 5_000.0, 10_000.0, 20_000.0, 50_000.0,
            ]
        );
        assert_eq!(
            SIZE_BUCKETS_BYTES,
            &[
                16.0,
                32.0,
                64.0,
                128.0,
                256.0,
                512.0,
                1_024.0,
                2_048.0,
                4_096.0,
                8_192.0,
                16_384.0,
                32_768.0,
                65_536.0,
                131_072.0,
                262_144.0,
                524_288.0,
                1_048_576.0,
                2_097_152.0,
                4_194_304.0,
                8_388_608.0,
            ]
        );
        assert_eq!(
            COUNT_BUCKETS,
            &[
                1.0,
                5.0,
                10.0,
                20.0,
                50.0,
                100.0,
                200.0,
                500.0,
                1_000.0,
                10_000.0,
                20_000.0,
                50_000.0,
                100_000.0,
                1_000_000.0,
            ]
        );
    }

    #[test]
    fn key_not_found_get_is_success_for_metrics() {
        let result = Err(Error::Oxia(OxiaError::KeyNotFound));
        assert_eq!(get_result_kind(&result), ResultKind::Success);
    }
}
