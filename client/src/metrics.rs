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
#[derive(Clone, Copy)]
pub(crate) struct BatchStart;
#[cfg(feature = "metrics")]
pub(crate) type BatchExecDuration = std::time::Duration;
#[cfg(not(feature = "metrics"))]
#[derive(Copy, Clone)]
pub(crate) struct BatchExecDuration;
#[cfg(feature = "metrics")]
pub(crate) type OperationStart = Instant;
#[cfg(not(feature = "metrics"))]
#[derive(Copy, Clone)]
pub(crate) struct OperationStart;

#[cfg(all(test, feature = "metrics"))]
pub(crate) const HISTOGRAM_NAMES: &[&str] = &[
    names::OP_DURATION,
    names::OP_SIZE,
    names::BATCH_TOTAL,
    names::BATCH_EXEC,
    names::BATCH_SIZE,
    names::BATCH_REQUEST_COUNT,
];

#[cfg(feature = "metrics")]
pub(crate) mod names {
    pub(crate) const OP_DURATION: &str = "db.client.operation.duration";
    pub(crate) const OP_SIZE: &str = "db.client.operation.size";
    pub(crate) const BATCH_TOTAL: &str = "db.client.batch.duration";
    pub(crate) const BATCH_EXEC: &str = "db.client.batch.exec_duration";
    pub(crate) const BATCH_SIZE: &str = "db.client.batch.size";
    pub(crate) const BATCH_REQUEST_COUNT: &str = "db.client.batch.request_count";
}

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
pub(crate) fn operation_start() -> OperationStart {
    Instant::now()
}

#[cfg(not(feature = "metrics"))]
pub(crate) const fn operation_start() -> OperationStart {
    OperationStart
}

#[cfg(feature = "metrics")]
pub(crate) fn record_operation_result<T, C>(
    metrics: &Metrics,
    operation: Operation,
    classify: C,
    start: OperationStart,
    result: &crate::Result<T>,
) where
    C: FnOnce(&crate::Result<T>) -> ResultKind,
{
    metrics.record_operation(operation, classify(result), start.elapsed());
    record_error_type(result);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_operation_result<T, C>(
    _metrics: &Metrics,
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
    metrics.record_operation(operation, result_kind, start.elapsed());
    metrics.record_operation_size(operation, result_kind, size(result));
    record_error_type(result);
}

#[cfg(feature = "metrics")]
pub(crate) fn record_batch_get_stream<S>(
    metrics: Metrics,
    start: OperationStart,
    stream: S,
) -> impl futures::Stream<Item = crate::batch_get::ResponseItem>
where
    S: futures::Stream<Item = crate::batch_get::ResponseItem>,
{
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use futures::StreamExt as _;

    struct Completion {
        failed: Arc<AtomicBool>,
        metrics: Metrics,
        recorded: bool,
        start: OperationStart,
    }

    impl Completion {
        fn record(&mut self, completed: bool) {
            if self.recorded {
                return;
            }

            let result = if completed && !self.failed.load(Ordering::Relaxed) {
                ResultKind::Success
            } else {
                ResultKind::Failure
            };
            self.metrics
                .record_operation(Operation::BatchGet, result, self.start.elapsed());
            self.recorded = true;
        }
    }

    impl Drop for Completion {
        fn drop(&mut self) {
            self.record(false);
        }
    }

    let failed = Arc::new(AtomicBool::new(false));
    let observed_failed = Arc::clone(&failed);
    let mut completion = Completion {
        failed,
        metrics,
        recorded: false,
        start,
    };
    stream
        .inspect(move |item| {
            if item.response.is_err() {
                observed_failed.store(true, Ordering::Relaxed);
            }
        })
        .map(Some)
        .chain(futures::stream::once(futures::future::lazy(move |_| {
            completion.record(true);
            None
        })))
        .filter_map(futures::future::ready)
}

#[cfg(not(feature = "metrics"))]
pub(crate) const fn record_batch_get_stream<S>(_: Metrics, _: OperationStart, stream: S) -> S {
    stream
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_operation_result_with_size<T, C, S>(
    _metrics: &Metrics,
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
    operation: Operation,
    result: ResultKind,
    total_start: BatchStart,
    exec_duration: BatchExecDuration,
    value_size: u64,
    request_count: u64,
) {
    metrics.record_batch(
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
pub(crate) const fn record_batch(
    _metrics: &Metrics,
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
pub(crate) const fn batch_start() -> BatchStart {
    BatchStart
}

#[cfg(not(feature = "metrics"))]
pub(crate) const fn batch_exec_duration(_: BatchStart) -> BatchExecDuration {
    BatchExecDuration
}

#[cfg(feature = "metrics")]
mod imp {
    use opentelemetry::KeyValue;
    use opentelemetry::global;
    #[cfg(feature = "go-metrics-compat")]
    use opentelemetry::metrics::Counter;
    use opentelemetry::metrics::Histogram;

    use super::DB_SYSTEM;
    use super::Operation;
    use super::ResultKind;
    use super::SCOPE_NAME;
    use super::names;

    #[cfg(feature = "go-metrics-compat")]
    mod compat_names {
        pub(super) const OP_DURATION_SUM: &str = "oxia_client_op_sum";
        pub(super) const OP_DURATION_COUNT: &str = "oxia_client_op_count";
        pub(super) const BATCH_TOTAL_SUM: &str = "oxia_client_batch_total_sum";
        pub(super) const BATCH_TOTAL_COUNT: &str = "oxia_client_batch_total_count";
        pub(super) const BATCH_EXEC_SUM: &str = "oxia_client_batch_exec_sum";
        pub(super) const BATCH_EXEC_COUNT: &str = "oxia_client_batch_exec_count";
    }

    pub(crate) type MeterProviderHandle = crate::config::MeterProviderHandle;

    #[derive(Clone)]
    pub(crate) struct Metrics {
        namespace: KeyValue,
        op_duration: Histogram<f64>,
        #[cfg(feature = "go-metrics-compat")]
        compat_op_duration: Timer,
        op_size: Histogram<u64>,
        batch_total_duration: Histogram<f64>,
        #[cfg(feature = "go-metrics-compat")]
        compat_batch_total_duration: Timer,
        batch_exec_duration: Histogram<f64>,
        #[cfg(feature = "go-metrics-compat")]
        compat_batch_exec_duration: Timer,
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
        pub(crate) fn new(provider: Option<&MeterProviderHandle>, namespace: &str) -> Self {
            let provider = provider.cloned().unwrap_or_else(global::meter_provider);
            let meter = provider.meter(SCOPE_NAME);

            Self {
                namespace: KeyValue::new("db.name", std::sync::Arc::<str>::from(namespace)),
                op_duration: meter
                    .f64_histogram(names::OP_DURATION)
                    .with_unit("s")
                    .build(),
                #[cfg(feature = "go-metrics-compat")]
                compat_op_duration: Timer::new(
                    &meter,
                    compat_names::OP_DURATION_SUM,
                    compat_names::OP_DURATION_COUNT,
                ),
                op_size: meter.u64_histogram(names::OP_SIZE).with_unit("By").build(),
                batch_total_duration: meter
                    .f64_histogram(names::BATCH_TOTAL)
                    .with_unit("s")
                    .build(),
                #[cfg(feature = "go-metrics-compat")]
                compat_batch_total_duration: Timer::new(
                    &meter,
                    compat_names::BATCH_TOTAL_SUM,
                    compat_names::BATCH_TOTAL_COUNT,
                ),
                batch_exec_duration: meter
                    .f64_histogram(names::BATCH_EXEC)
                    .with_unit("s")
                    .build(),
                #[cfg(feature = "go-metrics-compat")]
                compat_batch_exec_duration: Timer::new(
                    &meter,
                    compat_names::BATCH_EXEC_SUM,
                    compat_names::BATCH_EXEC_COUNT,
                ),
                batch_size: meter
                    .u64_histogram(names::BATCH_SIZE)
                    .with_unit("By")
                    .build(),
                batch_request_count: meter
                    .u64_histogram(names::BATCH_REQUEST_COUNT)
                    .with_unit("1")
                    .build(),
            }
        }

        pub(crate) fn record_operation(
            &self,
            operation: Operation,
            result: ResultKind,
            duration: std::time::Duration,
        ) {
            let attrs = self.attrs(operation, result);
            record_timer(&self.op_duration, duration, &attrs);
            #[cfg(feature = "go-metrics-compat")]
            self.compat_op_duration.record(duration, &attrs);
        }

        pub(crate) fn record_operation_size(
            &self,
            operation: Operation,
            result: ResultKind,
            size: u64,
        ) {
            self.op_size.record(size, &self.attrs(operation, result));
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) fn record_batch(
            &self,
            operation: Operation,
            result: ResultKind,
            total_duration: std::time::Duration,
            exec_duration: std::time::Duration,
            value_size: u64,
            request_count: u64,
        ) {
            let attrs = self.attrs(operation, result);
            record_timer(&self.batch_total_duration, total_duration, &attrs);
            record_timer(&self.batch_exec_duration, exec_duration, &attrs);
            #[cfg(feature = "go-metrics-compat")]
            {
                self.compat_batch_total_duration
                    .record(total_duration, &attrs);
                self.compat_batch_exec_duration
                    .record(exec_duration, &attrs);
            }
            self.batch_size.record(value_size, &attrs);
            self.batch_request_count.record(request_count, &attrs);
        }

        fn attrs(&self, operation: Operation, result: ResultKind) -> [KeyValue; 4] {
            [
                KeyValue::new("db.system", DB_SYSTEM),
                self.namespace.clone(),
                KeyValue::new("db.operation", operation.as_str()),
                KeyValue::new("result", result.as_str()),
            ]
        }
    }

    fn record_timer(timer: &Histogram<f64>, duration: std::time::Duration, attrs: &[KeyValue]) {
        timer.record(duration.as_secs_f64(), attrs);
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    pub(crate) type MeterProviderHandle = ();

    #[derive(Clone, Debug)]
    pub(crate) struct Metrics;

    impl Metrics {
        pub(crate) const fn new(_provider: Option<&MeterProviderHandle>, _namespace: &str) -> Self {
            Self
        }
    }
}

pub(crate) use imp::Metrics;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_not_found_get_is_success_for_metrics() {
        let result = Err(Error::Oxia(OxiaError::KeyNotFound));
        assert_eq!(get_result_kind(&result), ResultKind::Success);
    }
}

#[cfg(all(test, feature = "metrics"))]
mod sdk_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use opentelemetry_sdk::metrics::Aggregation;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use opentelemetry_sdk::metrics::InMemoryMetricExporterBuilder;
    use opentelemetry_sdk::metrics::Instrument;
    use opentelemetry_sdk::metrics::InstrumentKind;
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::Stream;
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;

    use super::*;

    const EXPONENTIAL_HISTOGRAM_MAX_SIZE: u32 = 160;
    const EXPONENTIAL_HISTOGRAM_MAX_SCALE: i8 = 20;

    #[test]
    fn semantic_metrics_use_exponential_histograms() {
        let (exporter, provider, metrics) = setup_metrics();

        metrics.record_operation(
            Operation::Get,
            ResultKind::Success,
            Duration::from_millis(250),
        );
        metrics.record_operation_size(Operation::Get, ResultKind::Success, 42);
        metrics.record_batch(
            Operation::Put,
            ResultKind::Success,
            Duration::from_secs(2),
            Duration::from_millis(500),
            128,
            3,
        );
        metrics.record_operation_size(Operation::Get, ResultKind::Success, 0);

        let rm = collect(&provider, &exporter);

        assert_exponential_f64(&rm, names::OP_DURATION, "s", 1, 0.25, true);
        assert_exponential_u64(&rm, names::OP_SIZE, "By", 2, 42, true, true);
        assert_exponential_f64(&rm, names::BATCH_TOTAL, "s", 1, 2.0, true);
        assert_exponential_f64(&rm, names::BATCH_EXEC, "s", 1, 0.5, true);
        assert_exponential_u64(&rm, names::BATCH_SIZE, "By", 1, 128, true, false);
        assert_exponential_u64(&rm, names::BATCH_REQUEST_COUNT, "1", 1, 3, true, false);
    }

    #[tokio::test]
    async fn batch_get_operation_records_stream_failure_on_completion() {
        use futures::StreamExt as _;

        let (exporter, provider, metrics) = setup_metrics();
        let stream = record_batch_get_stream(
            metrics,
            operation_start(),
            futures::stream::iter([crate::batch_get::ResponseItem::failed(
                "key",
                Error::Cancelled,
            )]),
        );

        assert_eq!(stream.collect::<Vec<_>>().await.len(), 1);
        let rm = collect(&provider, &exporter);
        let histogram = exponential_f64(&rm, names::OP_DURATION, "s");
        let result = histogram
            .data_points()
            .next()
            .expect("histogram data point")
            .attributes()
            .find(|attribute| attribute.key.as_str() == "result");
        assert_eq!(
            result.map(|attribute| attribute.value.to_string()),
            Some("failure".to_owned())
        );
    }

    #[tokio::test]
    async fn batch_get_operation_records_failure_when_stream_is_dropped() {
        let (exporter, provider, metrics) = setup_metrics();
        let stream = record_batch_get_stream(
            metrics,
            operation_start(),
            futures::stream::iter([crate::batch_get::ResponseItem::failed(
                "key",
                Error::Cancelled,
            )]),
        );

        drop(stream);
        let rm = collect(&provider, &exporter);
        let histogram = exponential_f64(&rm, names::OP_DURATION, "s");
        let result = histogram
            .data_points()
            .next()
            .expect("histogram data point")
            .attributes()
            .find(|attribute| attribute.key.as_str() == "result");
        assert_eq!(
            result.map(|attribute| attribute.value.to_string()),
            Some("failure".to_owned())
        );
    }

    #[cfg(feature = "go-metrics-compat")]
    #[test]
    fn go_metrics_compat_adds_only_latency_sum_count_metrics() {
        let (exporter, provider, metrics) = setup_metrics();

        metrics.record_operation(
            Operation::Get,
            ResultKind::Success,
            Duration::from_millis(250),
        );
        metrics.record_operation_size(Operation::Get, ResultKind::Success, 42);
        metrics.record_batch(
            Operation::Put,
            ResultKind::Success,
            Duration::from_secs(2),
            Duration::from_millis(500),
            128,
            3,
        );

        let rm = collect(&provider, &exporter);

        assert_exponential_f64(&rm, names::OP_DURATION, "s", 1, 0.25, true);
        assert_sum_f64(&rm, "oxia_client_op_sum", "ms", 250.0);
        assert_sum_u64(&rm, "oxia_client_op_count", "1", 1);
        assert_sum_f64(&rm, "oxia_client_batch_total_sum", "ms", 2_000.0);
        assert_sum_u64(&rm, "oxia_client_batch_total_count", "1", 1);
        assert_sum_f64(&rm, "oxia_client_batch_exec_sum", "ms", 500.0);
        assert_sum_u64(&rm, "oxia_client_batch_exec_count", "1", 1);
        assert_metric_absent(&rm, "oxia_client_op_value");
        assert_metric_absent(&rm, "oxia_client_batch_value");
        assert_metric_absent(&rm, "oxia_client_batch_request");
    }

    fn setup_metrics() -> (InMemoryMetricExporter, SdkMeterProvider, Metrics) {
        let exporter = InMemoryMetricExporterBuilder::new()
            .with_temporality(Temporality::Cumulative)
            .build();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_view(exponential_histogram_view)
            .build();
        let provider_handle: crate::config::MeterProviderHandle = Arc::new(provider.clone());
        let metrics = Metrics::new(Some(&provider_handle), "default");
        (exporter, provider, metrics)
    }

    fn exponential_histogram_view(instrument: &Instrument) -> Option<Stream> {
        (instrument.kind() == InstrumentKind::Histogram
            && HISTOGRAM_NAMES.contains(&instrument.name()))
        .then(|| {
            Stream::builder()
                .with_aggregation(Aggregation::Base2ExponentialHistogram {
                    max_size: EXPONENTIAL_HISTOGRAM_MAX_SIZE,
                    max_scale: EXPONENTIAL_HISTOGRAM_MAX_SCALE,
                    record_min_max: false,
                })
                .build()
                .expect("valid exponential histogram view")
        })
    }

    fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> ResourceMetrics {
        provider.force_flush().expect("metrics collection succeeds");
        exporter
            .get_finished_metrics()
            .expect("metrics export succeeds")
            .pop()
            .expect("metrics were exported")
    }

    fn assert_exponential_f64(
        rm: &ResourceMetrics,
        name: &str,
        unit: &str,
        count: usize,
        sum: f64,
        has_positive_bucket: bool,
    ) {
        let histogram = exponential_f64(rm, name, unit);
        assert_eq!(Temporality::Cumulative, histogram.temporality());
        let mut points = histogram.data_points();
        let point = points.next().expect("histogram data point");
        assert!(points.next().is_none());
        assert_eq!(count, point.count());
        assert!((point.sum() - sum).abs() < f64::EPSILON);
        assert_eq!(None, point.min());
        assert_eq!(None, point.max());
        assert_eq!(0, point.zero_count());
        assert_eq!(
            has_positive_bucket,
            point.positive_bucket().counts().any(|count| count > 0)
        );
    }

    fn assert_exponential_u64(
        rm: &ResourceMetrics,
        name: &str,
        unit: &str,
        count: usize,
        sum: u64,
        has_positive_bucket: bool,
        has_zero: bool,
    ) {
        let histogram = exponential_u64(rm, name, unit);
        assert_eq!(Temporality::Cumulative, histogram.temporality());
        let mut points = histogram.data_points();
        let point = points.next().expect("histogram data point");
        assert!(points.next().is_none());
        assert_eq!(count, point.count());
        assert_eq!(sum, point.sum());
        assert_eq!(None, point.min());
        assert_eq!(None, point.max());
        assert_eq!(u64::from(has_zero), point.zero_count());
        assert_eq!(
            has_positive_bucket,
            point.positive_bucket().counts().any(|count| count > 0)
        );
    }

    #[cfg(feature = "go-metrics-compat")]
    fn assert_sum_f64(rm: &ResourceMetrics, name: &str, unit: &str, value: f64) {
        let sum = sum_f64(rm, name, unit);
        let mut points = sum.data_points();
        let point = points.next().expect("sum data point");
        assert!(points.next().is_none());
        assert!((point.value() - value).abs() < f64::EPSILON);
    }

    #[cfg(feature = "go-metrics-compat")]
    fn assert_sum_u64(rm: &ResourceMetrics, name: &str, unit: &str, value: u64) {
        let sum = sum_u64(rm, name, unit);
        let mut points = sum.data_points();
        let point = points.next().expect("sum data point");
        assert!(points.next().is_none());
        assert_eq!(value, point.value());
    }

    #[cfg(feature = "go-metrics-compat")]
    fn assert_metric_absent(rm: &ResourceMetrics, name: &str) {
        assert!(
            find_metric(rm, name).is_none(),
            "metric {name} should not be exported"
        );
    }

    fn checked_metric<'a>(
        rm: &'a ResourceMetrics,
        name: &str,
        unit: &str,
    ) -> &'a opentelemetry_sdk::metrics::data::Metric {
        let metric = find_metric(rm, name).unwrap_or_else(|| panic!("metric {name} is exported"));
        assert_eq!(unit, metric.unit());
        metric
    }

    fn find_metric<'a>(
        rm: &'a ResourceMetrics,
        name: &str,
    ) -> Option<&'a opentelemetry_sdk::metrics::data::Metric> {
        rm.scope_metrics()
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
    }

    fn exponential_f64<'a>(
        rm: &'a ResourceMetrics,
        name: &str,
        unit: &str,
    ) -> &'a data::ExponentialHistogram<f64> {
        match checked_metric(rm, name, unit).data() {
            data::AggregatedMetrics::F64(data::MetricData::ExponentialHistogram(value)) => value,
            _ => panic!("metric {name} has expected data type"),
        }
    }

    fn exponential_u64<'a>(
        rm: &'a ResourceMetrics,
        name: &str,
        unit: &str,
    ) -> &'a data::ExponentialHistogram<u64> {
        match checked_metric(rm, name, unit).data() {
            data::AggregatedMetrics::U64(data::MetricData::ExponentialHistogram(value)) => value,
            _ => panic!("metric {name} has expected data type"),
        }
    }

    #[cfg(feature = "go-metrics-compat")]
    fn sum_f64<'a>(rm: &'a ResourceMetrics, name: &str, unit: &str) -> &'a data::Sum<f64> {
        match checked_metric(rm, name, unit).data() {
            data::AggregatedMetrics::F64(data::MetricData::Sum(value)) => value,
            _ => panic!("metric {name} has expected data type"),
        }
    }

    #[cfg(feature = "go-metrics-compat")]
    fn sum_u64<'a>(rm: &'a ResourceMetrics, name: &str, unit: &str) -> &'a data::Sum<u64> {
        match checked_metric(rm, name, unit).data() {
            data::AggregatedMetrics::U64(data::MetricData::Sum(value)) => value,
            _ => panic!("metric {name} has expected data type"),
        }
    }
}
