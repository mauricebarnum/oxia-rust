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

#[cfg(all(test, feature = "metrics"))]
pub(crate) const HISTOGRAM_NAMES: &[&str] = &[
    names::OP_DURATION,
    names::OP_SIZE,
    names::BATCH_TOTAL,
    names::BATCH_EXEC,
    names::BATCH_SIZE,
    names::BATCH_REQUEST_COUNT,
];

#[cfg(any(feature = "metrics", test))]
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
        pub(crate) fn new(provider: Option<&MeterProviderHandle>) -> Self {
            let provider = provider.cloned().unwrap_or_else(global::meter_provider);
            let meter = provider.meter(SCOPE_NAME);

            Self {
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
            namespace: &str,
            operation: Operation,
            result: ResultKind,
            duration: std::time::Duration,
        ) {
            let attrs = attrs(namespace, operation, result);
            record_timer(&self.op_duration, duration, &attrs);
            #[cfg(feature = "go-metrics-compat")]
            self.compat_op_duration.record(duration, &attrs);
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
    }

    fn attrs(namespace: &str, operation: Operation, result: ResultKind) -> [KeyValue; 4] {
        [
            KeyValue::new("db.system", DB_SYSTEM),
            KeyValue::new("db.name", namespace.to_string()),
            KeyValue::new("db.operation", operation.as_str()),
            KeyValue::new("result", result.as_str()),
        ]
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
    fn key_not_found_get_is_success_for_metrics() {
        let result = Err(Error::Oxia(OxiaError::KeyNotFound));
        assert_eq!(get_result_kind(&result), ResultKind::Success);
    }
}

#[cfg(all(test, feature = "metrics"))]
mod sdk_tests {
    use std::sync::Arc;
    use std::sync::Weak;
    use std::time::Duration;

    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::Aggregation;
    use opentelemetry_sdk::metrics::Instrument;
    use opentelemetry_sdk::metrics::InstrumentKind;
    use opentelemetry_sdk::metrics::ManualReader;
    use opentelemetry_sdk::metrics::MetricResult;
    use opentelemetry_sdk::metrics::Pipeline;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::Stream;
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::reader::MetricReader;

    use super::*;

    const EXPONENTIAL_HISTOGRAM_MAX_SIZE: u32 = 160;
    const EXPONENTIAL_HISTOGRAM_MAX_SCALE: i8 = 20;

    #[test]
    fn semantic_metrics_use_exponential_histograms() {
        let (reader, _provider, metrics) = setup_metrics();

        metrics.record_operation(
            "default",
            Operation::Get,
            ResultKind::Success,
            Duration::from_millis(250),
        );
        metrics.record_operation_size("default", Operation::Get, ResultKind::Success, 42);
        metrics.record_batch(
            "default",
            Operation::Put,
            ResultKind::Success,
            Duration::from_secs(2),
            Duration::from_millis(500),
            128,
            3,
        );
        metrics.record_operation_size("default", Operation::Get, ResultKind::Success, 0);

        let rm = collect(&reader);

        assert_exponential_f64(&rm, names::OP_DURATION, "s", 1, 0.25, true);
        assert_exponential_u64(&rm, names::OP_SIZE, "By", 2, 42, true, true);
        assert_exponential_f64(&rm, names::BATCH_TOTAL, "s", 1, 2.0, true);
        assert_exponential_f64(&rm, names::BATCH_EXEC, "s", 1, 0.5, true);
        assert_exponential_u64(&rm, names::BATCH_SIZE, "By", 1, 128, true, false);
        assert_exponential_u64(&rm, names::BATCH_REQUEST_COUNT, "1", 1, 3, true, false);
    }

    #[cfg(feature = "go-metrics-compat")]
    #[test]
    fn go_metrics_compat_adds_only_latency_sum_count_metrics() {
        let (reader, _provider, metrics) = setup_metrics();

        metrics.record_operation(
            "default",
            Operation::Get,
            ResultKind::Success,
            Duration::from_millis(250),
        );
        metrics.record_operation_size("default", Operation::Get, ResultKind::Success, 42);
        metrics.record_batch(
            "default",
            Operation::Put,
            ResultKind::Success,
            Duration::from_secs(2),
            Duration::from_millis(500),
            128,
            3,
        );

        let rm = collect(&reader);

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

    fn setup_metrics() -> (
        SharedManualReader,
        crate::config::MeterProviderHandle,
        Metrics,
    ) {
        let reader = SharedManualReader::new(
            ManualReader::builder()
                .with_temporality(Temporality::Cumulative)
                .build(),
        );
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .with_view(exponential_histogram_view)
            .build();
        let provider_handle: crate::config::MeterProviderHandle = Arc::new(provider);
        let metrics = Metrics::new(Some(&provider_handle));
        (reader, provider_handle, metrics)
    }

    fn exponential_histogram_view(instrument: &Instrument) -> Option<Stream> {
        (instrument.kind == Some(InstrumentKind::Histogram)
            && HISTOGRAM_NAMES.contains(&instrument.name.as_ref()))
        .then(|| {
            Stream::new()
                .name(instrument.name.clone())
                .description(instrument.description.clone())
                .unit(instrument.unit.clone())
                .aggregation(Aggregation::Base2ExponentialHistogram {
                    max_size: EXPONENTIAL_HISTOGRAM_MAX_SIZE,
                    max_scale: EXPONENTIAL_HISTOGRAM_MAX_SCALE,
                    record_min_max: false,
                })
        })
    }

    fn collect(reader: &SharedManualReader) -> ResourceMetrics {
        let mut rm = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: Vec::new(),
        };
        reader
            .collect(&mut rm)
            .expect("metrics collection succeeds");
        rm
    }

    fn assert_exponential_f64(
        rm: &ResourceMetrics,
        name: &str,
        unit: &str,
        count: usize,
        sum: f64,
        has_positive_bucket: bool,
    ) {
        let histogram = metric_data::<data::ExponentialHistogram<f64>>(rm, name, unit);
        assert_eq!(Temporality::Cumulative, histogram.temporality);
        assert_eq!(1, histogram.data_points.len());
        let point = &histogram.data_points[0];
        assert_eq!(count, point.count);
        assert!((point.sum - sum).abs() < f64::EPSILON);
        assert_eq!(None, point.min);
        assert_eq!(None, point.max);
        assert_eq!(0, point.zero_count);
        assert_eq!(
            has_positive_bucket,
            point.positive_bucket.counts.iter().any(|count| *count > 0)
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
        let histogram = metric_data::<data::ExponentialHistogram<u64>>(rm, name, unit);
        assert_eq!(Temporality::Cumulative, histogram.temporality);
        assert_eq!(1, histogram.data_points.len());
        let point = &histogram.data_points[0];
        assert_eq!(count, point.count);
        assert_eq!(sum, point.sum);
        assert_eq!(None, point.min);
        assert_eq!(None, point.max);
        assert_eq!(u64::from(has_zero), point.zero_count);
        assert_eq!(
            has_positive_bucket,
            point.positive_bucket.counts.iter().any(|count| *count > 0)
        );
    }

    #[cfg(feature = "go-metrics-compat")]
    fn assert_sum_f64(rm: &ResourceMetrics, name: &str, unit: &str, value: f64) {
        let sum = metric_data::<data::Sum<f64>>(rm, name, unit);
        assert_eq!(1, sum.data_points.len());
        assert!((sum.data_points[0].value - value).abs() < f64::EPSILON);
    }

    #[cfg(feature = "go-metrics-compat")]
    fn assert_sum_u64(rm: &ResourceMetrics, name: &str, unit: &str, value: u64) {
        let sum = metric_data::<data::Sum<u64>>(rm, name, unit);
        assert_eq!(1, sum.data_points.len());
        assert_eq!(value, sum.data_points[0].value);
    }

    #[cfg(feature = "go-metrics-compat")]
    fn assert_metric_absent(rm: &ResourceMetrics, name: &str) {
        assert!(
            metric(rm, name).is_none(),
            "metric {name} should not be exported"
        );
    }

    fn metric_data<'a, T>(rm: &'a ResourceMetrics, name: &str, unit: &str) -> &'a T
    where
        T: data::Aggregation,
    {
        let metric = metric(rm, name).unwrap_or_else(|| panic!("metric {name} is exported"));
        assert_eq!(unit, metric.unit);
        metric
            .data
            .as_any()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("metric {name} has expected data type"))
    }

    fn metric<'a>(
        rm: &'a ResourceMetrics,
        name: &str,
    ) -> Option<&'a opentelemetry_sdk::metrics::data::Metric> {
        rm.scope_metrics
            .iter()
            .flat_map(|scope| scope.metrics.iter())
            .find(|metric| metric.name == name)
    }

    #[derive(Clone, Debug)]
    struct SharedManualReader(Arc<ManualReader>);

    impl SharedManualReader {
        fn new(reader: ManualReader) -> Self {
            Self(Arc::new(reader))
        }
    }

    impl MetricReader for SharedManualReader {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }

        fn collect(&self, rm: &mut ResourceMetrics) -> MetricResult<()> {
            self.0.collect(rm)
        }

        fn force_flush(&self) -> MetricResult<()> {
            self.0.force_flush()
        }

        fn shutdown(&self) -> MetricResult<()> {
            self.0.shutdown()
        }

        fn temporality(&self, kind: InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }
}
