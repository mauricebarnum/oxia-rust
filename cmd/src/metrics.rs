// Copyright 2026 Maurice S. Barnum
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

use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use anyhow::Context as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::Aggregation;
use opentelemetry_sdk::metrics::Instrument;
use opentelemetry_sdk::metrics::InstrumentKind;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Stream;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

use crate::MetricsFormat;

mod otlp;

const EXPONENTIAL_HISTOGRAM_MAX_SIZE: u32 = 160;
const EXPONENTIAL_HISTOGRAM_MAX_SCALE: i8 = 20;

const HISTOGRAM_NAMES: &[&str] = &[
    "db.client.operation.duration",
    "db.client.operation.size",
    "db.client.batch.duration",
    "db.client.batch.exec_duration",
    "db.client.batch.size",
    "db.client.batch.request_count",
];

#[derive(Debug)]
pub struct MetricsOutput {
    provider: SdkMeterProvider,
    exporter: OutputExporter,
}

#[derive(Debug)]
enum MetricsWriter {
    Stderr,
    Stdout,
    File(File),
}

impl MetricsOutput {
    pub fn new(format: MetricsFormat, output: Option<&Path>) -> anyhow::Result<Self> {
        let writer = match output {
            None => MetricsWriter::Stderr,
            Some(path) if path == Path::new("-") => MetricsWriter::Stdout,
            Some(path) => MetricsWriter::File(
                File::create(path)
                    .with_context(|| format!("failed to open metrics output {}", path.display()))?,
            ),
        };
        let exporter = OutputExporter::new(format, writer)?;
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_view(exponential_histogram_view)
            .build();
        Ok(Self { provider, exporter })
    }

    pub fn meter_provider(&self) -> crate::client_lib::config::MeterProviderHandle {
        Arc::new(self.provider.clone())
    }

    pub fn export(self) -> anyhow::Result<()> {
        let shutdown_result = self
            .provider
            .shutdown()
            .context("failed to shut down metrics provider");
        if let Some(error) = self.exporter.error()? {
            anyhow::bail!("failed to write metrics: {error}");
        }
        shutdown_result
    }
}

#[derive(Clone, Debug)]
struct OutputExporter {
    format: MetricsFormat,
    state: Arc<Mutex<OutputState>>,
}

#[derive(Debug)]
struct OutputState {
    error: Option<String>,
    pending: Option<Vec<u8>>,
    writer: MetricsWriter,
}

impl OutputExporter {
    fn new(format: MetricsFormat, writer: MetricsWriter) -> io::Result<Self> {
        let pending = Some(format_output(format, &ResourceMetrics::default())?);
        Ok(Self {
            format,
            state: Arc::new(Mutex::new(OutputState {
                error: None,
                pending,
                writer,
            })),
        })
    }

    fn update(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        match format_output(self.format, metrics) {
            Ok(output) => {
                self.state
                    .lock()
                    .map_err(|error| lock_error(&error))?
                    .pending = Some(output);
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                self.state.lock().map_err(|error| lock_error(&error))?.error =
                    Some(message.clone());
                Err(OTelSdkError::InternalFailure(message))
            }
        }
    }

    fn error(&self) -> anyhow::Result<Option<String>> {
        Ok(self
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("metrics output lock poisoned: {error}"))?
            .error
            .clone())
    }
}

impl PushMetricExporter for OutputExporter {
    fn export(
        &self,
        metrics: &ResourceMetrics,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        std::future::ready(self.update(metrics))
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
        let mut state = self.state.lock().map_err(|error| lock_error(&error))?;
        if let Some(output) = state.pending.take()
            && let Err(error) = state.writer.write_all(&output)
        {
            let message = error.to_string();
            state.error = Some(message.clone());
            drop(state);
            return Err(OTelSdkError::InternalFailure(message));
        }
        drop(state);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

impl MetricsWriter {
    fn write_all(&mut self, output: &[u8]) -> io::Result<()> {
        match self {
            Self::Stderr => io::stderr().lock().write_all(output),
            Self::Stdout => io::stdout().lock().write_all(output),
            Self::File(file) => file.write_all(output),
        }
    }
}

fn format_output(format: MetricsFormat, metrics: &ResourceMetrics) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    match format {
        MetricsFormat::Text => format_metrics(&mut output, metrics)?,
        MetricsFormat::OtlpJson => otlp::format_json(&mut output, metrics)?,
        MetricsFormat::OtlpPb => otlp::format_protobuf(&mut output, metrics)?,
    }
    Ok(output)
}

fn lock_error<T>(error: &std::sync::PoisonError<T>) -> OTelSdkError {
    OTelSdkError::InternalFailure(format!("metrics output lock poisoned: {error}"))
}

fn format_metrics(writer: &mut dyn io::Write, metrics: &ResourceMetrics) -> io::Result<()> {
    writeln!(writer, "Metrics")?;
    writeln!(writer, "Resource")?;
    if let Some(schema_url) = metrics.resource().schema_url() {
        writeln!(writer, "\tResource SchemaUrl: {schema_url:?}")?;
    }
    for (key, value) in metrics.resource() {
        writeln!(writer, "\t ->  {key}={value:?}")?;
    }

    for (scope_index, scope_metrics) in metrics.scope_metrics().enumerate() {
        writeln!(writer, "\tInstrumentation Scope #{scope_index}")?;
        writeln!(
            writer,
            "\t\tName         : {}",
            scope_metrics.scope().name()
        )?;
        if let Some(version) = scope_metrics.scope().version() {
            writeln!(writer, "\t\tVersion      : {version:?}")?;
        }
        if let Some(schema_url) = scope_metrics.scope().schema_url() {
            writeln!(writer, "\t\tSchemaUrl    : {schema_url:?}")?;
        }
        for (index, attribute) in scope_metrics.scope().attributes().enumerate() {
            if index == 0 {
                writeln!(writer, "\t\tScope Attributes:")?;
            }
            writeln!(writer, "\t\t\t ->  {}: {}", attribute.key, attribute.value)?;
        }

        for (metric_index, metric) in scope_metrics.metrics().enumerate() {
            writeln!(writer, "Metric #{metric_index}")?;
            writeln!(writer, "\t\tName         : {}", metric.name())?;
            writeln!(writer, "\t\tDescription  : {}", metric.description())?;
            writeln!(writer, "\t\tUnit         : {}", metric.unit())?;
            format_aggregation(writer, metric.data(), metric.unit())?;
        }
    }
    Ok(())
}

fn format_aggregation(
    writer: &mut dyn io::Write,
    aggregation: &data::AggregatedMetrics,
    unit: &str,
) -> io::Result<()> {
    match aggregation {
        data::AggregatedMetrics::U64(value) => format_metric_data(writer, value, unit),
        data::AggregatedMetrics::I64(value) => format_metric_data(writer, value, unit),
        data::AggregatedMetrics::F64(value) => format_metric_data(writer, value, unit),
    }
}

fn format_metric_data<T: Copy + Debug>(
    writer: &mut dyn io::Write,
    data: &data::MetricData<T>,
    unit: &str,
) -> io::Result<()> {
    match data {
        data::MetricData::Gauge(value) => {
            writeln!(writer, "\t\tType         : Gauge")?;
            format_gauge(writer, value)
        }
        data::MetricData::Sum(value) => {
            writeln!(writer, "\t\tType         : Sum")?;
            format_sum(writer, value)
        }
        data::MetricData::Histogram(value) => {
            writeln!(writer, "\t\tType         : Histogram")?;
            format_histogram(writer, value)
        }
        data::MetricData::ExponentialHistogram(value) => {
            writeln!(writer, "\t\tType         : Exponential Histogram")?;
            format_exponential_histogram(writer, value, unit)
        }
    }
}

fn format_sum<T: Copy + Debug>(writer: &mut dyn io::Write, sum: &data::Sum<T>) -> io::Result<()> {
    writeln!(writer, "\t\tSum DataPoints")?;
    writeln!(writer, "\t\tMonotonic    : {}", sum.is_monotonic())?;
    format_temporality(writer, sum.temporality())?;
    format_time(writer, "StartTime", sum.start_time())?;
    format_time(writer, "EndTime", sum.time())?;
    format_sum_data_points(writer, sum.data_points())
}

fn format_gauge<T: Copy + Debug>(
    writer: &mut dyn io::Write,
    gauge: &data::Gauge<T>,
) -> io::Result<()> {
    writeln!(writer, "\t\tGauge DataPoints")?;
    if let Some(start_time) = gauge.start_time() {
        format_time(writer, "StartTime", start_time)?;
    }
    format_time(writer, "EndTime", gauge.time())?;
    format_gauge_data_points(writer, gauge.data_points())
}

fn format_histogram<T: Copy + Debug>(
    writer: &mut dyn io::Write,
    histogram: &data::Histogram<T>,
) -> io::Result<()> {
    format_temporality(writer, histogram.temporality())?;
    format_time(writer, "StartTime", histogram.start_time())?;
    format_time(writer, "EndTime", histogram.time())?;
    writeln!(writer, "\t\tHistogram DataPoints")?;
    for (index, point) in histogram.data_points().enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        writeln!(writer, "\t\t\tCount        : {}", point.count())?;
        writeln!(writer, "\t\t\tSum          : {:?}", point.sum())?;
        if let Some(min) = point.min() {
            writeln!(writer, "\t\t\tMin          : {min:?}")?;
        }
        if let Some(max) = point.max() {
            writeln!(writer, "\t\t\tMax          : {max:?}")?;
        }
        format_attributes(writer, point.attributes())?;
    }
    Ok(())
}

fn format_exponential_histogram<T: Copy + Debug>(
    writer: &mut dyn io::Write,
    histogram: &data::ExponentialHistogram<T>,
    unit: &str,
) -> io::Result<()> {
    format_temporality(writer, histogram.temporality())?;
    format_time(writer, "StartTime", histogram.start_time())?;
    format_time(writer, "EndTime", histogram.time())?;
    writeln!(writer, "\t\tExponential Histogram DataPoints")?;
    for (index, point) in histogram.data_points().enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        writeln!(writer, "\t\t\tCount        : {}", point.count())?;
        writeln!(writer, "\t\t\tSum          : {:?}", point.sum())?;
        if let Some(min) = point.min() {
            writeln!(writer, "\t\t\tMin          : {min:?}")?;
        }
        if let Some(max) = point.max() {
            writeln!(writer, "\t\t\tMax          : {max:?}")?;
        }
        let base = exponential_base(point.scale());
        writeln!(writer, "\t\t\tScale        : {}", point.scale())?;
        writeln!(writer, "\t\t\tBase         : {base:.12}")?;
        writeln!(writer, "\t\t\tZeroCount    : {}", point.zero_count())?;
        writeln!(
            writer,
            "\t\t\tZeroThreshold: {:.12e}",
            point.zero_threshold()
        )?;
        format_attributes(writer, point.attributes())?;
        format_exponential_buckets(writer, point, unit, base)?;
    }
    Ok(())
}

fn format_exponential_buckets<T>(
    writer: &mut dyn io::Write,
    point: &data::ExponentialHistogramDataPoint<T>,
    unit: &str,
    base: f64,
) -> io::Result<()> {
    writeln!(writer, "\t\t\tBuckets      :")?;
    for (position, count) in point.negative_bucket().counts().enumerate() {
        if count == 0 {
            continue;
        }
        let index = bucket_index(point.negative_bucket().offset(), position)?;
        let lower = -base.powi(index + 1);
        let upper = -base.powi(index);
        format_bucket(writer, '[', lower, upper, ')', count, unit)?;
    }
    if point.zero_count() > 0 {
        format_bucket(
            writer,
            '[',
            -point.zero_threshold(),
            point.zero_threshold(),
            ']',
            point.zero_count(),
            unit,
        )?;
    }
    for (position, count) in point.positive_bucket().counts().enumerate() {
        if count == 0 {
            continue;
        }
        let index = bucket_index(point.positive_bucket().offset(), position)?;
        let lower = base.powi(index);
        let upper = base.powi(index + 1);
        format_bucket(writer, '(', lower, upper, ']', count, unit)?;
    }
    Ok(())
}

fn format_bucket(
    writer: &mut dyn io::Write,
    left: char,
    lower: f64,
    upper: f64,
    right: char,
    count: u64,
    unit: &str,
) -> io::Result<()> {
    if unit.is_empty() {
        writeln!(
            writer,
            "\t\t\t\t{left}{lower:.12e}, {upper:.12e}{right}: {count}"
        )
    } else {
        writeln!(
            writer,
            "\t\t\t\t{left}{lower:.12e}, {upper:.12e}{right} {unit}: {count}"
        )
    }
}

fn bucket_index(offset: i32, position: usize) -> io::Result<i32> {
    let position = i32::try_from(position).map_err(io::Error::other)?;
    offset.checked_add(position).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "exponential histogram bucket index overflow",
        )
    })
}

fn exponential_base(scale: i8) -> f64 {
    2.0_f64.powi(-i32::from(scale)).exp2()
}

fn format_temporality(writer: &mut dyn io::Write, temporality: Temporality) -> io::Result<()> {
    let name = match temporality {
        Temporality::Cumulative => "Cumulative",
        Temporality::Delta => "Delta",
        other => return writeln!(writer, "\t\tTemporality  : {other:?}"),
    };
    writeln!(writer, "\t\tTemporality  : {name}")
}

fn format_gauge_data_points<'a, T: Copy + Debug + 'a>(
    writer: &mut dyn io::Write,
    data_points: impl Iterator<Item = &'a data::GaugeDataPoint<T>>,
) -> io::Result<()> {
    for (index, point) in data_points.enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        writeln!(writer, "\t\t\tValue        : {:?}", point.value())?;
        format_attributes(writer, point.attributes())?;
    }
    Ok(())
}

fn format_sum_data_points<'a, T: Copy + Debug + 'a>(
    writer: &mut dyn io::Write,
    data_points: impl Iterator<Item = &'a data::SumDataPoint<T>>,
) -> io::Result<()> {
    for (index, point) in data_points.enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        writeln!(writer, "\t\t\tValue        : {:?}", point.value())?;
        format_attributes(writer, point.attributes())?;
    }
    Ok(())
}

fn format_time(writer: &mut dyn io::Write, label: &str, time: SystemTime) -> io::Result<()> {
    writeln!(
        writer,
        "\t\t\t{label:<13}: {}",
        humantime::format_rfc3339_micros(time)
    )
}

fn format_attributes<'a>(
    writer: &mut dyn io::Write,
    attributes: impl Iterator<Item = &'a KeyValue>,
) -> io::Result<()> {
    writeln!(writer, "\t\t\tAttributes   :")?;
    for attribute in attributes {
        writeln!(
            writer,
            "\t\t\t\t ->  {}: {}",
            attribute.key, attribute.value
        )?;
    }
    Ok(())
}

fn exponential_histogram_view(instrument: &Instrument) -> Option<Stream> {
    (instrument.kind() == InstrumentKind::Histogram && HISTOGRAM_NAMES.contains(&instrument.name()))
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

#[cfg(test)]
mod tests {
    use std::fs;

    use opentelemetry::metrics::MeterProvider as _;

    use super::*;

    #[test]
    fn selects_default_stderr_and_explicit_stdout() {
        let stderr = MetricsOutput::new(MetricsFormat::Text, None).expect("stderr opens");
        assert!(matches!(
            stderr.exporter.state.lock().unwrap().writer,
            MetricsWriter::Stderr
        ));

        let stdout = MetricsOutput::new(MetricsFormat::OtlpJson, Some(Path::new("-")))
            .expect("stdout opens");
        assert!(matches!(
            stdout.exporter.state.lock().unwrap().writer,
            MetricsWriter::Stdout
        ));

        stderr.exporter.state.lock().unwrap().pending = None;
        stdout.exporter.state.lock().unwrap().pending = None;
    }

    #[test]
    fn opens_and_truncates_metrics_file_immediately() {
        let path = std::env::temp_dir().join(format!(
            "oxia-cmd-metrics-output-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::write(&path, b"existing data").expect("test file is writable");

        let output = MetricsOutput::new(MetricsFormat::OtlpPb, Some(&path))
            .expect("metrics output file opens");
        assert!(matches!(
            output.exporter.state.lock().unwrap().writer,
            MetricsWriter::File(_)
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);

        drop(output);
        fs::remove_file(path).expect("test file is removable");
    }

    #[test]
    fn reports_metrics_file_open_failure_immediately() {
        let path = std::env::temp_dir()
            .join("oxia-cmd-missing-directory")
            .join("metrics.out");
        let error = MetricsOutput::new(MetricsFormat::Text, Some(&path))
            .expect_err("missing parent directory must fail");
        assert!(error.to_string().contains("failed to open metrics output"));
    }

    #[test]
    fn formats_populated_exponential_histogram_buckets() {
        let (output, path) = file_output(MetricsFormat::Text);
        let histogram = output
            .provider
            .meter("test.scope")
            .f64_histogram("db.client.operation.duration")
            .with_unit("s")
            .build();
        for value in [-4.0, -1.0, 0.0, 0.0, 1.0, 4.0] {
            histogram.record(value, &[KeyValue::new("result", "success")]);
        }
        output.export().expect("metrics export succeeds");
        let output = fs::read_to_string(&path).expect("metrics output is readable");
        fs::remove_file(path).expect("test file is removable");

        assert!(output.contains("Type         : Exponential Histogram"));
        assert!(output.contains("Count        : 6"));
        assert!(output.contains("Sum          : 0.0"));
        assert!(output.contains("ZeroCount    : 2"));
        assert!(output.contains("Buckets      :"));
        assert!(output.contains(" s: 1"));
        assert!(output.contains(" ->  result: success"));
    }

    #[test]
    fn formats_u64_exponential_histogram_without_empty_zero_bucket() {
        let (output, path) = file_output(MetricsFormat::Text);
        output
            .provider
            .meter("test.scope")
            .u64_histogram("db.client.operation.size")
            .with_unit("By")
            .build()
            .record(3, &[]);
        output.export().expect("metrics export succeeds");
        let output = fs::read_to_string(&path).expect("metrics output is readable");
        fs::remove_file(path).expect("test file is removable");

        assert!(output.contains("Temporality  : Cumulative"));
        assert!(output.contains("Sum          : 3"));
        assert!(output.contains(" By: 1"));
        assert!(!output.contains("[0.000000000000e0, 0.000000000000e0]"));
    }

    fn file_output(format: MetricsFormat) -> (MetricsOutput, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "oxia-cmd-metrics-format-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let output = MetricsOutput::new(format, Some(&path)).expect("metrics output file opens");
        (output, path)
    }
}
