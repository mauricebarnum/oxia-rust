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
use std::path::Path;
use std::sync::Arc;
use std::sync::Weak;
use std::time::SystemTime;

use anyhow::Context as _;
use opentelemetry::KeyValue;
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

use crate::MetricsFormat;

mod json;
mod om2;

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
    reader: SharedManualReader,
    format: MetricsFormat,
    writer: MetricsWriter,
}

#[derive(Debug)]
enum MetricsWriter {
    Stderr,
    Stdout,
    File(File),
}

impl MetricsOutput {
    pub fn new(format: MetricsFormat, output: Option<&Path>) -> anyhow::Result<Self> {
        let reader = SharedManualReader::new(
            ManualReader::builder()
                .with_temporality(Temporality::Cumulative)
                .build(),
        );
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .with_view(exponential_histogram_view)
            .build();
        let writer = match output {
            None => MetricsWriter::Stderr,
            Some(path) if path == Path::new("-") => MetricsWriter::Stdout,
            Some(path) => MetricsWriter::File(
                File::create(path)
                    .with_context(|| format!("failed to open metrics output {}", path.display()))?,
            ),
        };
        Ok(Self {
            provider,
            reader,
            format,
            writer,
        })
    }

    pub fn meter_provider(&self) -> crate::client_lib::config::MeterProviderHandle {
        Arc::new(self.provider.clone())
    }

    pub fn export(mut self) -> anyhow::Result<()> {
        let mut metrics = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: Vec::new(),
        };
        self.reader
            .collect(&mut metrics)
            .context("failed to collect metrics")?;
        let format = self.format;
        let writer: &mut dyn io::Write = match &mut self.writer {
            MetricsWriter::Stderr => &mut io::stderr().lock(),
            MetricsWriter::Stdout => &mut io::stdout().lock(),
            MetricsWriter::File(file) => file,
        };
        let format_result = match format {
            MetricsFormat::Text => format_metrics(writer, &metrics),
            MetricsFormat::OtlpJson => json::format_metrics(writer, &metrics),
            MetricsFormat::Om2 => om2::format_metrics(writer, &metrics),
        };
        let shutdown_result = self
            .provider
            .shutdown()
            .context("failed to shut down metrics provider");
        format_result.context("failed to write metrics")?;
        shutdown_result
    }
}

fn format_metrics(writer: &mut dyn io::Write, metrics: &ResourceMetrics) -> io::Result<()> {
    writeln!(writer, "Metrics")?;
    writeln!(writer, "Resource")?;
    if let Some(schema_url) = metrics.resource.schema_url() {
        writeln!(writer, "\tResource SchemaUrl: {schema_url:?}")?;
    }
    for (key, value) in &metrics.resource {
        writeln!(writer, "\t ->  {key}={value:?}")?;
    }

    for (scope_index, scope_metrics) in metrics.scope_metrics.iter().enumerate() {
        writeln!(writer, "\tInstrumentation Scope #{scope_index}")?;
        writeln!(writer, "\t\tName         : {}", scope_metrics.scope.name())?;
        if let Some(version) = scope_metrics.scope.version() {
            writeln!(writer, "\t\tVersion      : {version:?}")?;
        }
        if let Some(schema_url) = scope_metrics.scope.schema_url() {
            writeln!(writer, "\t\tSchemaUrl    : {schema_url:?}")?;
        }
        for (index, attribute) in scope_metrics.scope.attributes().enumerate() {
            if index == 0 {
                writeln!(writer, "\t\tScope Attributes:")?;
            }
            writeln!(writer, "\t\t\t ->  {}: {}", attribute.key, attribute.value)?;
        }

        for (metric_index, metric) in scope_metrics.metrics.iter().enumerate() {
            writeln!(writer, "Metric #{metric_index}")?;
            writeln!(writer, "\t\tName         : {}", metric.name)?;
            writeln!(writer, "\t\tDescription  : {}", metric.description)?;
            writeln!(writer, "\t\tUnit         : {}", metric.unit)?;

            let aggregation = metric.data.as_any();
            if let Some(histogram) = aggregation.downcast_ref::<data::Histogram<u64>>() {
                writeln!(writer, "\t\tType         : Histogram")?;
                format_histogram(writer, histogram)?;
            } else if let Some(histogram) = aggregation.downcast_ref::<data::Histogram<f64>>() {
                writeln!(writer, "\t\tType         : Histogram")?;
                format_histogram(writer, histogram)?;
            } else if let Some(histogram) =
                aggregation.downcast_ref::<data::ExponentialHistogram<u64>>()
            {
                writeln!(writer, "\t\tType         : Exponential Histogram")?;
                format_exponential_histogram(writer, histogram, &metric.unit)?;
            } else if let Some(histogram) =
                aggregation.downcast_ref::<data::ExponentialHistogram<f64>>()
            {
                writeln!(writer, "\t\tType         : Exponential Histogram")?;
                format_exponential_histogram(writer, histogram, &metric.unit)?;
            } else if let Some(sum) = aggregation.downcast_ref::<data::Sum<u64>>() {
                writeln!(writer, "\t\tType         : Sum")?;
                format_sum(writer, sum)?;
            } else if let Some(sum) = aggregation.downcast_ref::<data::Sum<i64>>() {
                writeln!(writer, "\t\tType         : Sum")?;
                format_sum(writer, sum)?;
            } else if let Some(sum) = aggregation.downcast_ref::<data::Sum<f64>>() {
                writeln!(writer, "\t\tType         : Sum")?;
                format_sum(writer, sum)?;
            } else if let Some(gauge) = aggregation.downcast_ref::<data::Gauge<u64>>() {
                writeln!(writer, "\t\tType         : Gauge")?;
                format_gauge(writer, gauge)?;
            } else if let Some(gauge) = aggregation.downcast_ref::<data::Gauge<i64>>() {
                writeln!(writer, "\t\tType         : Gauge")?;
                format_gauge(writer, gauge)?;
            } else if let Some(gauge) = aggregation.downcast_ref::<data::Gauge<f64>>() {
                writeln!(writer, "\t\tType         : Gauge")?;
                format_gauge(writer, gauge)?;
            } else {
                writeln!(writer, "\t\tUnsupported data type")?;
            }
        }
    }
    Ok(())
}

fn format_sum<T: Debug>(writer: &mut dyn io::Write, sum: &data::Sum<T>) -> io::Result<()> {
    writeln!(writer, "\t\tSum DataPoints")?;
    writeln!(writer, "\t\tMonotonic    : {}", sum.is_monotonic)?;
    format_temporality(writer, sum.temporality)?;
    format_data_points(writer, &sum.data_points)
}

fn format_gauge<T: Debug>(writer: &mut dyn io::Write, gauge: &data::Gauge<T>) -> io::Result<()> {
    writeln!(writer, "\t\tGauge DataPoints")?;
    format_data_points(writer, &gauge.data_points)
}

fn format_histogram<T: Debug>(
    writer: &mut dyn io::Write,
    histogram: &data::Histogram<T>,
) -> io::Result<()> {
    format_temporality(writer, histogram.temporality)?;
    writeln!(writer, "\t\tHistogram DataPoints")?;
    for (index, point) in histogram.data_points.iter().enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        format_time(writer, "StartTime", point.start_time)?;
        format_time(writer, "EndTime", point.time)?;
        writeln!(writer, "\t\t\tCount        : {}", point.count)?;
        writeln!(writer, "\t\t\tSum          : {:?}", point.sum)?;
        if let Some(min) = &point.min {
            writeln!(writer, "\t\t\tMin          : {min:?}")?;
        }
        if let Some(max) = &point.max {
            writeln!(writer, "\t\t\tMax          : {max:?}")?;
        }
        format_attributes(writer, &point.attributes)?;
    }
    Ok(())
}

fn format_exponential_histogram<T: Debug>(
    writer: &mut dyn io::Write,
    histogram: &data::ExponentialHistogram<T>,
    unit: &str,
) -> io::Result<()> {
    format_temporality(writer, histogram.temporality)?;
    writeln!(writer, "\t\tExponential Histogram DataPoints")?;
    for (index, point) in histogram.data_points.iter().enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        format_time(writer, "StartTime", point.start_time)?;
        format_time(writer, "EndTime", point.time)?;
        writeln!(writer, "\t\t\tCount        : {}", point.count)?;
        writeln!(writer, "\t\t\tSum          : {:?}", point.sum)?;
        if let Some(min) = &point.min {
            writeln!(writer, "\t\t\tMin          : {min:?}")?;
        }
        if let Some(max) = &point.max {
            writeln!(writer, "\t\t\tMax          : {max:?}")?;
        }
        let base = exponential_base(point.scale);
        writeln!(writer, "\t\t\tScale        : {}", point.scale)?;
        writeln!(writer, "\t\t\tBase         : {base:.12}")?;
        writeln!(writer, "\t\t\tZeroCount    : {}", point.zero_count)?;
        writeln!(writer, "\t\t\tZeroThreshold: {:.12e}", point.zero_threshold)?;
        format_attributes(writer, &point.attributes)?;
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
    for (position, count) in point.negative_bucket.counts.iter().enumerate() {
        if *count == 0 {
            continue;
        }
        let index = bucket_index(point.negative_bucket.offset, position)?;
        let lower = -base.powi(index + 1);
        let upper = -base.powi(index);
        format_bucket(writer, '[', lower, upper, ')', *count, unit)?;
    }
    if point.zero_count > 0 {
        format_bucket(
            writer,
            '[',
            -point.zero_threshold,
            point.zero_threshold,
            ']',
            point.zero_count,
            unit,
        )?;
    }
    for (position, count) in point.positive_bucket.counts.iter().enumerate() {
        if *count == 0 {
            continue;
        }
        let index = bucket_index(point.positive_bucket.offset, position)?;
        let lower = base.powi(index);
        let upper = base.powi(index + 1);
        format_bucket(writer, '(', lower, upper, ']', *count, unit)?;
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

fn format_data_points<T: Debug>(
    writer: &mut dyn io::Write,
    data_points: &[data::DataPoint<T>],
) -> io::Result<()> {
    for (index, point) in data_points.iter().enumerate() {
        writeln!(writer, "\t\tDataPoint #{index}")?;
        if let Some(start_time) = point.start_time {
            format_time(writer, "StartTime", start_time)?;
        }
        if let Some(time) = point.time {
            format_time(writer, "EndTime", time)?;
        }
        writeln!(writer, "\t\t\tValue        : {:?}", point.value)?;
        format_attributes(writer, &point.attributes)?;
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

fn format_attributes(writer: &mut dyn io::Write, attributes: &[KeyValue]) -> io::Result<()> {
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

// SharedManualReader is a wrapper to address ownership issues with the
// MeterProvider builder, that takes ownership of a MetricReader. But we
// need to keep a reference for collecting the metrics at program shutdown,
// hence this Arc wrapper.

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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::fs;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use opentelemetry::InstrumentationScope;
    use opentelemetry_sdk::metrics::data::ExponentialBucket;
    use opentelemetry_sdk::metrics::data::ExponentialHistogram;
    use opentelemetry_sdk::metrics::data::ExponentialHistogramDataPoint;
    use opentelemetry_sdk::metrics::data::Metric;
    use opentelemetry_sdk::metrics::data::ScopeMetrics;

    use super::*;

    #[test]
    fn selects_default_stderr_and_explicit_stdout() {
        let stderr = MetricsOutput::new(MetricsFormat::Text, None).expect("stderr opens");
        assert!(matches!(stderr.writer, MetricsWriter::Stderr));

        let stdout = MetricsOutput::new(MetricsFormat::OtlpJson, Some(Path::new("-")))
            .expect("stdout opens");
        assert!(matches!(stdout.writer, MetricsWriter::Stdout));
    }

    #[test]
    fn opens_and_truncates_metrics_file_immediately() {
        let path = std::env::temp_dir().join(format!(
            "oxia-cmd-metrics-output-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::write(&path, b"existing data").expect("test file is writable");

        let output =
            MetricsOutput::new(MetricsFormat::Om2, Some(&path)).expect("metrics output file opens");
        assert!(matches!(output.writer, MetricsWriter::File(_)));
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
        let metrics = resource_metrics(Metric {
            name: Cow::Borrowed("test.duration"),
            description: Cow::Borrowed("test histogram"),
            unit: Cow::Borrowed("s"),
            data: Box::new(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    attributes: vec![KeyValue::new("result", "success")],
                    start_time: UNIX_EPOCH,
                    time: UNIX_EPOCH + Duration::from_secs(1),
                    count: 6,
                    min: Some(-4.0),
                    max: Some(4.0),
                    sum: 0.0_f64,
                    scale: 0,
                    zero_count: 2,
                    positive_bucket: ExponentialBucket {
                        offset: 0,
                        counts: vec![1, 0, 1],
                    },
                    negative_bucket: ExponentialBucket {
                        offset: 0,
                        counts: vec![1, 0, 1],
                    },
                    zero_threshold: 0.5,
                    exemplars: Vec::new(),
                }],
                temporality: Temporality::Cumulative,
            }),
        });

        let output = format_to_string(&metrics);

        assert!(output.contains("Type         : Exponential Histogram"));
        assert!(output.contains("Count        : 6"));
        assert!(output.contains("Sum          : 0.0"));
        assert!(output.contains("Scale        : 0"));
        assert!(output.contains("Base         : 2.000000000000"));
        assert!(output.contains("[-2.000000000000e0, -1.000000000000e0) s: 1"));
        assert!(output.contains("[-8.000000000000e0, -4.000000000000e0) s: 1"));
        assert!(output.contains("[-5.000000000000e-1, 5.000000000000e-1] s: 2"));
        assert!(output.contains("(1.000000000000e0, 2.000000000000e0] s: 1"));
        assert!(output.contains("(4.000000000000e0, 8.000000000000e0] s: 1"));
        assert!(!output.contains("2.000000000000e0, 4.000000000000e0"));
        assert!(output.contains(" ->  result: success"));
    }

    #[test]
    fn formats_u64_exponential_histogram_without_empty_zero_bucket() {
        let metrics = resource_metrics(Metric {
            name: Cow::Borrowed("test.size"),
            description: Cow::Borrowed(""),
            unit: Cow::Borrowed("By"),
            data: Box::new(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    attributes: Vec::new(),
                    start_time: UNIX_EPOCH,
                    time: UNIX_EPOCH,
                    count: 1,
                    min: None,
                    max: None,
                    sum: 3_u64,
                    scale: 0,
                    zero_count: 0,
                    positive_bucket: ExponentialBucket {
                        offset: 1,
                        counts: vec![1],
                    },
                    negative_bucket: ExponentialBucket {
                        offset: 0,
                        counts: Vec::new(),
                    },
                    zero_threshold: 0.0,
                    exemplars: Vec::new(),
                }],
                temporality: Temporality::Delta,
            }),
        });

        let output = format_to_string(&metrics);

        assert!(output.contains("Temporality  : Delta"));
        assert!(output.contains("Sum          : 3"));
        assert!(output.contains("(2.000000000000e0, 4.000000000000e0] By: 1"));
        assert!(!output.contains("[0.000000000000e0, 0.000000000000e0]"));
    }

    fn resource_metrics(metric: Metric) -> ResourceMetrics {
        ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: vec![ScopeMetrics {
                scope: InstrumentationScope::builder("test-scope").build(),
                metrics: vec![metric],
            }],
        }
    }

    fn format_to_string(metrics: &ResourceMetrics) -> String {
        let mut output = Vec::new();
        format_metrics(&mut output, metrics).expect("formatting succeeds");
        String::from_utf8(output).expect("formatter emits UTF-8")
    }
}
