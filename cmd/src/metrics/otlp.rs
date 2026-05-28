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

use std::io;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::metrics::data;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use prost::Message as _;

pub(super) fn format_json(writer: &mut dyn io::Write, metrics: &ResourceMetrics) -> io::Result<()> {
    let request = export_request(metrics)?;

    #[cfg(feature = "otlp-json-compat")]
    let request = compatibility_json(&request)?;

    serde_json::to_writer(&mut *writer, &request).map_err(io::Error::other)?;
    writeln!(writer)
}

pub(super) fn format_protobuf(
    writer: &mut dyn io::Write,
    metrics: &ResourceMetrics,
) -> io::Result<()> {
    writer.write_all(&export_request(metrics)?.encode_to_vec())
}

fn export_request(metrics: &ResourceMetrics) -> io::Result<ExportMetricsServiceRequest> {
    validate_metrics(metrics)?;
    Ok(ExportMetricsServiceRequest::from(metrics))
}

fn validate_metrics(metrics: &ResourceMetrics) -> io::Result<()> {
    for scope in metrics.scope_metrics() {
        for metric in scope.metrics() {
            if let data::AggregatedMetrics::U64(value) = metric.data() {
                validate_u64_metric(metric.name(), value)?;
            }
        }
    }
    Ok(())
}

fn validate_u64_metric(name: &str, metric: &data::MetricData<u64>) -> io::Result<()> {
    match metric {
        data::MetricData::Gauge(value) => {
            for point in value.data_points() {
                validate_otlp_int(name, point.value())?;
                validate_exemplars(name, point.exemplars())?;
            }
        }
        data::MetricData::Sum(value) => {
            for point in value.data_points() {
                validate_otlp_int(name, point.value())?;
                validate_exemplars(name, point.exemplars())?;
            }
        }
        data::MetricData::Histogram(value) => {
            for point in value.data_points() {
                validate_exemplars(name, point.exemplars())?;
            }
        }
        data::MetricData::ExponentialHistogram(value) => {
            for point in value.data_points() {
                validate_exemplars(name, point.exemplars())?;
            }
        }
    }
    Ok(())
}

fn validate_exemplars<'a>(
    name: &str,
    exemplars: impl Iterator<Item = &'a data::Exemplar<u64>>,
) -> io::Result<()> {
    for exemplar in exemplars {
        validate_otlp_int(name, exemplar.value)?;
    }
    Ok(())
}

fn validate_otlp_int(name: &str, value: u64) -> io::Result<()> {
    i64::try_from(value).map(|_| ()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metric {name} value {value} exceeds OTLP int64 range"),
        )
    })
}

#[cfg(feature = "otlp-json-compat")]
fn compatibility_json(request: &ExportMetricsServiceRequest) -> io::Result<serde_json::Value> {
    let mut value = serde_json::to_value(request).map_err(io::Error::other)?;
    normalize_otlp_json(&mut value);
    Ok(value)
}

#[cfg(feature = "otlp-json-compat")]
fn normalize_otlp_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_otlp_json(value);
            }
        }
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(
                    name.as_str(),
                    "asInt"
                        | "bucketCounts"
                        | "count"
                        | "startTimeUnixNano"
                        | "timeUnixNano"
                        | "zeroCount"
                ) {
                    stringify_integer(value);
                }
                normalize_otlp_json(value);
            }
        }
        _ => {}
    }
}

#[cfg(feature = "otlp-json-compat")]
fn stringify_integer(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                stringify_integer(value);
            }
        }
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => {
            *value = serde_json::Value::String(number.to_string());
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    // This passes with the compatibility feature. Without that feature, it
    // passes only when the upstream SDK emits canonical OTLP JSON by itself.
    #[test]
    fn emits_canonical_otlp_json() {
        let (_provider, metrics) = test_metrics(42);
        let mut output = Vec::new();
        format_json(&mut output, &metrics).expect("JSON formatting succeeds");
        assert_eq!(output.last(), Some(&b'\n'));

        let document: serde_json::Value =
            serde_json::from_slice(&output).expect("valid JSON document");
        assert_canonical_otlp_json(&document);
    }

    #[test]
    fn rejects_unsigned_values_outside_otlp_int64_range() {
        let (_provider, metrics) = test_metrics(u64::MAX);
        let error = format_json(&mut Vec::new(), &metrics).expect_err("value is rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds OTLP int64 range"));
    }

    #[test]
    fn emits_otlp_protobuf() {
        let (_provider, metrics) = test_metrics(42);
        let mut output = Vec::new();
        format_protobuf(&mut output, &metrics).expect("protobuf formatting succeeds");

        let request = ExportMetricsServiceRequest::decode(output.as_slice())
            .expect("valid ExportMetricsServiceRequest protobuf");
        assert_eq!(request.resource_metrics.len(), 1);
        assert_eq!(request.resource_metrics[0].scope_metrics.len(), 1);
        assert_eq!(
            request.resource_metrics[0].scope_metrics[0].metrics.len(),
            2
        );
    }

    #[test]
    fn protobuf_rejects_unsigned_values_outside_otlp_int64_range() {
        let (_provider, metrics) = test_metrics(u64::MAX);
        let error = format_protobuf(&mut Vec::new(), &metrics).expect_err("value is rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds OTLP int64 range"));
    }

    fn assert_canonical_otlp_json(document: &serde_json::Value) {
        let resource_metrics = &document["resourceMetrics"][0];
        assert_eq!(
            resource_metrics["scopeMetrics"][0]["scope"]["name"],
            "test.scope"
        );

        let metrics = resource_metrics["scopeMetrics"][0]["metrics"]
            .as_array()
            .expect("metrics array");
        let sum = &metrics
            .iter()
            .find(|metric| metric["name"] == "requests.total")
            .expect("sum metric")["sum"];
        assert_eq!(sum["aggregationTemporality"], 2);
        assert_eq!(sum["dataPoints"][0]["asInt"], "42");
        assert!(sum["dataPoints"][0]["startTimeUnixNano"].is_string());
        assert!(sum["dataPoints"][0]["timeUnixNano"].is_string());

        let histogram = &metrics
            .iter()
            .find(|metric| metric["name"] == "request.duration")
            .expect("histogram metric")["histogram"]["dataPoints"][0];
        assert_eq!(histogram["count"], "3");
        assert_eq!(
            histogram["bucketCounts"],
            serde_json::json!(["1", "1", "1"])
        );
    }

    fn test_metrics(sum_value: u64) -> (SdkMeterProvider, ResourceMetrics) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("test.scope");
        meter
            .u64_counter("requests.total")
            .with_description("request count")
            .with_unit("1")
            .build()
            .add(sum_value, &[]);
        let histogram = meter
            .f64_histogram("request.duration")
            .with_description("duration")
            .with_unit("s")
            .with_boundaries(vec![0.1, 1.0])
            .build();
        for value in [0.01, 0.5, 2.0] {
            histogram.record(value, &[]);
        }
        provider.force_flush().expect("metrics collection succeeds");
        let metrics = exporter
            .get_finished_metrics()
            .expect("metrics export succeeds")
            .pop()
            .expect("metrics were exported");
        (provider, metrics)
    }
}
