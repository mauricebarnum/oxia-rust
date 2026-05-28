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

pub(super) fn format_metrics(
    writer: &mut dyn io::Write,
    metrics: &ResourceMetrics,
) -> io::Result<()> {
    validate_metrics(metrics)?;
    let request = ExportMetricsServiceRequest::from(metrics);

    #[cfg(feature = "otlp-json-compat")]
    let request = compatibility_json(&request)?;

    serde_json::to_writer(&mut *writer, &request).map_err(io::Error::other)?;
    writeln!(writer)
}

fn validate_metrics(metrics: &ResourceMetrics) -> io::Result<()> {
    for scope in &metrics.scope_metrics {
        for metric in &scope.metrics {
            let aggregation = metric.data.as_any();
            if let Some(value) = aggregation.downcast_ref::<data::Gauge<u64>>() {
                validate_number_points(&metric.name, &value.data_points)?;
            } else if let Some(value) = aggregation.downcast_ref::<data::Sum<u64>>() {
                validate_number_points(&metric.name, &value.data_points)?;
            } else if let Some(value) = aggregation.downcast_ref::<data::Histogram<u64>>() {
                for point in &value.data_points {
                    validate_exemplars(&metric.name, &point.exemplars)?;
                }
            } else if let Some(value) =
                aggregation.downcast_ref::<data::ExponentialHistogram<u64>>()
            {
                for point in &value.data_points {
                    validate_exemplars(&metric.name, &point.exemplars)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_number_points(name: &str, points: &[data::DataPoint<u64>]) -> io::Result<()> {
    for point in points {
        validate_otlp_int(name, point.value)?;
        validate_exemplars(name, &point.exemplars)?;
    }
    Ok(())
}

fn validate_exemplars(name: &str, exemplars: &[data::Exemplar<u64>]) -> io::Result<()> {
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
    use std::borrow::Cow;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use opentelemetry::InstrumentationScope;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data::DataPoint;
    use opentelemetry_sdk::metrics::data::Exemplar;
    use opentelemetry_sdk::metrics::data::Histogram;
    use opentelemetry_sdk::metrics::data::HistogramDataPoint;
    use opentelemetry_sdk::metrics::data::Metric;
    use opentelemetry_sdk::metrics::data::ScopeMetrics;
    use opentelemetry_sdk::metrics::data::Sum;

    use super::*;

    // This passes with the compatibility feature. Without that feature, it
    // passes only when the upstream SDK emits canonical OTLP JSON by itself.
    #[test]
    fn emits_canonical_otlp_json() {
        let metrics = test_metrics(42);
        let mut output = Vec::new();
        format_metrics(&mut output, &metrics).expect("JSON formatting succeeds");
        assert_eq!(output.last(), Some(&b'\n'));

        let document: serde_json::Value =
            serde_json::from_slice(&output).expect("valid JSON document");
        assert_canonical_otlp_json(&document);
    }

    #[test]
    fn rejects_unsigned_values_outside_otlp_int64_range() {
        let metrics = test_metrics(u64::MAX);
        let error = format_metrics(&mut Vec::new(), &metrics).expect_err("value is rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds OTLP int64 range"));
    }

    fn assert_canonical_otlp_json(document: &serde_json::Value) {
        let resource_metrics = &document["resourceMetrics"][0];
        assert_eq!(
            resource_metrics["scopeMetrics"][0]["scope"]["name"],
            "test.scope"
        );

        let sum = &resource_metrics["scopeMetrics"][0]["metrics"][0]["sum"];
        assert_eq!(sum["aggregationTemporality"], 2);
        assert_eq!(sum["dataPoints"][0]["asInt"], "42");
        assert_eq!(sum["dataPoints"][0]["startTimeUnixNano"], "123");
        assert_eq!(sum["dataPoints"][0]["timeUnixNano"], "456");
        assert_eq!(
            sum["dataPoints"][0]["exemplars"][0]["spanId"],
            "0102030405060708"
        );
        assert_eq!(
            sum["dataPoints"][0]["exemplars"][0]["traceId"],
            "0102030405060708090a0b0c0d0e0f10"
        );

        let histogram =
            &resource_metrics["scopeMetrics"][0]["metrics"][1]["histogram"]["dataPoints"][0];
        assert_eq!(histogram["count"], "3");
        assert_eq!(
            histogram["bucketCounts"],
            serde_json::json!(["1", "1", "1"])
        );
    }

    fn test_metrics(sum_value: u64) -> ResourceMetrics {
        let start = UNIX_EPOCH + Duration::from_nanos(123);
        let end = UNIX_EPOCH + Duration::from_nanos(456);
        ResourceMetrics {
            resource: Resource::new([KeyValue::new("service.name", "oxia-cmd")]),
            scope_metrics: vec![ScopeMetrics {
                scope: InstrumentationScope::builder("test.scope").build(),
                metrics: vec![
                    Metric {
                        name: Cow::Borrowed("requests.total"),
                        description: Cow::Borrowed("request count"),
                        unit: Cow::Borrowed("1"),
                        data: Box::new(Sum {
                            data_points: vec![DataPoint {
                                attributes: Vec::new(),
                                start_time: Some(start),
                                time: Some(end),
                                value: sum_value,
                                exemplars: vec![Exemplar {
                                    filtered_attributes: Vec::new(),
                                    time: end,
                                    value: 7,
                                    span_id: [1, 2, 3, 4, 5, 6, 7, 8],
                                    trace_id: [
                                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                    ],
                                }],
                            }],
                            temporality: Temporality::Cumulative,
                            is_monotonic: true,
                        }),
                    },
                    Metric {
                        name: Cow::Borrowed("request.duration"),
                        description: Cow::Borrowed("duration"),
                        unit: Cow::Borrowed("s"),
                        data: Box::new(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: Vec::new(),
                                start_time: start,
                                time: end,
                                count: 3,
                                bounds: vec![0.1, 1.0],
                                bucket_counts: vec![1, 1, 1],
                                min: Some(0.01),
                                max: Some(2.0),
                                sum: 2.5,
                                exemplars: Vec::new(),
                            }],
                            temporality: Temporality::Cumulative,
                        }),
                    },
                ],
            }],
        }
    }
}
