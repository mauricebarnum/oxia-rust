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

use std::collections::BTreeMap;
use std::io;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data;
use opentelemetry_sdk::metrics::data::ResourceMetrics;

const MAX_SCHEMA: i8 = 8;
const MIN_SCHEMA: i8 = -4;

pub(super) fn format_metrics(
    writer: &mut dyn io::Write,
    metrics: &ResourceMetrics,
) -> io::Result<()> {
    if metrics.resource.iter().next().is_some() {
        writeln!(writer, "# TYPE target_info info")?;
        writeln!(writer, "# HELP target_info Target metadata")?;
        let attributes = metrics
            .resource
            .iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone()));
        write_sample_name(writer, "target_info", attributes)?;
        writeln!(writer, " 1")?;
    }

    for scope in &metrics.scope_metrics {
        for metric in &scope.metrics {
            format_metric(writer, metric)?;
        }
    }
    writeln!(writer, "# EOF")
}

fn format_metric(writer: &mut dyn io::Write, metric: &data::Metric) -> io::Result<()> {
    let aggregation = metric.data.as_any();
    if let Some(value) = aggregation.downcast_ref::<data::Gauge<u64>>() {
        format_gauge(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Gauge<i64>>() {
        format_gauge(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Gauge<f64>>() {
        format_gauge(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Sum<u64>>() {
        format_sum(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Sum<i64>>() {
        format_sum(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Sum<f64>>() {
        format_sum(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Histogram<u64>>() {
        format_histogram(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::Histogram<f64>>() {
        format_histogram(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::ExponentialHistogram<u64>>() {
        format_exponential_histogram(writer, metric, value)
    } else if let Some(value) = aggregation.downcast_ref::<data::ExponentialHistogram<f64>>() {
        format_exponential_histogram(writer, metric, value)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported metric aggregation for {}", metric.name),
        ))
    }
}

trait OmNumber: Copy {
    fn om(self) -> String;
}

impl OmNumber for u64 {
    fn om(self) -> String {
        self.to_string()
    }
}

impl OmNumber for i64 {
    fn om(self) -> String {
        self.to_string()
    }
}

impl OmNumber for f64 {
    fn om(self) -> String {
        format_float(self)
    }
}

fn format_gauge<T: OmNumber>(
    writer: &mut dyn io::Write,
    metric: &data::Metric,
    gauge: &data::Gauge<T>,
) -> io::Result<()> {
    metadata(writer, metric, "gauge")?;
    for point in &gauge.data_points {
        write_sample_name(writer, &metric.name, point.attributes.iter().cloned())?;
        write!(writer, " {}", point.value.om())?;
        if let Some(time) = point.time {
            write!(writer, " {}", timestamp(time)?)?;
        }
        format_exemplars(writer, &point.exemplars)?;
        writeln!(writer)?;
    }
    Ok(())
}

fn format_sum<T: OmNumber>(
    writer: &mut dyn io::Write,
    metric: &data::Metric,
    sum: &data::Sum<T>,
) -> io::Result<()> {
    let is_counter = sum.is_monotonic && sum.temporality == Temporality::Cumulative;
    metadata(writer, metric, if is_counter { "counter" } else { "gauge" })?;
    for point in &sum.data_points {
        write_sample_name(writer, &metric.name, point.attributes.iter().cloned())?;
        write!(writer, " {}", point.value.om())?;
        if let Some(time) = point.time {
            write!(writer, " {}", timestamp(time)?)?;
        }
        if is_counter && let Some(start_time) = point.start_time {
            write!(writer, " st@{}", timestamp(start_time)?)?;
        }
        format_exemplars(writer, &point.exemplars)?;
        writeln!(writer)?;
    }
    Ok(())
}

fn format_histogram<T: OmNumber>(
    writer: &mut dyn io::Write,
    metric: &data::Metric,
    histogram: &data::Histogram<T>,
) -> io::Result<()> {
    metadata(writer, metric, "histogram")?;
    for point in &histogram.data_points {
        write_sample_name(writer, &metric.name, point.attributes.iter().cloned())?;
        write!(
            writer,
            " {{count:{},sum:{},bucket:[",
            point.count,
            point.sum.om()
        )?;
        let mut cumulative = 0_u64;
        for (index, count) in point.bucket_counts.iter().enumerate() {
            cumulative = cumulative.checked_add(*count).ok_or_else(count_overflow)?;
            if index > 0 {
                write!(writer, ",")?;
            }
            if let Some(bound) = point.bounds.get(index) {
                write!(writer, "{}:{cumulative}", format_float(*bound))?;
            } else {
                write!(writer, "+Inf:{cumulative}")?;
            }
        }
        write!(writer, "]}}")?;
        write!(
            writer,
            " {} st@{}",
            timestamp(point.time)?,
            timestamp(point.start_time)?
        )?;
        format_exemplars(writer, &point.exemplars)?;
        writeln!(writer)?;
    }
    Ok(())
}

fn format_exponential_histogram<T: OmNumber>(
    writer: &mut dyn io::Write,
    metric: &data::Metric,
    histogram: &data::ExponentialHistogram<T>,
) -> io::Result<()> {
    metadata(writer, metric, "histogram")?;
    for point in &histogram.data_points {
        write_sample_name(writer, &metric.name, point.attributes.iter().cloned())?;
        if point.scale < MIN_SCHEMA {
            format_exponential_as_classic(writer, point)?;
        } else {
            format_native_histogram(writer, point)?;
        }
        write!(
            writer,
            " {} st@{}",
            timestamp(point.time)?,
            timestamp(point.start_time)?
        )?;
        format_exemplars(writer, &point.exemplars)?;
        writeln!(writer)?;
    }
    Ok(())
}

fn format_native_histogram<T: OmNumber>(
    writer: &mut dyn io::Write,
    point: &data::ExponentialHistogramDataPoint<T>,
) -> io::Result<()> {
    let schema = point.scale.min(MAX_SCHEMA);
    let positive = normalized_buckets(&point.positive_bucket, point.scale, schema)?;
    let negative = normalized_buckets(&point.negative_bucket, point.scale, schema)?;
    write!(
        writer,
        " {{count:{},sum:{},schema:{schema},zero_threshold:{},zero_count:{}",
        point.count,
        point.sum.om(),
        format_float(point.zero_threshold),
        point.zero_count
    )?;
    write_sparse_buckets(writer, "negative", &negative)?;
    write_sparse_buckets(writer, "positive", &positive)?;
    write!(writer, "}}")
}

fn normalized_buckets(
    buckets: &data::ExponentialBucket,
    source_scale: i8,
    target_scale: i8,
) -> io::Result<BTreeMap<i32, u64>> {
    let shift = u32::from(source_scale.saturating_sub(target_scale).cast_unsigned());
    let divisor = 1_i32.checked_shl(shift).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "histogram scale difference is too large",
        )
    })?;
    let mut output = BTreeMap::new();
    for (position, count) in buckets.counts.iter().copied().enumerate() {
        if count == 0 {
            continue;
        }
        let position = i32::try_from(position).map_err(io::Error::other)?;
        let index = buckets
            .offset
            .checked_add(position)
            .ok_or_else(index_overflow)?
            .div_euclid(divisor);
        let value = output.entry(index).or_insert(0_u64);
        *value = value.checked_add(count).ok_or_else(count_overflow)?;
    }
    Ok(output)
}

fn write_sparse_buckets(
    writer: &mut dyn io::Write,
    prefix: &str,
    buckets: &BTreeMap<i32, u64>,
) -> io::Result<()> {
    if buckets.is_empty() {
        return Ok(());
    }
    let mut spans = Vec::new();
    let mut counts = Vec::new();
    let mut iterator = buckets.iter().peekable();
    let mut previous_end = 0_i32;
    while let Some((&start, &count)) = iterator.next() {
        let offset = if spans.is_empty() {
            start
        } else {
            start.checked_sub(previous_end).ok_or_else(index_overflow)?
        };
        let mut length = 1_u32;
        counts.push(count);
        let mut end = start.checked_add(1).ok_or_else(index_overflow)?;
        while let Some(&(&next, &next_count)) = iterator.peek() {
            if next != end {
                break;
            }
            iterator.next();
            counts.push(next_count);
            length = length.checked_add(1).ok_or_else(count_overflow)?;
            end = end.checked_add(1).ok_or_else(index_overflow)?;
        }
        spans.push((offset, length));
        previous_end = end;
    }
    write!(writer, ",{prefix}_spans:[")?;
    for (index, (offset, length)) in spans.iter().enumerate() {
        if index > 0 {
            write!(writer, ",")?;
        }
        write!(writer, "{offset}:{length}")?;
    }
    write!(writer, "],{prefix}_buckets:[")?;
    for (index, count) in counts.iter().enumerate() {
        if index > 0 {
            write!(writer, ",")?;
        }
        write!(writer, "{count}")?;
    }
    write!(writer, "]")
}

fn format_exponential_as_classic<T: OmNumber>(
    writer: &mut dyn io::Write,
    point: &data::ExponentialHistogramDataPoint<T>,
) -> io::Result<()> {
    let base = 2.0_f64.powi(-i32::from(point.scale)).exp2();
    let mut buckets = Vec::new();
    for (position, count) in point.negative_bucket.counts.iter().copied().enumerate() {
        let index = bucket_index(point.negative_bucket.offset, position)?;
        buckets.push((-base.powi(index), count));
    }
    buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
    buckets.push((point.zero_threshold, point.zero_count));
    for (position, count) in point.positive_bucket.counts.iter().copied().enumerate() {
        let index = bucket_index(point.positive_bucket.offset, position)?;
        buckets.push((base.powi(index + 1), count));
    }
    buckets.sort_by(|left, right| left.0.total_cmp(&right.0));

    write!(
        writer,
        " {{count:{},sum:{},bucket:[",
        point.count,
        point.sum.om()
    )?;
    let mut cumulative = 0_u64;
    for (index, (bound, count)) in buckets.iter().enumerate() {
        cumulative = cumulative.checked_add(*count).ok_or_else(count_overflow)?;
        if index > 0 {
            write!(writer, ",")?;
        }
        write!(writer, "{}:{cumulative}", format_float(*bound))?;
    }
    if !buckets.is_empty() {
        write!(writer, ",")?;
    }
    write!(writer, "+Inf:{}]}}", point.count)
}

fn metadata(writer: &mut dyn io::Write, metric: &data::Metric, kind: &str) -> io::Result<()> {
    writeln!(writer, "# TYPE {} {kind}", metadata_name(&metric.name))?;
    if let Some(unit) = unit(&metric.unit) {
        writeln!(writer, "# UNIT {} {unit}", metadata_name(&metric.name))?;
    }
    if !metric.description.is_empty() {
        writeln!(
            writer,
            "# HELP {} {}",
            metadata_name(&metric.name),
            escape(&metric.description)
        )?;
    }
    Ok(())
}

fn metadata_name(name: &str) -> String {
    if valid_metric_name(name) {
        name.to_owned()
    } else {
        format!("\"{}\"", escape(name))
    }
}

fn write_sample_name(
    writer: &mut dyn io::Write,
    name: &str,
    attributes: impl IntoIterator<Item = KeyValue>,
) -> io::Result<()> {
    let attributes = attributes.into_iter().collect::<Vec<_>>();
    if valid_metric_name(name) {
        write!(writer, "{name}")?;
        if !attributes.is_empty() {
            write!(writer, "{{")?;
            write_labels(writer, &attributes)?;
            write!(writer, "}}")?;
        }
    } else {
        write!(writer, "{{\"{}\"", escape(name))?;
        if !attributes.is_empty() {
            write!(writer, ",")?;
            write_labels(writer, &attributes)?;
        }
        write!(writer, "}}")?;
    }
    Ok(())
}

fn write_labels(writer: &mut dyn io::Write, attributes: &[KeyValue]) -> io::Result<()> {
    for (index, attribute) in attributes.iter().enumerate() {
        if index > 0 {
            write!(writer, ",")?;
        }
        let key = attribute.key.as_str();
        if valid_label_name(key) {
            write!(writer, "{key}")?;
        } else {
            write!(writer, "\"{}\"", escape(key))?;
        }
        write!(writer, "=\"{}\"", escape(&attribute.value.to_string()))?;
    }
    Ok(())
}

fn format_exemplars<T: OmNumber>(
    writer: &mut dyn io::Write,
    exemplars: &[data::Exemplar<T>],
) -> io::Result<()> {
    for exemplar in exemplars {
        let mut labels = exemplar.filtered_attributes.clone();
        if exemplar.trace_id != [0; 16] {
            labels.push(KeyValue::new("trace_id", hex::encode(exemplar.trace_id)));
        }
        if exemplar.span_id != [0; 8] {
            labels.push(KeyValue::new("span_id", hex::encode(exemplar.span_id)));
        }
        write!(writer, " # {{")?;
        write_labels(writer, &labels)?;
        write!(
            writer,
            "}} {} {}",
            exemplar.value.om(),
            timestamp(exemplar.time)?
        )?;
    }
    Ok(())
}

fn timestamp(value: SystemTime) -> io::Result<String> {
    let duration = value.duration_since(UNIX_EPOCH).map_err(io::Error::other)?;
    if duration.subsec_nanos() == 0 {
        Ok(duration.as_secs().to_string())
    } else {
        Ok(format!(
            "{}.{:09}",
            duration.as_secs(),
            duration.subsec_nanos()
        ))
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "+Inf".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_owned();
    }
    let mut output = value.to_string();
    if !output.contains(['.', 'e', 'E']) {
        output.push_str(".0");
    }
    output
}

fn unit(value: &str) -> Option<&str> {
    match value {
        "" | "1" => None,
        "s" => Some("seconds"),
        "By" => Some("bytes"),
        other => Some(other),
    }
}

fn valid_metric_name(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|value| value.is_ascii_alphabetic() || matches!(value, '_' | ':'))
        && chars.all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | ':'))
}

fn valid_label_name(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|value| value.is_ascii_alphabetic() || value == '_')
        && chars.all(|value| value.is_ascii_alphanumeric() || value == '_')
}

fn escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\n' => output.push_str("\\n"),
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            other => output.push(other),
        }
    }
    output
}

fn bucket_index(offset: i32, position: usize) -> io::Result<i32> {
    let position = i32::try_from(position).map_err(io::Error::other)?;
    offset.checked_add(position).ok_or_else(index_overflow)
}

fn count_overflow() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "histogram count overflow")
}

fn index_overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "histogram bucket index overflow",
    )
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::time::Duration;

    use opentelemetry::InstrumentationScope;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::data::DataPoint;
    use opentelemetry_sdk::metrics::data::ExponentialBucket;
    use opentelemetry_sdk::metrics::data::ExponentialHistogram;
    use opentelemetry_sdk::metrics::data::ExponentialHistogramDataPoint;
    use opentelemetry_sdk::metrics::data::Gauge;
    use opentelemetry_sdk::metrics::data::Metric;
    use opentelemetry_sdk::metrics::data::ScopeMetrics;
    use opentelemetry_sdk::metrics::data::Sum;

    use super::*;

    #[test]
    fn emits_quoted_names_metadata_target_info_and_eof() {
        let metrics = resource_metrics(
            Resource::new([KeyValue::new("service.name", "oxia\ncmd")]),
            vec![
                Metric {
                    name: Cow::Borrowed("db.requests"),
                    description: Cow::Borrowed("requests \"served\""),
                    unit: Cow::Borrowed("1"),
                    data: Box::new(Sum {
                        data_points: vec![DataPoint {
                            attributes: vec![KeyValue::new("db.system", "oxia")],
                            start_time: Some(UNIX_EPOCH + Duration::from_secs(1)),
                            time: Some(UNIX_EPOCH + Duration::from_secs(2)),
                            value: 4_u64,
                            exemplars: Vec::new(),
                        }],
                        temporality: Temporality::Cumulative,
                        is_monotonic: true,
                    }),
                },
                Metric {
                    name: Cow::Borrowed("queue_depth"),
                    description: Cow::Borrowed("queue depth"),
                    unit: Cow::Borrowed("By"),
                    data: Box::new(Gauge {
                        data_points: vec![DataPoint {
                            attributes: Vec::new(),
                            start_time: None,
                            time: None,
                            value: 2_i64,
                            exemplars: Vec::new(),
                        }],
                    }),
                },
            ],
        );

        let output = format_to_string(&metrics);
        assert!(output.starts_with("# TYPE target_info info\n"));
        assert!(output.contains("target_info{\"service.name\"=\"oxia\\ncmd\"} 1"));
        assert!(output.contains("# TYPE \"db.requests\" counter"));
        assert!(output.contains("# HELP \"db.requests\" requests \\\"served\\\""));
        assert!(output.contains("{\"db.requests\",\"db.system\"=\"oxia\"} 4 2 st@1"));
        assert!(output.contains("# TYPE queue_depth gauge"));
        assert!(output.contains("# UNIT queue_depth bytes"));
        assert!(output.ends_with("# EOF\n"));
        assert!(!output.contains("M:"));
    }

    #[test]
    fn emits_sparse_native_histogram_and_downsamples_scale() {
        let metrics = resource_metrics(
            Resource::empty(),
            vec![exponential_metric(10, 2, vec![1, 0, 2, 3], 1)],
        );
        let output = format_to_string(&metrics);

        assert!(output.contains("# TYPE \"latency.seconds\" histogram"));
        assert!(output.contains("# UNIT \"latency.seconds\" seconds"));
        assert!(output.contains("schema:8,zero_threshold:0.0,zero_count:1"));
        assert!(output.contains("positive_spans:[0:2],positive_buckets:[1,5]"));
        assert!(output.contains("negative_spans:[-1:1],negative_buckets:[1]"));
    }

    #[test]
    fn falls_back_to_classic_buckets_below_minimum_schema() {
        let metrics = resource_metrics(
            Resource::empty(),
            vec![exponential_metric(-5, 0, vec![2], 1)],
        );
        let output = format_to_string(&metrics);

        assert!(output.contains("bucket:["));
        assert!(output.contains("+Inf:4"));
        assert!(!output.contains("schema:-5"));
    }

    fn exponential_metric(
        scale: i8,
        positive_offset: i32,
        positive_counts: Vec<u64>,
        negative_count: u64,
    ) -> Metric {
        let count = positive_counts.iter().sum::<u64>() + negative_count + 1;
        Metric {
            name: Cow::Borrowed("latency.seconds"),
            description: Cow::Borrowed("latency"),
            unit: Cow::Borrowed("s"),
            data: Box::new(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    attributes: Vec::new(),
                    start_time: UNIX_EPOCH + Duration::from_secs(1),
                    time: UNIX_EPOCH + Duration::from_secs(2),
                    count: usize::try_from(count).unwrap(),
                    min: None,
                    max: None,
                    sum: 10.0_f64,
                    scale,
                    zero_count: 1,
                    positive_bucket: ExponentialBucket {
                        offset: positive_offset,
                        counts: positive_counts,
                    },
                    negative_bucket: ExponentialBucket {
                        offset: -1,
                        counts: vec![negative_count],
                    },
                    zero_threshold: 0.0,
                    exemplars: Vec::new(),
                }],
                temporality: Temporality::Cumulative,
            }),
        }
    }

    fn resource_metrics(resource: Resource, metrics: Vec<Metric>) -> ResourceMetrics {
        ResourceMetrics {
            resource,
            scope_metrics: vec![ScopeMetrics {
                scope: InstrumentationScope::builder("test.scope").build(),
                metrics,
            }],
        }
    }

    fn format_to_string(metrics: &ResourceMetrics) -> String {
        let mut output = Vec::new();
        format_metrics(&mut output, metrics).expect("OM2 formatting succeeds");
        String::from_utf8(output).expect("OM2 output is UTF-8")
    }
}
