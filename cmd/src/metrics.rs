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

use std::sync::Arc;
use std::sync::Weak;

use anyhow::Context as _;
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
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::reader::MetricReader;

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
    exporter: opentelemetry_stdout::MetricExporter,
}

impl MetricsOutput {
    pub fn new() -> Self {
        let exporter = opentelemetry_stdout::MetricExporter::default();
        let reader = SharedManualReader::new(
            ManualReader::builder()
                .with_temporality(exporter.temporality())
                .build(),
        );
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .with_view(exponential_histogram_view)
            .build();
        Self {
            provider,
            reader,
            exporter,
        }
    }

    pub fn meter_provider(&self) -> crate::client_lib::config::MeterProviderHandle {
        Arc::new(self.provider.clone())
    }

    pub async fn export(self) -> anyhow::Result<()> {
        let mut metrics = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: Vec::new(),
        };
        self.reader
            .collect(&mut metrics)
            .context("failed to collect metrics")?;
        self.exporter
            .export(&mut metrics)
            .await
            .context("failed to export metrics to stdout")?;
        self.provider
            .shutdown()
            .context("failed to shut down metrics provider")
    }
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
