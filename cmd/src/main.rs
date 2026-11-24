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

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use clap::ValueEnum;
use mauricebarnum_oxia_client as client_lib;

mod commands;
use commands::CommandRunnable;
use commands::Commands;

mod context;
use context::Context;

mod compress;

mod log;
use log::LogArgs;

mod metrics;
use metrics::MetricsOutput;

mod utils;

fn parse_duration(
    s: &str,
) -> Result<std::time::Duration, Box<dyn std::error::Error + Send + Sync>> {
    let signed: jiff::SignedDuration = s.parse()?;
    Ok(std::time::Duration::try_from(signed)?)
}

fn default_max_parallel_requests() -> usize {
    16 * std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

#[derive(Parser)]
#[command(name = "oxia-cmd", version, about = "oxia client")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[command(flatten)]
    log: LogArgs,

    #[arg(long, default_value = "localhost:6648")]
    service_address: String,

    #[arg(long, default_value_t = default_max_parallel_requests())]
    max_parallel_requests: usize,

    #[arg(long, default_value = "2000ms", value_parser = parse_duration)]
    session_timeout: Duration,

    #[arg(long, default_value = "100ms", value_parser = parse_duration)]
    request_timeout: Duration,

    #[arg(long, default_value_t = false)]
    retry_on_stale_shard_map: bool,

    #[arg(
        long,
        value_enum,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "text",
        help = "Export collected metrics",
        long_help = "Export collected metrics. Machine-readable output should use a dedicated file.\n\nFormats:\n  text       Human-readable diagnostic output\n  otlp-json  Canonical OTLP/JSON ExportMetricsServiceRequest\n  otlp-pb    Binary OTLP protobuf ExportMetricsServiceRequest"
    )]
    metrics: Option<MetricsFormat>,

    /// Metrics destination: stderr by default, stdout for '-', or a file path.
    #[arg(long, requires = "metrics")]
    metrics_output: Option<PathBuf>,

    #[arg(long, value_enum, value_parser = compress::argvalue_parser())]
    compress_send: Option<compress::ArgValue>,

    #[arg(long, value_enum, default_value = "auto")]
    compress_accept: Vec<compress::ArgValue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum MetricsFormat {
    Text,
    OtlpJson,
    OtlpPb,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.log.setup("off")?;

    let metrics = cli
        .metrics
        .map(|format| MetricsOutput::new(format, cli.metrics_output.as_deref()))
        .transpose()?;
    let ctx = Context::new(&cli, metrics.as_ref());
    let result = cli.command.run(ctx).await;

    let export_result = if let Some(metrics) = metrics {
        metrics.export()
    } else {
        Ok(())
    };

    match (result, export_result) {
        (Ok(()), export) => export,
        (Err(command_error), Ok(())) => Err(command_error),
        (Err(command_error), Err(export_error)) => {
            eprintln!("failed to export metrics: {export_error:#}");
            Err(command_error)
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn parses_metrics_formats() {
        for (argument, expected) in [
            ("--metrics", MetricsFormat::Text),
            ("--metrics=text", MetricsFormat::Text),
            ("--metrics=otlp-json", MetricsFormat::OtlpJson),
            ("--metrics=otlp-pb", MetricsFormat::OtlpPb),
        ] {
            let cli = Cli::try_parse_from(["oxia-cmd", argument, "get", "key"])
                .expect("valid metrics argument");
            assert_eq!(cli.metrics, Some(expected));
        }
    }

    #[test]
    fn metrics_help_describes_formats() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("text       Human-readable diagnostic output"));
        assert!(help.contains("otlp-json  Canonical OTLP/JSON ExportMetricsServiceRequest"));
        assert!(help.contains("otlp-pb    Binary OTLP protobuf ExportMetricsServiceRequest"));
    }

    #[test]
    fn metrics_value_requires_equals() {
        let cli = Cli::try_parse_from(["oxia-cmd", "--metrics", "get", "key"])
            .expect("subcommand is not consumed as a metrics format");
        assert_eq!(cli.metrics, Some(MetricsFormat::Text));
    }

    #[test]
    fn rejects_invalid_metrics_format() {
        assert!(Cli::try_parse_from(["oxia-cmd", "--metrics=yaml", "get", "key"]).is_err());
        assert!(Cli::try_parse_from(["oxia-cmd", "--metrics=json", "get", "key"]).is_err());
        assert!(Cli::try_parse_from(["oxia-cmd", "--metrics=om2", "get", "key"]).is_err());
    }

    #[test]
    fn metrics_output_requires_metrics() {
        assert!(Cli::try_parse_from(["oxia-cmd", "--metrics-output", "-", "get", "key"]).is_err());
    }

    #[test]
    fn compress_send_accepts_none_auto_and_enabled_codecs() {
        let cli_none = Cli::try_parse_from(["oxia-cmd", "--compress-send", "none", "get", "key"])
            .expect("none is always accepted");
        assert_eq!(cli_none.compress_send, Some(compress::ArgValue::None));

        let cli_auto = Cli::try_parse_from(["oxia-cmd", "--compress-send", "auto", "get", "key"])
            .expect("auto is always accepted");
        assert_eq!(cli_auto.compress_send, Some(compress::ArgValue::Auto));

        #[cfg(feature = "deflate")]
        {
            let cli = Cli::try_parse_from(["oxia-cmd", "--compress-send", "deflate", "get", "key"])
                .expect("deflate should be accepted when feature is enabled");
            assert_eq!(cli.compress_send, Some(compress::ArgValue::Deflate));
        }

        #[cfg(feature = "gzip")]
        {
            let cli = Cli::try_parse_from(["oxia-cmd", "--compress-send", "gzip", "get", "key"])
                .expect("gzip should be accepted when feature is enabled");
            assert_eq!(cli.compress_send, Some(compress::ArgValue::Gzip));
        }

        #[cfg(feature = "zstd")]
        {
            let cli = Cli::try_parse_from(["oxia-cmd", "--compress-send", "zstd", "get", "key"])
                .expect("zstd should be accepted when feature is enabled");
            assert_eq!(cli.compress_send, Some(compress::ArgValue::Zstd));
        }
    }

    #[cfg(not(all(feature = "deflate", feature = "gzip", feature = "zstd")))]
    #[test]
    fn compress_send_rejects_disabled_codecs_with_build_message() {
        for (name, enabled) in [
            ("deflate", cfg!(feature = "deflate")),
            ("gzip", cfg!(feature = "gzip")),
            ("zstd", cfg!(feature = "zstd")),
        ] {
            if enabled {
                continue;
            }
            let err = Cli::try_parse_from(["oxia-cmd", "--compress-send", name, "get", "key"])
                .err()
                .expect("disabled codec should be rejected");
            assert!(
                err.to_string().contains("not implemented in this build"),
                "unexpected error for {name}: {err}"
            );
        }
    }

    #[test]
    fn compress_send_rejects_unknown_codec() {
        let err = Cli::try_parse_from(["oxia-cmd", "--compress-send", "brotli", "get", "key"])
            .err()
            .expect("unknown codec should be rejected");
        assert!(
            !err.to_string().contains("not implemented in this build"),
            "unknown codec should fail enum parsing, got: {err}"
        );
    }

    #[test]
    fn compress_accept_defaults_to_auto_and_collects_repeated_flags() {
        let default_cli =
            Cli::try_parse_from(["oxia-cmd", "get", "key"]).expect("default args should parse");
        assert_eq!(default_cli.compress_accept, [compress::ArgValue::Auto]);

        let repeated = Cli::try_parse_from([
            "oxia-cmd",
            "--compress-accept",
            "deflate",
            "--compress-accept",
            "gzip",
            "--compress-accept",
            "zstd",
            "get",
            "key",
        ])
        .expect("compress-accept should accept all enum variants regardless of enabled features");
        assert_eq!(
            repeated.compress_accept,
            [
                compress::ArgValue::Deflate,
                compress::ArgValue::Gzip,
                compress::ArgValue::Zstd,
            ]
        );
    }
}
