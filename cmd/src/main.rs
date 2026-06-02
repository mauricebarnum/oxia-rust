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

use std::time::Duration;

use clap::Parser;
use mauricebarnum_oxia_client as client_lib;

mod commands;
use commands::CommandRunnable;
use commands::Commands;

mod context;
use context::Context;

mod log;
use log::LogArgs;

mod metrics;
use metrics::MetricsOutput;

mod utils;

#[derive(Parser)]
#[command(name = "oxia-cmd", version, about = "oxia client")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[command(flatten)]
    log: LogArgs,

    #[arg(long, default_value = "localhost:6648")]
    service_address: String,

    #[arg(long, default_value_t = 16 * num_cpus::get() as usize)]
    max_parallel_requests: usize,

    #[arg(long, default_value = "2000ms", value_parser = humantime::parse_duration)]
    session_timeout: Duration,

    #[arg(long, default_value = "100ms", value_parser = humantime::parse_duration)]
    request_timeout: Duration,

    #[arg(long, default_value_t = false)]
    retry_on_stale_shard_map: bool,

    #[arg(long, default_value_t = false)]
    metrics: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.log.setup("off")?;

    let metrics = cli.metrics.then(MetricsOutput::new);
    let ctx = Context::new(&cli, metrics.as_ref());
    let result = cli.command.run(ctx).await;

    if let Some(m) = metrics {
        m.export()
            .await
            .inspect_err(|e| eprintln!("failed to export metrics: {e:#}"))
            .ok();
    }

    result
}
