// Copyright 2025 Maurice S. Barnum
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

use std::env;

use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
pub struct LogArgs {
    /// Override log level or filter.
    /// Simple levels: `trace`, `debug`, `info`, `warn`, `error`
    ///
    /// Extended syntax (like `RUST_LOG`) is also supported,
    /// e.g. "mycrate=debug,other=warn"
    #[arg(long = "log-level", value_parser = parse_env_filter)]
    pub log_level: Option<EnvFilter>,
}

fn parse_env_filter(s: &str) -> Result<EnvFilter, String> {
    EnvFilter::try_new(s).map_err(|e| e.to_string())
}

impl LogArgs {
    /// Build an `EnvFilter` from CLI args or `RUST_LOG`, with a fallback default
    pub fn to_filter(&self, default: &str) -> EnvFilter {
        if env::var("RUST_LOG").is_ok() {
            EnvFilter::from_default_env()
        } else if let Some(filter) = &self.log_level {
            filter.clone()
        } else {
            EnvFilter::new(default)
        }
    }

    /// Set up default subscriber, using the parsed options, with a fallback default
    /// and return it
    pub fn setup(&self, default: &str) -> anyhow::Result<()> {
        let filter = self.to_filter(default);
        let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber).map_err(Into::into)
    }
}
