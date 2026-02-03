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

use clap::Args;
use futures::stream::StreamExt;

use mauricebarnum_oxia_client::batch_get;
use tracing::trace;

use super::CommandRunnable;

#[derive(Args, Debug)]
pub struct BatchGetCommand {
    /// Key to retrieve
    keys: Vec<String>,

    /// Partition key to override shard routing
    #[arg(short, long)]
    partition: Option<String>,

    /// Secondary index key
    #[arg(short, long)]
    index: Option<String>,

    /// Indicate if key exists, don't return a value
    #[arg(short, long, default_value_t = false)]
    exists: bool,

    /// Include the record version
    #[arg(short, long("include-version"), default_value_t = false)]
    version: bool,
}

#[async_trait::async_trait]
impl CommandRunnable for BatchGetCommand {
    async fn run(self, ctx: crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        let opts = batch_get::Options::builder()
            .include_value(!self.exists)
            .maybe_secondary_index_name(self.index)
            .maybe_partition_key(self.partition)
            .build();

        let req = batch_get::Request::builder()
            .options(opts)
            .add_keys(self.keys)
            .build();

        let mut gets = ctx.client().await?.batch_get(req)?;
        while let Some(item) = gets.next().await {
            println!("{item:?}");
        }
        Ok(())
    }
}
