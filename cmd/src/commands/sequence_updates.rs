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

use clap::ArgGroup;
use clap::Args;
use futures_util::stream::StreamExt;
use mauricebarnum_oxia_client::SequenceUpdateRouting;
use tracing::trace;

use super::CommandRunnable;

#[derive(Args, Debug)]
#[command(
    group(
        ArgGroup::new("target")
            .args(["partition", "shard"])
            .required(true)
            .multiple(false)
    )
)]
pub struct SequenceUpdatesCommand {
    key: String,

    /// Partition key to map shard
    #[arg(short, long, conflicts_with("shard"))]
    partition: Option<String>,

    /// Shard to connect
    #[arg(short, long, conflicts_with("partition"))]
    shard: Option<i64>,
}

#[async_trait::async_trait]
impl CommandRunnable for SequenceUpdatesCommand {
    async fn run(self, ctx: crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");

        let routing = if let Some(k) = self.partition {
            SequenceUpdateRouting::PartitionKey(k)
        } else if let Some(s) = self.shard {
            SequenceUpdateRouting::Shard(s)
        } else {
            panic!("partition or shard required")
        };

        let mut updates_stream = ctx
            .client()
            .await?
            .get_sequence_updates(self.key, routing)
            .await?;
        trace!("sequence updates stream created");
        while let Some(item) = updates_stream.next().await {
            trace!(?item);
            if let Ok(r) = item {
                println!("shard={} seq={}", r.shard, r.highest_sequence_key);
            }
        }
        Ok(())
    }
}
