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

use tracing::trace;

use super::CommandRunnable;
use crate::client_lib::DeleteRangeOptions;

#[derive(Args, Debug)]
pub struct DeleteRangeCommand {
    /// Key range minimum (inclusive)
    #[arg(short('m'), long, default_value = "")]
    key_min: String,

    /// Key range maximum (exclusive)
    #[arg(short('n'), long, default_value = "")]
    key_max: String,

    /// Partition key to override shard routing
    #[arg(short, long)]
    partition: Option<String>,
}

#[async_trait::async_trait]
impl CommandRunnable for DeleteRangeCommand {
    async fn run(self, ctx: crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        let opts = DeleteRangeOptions::builder()
            .maybe_partition_key(self.partition)
            .build();
        let key_min = self.key_min;
        let key_max = self.key_max;

        let result = ctx
            .client()
            .await?
            .delete_range_with_options(key_min, key_max, opts)
            .await;
        trace!(?result, "result");
        println!("{result:?}");
        super::to_result(result)
    }
}
