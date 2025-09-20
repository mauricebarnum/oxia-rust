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

use clap::Args;
use tracing::trace;

use super::CommandRunnable;
use crate::client_lib::DeleteOptions;

#[derive(Args, Debug)]
pub struct DeleteCommand {
    /// Key to delete
    pub key: String,

    /// Partition key to override shard routing
    #[arg(short, long)]
    pub partition: Option<String>,

    /// Expected version
    #[arg(long)]
    pub expected_version: Option<i64>,
}

#[async_trait::async_trait]
impl CommandRunnable for DeleteCommand {
    async fn run(self, ctx: &mut crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        let opts = DeleteOptions::new().with(|opts| {
            if let Some(pk) = self.partition {
                opts.partition_key(pk);
            }

            if let Some(x) = self.expected_version {
                opts.expected_version_id(x);
            }
        });
        let result = ctx
            .client()
            .await?
            .delete_with_options(self.key, opts)
            .await;
        trace!(?result, "result");

        println!("{result:?}");
        super::to_result(result)
    }
}
