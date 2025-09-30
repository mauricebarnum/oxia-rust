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
use clap::ValueEnum;

use mauricebarnum_oxia_client::{GetOptions, KeyComparisonType};
use tracing::trace;

use super::CommandRunnable;

// TODO: move this to a common module?
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum KeyCompareArg {
    Equal,
    Floor,
    Ceiling,
    Lower,
    Higher,
}

impl From<KeyCompareArg> for KeyComparisonType {
    fn from(value: KeyCompareArg) -> Self {
        match value {
            KeyCompareArg::Equal => KeyComparisonType::Equal,
            KeyCompareArg::Floor => KeyComparisonType::Floor,
            KeyCompareArg::Ceiling => KeyComparisonType::Ceiling,
            KeyCompareArg::Lower => KeyComparisonType::Lower,
            KeyCompareArg::Higher => KeyComparisonType::Higher,
        }
    }
}

#[derive(Args, Debug)]
pub struct GetCommand {
    /// Key to retrieve
    key: String,

    /// Partition key to override shard routing
    #[arg(short, long)]
    partition: Option<String>,

    /// Secondary index key
    #[arg(short, long)]
    index: Option<String>,

    /// Indicate if key exists, don't return a value
    #[arg(short, long, default_value_t = false)]
    exists: bool,

    /// Key comparison to use
    #[arg(short, long, value_enum, default_value_t = KeyCompareArg::Equal)]
    key_comp: KeyCompareArg,

    /// Include the record version
    #[arg(short, long("include-version"), default_value_t = false)]
    version: bool,
}

#[async_trait::async_trait]
impl CommandRunnable for GetCommand {
    async fn run(self, ctx: crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        let opts = GetOptions::new().with(|opts| {
            opts.comparison_type(self.key_comp.into());

            if self.exists {
                opts.exclude_value();
            }

            if let Some(index) = self.index {
                opts.secondary_index_name(index);
            }

            if let Some(pkey) = self.partition {
                opts.partition_key(pkey);
            }
        });

        let result = ctx.client().await?.get_with_options(self.key, opts).await;
        trace!(?result, "result");
        println!("{result:?}");
        super::to_result(result)
    }
}
