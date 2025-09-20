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

use base64::Engine as _;

use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use bytes::Bytes;
use clap::Args;
use clap::ValueEnum;
use tracing::trace;

use super::CommandRunnable;
use crate::client_lib::PutOptions;
use crate::utils::unicode;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub enum ValueEncoding {
    None,
    #[value(alias = "b64")]
    Base64Url,
    Hex,
    #[value(alias = "uu")]
    UnicodeEscape,
}

fn decode(enc: ValueEncoding, s: String) -> anyhow::Result<Bytes> {
    if s.is_empty() {
        return Ok(Bytes::new());
    }
    match enc {
        ValueEncoding::None => Ok(s.into_bytes().into()),
        ValueEncoding::Base64Url => BASE64_URL_SAFE_NO_PAD
            .decode(s)
            .map(Bytes::from)
            .map_err(Into::into),
        ValueEncoding::Hex => hex::decode(s).map(Bytes::from).map_err(Into::into),
        ValueEncoding::UnicodeEscape => unicode::decode_escapes(s).map_err(Into::into),
    }
}

#[derive(Args, Debug)]
pub struct PutCommand {
    /// Key to insert
    pub key: String,

    /// Value to insert
    pub value: String,

    /// Encoding to use for the value
    #[arg(short, long, value_enum, default_value_t = ValueEncoding::None)]
    pub encoding: ValueEncoding,

    /// Partition key to override shard routing
    #[arg(short, long)]
    pub partition: Option<String>,

    /// Expected version
    #[arg(long)]
    pub expected_version: Option<i64>,

    /// Sequence keys deltas
    #[arg(long,value_delimiter=',',value_parser=clap::value_parser!(u64))]
    pub sequence_keys_deltas: Option<Vec<u64>>,
}

#[async_trait::async_trait]
impl CommandRunnable for PutCommand {
    async fn run(self, ctx: &mut crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        let value = decode(self.encoding, self.value)?;
        let opts = PutOptions::new().with(|opts| {
            if let Some(pk) = self.partition {
                opts.partition_key(pk);
            }

            if let Some(x) = self.expected_version {
                opts.expected_version_id(x);
            }

            if let Some(deltas) = self.sequence_keys_deltas {
                opts.sequence_key_deltas(deltas);
            }
        });
        let result = ctx
            .client()
            .await?
            .put_with_options(self.key, value, opts)
            .await;
        trace!(?result, "result");

        println!("{result:?}");
        super::to_result(result)
    }
}
