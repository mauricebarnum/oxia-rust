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

use std::collections::BTreeMap;
use std::sync::Arc;

use bon::Builder;
use futures::stream::BoxStream;
use futures::stream::StreamExt;

use crate::Error;
use crate::GetResponse;
use crate::KeyComparisonType;
use crate::Result;
use crate::ShardId;
use crate::shard;

#[derive(Builder, Clone, Debug)]
#[builder(on(String, into))]
pub struct Options {
    #[builder(default = true)]
    include_value: bool,
    partition_key: Option<String>,
    secondary_index_name: Option<String>,
}

impl<S: options_builder::State> OptionsBuilder<S> {
    pub fn exclude_value(self) -> OptionsBuilder<options_builder::SetIncludeValue<S>>
    where
        S::IncludeValue: options_builder::IsUnset,
    {
        self.include_value(false)
    }
}

impl From<Options> for crate::GetOptions {
    fn from(opts: Options) -> Self {
        Self::builder()
            .include_value(opts.include_value)
            .comparison_type(KeyComparisonType::Equal)
            .maybe_partition_key(opts.partition_key)
            .maybe_secondary_index_name(opts.secondary_index_name)
            .build()
    }
}

#[derive(Debug, Default)]
pub struct RequestBuilder {
    opts: Option<Options>,
    keys: Vec<Arc<str>>,
}

impl RequestBuilder {
    #[must_use]
    pub fn options(mut self, opts: Options) -> Self {
        self.opts = Some(opts);
        self
    }

    #[must_use]
    pub fn add_key(mut self, key: impl Into<Arc<str>>) -> Self {
        self.keys.push(key.into());
        self
    }

    #[must_use]
    pub fn add_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }

    pub fn build(self) -> Request {
        Request {
            keys: self.keys,
            opts: self.opts,
        }
    }
}

#[derive(Debug, Default)]
pub struct Request {
    pub keys: Vec<Arc<str>>,
    pub opts: Option<Options>,
}

impl Request {
    pub fn builder() -> RequestBuilder {
        RequestBuilder::default()
    }
}

#[derive(Debug)]
pub struct ResponseItem {
    pub key: Arc<str>,
    pub response: Result<Option<GetResponse>>,
}

impl ResponseItem {
    pub fn new(key: impl Into<Arc<str>>, response: Result<Option<GetResponse>>) -> Self {
        let key = key.into();
        Self { key, response }
    }

    pub fn failed(key: impl Into<Arc<str>>, err: Error) -> Self {
        Self::new(key, Err(err))
    }
}

/// Split `req` and construct per-shard requests.  Return futures so that the caller
/// can drive the network requests.
pub(super) fn prepare_requests(
    client_req: Request,
    shard_manager: &Arc<shard::Manager>,
) -> (
    Vec<impl Future<Output = BoxStream<'static, ResponseItem>> + use<>>,
    Vec<ResponseItem>,
) {
    let mut failures: Vec<ResponseItem> = Vec::new();
    let mut reqs: BTreeMap<ShardId, Request> = BTreeMap::new();

    let partition_key = client_req
        .opts
        .as_ref()
        .and_then(|o| o.partition_key.as_deref());

    for key in client_req.keys {
        let selector = partition_key.unwrap_or(&key);

        let Some(shard_id) = shard_manager.get_shard_id(selector) else {
            let err = Error::NoShardMappingForKey(selector.into());
            failures.push(ResponseItem::failed(key, err));
            continue;
        };

        reqs.entry(shard_id)
            .or_insert_with(|| Request {
                keys: vec![],
                opts: client_req.opts.clone(),
            })
            .keys
            .push(key);
    }

    let mut batch_get_futures = Vec::with_capacity(reqs.len());

    for (shard_id, req) in reqs {
        match shard_manager.get_client_by_shard_id(shard_id) {
            Ok(shard) => {
                batch_get_futures.push(async move { shard.batch_get(req).await.boxed() });
            }
            Err(err) => {
                failures.extend(
                    req.keys
                        .into_iter()
                        .map(|key| ResponseItem::failed(key, err.clone())),
                );
            }
        }
    }
    (batch_get_futures, failures)
}
