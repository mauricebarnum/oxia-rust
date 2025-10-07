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

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::Stream;

use crate::ClientError;
use crate::Error;
use crate::GetOptions;
use crate::GetResponse;
use crate::KeyComparisonType;
use crate::Result;
use crate::ShardId;
use crate::shard;

#[derive(Debug)]
pub struct RequestItem {
    pub key: Arc<str>,
    pub opts: Option<GetOptions>,
}

impl RequestItem {
    fn new(key: impl Into<Arc<str>>, opts: Option<GetOptions>) -> Self {
        let key = key.into();
        Self { key, opts }
    }
}

#[derive(Debug)]
pub struct Request {
    pub items: Vec<RequestItem>,
    pub opts: Option<GetOptions>,
}

impl Request {
    pub fn builder() -> Builder {
        Builder::new(None)
    }

    // Supply default GetOptions for each key
    pub fn builder_with_options(opts: GetOptions) -> Builder {
        Builder::new(Some(opts))
    }
}

#[derive(Debug)]
pub struct Builder {
    items: Vec<RequestItem>,
    opts: Option<GetOptions>,
}

impl Builder {
    fn new(opts: Option<GetOptions>) -> Self {
        Self {
            items: Vec::new(),
            opts,
        }
    }

    pub fn with(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }

    pub fn add(&mut self, key: impl Into<Arc<str>>) {
        self.items.push(RequestItem::new(key.into(), None));
    }

    pub fn add_with_options(&mut self, key: impl Into<Arc<str>>, opts: GetOptions) {
        self.items.push(RequestItem::new(key.into(), Some(opts)));
    }

    pub fn add_keys<I, S>(&mut self, keys: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.items
            .extend(keys.into_iter().map(|k| RequestItem::new(k.into(), None)));
    }

    pub fn add_keys_with_options<I, S>(&mut self, keys: I, opts: GetOptions)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.items.extend(
            keys.into_iter()
                .map(|k| RequestItem::new(k.into(), Some(opts.clone()))),
        );
    }

    pub fn build(self) -> Request {
        Request {
            items: self.items,
            opts: self.opts,
        }
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
pub(super) async fn prepare_requests(
    client_req: Request,
    shard_manager: Arc<shard::Manager>,
) -> Result<(
    Vec<impl Future<Output = impl Stream<Item = ResponseItem>>>,
    Vec<ResponseItem>,
)> {
    let mut failures: Vec<ResponseItem> = Vec::new();
    let mut reqs: BTreeMap<ShardId, Request> = BTreeMap::new();

    for item in client_req.items {
        if let Some(opts) = &item.opts
            && opts.comparison_type != KeyComparisonType::Equal
        {
            return Err(ClientError::UnsupportedKeyComparator(opts.comparison_type).into());
        }

        let selector = item
            .opts
            .as_ref()
            .and_then(|o| o.partition_key.as_deref())
            .unwrap_or(&item.key);

        match shard_manager.get_shard_id(selector).await {
            Some(shard_id) => {
                let request_ref = reqs.entry(shard_id).or_insert_with(|| Request {
                    items: Vec::new(),
                    opts: client_req.opts.clone(),
                });
                request_ref.items.push(item);
            }
            None => {
                let err = Error::NoShardMappingForKey(selector.into());
                failures.push(ResponseItem::failed(item.key, err));
            }
        }
    }

    let mut batch_get_futures = Vec::with_capacity(reqs.len());

    for (shard_id, req) in reqs {
        match shard_manager.get_client_by_shard_id(shard_id).await {
            Ok(shard) => {
                batch_get_futures.push(async move { shard.batch_get(req).await });
            }
            Err(err) => {
                let shared_err = Arc::new(err);
                for item in req.items.into_iter() {
                    failures.push(ResponseItem::failed(item.key, shared_err.clone().into()));
                }
            }
        }
    }
    Ok((batch_get_futures, failures))
}
