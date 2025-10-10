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

use futures::stream::BoxStream;
use futures::stream::StreamExt;
use tonic;
use tracing::warn;

use mauricebarnum_oxia_common::proto as oxia_proto;

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
pub(crate) async fn prepare_requests(
    client_req: Request,
    shard_manager: Arc<shard::Manager>,
) -> Result<(
    Vec<impl Future<Output = BoxStream<'static, ResponseItem>>>,
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

type ResponseStream = tonic::Streaming<oxia_proto::ReadResponse>;
type GetsIter = std::vec::IntoIter<oxia_proto::GetResponse>;
type KeysIter = std::vec::IntoIter<Arc<str>>;

enum State {
    Reading {
        read_stream: ResponseStream,
        keys_iter: KeysIter,
    },
    Demuxing {
        read_stream: ResponseStream,
        gets_iter: GetsIter,
        keys_iter: KeysIter,
    },
    Failing {
        err: Arc<Error>,
        keys_iter: KeysIter,
    },
}

enum Handled {
    Return(Option<(ResponseItem, State)>),
    Next(State),
}

impl State {
    fn new(r: OxiaReadResult, keys: Vec<Arc<str>>) -> Self {
        match r {
            Ok(rr) => State::Reading {
                read_stream: rr.into_inner(),
                keys_iter: keys.into_iter(),
            },
            Err(status) => State::Failing {
                err: Arc::<Error>::new(status.into()),
                keys_iter: keys.into_iter(),
            },
        }
    }

    async fn handle_reading(mut read_stream: ResponseStream, keys_iter: KeysIter) -> Handled {
        let Some(rsp) = read_stream.next().await else {
            // Stream is closed.  If we haven't processed all of the keys, something went wrong.
            return if keys_iter.len() == 0 {
                Handled::Return(None)
            } else {
                let msg = "FIXME: host, shard id".to_string();
                let err = Arc::<Error>::new(Error::NoResponseFromServer(msg));
                Handled::Next(State::Failing { err, keys_iter })
            };
        };

        let next = match rsp {
            Ok(rr) => State::Demuxing {
                read_stream,
                gets_iter: rr.gets.into_iter(),
                keys_iter,
            },
            Err(status) => State::Failing {
                err: Arc::new(status.into()),
                keys_iter,
            },
        };
        Handled::Next(next)
    }

    fn handle_demuxing(
        read_stream: ResponseStream,
        mut gets_iter: GetsIter,
        mut keys_iter: KeysIter,
    ) -> Handled {
        let Some(r) = gets_iter.next() else {
            // Done processing this block of gets, read the next response
            return Handled::Next(State::Reading {
                read_stream,
                keys_iter,
            });
        };

        let Some(k) = keys_iter.next() else {
            // Expected a key to match the response!
            warn!(?r, "protocol error: response without matching request key");
            return Handled::Return(None);
        };

        let item = ResponseItem::new(k, crate::shard::get_response_as_result(r));
        let next = State::Demuxing {
            read_stream,
            gets_iter,
            keys_iter,
        };
        Handled::Return(Some((item, next)))
    }

    fn handle_failing(err: Arc<Error>, mut keys_iter: KeysIter) -> Handled {
        match keys_iter.next() {
            Some(k) => {
                let failure = ResponseItem::failed(k, err.clone().into());
                Handled::Return(Some((failure, State::Failing { err, keys_iter })))
            }
            None => Handled::Return(None),
        }
    }

    async fn next(mut self) -> Option<(ResponseItem, State)> {
        loop {
            let handled = match self {
                State::Reading {
                    read_stream,
                    keys_iter,
                } => Self::handle_reading(read_stream, keys_iter).await,

                State::Demuxing {
                    read_stream,
                    gets_iter,
                    keys_iter,
                } => Self::handle_demuxing(read_stream, gets_iter, keys_iter),

                State::Failing { err, keys_iter } => Self::handle_failing(err, keys_iter),
            };

            match handled {
                Handled::Return(rsp) => return rsp,
                Handled::Next(next) => self = next,
            }
        }
    }
}

type OxiaReadResult =
    std::result::Result<tonic::Response<tonic::Streaming<oxia_proto::ReadResponse>>, tonic::Status>;

pub(crate) async fn response_stream(
    keys: Vec<Arc<str>>,
    rr: OxiaReadResult,
) -> BoxStream<'static, ResponseItem> {
    futures::stream::unfold(
        State::new(rr, keys),
        |state| async move { state.next().await },
    )
    .boxed()
}
