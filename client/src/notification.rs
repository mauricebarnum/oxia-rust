#![allow(unused)]

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use backon::FibonacciBackoff;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::sync::watch;
use tracing::info;

use crate::Error;
use crate::NotificationBatch;
use crate::Result;
use crate::config;
use crate::config::Config;
use crate::errors;
use crate::shard;

struct ReconnectStrategy {}

enum ShardState {
    Start,
    Connecting {
        fut: BoxFuture<'static, Result<shard::NotificationsStream>>,
        cause: Option<errors::Error>,
    },
    Active(shard::NotificationsStream),
    Done,
}

struct ShardInfo {
    id: i64,
    offset: Option<i64>,
    state: ShardState,
}

enum StreamState {
    Active { shards: Vec<ShardInfo>, next: usize },
    Building(BoxFuture<'static, Vec<ShardInfo>>),
    Done,
}

pub struct NotificationsStream {
    config: Arc<config::Config>,
    shard_manager: Arc<shard::Manager>,
    watcher: watch::Receiver<std::result::Result<(), Arc<Error>>>,
    state: StreamState,
    reconnect_on_done: bool,
    reconnect_on_error: bool,
}

impl NotificationsStream {
    pub(crate) fn new(config: Arc<Config>, shard_manager: Arc<shard::Manager>) -> Self {
        let watcher = shard_manager.recv_changed().clone();
        let state = Self::setup_stream(shard_manager.clone(), None);

        Self {
            shard_manager,
            config,
            watcher,
            state,
            reconnect_on_done: true,
            reconnect_on_error: true,
        }
    }

    fn poll_shard_start(
        &self,
        cx: &mut Context,
        info: &ShardInfo,
    ) -> (Poll<Option<Result<NotificationBatch>>>, ShardState) {
        self.reconnect_shard(cx, info, None)
    }

    fn poll_shard_connecting(
        &self,
        cx: &mut Context,
        info: &ShardInfo,
        mut fut: BoxFuture<'static, Result<shard::NotificationsStream>>,
        cause: Option<errors::Error>,
    ) -> (Poll<Option<Result<NotificationBatch>>>, ShardState) {
        match fut.poll_unpin(cx) {
            Poll::Ready(pr) => match pr {
                Ok(s) => self.poll_shard_active(cx, info, s),
                Err(e) => (Poll::Ready(Some(Err(e))), ShardState::Done),
            },
            Poll::Pending => (Poll::Pending, ShardState::Connecting { fut, cause }),
        }
    }

    fn poll_shard_active(
        &self,
        cx: &mut Context,
        info: &ShardInfo,
        mut s: shard::NotificationsStream,
    ) -> (Poll<Option<<Self as Stream>::Item>>, ShardState) {
        match s.poll_next_unpin(cx) {
            Poll::Ready(Some(r)) => match r {
                Ok(b) => (
                    Poll::Ready(Some(Result::<NotificationBatch>::Ok(b))),
                    ShardState::Active(s),
                ),
                Err(e) => {
                    if self.reconnect_on_error {
                        self.reconnect_shard(cx, info, Some(e))
                    } else {
                        (Poll::Pending, ShardState::Done)
                    }
                }
            },
            Poll::Ready(None) => {
                if self.reconnect_on_done {
                    self.reconnect_shard(cx, info, None)
                } else {
                    (Poll::Ready(None), ShardState::Done)
                }
            }
            Poll::Pending => (Poll::Pending, ShardState::Active(s)),
        }
    }

    fn reconnect_shard(
        &self,
        cx: &mut Context<'_>,
        info: &ShardInfo,
        e: Option<Error>,
    ) -> (Poll<Option<<Self as Stream>::Item>>, ShardState) {
        let shard_manager = self.shard_manager.clone();
        let config = self.config.clone();
        let shard_id = info.id;
        let offset = info.offset;
        let fut = async move {
            let client = shard_manager.get_client_by_shard_id(shard_id).await?;
            crate::execute_with_retry(&config, move || {
                let client = client.clone();
                async move { client.get_notifications(offset).await }
            })
            .await
        }
        .boxed();
        self.poll_shard_connecting(cx, info, fut, e)
    }

    #[must_use]
    fn setup_stream(
        shard_manager: Arc<shard::Manager>,
        existing_shards: Option<Vec<ShardInfo>>,
    ) -> StreamState {
        let offsets = match existing_shards {
            None => Vec::new(),
            Some(shards) => {
                let mut offsets: Vec<_> = shards.iter().map(|s| (s.id, s.offset)).collect();
                offsets.sort_unstable_by_key(|k| k.0);
                offsets
            }
        };

        StreamState::Building(
            async move {
                shard_manager
                    .shard_stream()
                    .await
                    .map(|shard| ShardInfo {
                        id: shard.id(),
                        offset: offsets
                            .binary_search_by_key(&shard.id(), |s| s.0)
                            .ok()
                            .and_then(|i| offsets[i].1),
                        state: ShardState::Start,
                    })
                    .collect()
                    .await
            }
            .boxed(),
        )
    }

    fn poll_stream_active(
        &mut self,
        cx: &mut Context<'_>,
        mut shards: Vec<ShardInfo>,
        mut next: usize,
    ) -> (Poll<Option<<Self as Stream>::Item>>, StreamState) {
        let to_check = shards.len();
        for _ in 0..to_check {
            let idx = next;
            next = (next + 1) % shards.len();
            let current = std::mem::replace(&mut shards[idx].state, ShardState::Done);
            let info = &shards[idx];
            let (poll_result, state) = match current {
                ShardState::Start => self.poll_shard_start(cx, info),
                ShardState::Connecting { fut, cause } => {
                    self.poll_shard_connecting(cx, info, fut, cause)
                }
                ShardState::Active(s) => self.poll_shard_active(cx, info, s),
                ShardState::Done => (Poll::Ready(None), ShardState::Done),
            };
            shards[idx].state = state;
            if let Poll::Ready(None) = poll_result {
                shards.swap_remove(idx);
                next = idx;
            } else if poll_result.is_ready() {
                return (poll_result, StreamState::Active { shards, next });
            }
        }
        if shards.is_empty() {
            (Poll::Ready(None), StreamState::Done)
        } else {
            (Poll::Pending, StreamState::Active { shards, next })
        }
    }

    fn poll_stream_building(
        &mut self,
        cx: &mut Context<'_>,
        mut fut: BoxFuture<'static, Vec<ShardInfo>>,
    ) -> (Poll<Option<<Self as Stream>::Item>>, StreamState) {
        match fut.poll_unpin(cx) {
            Poll::Ready(shards) => self.poll_stream_active(cx, shards, 0),
            Poll::Pending => (Poll::Pending, StreamState::Building(fut)),
        }
    }

    fn check_shards_changed(&mut self) {
        let changed = self.watcher.has_changed();
        if let Err(e) = changed {
            info!(?e, "client done");
            self.state = StreamState::Done;
            return;
        }

        if let Ok(false) = changed {
            return;
        }

        self.state = match std::mem::replace(&mut self.state, StreamState::Done) {
            // Reconnect, use current offsets
            // TODO: are we sure the shard ids are stable?
            StreamState::Active { shards, next } => {
                self.watcher.mark_unchanged();
                Self::setup_stream(self.shard_manager.clone(), Some(shards))
            }

            // There isn't anything more for this stream, so clear the changed state
            StreamState::Done => {
                self.watcher.mark_unchanged();
                StreamState::Done
            }

            // Otherwise, we leave the changed state to re-enter here on each poll.  If, for
            // example, we have a Building state in flight, we don't have a good way to get the
            // offsets until that operation completes.  Once it does, we'll need to reconnect
            // again.
            x @ StreamState::Building(_) => x,
        };
    }
}

impl Stream for NotificationsStream {
    type Item = crate::Result<NotificationBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.check_shards_changed();
        let current = std::mem::replace(&mut this.state, StreamState::Done);
        let (poll_result, next_state) = match current {
            StreamState::Active { shards, next } => this.poll_stream_active(cx, shards, next),
            StreamState::Building(fut) => this.poll_stream_building(cx, fut),
            StreamState::Done => (Poll::Ready(None), StreamState::Done),
        };
        this.state = next_state;
        poll_result
    }
}
