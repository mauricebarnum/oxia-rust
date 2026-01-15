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

use std::cmp::Ordering;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio_stream::StreamMap;
use tokio_stream::StreamNotifyClose;
use tracing::debug;
use tracing::info;

use crate::NotificationBatch;
use crate::Result;
use crate::ShardId;
use crate::config::Config;
use crate::shard;

#[derive(Default)]
struct ShardOffsetMap(shard::ShardIdMap<i64>);

impl ShardOffsetMap {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, id: ShardId, offset: i64) {
        self.0.insert(id, offset);
    }

    fn get(&self, id: ShardId) -> Option<i64> {
        self.0.get(id).copied()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn remove(&mut self, id: ShardId) {
        self.0.remove(id);
    }
}

type ShardStream = StreamNotifyClose<BoxStream<'static, Result<NotificationBatch>>>;
type ShardStreamMap = StreamMap<ShardId, ShardStream>;

#[derive(Default)]
struct StreamingState {
    streams: ShardStreamMap,
    offsets: ShardOffsetMap,
}

// State is rarely copied, so the smaller enum size from boxing StreamingState
// is not a good tradeoff for the extra heap allocation and indirection on access.
#[allow(clippy::large_enum_variant)]
enum State {
    Streaming(StreamingState),
    Configuring(BoxFuture<'static, State>),
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Streaming(_) => f.debug_tuple("Streaming").finish(),
            Self::Configuring(_) => f.debug_tuple("Configuring").finish(),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ConfigEpoch(usize);

impl ConfigEpoch {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn next(&mut self) {
        self.0 += 1
    }
}

enum Cmd {
    Configure,
    #[allow(dead_code)]
    Attach {
        epoch: ConfigEpoch,
        id: ShardId,
        stream: ShardStream,
    },
}

#[derive(Debug)]
pub struct NotificationsStream {
    config: Arc<Config>,
    shard_manager: Arc<shard::Manager>,
    state: State,
    config_rx: mpsc::Receiver<Cmd>,
    epoch: ConfigEpoch,
    _watcher_handle: tokio::task::JoinHandle<()>,
}

impl NotificationsStream {
    pub(crate) fn new(config: Arc<Config>, shard_manager: Arc<shard::Manager>) -> Self {
        let (config_tx, config_rx) = mpsc::channel::<Cmd>(1);
        let watcher_handle = tokio::spawn({
            let mut watcher = shard_manager.recv_changed().clone();
            async move {
                loop {
                    if let Err(e) = watcher.changed().await {
                        info!(?e, "NotificationsStream shard watcher terminating");
                        return;
                    }
                    if let Err(e) = config_tx.reserve().await.map(|p| p.send(Cmd::Configure)) {
                        info!(?e, "NotificationsStream shard watcher terminating");
                        return;
                    }
                }
            }
        });

        Self {
            state: Self::configure(config.clone(), shard_manager.clone(), None),
            config,
            shard_manager,
            config_rx,
            epoch: ConfigEpoch::new(),
            _watcher_handle: watcher_handle,
        }
    }

    fn configure(
        config: Arc<Config>,
        shard_manager: Arc<shard::Manager>,
        current: Option<StreamingState>,
    ) -> State {
        let max_parallel_requests = config.max_parallel_requests();
        let fut = async move {
            let (mut streams, offsets) = match current {
                Some(c) => (c.streams, c.offsets),
                None => (ShardStreamMap::new(), ShardOffsetMap::new()),
            };

            // Clients are sorted by shard id, let's sort the active ids so we can easily
            // find the differences between the old and new configuration
            let active_shard_ids = {
                let mut keys: Vec<ShardId> = streams.keys().copied().collect();
                keys.sort_unstable();
                keys
            };
            let mut active_iter = active_shard_ids.iter().peekable();

            let clients = shard_manager.get_shard_clients().await;
            let mut futs = Vec::new();

            'next_client: for client in clients {
                let id = client.id();
                while let Some(active_id) = active_iter.peek() {
                    match id.cmp(active_id) {
                        Ordering::Less => break,
                        Ordering::Equal => {
                            debug!(?id, "skipping shard: already active");
                            continue 'next_client;
                        }
                        Ordering::Greater => {
                            debug!(?active_id, "removing stream for shard");
                            streams.remove(active_id);
                            active_iter.next();
                        }
                    }
                }

                debug!(?id, "creating notification stream for shard");
                futs.push({
                    let config = config.clone();
                    let offset = offsets.get(id);
                    async move {
                        (
                            id,
                            crate::execute_with_retry(&config, move || {
                                let client = client.clone();
                                async move { client.get_notifications(offset).await }
                            })
                            .await,
                        )
                    }
                });
            }

            // Execute the requests to create notification streams and collect them
            // to update the stream map
            futures::stream::iter(futs.into_iter())
                .buffer_unordered(max_parallel_requests)
                .then(|(id, r)| async move {
                    let s = StreamNotifyClose::new(match r {
                        Ok(s) => s.boxed(),
                        Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
                    });
                    (id, s)
                })
                .collect::<Vec<(ShardId, ShardStream)>>()
                .await
                .into_iter()
                .for_each(|(id, s)| {
                    streams.insert(id, s);
                });

            State::Streaming(StreamingState { streams, offsets })
        }
        .boxed();
        State::Configuring(fut)
    }
}

impl Stream for NotificationsStream {
    type Item = Result<NotificationBatch>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Streaming(s) => {
                    if let Poll::Ready(Some(cmd)) = this.config_rx.poll_recv(cx) {
                        match cmd {
                            Cmd::Configure => {
                                debug!("config change signal received");
                                this.state = Self::configure(
                                    this.config.clone(),
                                    this.shard_manager.clone(),
                                    Some(std::mem::take(s)),
                                );
                            }
                            Cmd::Attach { epoch, id, stream } => {
                                debug!(?epoch, ?id, "attach command");
                                if epoch == this.epoch {
                                    s.streams.insert(id, stream);
                                }
                            }
                        }

                        // Check for new commands and re-poll after state change
                        continue;
                    }

                    match s.streams.poll_next_unpin(cx) {
                        Poll::Ready(Some((id, r))) => {
                            if r.is_none() {
                                // This shard stream has closed.  If we're configured to
                                // reconnect on close,this is the place.  It's also where
                                // we can let caller see the clsoure directly by yielding
                                // a specific error, although the client has already likely
                                // already seen a tonic error
                                debug!(?id, "shard stream terminated");
                                continue;
                            }
                            if let Some(Ok(b)) = &r {
                                s.offsets.insert(id, b.offset);
                            }
                            return Poll::Ready(r);
                        }
                        Poll::Ready(None) => {
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }

                State::Configuring(fut) => match fut.poll_unpin(cx) {
                    Poll::Ready(s) => {
                        this.state = s;
                        this.epoch.next();
                        continue;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_map() {
        let mut offsets = ShardOffsetMap::new();

        let id_zero: ShardId = 0.into();
        assert!(offsets.get(id_zero).is_none());
        offsets.insert(id_zero, 42);
        assert_eq!(offsets.get(id_zero).unwrap(), 42);
        offsets.remove(id_zero);
        assert!(offsets.get(id_zero).is_none());

        // Test offset -1 (start of log, no commits yet)
        offsets.insert(id_zero, -1);
        assert_eq!(offsets.get(id_zero).unwrap(), -1);
        offsets.remove(id_zero);
        assert!(offsets.get(id_zero).is_none());

        // Test offset 0 (first commit)
        offsets.insert(id_zero, 0);
        assert_eq!(offsets.get(id_zero).unwrap(), 0);
        offsets.remove(id_zero);
        assert!(offsets.get(id_zero).is_none());

        let id_very_large: ShardId = i64::MAX.into();
        assert!(offsets.get(id_very_large).is_none());
        offsets.insert(id_very_large, 42);
        assert_eq!(offsets.get(id_very_large).unwrap(), 42);
        offsets.remove(id_very_large);
        assert!(offsets.get(id_very_large).is_none());
    }
}
