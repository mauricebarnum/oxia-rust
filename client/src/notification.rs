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
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio_stream::StreamMap;
use tokio_stream::StreamNotifyClose;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::NotificationBatch;
use crate::NotificationsOptions;
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
type ReconnectFuture = BoxFuture<'static, (ShardId, crate::Result<ShardStream>)>;

struct StreamingState {
    streams: ShardStreamMap,
    offsets: ShardOffsetMap,
    pending_reconnects: Option<Box<FuturesUnordered<ReconnectFuture>>>,
}

impl Default for StreamingState {
    fn default() -> Self {
        Self {
            streams: ShardStreamMap::new(),
            offsets: ShardOffsetMap::new(),
            pending_reconnects: None,
        }
    }
}

struct WaitingReadyState {
    /// Streams still waiting for first batch
    waiting: ShardStreamMap,
    /// Streams that have received their first batch
    ready: ShardStreamMap,
    /// Cursor offsets
    offsets: ShardOffsetMap,
    /// Reconnect state (carried through)
    pending_reconnects: Option<Box<FuturesUnordered<ReconnectFuture>>>,
    /// Buffered non-empty first batches (max N shards)
    buffered: VecDeque<crate::NotificationBatch>,
}

// State is rarely copied, so the smaller enum size from boxing StreamingState
// is not a good tradeoff for the extra heap allocation and indirection on access.
// WaitingReadyState, on the other hand, is optionally used and only during initialization
// of the stream, so we'll keep that on the heap so it's size is not important.
#[allow(clippy::large_enum_variant)]
enum State {
    Streaming(StreamingState),
    Configuring(BoxFuture<'static, State>),
    WaitingReady(Box<WaitingReadyState>),
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Streaming(_) => f.debug_tuple("Streaming").finish(),
            Self::Configuring(_) => f.debug_tuple("Configuring").finish(),
            Self::WaitingReady(_) => f.debug_tuple("WaitingReady").finish(),
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
        self.0 += 1;
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
    options: NotificationsOptions,
    state: State,
    config_rx: mpsc::Receiver<Cmd>,
    epoch: ConfigEpoch,
    _watcher_handle: tokio::task::JoinHandle<()>,
}

impl NotificationsStream {
    pub(crate) fn new(
        config: Arc<Config>,
        shard_manager: Arc<shard::Manager>,
        options: NotificationsOptions,
    ) -> Self {
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
            state: Self::configure(Arc::clone(&config), Arc::clone(&shard_manager), None),
            config,
            shard_manager,
            options,
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
            let (mut streams, offsets, pending_reconnects) = match current {
                Some(c) => (c.streams, c.offsets, c.pending_reconnects),
                None => (ShardStreamMap::new(), ShardOffsetMap::new(), None),
            };

            // Clients are sorted by shard id, let's sort the active ids so we can easily
            // find the differences between the old and new configuration
            let active_shard_ids = {
                let mut keys: Vec<ShardId> = streams.keys().copied().collect();
                keys.sort_unstable();
                keys
            };
            let mut active_iter = active_shard_ids.iter().peekable();

            let clients = shard_manager.get_shard_clients();
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
                    let config = Arc::clone(&config);
                    let offset = offsets.get(id);
                    async move {
                        let r = crate::execute_with_retry(&config, None, move || {
                            let client = client.clone();
                            async move { client.get_notifications(offset).await }
                        })
                        .await;
                        let s = StreamNotifyClose::new(match r {
                            Ok(s) => s.boxed(),
                            Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                        });
                        (id, s)
                    }
                });
            }

            let results: Vec<_> = futures::stream::iter(futs.into_iter())
                .buffer_unordered(max_parallel_requests)
                .collect()
                .await;

            for (id, s) in results {
                streams.insert(id, s);
            }

            State::Streaming(StreamingState {
                streams,
                offsets,
                pending_reconnects,
            })
        }
        .boxed();
        State::Configuring(fut)
    }

    /// Poll pending reconnects. Returns an error to propagate if a close-triggered
    /// reconnect fails and `reconnect_on_error` is not enabled.
    fn poll_reconnects(
        s: &mut StreamingState,
        cx: &mut Context<'_>,
        config: &Arc<Config>,
        shard_manager: &Arc<shard::Manager>,
        reconnect_on_error: bool,
    ) -> Option<crate::Error> {
        let Some(reconnects) = &mut s.pending_reconnects else {
            return None;
        };

        // Collect retries to schedule after releasing the borrow on pending_reconnects
        let mut retries: Vec<(ShardId, Option<i64>)> = Vec::new();

        while let Poll::Ready(Some((id, result))) = reconnects.poll_next_unpin(cx) {
            match result {
                Ok(stream) => {
                    debug!(?id, "reconnected shard stream");
                    s.streams.insert(id, stream);
                }
                Err(e) if e.is_shard_unavailable() => {
                    debug!(?id, ?e, "shard no longer available, discarding reconnect");
                }
                Err(e) if reconnect_on_error => {
                    debug!(?id, ?e, "reconnect failed, scheduling retry");
                    retries.push((id, s.offsets.get(id)));
                }
                Err(e) => {
                    debug!(?id, ?e, "reconnect failed, propagating error");
                    return Some(e);
                }
            }
        }

        for (id, offset) in retries {
            Self::schedule_reconnect(s, config, shard_manager, id, offset);
        }

        None
    }

    /// Schedule a reconnect future (lazily allocates the collection).
    fn schedule_reconnect(
        s: &mut StreamingState,
        config: &Arc<Config>,
        shard_manager: &Arc<shard::Manager>,
        id: ShardId,
        offset: Option<i64>,
    ) {
        let config = Arc::clone(config);
        let shard_manager = Arc::clone(shard_manager);
        let future = async move {
            let client = match shard_manager.get_client_by_shard_id(id) {
                Ok(c) => c,
                Err(e) => return (id, Err(e)),
            };

            let result = crate::execute_with_retry(&config, None, move || {
                let client = client.clone();
                async move { client.get_notifications(offset).await }
            })
            .await;

            match result {
                Ok(s) => (id, Ok(StreamNotifyClose::new(s.boxed()))),
                Err(e) => (id, Err(e)),
            }
        }
        .boxed();

        s.pending_reconnects
            .get_or_insert_with(|| Box::new(FuturesUnordered::new()))
            .push(future);
    }

    /// Wait until all shard notification cursors are established.
    ///
    /// On initial connection, the Oxia server positions the notification cursor at
    /// the current commit offset. Operations performed **after** the cursor is
    /// established will generate notifications; operations performed before may not.
    ///
    /// Call this method after creating the stream and before performing operations
    /// whose notifications you need to capture.
    ///
    /// # Implementation
    ///
    /// Each shard is polled exactly once to receive its first batch (which establishes
    /// the cursor). If a first batch is unexpectedly non-empty, it is buffered and
    /// will be returned by subsequent `poll_next` calls. Buffering is bounded to at
    /// most N batches (one per shard).
    ///
    /// # Errors
    ///
    /// - `Error::RequestTimeout` if the timeout expires before all shards are ready
    /// - Any error from the underlying notification streams
    pub async fn wait_ready(&mut self, timeout: Duration) -> crate::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;

        // Drive Configuring to completion if needed
        while matches!(self.state, State::Configuring(_)) {
            let poll_result = tokio::time::timeout_at(deadline, self.next()).await;

            // State might have transitioned during the poll - check before handling result
            if !matches!(self.state, State::Configuring(_)) {
                break;
            }

            match poll_result {
                Err(_) => return Err(crate::Error::RequestTimeout),
                Ok(Some(Err(e))) => return Err(e),
                Ok(_) => (),
            }
        }

        // Transition Streaming → WaitingReady
        if let State::Streaming(s) = &mut self.state {
            // Separate streams into waiting (no offset yet) and ready (offset already set)
            let mut waiting = ShardStreamMap::new();
            let mut ready = ShardStreamMap::new();
            let shard_ids: Vec<_> = s.streams.keys().copied().collect();
            for id in shard_ids {
                if let Some(stream) = s.streams.remove(&id) {
                    if s.offsets.get(id).is_some() {
                        ready.insert(id, stream);
                    } else {
                        waiting.insert(id, stream);
                    }
                }
            }

            // If all streams are already ready, put them back and return
            if waiting.is_empty() {
                s.streams = ready;
                return Ok(());
            }

            self.state = State::WaitingReady(Box::new(WaitingReadyState {
                waiting,
                ready,
                offsets: std::mem::take(&mut s.offsets),
                pending_reconnects: s.pending_reconnects.take(),
                buffered: VecDeque::new(),
            }));
        }

        // Poll until back in Streaming state (all shards ready, buffer drained)
        loop {
            match &self.state {
                State::Streaming(_) => return Ok(()),
                _ => {
                    let poll_result = tokio::time::timeout_at(deadline, self.next()).await;
                    match poll_result {
                        Err(_) => return Err(crate::Error::RequestTimeout),
                        Ok(Some(Err(e))) => return Err(e),
                        Ok(_) => continue,
                    }
                }
            }
        }
    }
}

impl Stream for NotificationsStream {
    type Item = Result<NotificationBatch>;
    #[allow(clippy::too_many_lines)] // FIXME
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Streaming(s) => {
                    if let Some(e) = Self::poll_reconnects(
                        s,
                        cx,
                        &this.config,
                        &this.shard_manager,
                        this.options.reconnect_on_error,
                    ) {
                        return Poll::Ready(Some(Err(e)));
                    }

                    if let Poll::Ready(Some(cmd)) = this.config_rx.poll_recv(cx) {
                        match cmd {
                            Cmd::Configure => {
                                debug!("config change signal received");
                                this.state = Self::configure(
                                    Arc::clone(&this.config),
                                    Arc::clone(&this.shard_manager),
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
                                // Stream closed normally
                                debug!(?id, "shard stream terminated");
                                if this.options.reconnect_on_close {
                                    debug!(?id, "scheduling reconnect on close");
                                    let offset = s.offsets.get(id);
                                    Self::schedule_reconnect(
                                        s,
                                        &this.config,
                                        &this.shard_manager,
                                        id,
                                        offset,
                                    );
                                }
                                continue;
                            }
                            if let Some(Ok(b)) = &r {
                                let first_batch = s.offsets.get(id).is_none();
                                s.offsets.insert(id, b.offset);
                                // Server sends empty batch on first connection to establish cursor
                                // position (../../ext/oxia/oxiad/dataserver/controller/lead/leader_controller.go:882-898).
                                // Go client discards first batch similarly (../../ext/oxia/oxia/notifications.go:175-186).
                                if first_batch && b.notifications.is_empty() {
                                    debug!(?id, offset = b.offset, "discarding empty first batch");
                                    continue;
                                }
                                if first_batch {
                                    warn!(?id, "first batch unexpectedly non-empty, delivering");
                                }
                            } else if let Some(Err(e)) = &r
                                && this.options.reconnect_on_error
                                && !e.is_shard_unavailable()
                            {
                                debug!(?id, ?e, "scheduling reconnect on error");
                                let offset = s.offsets.get(id);
                                Self::schedule_reconnect(
                                    s,
                                    &this.config,
                                    &this.shard_manager,
                                    id,
                                    offset,
                                );
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
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },

                State::WaitingReady(w) => {
                    // Phase 1: Poll waiting shards until all are ready
                    if !w.waiting.is_empty() {
                        match w.waiting.poll_next_unpin(cx) {
                            Poll::Ready(Some((id, batch_opt))) => {
                                // Move stream from waiting to ready
                                if let Some(stream) = w.waiting.remove(&id) {
                                    w.ready.insert(id, stream);
                                }

                                match batch_opt {
                                    None => {
                                        // Stream closed (StreamNotifyClose signal)
                                        continue;
                                    }
                                    Some(Err(e)) => {
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                    Some(Ok(batch)) => {
                                        // Cursor established
                                        w.offsets.insert(id, batch.offset);
                                        // Buffer if non-empty (unexpected but possible)
                                        if !batch.notifications.is_empty() {
                                            warn!(
                                                ?id,
                                                "first batch unexpectedly non-empty, buffering"
                                            );
                                            w.buffered.push_back(batch);
                                        }
                                        continue;
                                    }
                                }
                            }
                            Poll::Ready(None) => {
                                // All waiting streams exhausted - fall through to drain
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }

                    // Phase 2: All shards ready - drain buffered batches
                    if let Some(batch) = w.buffered.pop_front() {
                        return Poll::Ready(Some(Ok(batch)));
                    }

                    // Phase 3: Buffer empty - transition to Streaming
                    this.state = State::Streaming(StreamingState {
                        streams: std::mem::take(&mut w.ready),
                        offsets: std::mem::take(&mut w.offsets),
                        pending_reconnects: w.pending_reconnects.take(),
                    });
                }
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

    #[test]
    fn test_notifications_options_default() {
        let opts = NotificationsOptions::default();
        assert!(!opts.reconnect_on_close);
        assert!(!opts.reconnect_on_error);
    }

    #[test]
    fn test_notifications_options_reconnect_on_close() {
        let opts = NotificationsOptions::builder()
            .reconnect_on_close(true)
            .build();
        assert!(opts.reconnect_on_close);
        assert!(!opts.reconnect_on_error);
    }

    #[test]
    fn test_notifications_options_reconnect_on_error() {
        let opts = NotificationsOptions::builder()
            .reconnect_on_error(true)
            .build();
        assert!(!opts.reconnect_on_close);
        assert!(opts.reconnect_on_error);
    }

    #[test]
    fn test_notifications_options_both() {
        let opts = NotificationsOptions::builder()
            .reconnect_on_close(true)
            .reconnect_on_error(true)
            .build();
        assert!(opts.reconnect_on_close);
        assert!(opts.reconnect_on_error);
    }
}
