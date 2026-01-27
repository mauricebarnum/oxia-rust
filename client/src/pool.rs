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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::info;
use tracing::warn;

use crate::Error;
use crate::Result;
use crate::config::Config;

type CompletionResult = crate::Result<Channel>;

mod inner {
    use super::{Arc, Channel, CompletionResult, Config, HashMap, RwLock, broadcast};

    #[derive(Clone, Debug)]
    pub(super) enum Item {
        Channel(Channel),
        Pending(broadcast::Sender<CompletionResult>),
    }

    #[derive(Debug, Default)]
    pub(super) struct State {
        pub(super) clients: HashMap<String, Item>,
        // todo: metrics
    }

    #[derive(Clone, Debug)]
    pub(super) struct Inner {
        pub(super) config: Arc<Config>,
        pub(super) state: Arc<RwLock<State>>,
    }

    impl Inner {
        pub(super) fn new(config: Arc<Config>) -> Self {
            Self {
                config,
                state: Arc::new(RwLock::new(State::default())),
            }
        }
    }
} // mod inner

pub trait ChannelFactory: Clone + Send + Sync {
    async fn create(&self, config: &Arc<Config>, target: &str) -> Result<Channel>;
}

#[derive(Clone, Debug)]
pub struct DefaultChannelFactory;

impl ChannelFactory for DefaultChannelFactory {
    async fn create(&self, _: &Arc<Config>, target: &str) -> Result<Channel> {
        let endpoint: Endpoint = target.parse()?;
        endpoint.connect().await.map_err(Error::from)
    }
}

#[derive(Clone, Debug)]
pub struct ChannelPool<F = DefaultChannelFactory> {
    inner: Arc<inner::Inner>,
    factory: F,
}

#[allow(dead_code)]
impl ChannelPool<DefaultChannelFactory> {
    pub fn new(config: &Arc<Config>) -> Self {
        Self {
            inner: Arc::new(inner::Inner::new(Arc::clone(config))),
            factory: DefaultChannelFactory {},
        }
    }
}

#[allow(dead_code)]
impl<F: ChannelFactory> ChannelPool<F> {
    pub fn with_channel_factory(config: &Arc<Config>, factory: F) -> Self {
        Self {
            inner: Arc::new(inner::Inner::new(Arc::clone(config))),
            factory,
        }
    }

    async fn await_pending_op(mut rx: broadcast::Receiver<CompletionResult>) -> CompletionResult {
        let rv = rx.recv().await;
        match rv {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Error::other(e)),
        }
    }

    // Return a pooled item, waiting for a pending insert if one is in progress.
    // The pending insert may fail, which is why this is wrapped in Result<>
    async fn try_get(&self, target: &str) -> Option<CompletionResult> {
        use inner::Item;
        let guard = self.inner.state.read().await;
        let item = guard.clients.get(target)?;
        match item {
            Item::Channel(c) => Some(Ok(c.clone())),
            Item::Pending(tx) => {
                let rx = tx.subscribe();
                drop(guard); // allow pending operation to complete
                Some(Self::await_pending_op(rx).await)
            }
        }
    }

    async fn create_channel(&self, target: &str) -> Result<Channel> {
        self.factory.create(&self.inner.config, target).await
    }

    async fn send_completion(
        &self,
        target: &str,
        tx: broadcast::Sender<CompletionResult>,
        r: CompletionResult,
    ) {
        let mut guard = self.inner.state.write().await;

        match tx.send(r.clone()) {
            Err(err) => warn!(?err, "ChannelPool::get() send completion failed"),
            Ok(subscriptions) => {
                info!(subscriptions, "ChannelPool::get() send completion");
            }
        }

        if let Ok(c) = r {
            guard
                .clients
                .insert(target.to_string(), inner::Item::Channel(c));
        } else {
            match tx.send(r) {
                Err(err) => warn!(?err, "ChannelPool::get() send completion failed"),
                Ok(subscriptions) => {
                    info!(subscriptions, "ChannelPool::get() send completion");
                }
            }
            guard.clients.remove(target);
        }
    }

    async fn complete(
        &self,
        target: &str,
        tx: broadcast::Sender<CompletionResult>,
        r: Result<Channel>,
    ) -> Result<Channel> {
        use inner::Item;

        // If the entry was removed while we had the lock dropped, discard the channel.
        // There's no need to notify anyone waiting for a completion, when the the
        // last sender is dropped, they'll see a cancellation
        let mut guard = self.inner.state.write().await;
        match guard.clients.entry(target.to_string()) {
            Entry::Vacant(_) => Err(Error::Cancelled),
            Entry::Occupied(mut o) => {
                match o.get() {
                    Item::Pending(otx) => {
                        // While the lock was dropped, our request could have been cancelled and replaced
                        // by anoter request.  Verify we still "own" the pending state.
                        if otx.same_channel(&tx) {
                            // Still our pending operation, so complete it
                            let notification = match r {
                                Ok(c) => {
                                    o.insert(Item::Channel(c.clone()));
                                    Ok(c)
                                }
                                Err(e) => {
                                    o.remove();
                                    Err(e)
                                }
                            };

                            match tx.send(notification.clone()) {
                                Err(err) => {
                                    warn!(?err, "ChannelPool::get() send completion failed");
                                }
                                Ok(subscriptions) => {
                                    info!(subscriptions, "ChannelPool::get() send completion");
                                }
                            }

                            // Now map the notification back to a cloneable Result
                            notification
                        } else {
                            // Our operation was cancelled, so don't notify and return an error.
                            // Waiting for the new operation is likely useless: we got caoncelled for a
                            // reason.
                            Err(Error::Cancelled)
                        }
                    }
                    Item::Channel(c) => Ok(c.clone()),
                }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn get(&self, target: &str) -> Result<Channel> {
        use inner::Item;

        // Already there, we're done and don't need the write lock
        if let Some(item) = self.try_get(target).await {
            return item;
        }

        let state = &self.inner.state;
        let mut guard = state.write().await;
        let entry = guard.clients.entry(target.to_string());

        if let Entry::Occupied(item) = entry {
            return match item.get() {
                Item::Channel(c) => Ok(c.clone()),
                Item::Pending(tx) => {
                    let rx = tx.subscribe();
                    drop(guard); // allow pending operation to complete
                    Self::await_pending_op(rx).await
                }
            };
        }

        // Set up the notification channel, let's others know we're working on it...
        let tx = {
            let (tx, _) = broadcast::channel(1);
            entry.insert_entry(Item::Pending(tx.clone()));
            tx
        };

        // Create the channel without the write lock to avoid blocking all
        drop(guard);
        let cr = self.create_channel(target).await;

        self.complete(target, tx, cr).await
    }

    pub(crate) async fn create(&self, target: &str) -> Result<Channel> {
        // We could try to be smarter here, but this method is unlikely to be called often
        self.inner.state.write().await.clients.remove(target);
        self.get(target).await
    }

    pub(crate) async fn remove(&self, target: &str) -> Option<Channel> {
        let state = &self.inner.state;
        let mut guard = state.write().await;
        if let Some(inner::Item::Channel(c)) = guard.clients.remove(target) {
            Some(c)
        } else {
            None
        }
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test_log::test(tokio::test(flavor = "current_thread"))]
    async fn test_construct() -> anyhow::Result<()> {
        let config = Config::builder().service_addr("localhost").build();
        let pool = ChannelPool::new(&config);
        let _ = pool.get("").await;
        Ok(())
    }

    #[derive(Clone)]
    struct FailDefaultChannelFactory;

    impl ChannelFactory for FailDefaultChannelFactory {
        async fn create(&self, _: &Arc<Config>, _: &str) -> Result<Channel> {
            Err(Error::Custom("not implemented".to_string()))
        }
    }

    #[test_log::test(tokio::test(flavor = "current_thread"))]
    async fn test_construct_factory() -> anyhow::Result<()> {
        let config = Config::builder().service_addr("localhost").build();
        let pool = ChannelPool::with_channel_factory(&config, FailDefaultChannelFactory {});
        assert!(pool.get("").await.is_err());
        Ok(())
    }
}
