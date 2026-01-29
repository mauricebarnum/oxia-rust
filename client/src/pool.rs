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

use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::Error;
use crate::Result;
use crate::config::Config;

type CompletionResult = crate::Result<Channel>;

mod state {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use tokio::sync::Mutex;
    use tokio::sync::broadcast;
    use tonic::transport::Channel;

    use crate::pool::CompletionResult;

    #[derive(Clone, Debug)]
    pub enum Entry {
        Connected(Channel),
        Pending(broadcast::Sender<CompletionResult>),
    }

    pub type EntryMap = HashMap<String, Entry>;

    pub struct Txn<'a> {
        entries: &'a ArcSwap<EntryMap>,
    }

    impl<'a> Txn<'a> {
        pub const fn new(entries: &'a ArcSwap<EntryMap>) -> Self {
            Self { entries }
        }

        pub fn load(&self) -> Arc<EntryMap> {
            // use load_full() to avoid borrowing self.entries
            self.entries.load_full()
        }

        pub fn store(&self, x: EntryMap) {
            self.entries.store(Arc::new(x));
        }
    }

    #[derive(Debug, Default)]
    pub struct State {
        entries: ArcSwap<EntryMap>,
        lock: Mutex<()>,
    }

    impl State {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn load(&self) -> arc_swap::Guard<Arc<EntryMap>> {
            self.entries.load()
        }

        pub async fn update<F, R>(&self, f: F) -> R
        where
            F: FnOnce(Txn<'_>) -> R,
        {
            let _lock = self.lock.lock().await;
            f(Txn::new(&self.entries))
        }
    }
}

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
    state: Arc<state::State>,
    config: Arc<Config>,
    factory: F,
}

impl ChannelPool<DefaultChannelFactory> {
    pub fn new(config: &Arc<Config>) -> Self {
        Self {
            state: Arc::new(state::State::new()),
            config: Arc::clone(config),
            factory: DefaultChannelFactory {},
        }
    }
}

impl<Fact: ChannelFactory> ChannelPool<Fact> {
    #[allow(dead_code)]
    pub fn with_channel_factory(config: &Arc<Config>, factory: Fact) -> Self {
        Self {
            state: Arc::new(state::State::new()),
            config: Arc::clone(config),
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

    async fn create_channel(&self, target: &str) -> Result<Channel> {
        self.factory.create(&self.config, target).await
    }

    async fn finalize_connection(
        &self,
        target: &str,
        tx: Sender<CompletionResult>,
        mut res: Result<Channel>,
    ) -> Result<Channel> {
        use state::Entry;
        self.state
            .update(|txn| {
                let current = txn.load();
                let entry = current.get(target);
                if let Some(Entry::Connected(c)) = entry {
                    res = Ok(c.clone());
                } else if let Some(Entry::Pending(etx)) = entry
                    && etx.same_channel(&tx)
                {
                    let mut new_map = current.as_ref().clone();
                    if let Ok(c) = &res {
                        new_map.insert(target.to_string(), Entry::Connected(c.clone()));
                    } else {
                        new_map.remove(target);
                    }
                    txn.store(new_map);
                }

                let _ = tx.send(res.clone());
                res
            })
            .await
    }

    pub(crate) async fn get(&self, target: &str) -> Result<Channel> {
        use state::Entry;

        enum Next<F>
        where
            F: Future<Output = Result<Channel>>,
        {
            Connected(Channel),
            Pending(Receiver<CompletionResult>),
            Prepared((Sender<CompletionResult>, F)),
        }

        if let Some(Entry::Connected(c)) = self.state.load().get(target) {
            return Ok(c.clone());
        }

        let next = self
            .state
            .update(|txn| {
                let current = txn.load();
                match current.get(target) {
                    Some(Entry::Connected(c)) => Next::Connected(c.clone()),
                    Some(Entry::Pending(tx)) => Next::Pending(tx.subscribe()),
                    None => {
                        let (tx, _) = broadcast::channel(1);
                        let mut new_map = current.as_ref().clone();
                        new_map.insert(target.to_string(), Entry::Pending(tx.clone()));
                        txn.store(new_map);
                        Next::Prepared((tx, self.create_channel(target)))
                    }
                }
            })
            .await;

        match next {
            Next::Connected(c) => Ok(c),
            Next::Pending(rx) => Self::await_pending_op(rx).await,
            Next::Prepared((tx, fut)) => self.finalize_connection(target, tx, fut.await).await,
        }
    }

    #[allow(dead_code)] // do we need this for anything?
    pub(crate) async fn replace(&self, target: &str) -> Result<Channel> {
        self.remove(target).await;
        self.get(target).await
    }

    pub(crate) async fn remove(&self, target: &str) {
        if !self.state.load().contains_key(target) {
            // Almost always, this early exit won't happen. But the lookup isn't a waste: under
            // extreme load we can be losing races with other tasks and that's when avoiding lock
            // contention is most critical.
            return;
        }
        self.state
            .update(|txn| {
                let current = txn.load();
                if current.contains_key(target) {
                    let mut new_map = current.as_ref().clone();
                    let _ = new_map.remove(target);
                    txn.store(new_map);
                }
            })
            .await;
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
