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

#![allow(dead_code)]

use std::sync::Arc;

use grpc::client::Channel;
use grpc::client::ChannelOptions;
use grpc::credentials::LocalChannelCredentials;

use crate::config::Config;

mod state {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use grpc::client::Channel;
    use tokio::sync::Mutex;
    pub type ChannelMap = HashMap<String, Channel>;

    pub struct Txn<'a> {
        entries: &'a ArcSwap<ChannelMap>,
    }

    impl<'a> Txn<'a> {
        pub const fn new(entries: &'a ArcSwap<ChannelMap>) -> Self {
            Self { entries }
        }

        pub fn load(&self) -> Arc<ChannelMap> {
            // use load_full() to avoid borrowing self.entries
            self.entries.load_full()
        }

        pub fn store(&self, x: ChannelMap) {
            self.entries.store(Arc::new(x));
        }
    }

    #[derive(Default)]
    pub struct State {
        entries: ArcSwap<ChannelMap>,
        lock: Mutex<()>,
    }

    impl State {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn load(&self) -> arc_swap::Guard<Arc<ChannelMap>> {
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

    impl std::fmt::Debug for State {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("State")
                .field(
                    "entries",
                    &format_args!("<{} channels>", self.entries.load().len()),
                )
                .finish_non_exhaustive()
        }
    }
}

pub trait ChannelFactory: Clone + Send + Sync {
    fn create(&self, config: &Arc<Config>, target: &str) -> Channel;
}

#[derive(Clone, Debug)]
pub struct DefaultChannelFactory;

impl ChannelFactory for DefaultChannelFactory {
    fn create(&self, _: &Arc<Config>, target: &str) -> Channel {
        use grpc::client::Channel;
        // FIXME: add more params such as credentials
        let credentials = Arc::new(LocalChannelCredentials::new());
        Channel::new(target, credentials, ChannelOptions::default())
        // Channel::builder(target, credentials).build();
    }
}

#[derive(Clone, Debug)]
pub struct ChannelPool<F = DefaultChannelFactory> {
    state: Arc<state::State>,
    config: Arc<Config>,
    factory: F,
}

impl ChannelPool<DefaultChannelFactory> {
    #[inline]
    pub fn new(config: &Arc<Config>) -> Self {
        Self::with_channel_factory(config, DefaultChannelFactory {})
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

    fn create_channel(&self, target: &str) -> Channel {
        self.factory.create(&self.config, target)
    }

    pub(crate) async fn get(&self, target: &str) -> Channel {
        if let Some(c) = self.state.load().get(target) {
            return c.clone();
        }
        self.state
            .update(|txn| {
                let current = txn.load();

                if let Some(channel) = current.get(target) {
                    return channel.clone();
                }

                let channel = self.create_channel(target);
                let mut entries = current.as_ref().clone();
                entries.insert(target.to_string(), channel.clone());
                txn.store(entries);
                channel
            })
            .await
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
