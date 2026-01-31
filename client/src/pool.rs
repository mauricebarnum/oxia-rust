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
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::info;
use tracing::warn;

use crate::Error;
use crate::Result;
use crate::config::Config;

type CompletionResult = crate::Result<Channel>;

mod guarded {
    use std::ops::Deref;
    use std::sync::Arc;

    use arc_swap::{ArcSwap, Guard};
    use tokio::sync::{Mutex, MutexGuard};

    /// An `ArcSwap<T>` whose `.store()` is only reachable through a
    /// [`WriteGuard`], giving compile-time serialization of mutations.
    /// Follows an RwLock-like pattern: [`ReadGuard`] for reads,
    /// [`WriteGuard`] for writes.
    #[derive(Debug)]
    pub(super) struct GuardedArcSwap<T> {
        data: ArcSwap<T>,
        write_mutex: Mutex<()>,
    }

    impl<T> GuardedArcSwap<T> {
        pub(super) fn new(value: T) -> Self {
            Self {
                data: ArcSwap::from_pointee(value),
                write_mutex: Mutex::new(()),
            }
        }

        /// Lock-free snapshot (hot path).
        pub(super) fn read(&self) -> ReadGuard<T> {
            ReadGuard(self.data.load())
        }

        /// Acquire exclusive write access.
        pub(super) async fn write(&self) -> WriteGuard<'_, T> {
            let guard = self.write_mutex.lock().await;
            WriteGuard {
                data: &self.data,
                _guard: guard,
            }
        }
    }

    /// Zero-cost read handle. Derefs to `T`.
    pub(super) struct ReadGuard<T>(Guard<Arc<T>>);

    impl<T> Deref for ReadGuard<T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    /// Proof-of-write-permission token.  The only way to call `.store()`.
    pub(super) struct WriteGuard<'a, T> {
        data: &'a ArcSwap<T>,
        _guard: MutexGuard<'a, ()>,
    }

    impl<T> WriteGuard<'_, T> {
        pub(super) fn load(&self) -> ReadGuard<T> {
            ReadGuard(self.data.load())
        }

        pub(super) fn store(&self, value: Arc<T>) {
            self.data.store(value);
        }
    }
}

mod inner {
    use super::*;

    #[derive(Debug)]
    pub(super) struct Inner {
        pub(super) config: Arc<Config>,
        /// Lock-free snapshot of established channels (hot path).
        /// Writes are serialized via [`GuardedArcSwap::write()`].
        pub(super) channels: guarded::GuardedArcSwap<HashMap<String, Channel>>,
        /// Pending connection coordination (cold path, cache-miss only).
        pub(super) pending: Mutex<HashMap<String, broadcast::Sender<CompletionResult>>>,
    }

    impl Inner {
        pub(super) fn new(config: Arc<Config>) -> Self {
            Self {
                config,
                channels: guarded::GuardedArcSwap::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
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

    async fn create_channel(&self, target: &str) -> Result<Channel> {
        self.factory.create(&self.inner.config, target).await
    }

    async fn complete(
        &self,
        target: &str,
        tx: broadcast::Sender<CompletionResult>,
        r: Result<Channel>,
    ) -> Result<Channel> {
        let mut pending = self.inner.pending.lock().await;

        // Verify we still own the pending entry (same_channel guard)
        match pending.get(target) {
            Some(otx) if otx.same_channel(&tx) => {
                // Still ours — complete it
                pending.remove(target);
            }
            Some(_) => {
                // Replaced by another operation — we've been cancelled
                return Err(Error::Cancelled);
            }
            None => {
                // Entry was removed (invalidated) while we were connecting
                return Err(Error::Cancelled);
            }
        }

        let notification = match r {
            Ok(c) => {
                // Clone-modify-swap: insert into channels
                let write = self.inner.channels.write().await;
                let mut map = HashMap::clone(&write.load());
                map.insert(target.to_string(), c.clone());
                write.store(Arc::new(map));
                Ok(c)
            }
            Err(e) => Err(e),
        };

        // Notify waiters (best-effort)
        match tx.send(notification.clone()) {
            Err(err) => warn!(?err, "ChannelPool::complete() send failed"),
            Ok(n) => info!(n, "ChannelPool::complete() notified waiters"),
        }

        notification
    }

    #[allow(dead_code)]
    pub(crate) async fn get(&self, target: &str) -> Result<Channel> {
        // Fast path: lock-free read
        {
            let snap = self.inner.channels.read();
            if let Some(c) = snap.get(target) {
                return Ok(c.clone());
            }
        }

        // Slow path: check pending or start new connection
        let mut pending = self.inner.pending.lock().await;

        // Double-check channels (may have been inserted while we waited for Mutex)
        {
            let snap = self.inner.channels.read();
            if let Some(c) = snap.get(target) {
                return Ok(c.clone());
            }
        }

        // Check if another task is already connecting
        if let Some(tx) = pending.get(target) {
            let rx = tx.subscribe();
            drop(pending);
            return Self::await_pending_op(rx).await;
        }

        // We're the first — install pending entry
        let (tx, _) = broadcast::channel(1);
        pending.insert(target.to_string(), tx.clone());
        drop(pending);

        // Create channel without any lock held
        let cr = self.create_channel(target).await;
        self.complete(target, tx, cr).await
    }

    pub(crate) async fn create(&self, target: &str) -> Result<Channel> {
        self.remove(target).await;
        self.get(target).await
    }

    pub(crate) async fn remove(&self, target: &str) -> Option<Channel> {
        let write = self.inner.channels.write().await;
        let old = write.load();
        if old.contains_key(target) {
            let mut map = HashMap::clone(&old);
            let removed = map.remove(target);
            write.store(Arc::new(map));
            removed
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
