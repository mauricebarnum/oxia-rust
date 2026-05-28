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
use std::time::Duration;

use bon::Builder;

use crate::discovery::IntoServiceDiscovery;
use crate::discovery::ServiceDiscovery;

#[cfg(feature = "metrics")]
pub type MeterProviderHandle = Arc<dyn opentelemetry::metrics::MeterProvider + Send + Sync>;
#[cfg(not(feature = "metrics"))]
pub type MeterProviderHandle = ();

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    pub(crate) attempts: usize,
    pub(crate) initial_delay: Duration,
    pub(crate) max_delay: Duration,
}

impl RetryConfig {
    #[inline]
    pub const fn new(attempts: usize, initial_delay: Duration) -> Self {
        Self {
            attempts,
            initial_delay,
            max_delay: initial_delay.saturating_mul(10),
        }
    }
}

#[derive(Builder, Clone)]
#[builder(finish_fn(name = do_build, vis = ""))]
#[builder(on(String, into))]
#[builder(state_mod(vis = "pub"))]
pub struct Config {
    #[builder(with = |p: impl IntoServiceDiscovery| p.into_arc())]
    service_discovery: Arc<dyn ServiceDiscovery>,
    #[builder(default = "default")]
    namespace: String,
    #[builder(default)]
    identity: String, // used for ephemeral records
    #[builder(default)]
    session_timeout: Duration, // if non-zero, create a session with the specified timeout
    #[builder(default = 1)]
    max_parallel_requests: usize, // maximum number of parallel requests if non-zero
    request_timeout: Option<Duration>, // timeout if non-zero
    retry: Option<RetryConfig>,
    #[builder(default = false)]
    retry_on_stale_shard_map: bool,
    #[cfg(feature = "metrics")]
    meter_provider: Option<MeterProviderHandle>,
}

impl Config {
    #[inline]
    pub fn service_discovery(&self) -> Arc<dyn ServiceDiscovery> {
        Arc::clone(&self.service_discovery)
    }

    #[inline]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[inline]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    #[inline]
    pub const fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    #[inline]
    pub const fn max_parallel_requests(&self) -> usize {
        self.max_parallel_requests
    }

    #[inline]
    pub const fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }

    #[inline]
    pub const fn retry(&self) -> Option<RetryConfig> {
        self.retry
    }

    #[inline]
    pub const fn retry_on_stale_shard_map(&self) -> bool {
        self.retry_on_stale_shard_map
    }

    #[cfg(feature = "metrics")]
    #[inline]
    pub const fn meter_provider(&self) -> Option<&MeterProviderHandle> {
        self.meter_provider.as_ref()
    }

    #[cfg(not(feature = "metrics"))]
    #[inline]
    #[expect(clippy::unused_self)]
    pub(crate) const fn meter_provider(&self) -> Option<&MeterProviderHandle> {
        None
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Config");
        debug
            .field("service_discovery", &"<service discovery>")
            .field("namespace", &self.namespace)
            .field("identity", &self.identity)
            .field("session_timeout", &self.session_timeout)
            .field("max_parallel_requests", &self.max_parallel_requests)
            .field("request_timeout", &self.request_timeout)
            .field("retry", &self.retry)
            .field("retry_on_stale_shard_map", &self.retry_on_stale_shard_map);
        #[cfg(feature = "metrics")]
        debug.field("meter_provider", &self.meter_provider.is_some());
        debug.finish()
    }
}

impl<S: config_builder::State> ConfigBuilder<S> {
    /// Convenience for the simple case
    #[inline]
    pub fn service_addr(
        self,
        service_addr: impl Into<String>,
    ) -> ConfigBuilder<config_builder::SetServiceDiscovery<S>>
    where
        S::ServiceDiscovery: config_builder::IsUnset,
    {
        use crate::discovery::StaticServiceDiscovery;
        self.service_discovery(StaticServiceDiscovery::single(service_addr))
    }

    #[inline]
    pub fn build(self) -> Arc<Config>
    where
        S: config_builder::IsComplete,
    {
        Arc::new(self.do_build())
    }
}
