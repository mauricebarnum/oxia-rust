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
#[cfg(feature = "_compression_enabled")]
use tonic::codec::CompressionEncoding;

use crate::discovery::IntoServiceDiscovery;
use crate::discovery::ServiceDiscovery;

#[cfg(feature = "metrics")]
pub type MeterProviderHandle = Arc<dyn opentelemetry::metrics::MeterProvider + Send + Sync>;

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

    #[inline]
    pub const fn attempts(&self) -> usize {
        self.attempts
    }

    #[inline]
    pub const fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    #[inline]
    pub const fn max_delay(&self) -> Duration {
        self.max_delay
    }
}

#[cfg(feature = "_compression_enabled")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CompressionCodec {
    #[cfg(feature = "deflate")]
    Deflate,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "zstd")]
    Zstd,
}

#[cfg(feature = "_compression_enabled")]
impl CompressionCodec {
    const ALL: &[Self] = &[
        #[cfg(feature = "deflate")]
        Self::Deflate,
        #[cfg(feature = "gzip")]
        Self::Gzip,
        #[cfg(feature = "zstd")]
        Self::Zstd,
    ];

    pub(crate) const fn to_proto(self) -> CompressionEncoding {
        match self {
            #[cfg(feature = "deflate")]
            Self::Deflate => CompressionEncoding::Deflate,
            #[cfg(feature = "gzip")]
            Self::Gzip => CompressionEncoding::Gzip,
            #[cfg(feature = "zstd")]
            Self::Zstd => CompressionEncoding::Zstd,
        }
    }
}

#[cfg(feature = "_compression_enabled")]
#[derive(Default, Copy, Clone, PartialEq, Eq)]
pub struct CompressionCodecSet(u8);

#[cfg(feature = "_compression_enabled")]
impl std::fmt::Debug for CompressionCodecSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(feature = "_compression_enabled")]
impl CompressionCodecSet {
    pub const EMPTY: Self = Self(0);
    pub const ALL: Self = Self::from_slice(CompressionCodec::ALL);

    #[inline]
    pub const fn new() -> Self {
        Self::EMPTY
    }

    #[inline]
    pub const fn from_codec(codec: CompressionCodec) -> Self {
        Self(Self::bit(codec))
    }

    #[inline]
    pub const fn from_slice(codecs: &[CompressionCodec]) -> Self {
        let mut set = Self::EMPTY;
        let mut i = 0;
        while i < codecs.len() {
            set.insert(codecs[i]);
            i += 1;
        }
        set
    }

    #[inline]
    const fn bit(x: CompressionCodec) -> u8 {
        1 << (x as u8)
    }

    #[inline]
    pub const fn insert(&mut self, x: CompressionCodec) {
        self.0 |= Self::bit(x);
    }

    #[inline]
    pub const fn contains(self, x: CompressionCodec) -> bool {
        self.0 & Self::bit(x) != 0
    }

    #[inline]
    pub fn iter(self) -> impl Iterator<Item = CompressionCodec> {
        CompressionCodec::ALL
            .iter()
            .copied()
            .filter(move |&c| self.contains(c))
    }
}

#[cfg(feature = "_compression_enabled")]
const _: () = {
    // Verify no duplicates in CompressionCodec::ALL.
    assert!(CompressionCodec::ALL.len() == CompressionCodecSet::ALL.0.count_ones() as usize);

    // Inline const blocks verify every variant is present in ALL.
    let _ = |c: CompressionCodec| match c {
        #[cfg(feature = "deflate")]
        CompressionCodec::Deflate => {
            const { assert!(CompressionCodecSet::ALL.contains(CompressionCodec::Deflate)) }
        }
        #[cfg(feature = "gzip")]
        CompressionCodec::Gzip => {
            const { assert!(CompressionCodecSet::ALL.contains(CompressionCodec::Gzip)) }
        }
        #[cfg(feature = "zstd")]
        CompressionCodec::Zstd => {
            const { assert!(CompressionCodecSet::ALL.contains(CompressionCodec::Zstd)) }
        }
    };
};

#[cfg(feature = "_compression_enabled")]
impl FromIterator<CompressionCodec> for CompressionCodecSet {
    fn from_iter<I: IntoIterator<Item = CompressionCodec>>(iter: I) -> Self {
        let mut set = Self::EMPTY;
        for x in iter {
            set.insert(x);
        }
        set
    }
}

#[cfg(feature = "_compression_enabled")]
impl From<CompressionCodec> for CompressionCodecSet {
    #[inline]
    fn from(x: CompressionCodec) -> Self {
        Self::from_codec(x)
    }
}

#[cfg(feature = "_compression_enabled")]
impl<const N: usize> From<[CompressionCodec; N]> for CompressionCodecSet {
    #[inline]
    fn from(xs: [CompressionCodec; N]) -> Self {
        Self::from_slice(&xs)
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
    #[cfg(feature = "_compression_enabled")]
    #[builder(default = CompressionCodecSet::EMPTY)]
    accept_compression: CompressionCodecSet,
    #[cfg(feature = "_compression_enabled")]
    send_compressed: Option<CompressionCodec>,
}

impl Config {
    #[inline]
    pub fn service_discovery(&self) -> &Arc<dyn ServiceDiscovery> {
        &self.service_discovery
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

    #[cfg(feature = "_compression_enabled")]
    #[inline]
    pub const fn accept_compression(&self) -> CompressionCodecSet {
        self.accept_compression
    }

    #[cfg(feature = "_compression_enabled")]
    #[inline]
    pub const fn send_compressed(&self) -> Option<CompressionCodec> {
        self.send_compressed
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

        #[cfg(feature = "_compression_enabled")]
        {
            debug.field("accept_compression", &self.accept_compression);
            debug.field("send_compressed", &self.send_compressed);
        }

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

#[cfg(all(test, feature = "_compression_enabled"))]
mod compression_tests {
    use super::*;

    #[test]
    fn codec_set_empty_and_default() {
        let empty = CompressionCodecSet::EMPTY;
        assert_eq!(empty, CompressionCodecSet::default());
        assert_eq!(empty, CompressionCodecSet::from_slice(&[]));
        assert_eq!(empty, CompressionCodecSet::from([]));
        assert_eq!(empty, CompressionCodecSet::new());
        assert_eq!(
            empty,
            std::iter::empty::<CompressionCodec>().collect::<CompressionCodecSet>()
        );
        assert_eq!(empty.iter().count(), 0);
        for &codec in CompressionCodec::ALL {
            assert!(!empty.contains(codec));
        }
    }

    #[test]
    fn codec_set_test_from_codec() {
        CompressionCodecSet::ALL.iter().for_each(|c| {
            let cs = CompressionCodecSet::from_codec(c);
            assert!(cs.contains(c));
            assert_eq!(cs.iter().count(), 1);
        });
    }

    #[test]
    fn codec_to_proto_maps_each_enabled_variant() {
        #[cfg(feature = "deflate")]
        assert_eq!(
            CompressionCodec::Deflate.to_proto(),
            CompressionEncoding::Deflate
        );
        #[cfg(feature = "gzip")]
        assert_eq!(CompressionCodec::Gzip.to_proto(), CompressionEncoding::Gzip);
        #[cfg(feature = "zstd")]
        assert_eq!(CompressionCodec::Zstd.to_proto(), CompressionEncoding::Zstd);
    }

    #[test]
    fn codec_set_single_codec_round_trip() {
        for &codec in CompressionCodec::ALL {
            let set = CompressionCodecSet::from(codec);
            assert_eq!(CompressionCodecSet::from([codec]), set);
            assert!(set.contains(codec));
            for &other in CompressionCodec::ALL {
                assert_eq!(set.contains(other), other == codec);
            }
            let iterated: Vec<_> = set.iter().collect();
            assert_eq!(iterated, vec![codec]);
        }
    }

    #[test]
    fn codec_set_subsets() {
        let count = CompressionCodec::ALL.len();
        let all_mask = (1usize << count) - 1;
        for mask in 0usize..=all_mask {
            let expected: Vec<_> = CompressionCodec::ALL
                .iter()
                .enumerate()
                .filter_map(|(i, &codec)| ((mask & (1usize << i)) != 0).then_some(codec))
                .collect();

            let set = CompressionCodecSet::from_slice(&expected);
            let from_iter: CompressionCodecSet = expected.iter().copied().collect();

            assert_eq!(set, from_iter);
            if mask == 0 {
                assert_eq!(set, CompressionCodecSet::EMPTY);
            }
            if mask == all_mask {
                assert_eq!(set, CompressionCodecSet::ALL);
            }

            for &codec in CompressionCodec::ALL {
                assert_eq!(set.contains(codec), expected.contains(&codec));
            }

            let iterated: Vec<_> = set.iter().collect();
            assert_eq!(iterated, expected);
        }
    }
}
