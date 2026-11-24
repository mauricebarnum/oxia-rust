// Copyright 2026 Maurice S. Barnum
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

use tokio::sync::Mutex;

use crate::client_lib::Client;
use crate::client_lib::Result;
use crate::client_lib::config;

#[derive(Clone, Debug)]
struct State {
    client: Client,
}

#[derive(Clone, Debug)]
pub struct Context {
    state: Arc<Mutex<State>>,
}

impl Context {
    pub(super) fn new(cli: &crate::Cli, metrics: Option<&crate::metrics::MetricsOutput>) -> Self {
        let builder = config::Config::builder()
            .service_addr(cli.service_address.clone())
            .max_parallel_requests(cli.max_parallel_requests)
            .session_timeout(cli.session_timeout)
            .request_timeout(cli.request_timeout)
            .retry_on_stale_shard_map(cli.retry_on_stale_shard_map)
            .maybe_meter_provider(metrics.map(crate::metrics::MetricsOutput::meter_provider));

        #[cfg(feature = "_compression_enabled")]
        let builder = builder
            .maybe_send_compressed(
                cli.compress_send
                    .and_then(crate::compress::ArgValue::to_codec),
            )
            .accept_compression(crate::compress::ArgValue::to_codec_set(
                &cli.compress_accept,
            ));

        let client = Client::new(builder.build());
        Self {
            state: Arc::new(Mutex::new(State { client })),
        }
    }

    pub async fn client(&self) -> Result<Client> {
        let mut state = self.state.lock().await;
        state.client.connect().await?;
        Ok(state.client.clone())
    }

    pub async fn make_subcontext(&self) -> Self {
        let config = {
            let state = self.state.lock().await;
            Arc::clone(state.client.config())
        };
        let client = Client::new(config);
        Self {
            state: Arc::new(Mutex::new(State { client })),
        }
    }
}

#[cfg(all(test, feature = "_compression_enabled"))]
mod tests {
    use clap::Parser as _;

    use super::*;
    use crate::client_lib::config::CompressionCodec;
    use crate::client_lib::config::CompressionCodecSet;

    fn config_for_args(args: &[&str]) -> Arc<config::Config> {
        let cli = crate::Cli::try_parse_from(args).expect("CLI args should parse");
        let ctx = Context::new(&cli, None);
        let state = ctx.state.try_lock().expect("uncontended context state");
        Arc::clone(state.client.config())
    }

    #[test]
    fn default_cli_enables_all_accept_compression_and_no_send_compression() {
        let cfg = config_for_args(&["oxia-cmd", "get", "key"]);
        assert_eq!(cfg.accept_compression(), CompressionCodecSet::ALL);
        assert_eq!(cfg.send_compressed(), None);
    }

    #[test]
    fn cli_compress_accept_none_disables_accept_compression() {
        let cfg = config_for_args(&["oxia-cmd", "--compress-accept", "none", "get", "key"]);
        assert_eq!(cfg.accept_compression(), CompressionCodecSet::EMPTY);
    }

    #[test]
    fn cli_explicit_compression_flags_propagate_to_config() {
        #[cfg(feature = "deflate")]
        {
            let cfg = config_for_args(&[
                "oxia-cmd",
                "--compress-send",
                "deflate",
                "--compress-accept",
                "deflate",
                "get",
                "key",
            ]);
            assert_eq!(cfg.send_compressed(), Some(CompressionCodec::Deflate));
            assert_eq!(
                cfg.accept_compression(),
                CompressionCodecSet::from(CompressionCodec::Deflate)
            );
        }

        #[cfg(feature = "gzip")]
        {
            let cfg = config_for_args(&[
                "oxia-cmd",
                "--compress-send",
                "gzip",
                "--compress-accept",
                "gzip",
                "get",
                "key",
            ]);
            assert_eq!(cfg.send_compressed(), Some(CompressionCodec::Gzip));
            assert_eq!(
                cfg.accept_compression(),
                CompressionCodecSet::from(CompressionCodec::Gzip)
            );
        }

        #[cfg(feature = "zstd")]
        {
            let cfg = config_for_args(&[
                "oxia-cmd",
                "--compress-send",
                "zstd",
                "--compress-accept",
                "zstd",
                "get",
                "key",
            ]);
            assert_eq!(cfg.send_compressed(), Some(CompressionCodec::Zstd));
            assert_eq!(
                cfg.accept_compression(),
                CompressionCodecSet::from(CompressionCodec::Zstd)
            );
        }

        #[cfg(all(feature = "deflate", feature = "zstd"))]
        {
            let cfg = config_for_args(&[
                "oxia-cmd",
                "--compress-accept",
                "deflate",
                "--compress-accept",
                "zstd",
                "get",
                "key",
            ]);
            assert_eq!(
                cfg.accept_compression(),
                CompressionCodecSet::from([CompressionCodec::Deflate, CompressionCodec::Zstd])
            );
            #[cfg(feature = "gzip")]
            assert!(!cfg.accept_compression().contains(CompressionCodec::Gzip));
        }

        let cfg_override = config_for_args(&[
            "oxia-cmd",
            "--compress-send",
            "none",
            "--compress-accept",
            "auto",
            "--compress-accept",
            "none",
            "get",
            "key",
        ]);
        assert_eq!(cfg_override.send_compressed(), None);
        assert_eq!(
            cfg_override.accept_compression(),
            CompressionCodecSet::EMPTY
        );
    }
}
