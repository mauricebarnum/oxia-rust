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

use clap::Args;
use clap::ValueEnum;
use futures::stream::StreamExt;
use mauricebarnum_oxia_client::NotificationBatch;
use mauricebarnum_oxia_client::NotificationsOptions;
use tracing::trace;

use super::CommandRunnable;

/// Control automatic reconnection behavior for shard streams.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ReconnectMode {
    /// Reconnect for all reasons (close and error)
    All,
    /// Reconnect when stream is closed by peer
    Close,
    /// Reconnect on network errors
    Error,
}

#[derive(Args, Debug)]
pub struct NotificationsCommand {
    /// Control automatic reconnection behavior for shard streams.
    /// Can specify multiple values (e.g., --reconnect close,error or --reconnect all).
    #[arg(long, value_enum, value_delimiter = ',')]
    reconnect: Vec<ReconnectMode>,
}

#[async_trait::async_trait]
impl CommandRunnable for NotificationsCommand {
    async fn run(self, ctx: crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");

        let reconnect_on_close = self
            .reconnect
            .iter()
            .any(|m| matches!(m, ReconnectMode::All | ReconnectMode::Close));
        let reconnect_on_error = self
            .reconnect
            .iter()
            .any(|m| matches!(m, ReconnectMode::All | ReconnectMode::Error));
        let opts = NotificationsOptions::builder()
            .reconnect_on_close(reconnect_on_close)
            .reconnect_on_error(reconnect_on_error)
            .build();

        let mut notifications_stream = ctx
            .client()
            .await?
            .create_notifications_stream_with_options(opts)?;
        trace!("notifications stream created");
        while let Some(item) = notifications_stream.next().await {
            println!("{item:?}");
            if let Ok(NotificationBatch { notifications, .. }) = item {
                println!("{}", notifications.len());
                for (key, value) in notifications {
                    println!("{key:?} {value:?}");
                }
            }
        }
        Ok(())
    }
}
