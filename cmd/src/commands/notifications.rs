// Copyright 2025 Maurice S. Barnum
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
use futures::stream::StreamExt;
use mauricebarnum_oxia_client::NotificationBatch;
use tracing::trace;

use super::CommandRunnable;

#[derive(Args, Debug)]
pub struct NotificationsCommand {}

#[async_trait::async_trait]
impl CommandRunnable for NotificationsCommand {
    async fn run(self, ctx: crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        let result = ctx.client().await?.create_notifications_stream();
        trace!(?result, "result");

        let mut notifications_stream = result?;
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
