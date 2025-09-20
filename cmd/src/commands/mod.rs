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

use async_trait::async_trait;
use clap::Subcommand;

mod completions;
mod delete;
mod delete_range;
mod get;
mod list;
mod notifications;
mod put;
mod range_scan;

pub use completions::CompletionsCommand;
pub use delete::DeleteCommand;
pub use delete_range::DeleteRangeCommand;
pub use get::GetCommand;
pub use list::ListCommand;
pub use notifications::NotificationsCommand;
pub use put::PutCommand;
pub use range_scan::RangeScanCommand;

#[async_trait]
pub trait CommandRunnable {
    async fn run(self, ctx: &mut crate::Context) -> anyhow::Result<()>;
}

#[derive(Subcommand)]
pub enum Commands {
    /// Generate shell completions
    Completions(CompletionsCommand),

    /// Put a key-value pair
    Put(PutCommand),

    /// Get one or more keys
    Get(GetCommand),

    /// Delete one or more keys
    Delete(DeleteCommand),

    /// Delete one or more keys
    DeleteRange(DeleteRangeCommand),

    /// List keys in range
    List(ListCommand),

    /// Retrieve notifications from the server(s)
    Notifications(NotificationsCommand),
    /// Get values in range
    RangeScan(RangeScanCommand),
}

#[async_trait]
impl CommandRunnable for Commands {
    async fn run(self, ctx: &mut crate::Context) -> anyhow::Result<()> {
        match self {
            Commands::Completions(cmd) => cmd.run(ctx).await,
            Commands::Delete(cmd) => cmd.run(ctx).await,
            Commands::DeleteRange(cmd) => cmd.run(ctx).await,
            Commands::Get(cmd) => cmd.run(ctx).await,
            Commands::List(cmd) => cmd.run(ctx).await,
            Commands::Notifications(cmd) => cmd.run(ctx).await,
            Commands::Put(cmd) => cmd.run(ctx).await,
            Commands::RangeScan(cmd) => cmd.run(ctx).await,
        }
    }
}

#[inline]
fn to_result<T, E>(r: std::result::Result<T, E>) -> anyhow::Result<()>
where
    E: Into<anyhow::Error>,
{
    r.map(|_| ()).map_err(Into::into)
}
