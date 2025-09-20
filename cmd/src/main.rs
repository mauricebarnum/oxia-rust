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

use clap::Parser;
use clap::Subcommand;

mod commands;

use commands::CommandRunnable;
use commands::completions::CompletionsCommand;
use commands::delete::DeleteCommand;
use commands::get::GetCommand;
use commands::put::PutCommand;

#[derive(Parser)]
#[command(name = "mycli", version, about = "A Git/Cargo-style CLI example")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
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
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Completions(cmd) => cmd.run(),
        Commands::Put(cmd) => cmd.run(),
        Commands::Get(cmd) => cmd.run(),
        Commands::Delete(cmd) => cmd.run(),
    }
}
