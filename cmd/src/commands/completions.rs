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

use std::io;

use clap::Args;
use clap::CommandFactory;
use clap_complete::generate;
use clap_complete::shells;

use super::CommandRunnable;
use crate::Cli;

#[derive(Args)]
pub struct CompletionsCommand {
    /// Shell to generate completions for (bash, zsh, fish, powershell, elvish)
    pub shell: String,
}

#[async_trait::async_trait]
impl CommandRunnable for CompletionsCommand {
    async fn run(self, _: crate::Context) -> anyhow::Result<()> {
        let mut cmd = Cli::command();
        let prog_name = cmd.get_name().to_string();

        match self.shell.as_str() {
            "bash" => generate(shells::Bash, &mut cmd, prog_name, &mut io::stdout()),
            "zsh" => generate(shells::Zsh, &mut cmd, prog_name, &mut io::stdout()),
            "fish" => generate(shells::Fish, &mut cmd, prog_name, &mut io::stdout()),
            "powershell" => generate(shells::PowerShell, &mut cmd, prog_name, &mut io::stdout()),
            "elvish" => generate(shells::Elvish, &mut cmd, prog_name, &mut io::stdout()),
            other => eprintln!("Unsupported shell: {other}"),
        }

        Ok(())
    }
}
