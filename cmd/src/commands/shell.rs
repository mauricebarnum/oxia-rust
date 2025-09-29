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
use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use clap::error::ErrorKind;
use rustyline::error::ReadlineError;
use tracing::trace;

use super::CommandRunnable;
use super::Commands;
use crate::context::Context;

#[derive(Args, Debug)]
pub struct ShellCommand {}

#[derive(Subcommand)]
enum ShellCommands {
    Quit,
    Exit,
    #[command(flatten)]
    Command(Commands),
}

#[derive(Parser)]
#[command(name = "", no_binary_name = true)]
struct ShellParser {
    #[command(subcommand)]
    command: ShellCommands,
}

impl ShellCommand {
    async fn run_shell(ctx_arg: &mut Context) -> anyhow::Result<()> {
        let should_exit = |c: &mut Vec<Context>| -> bool {
            let result = c.is_empty();
            c.pop();
            result
        };

        let mut ctxts: Vec<Context> = Vec::new();

        let basic_prompt: &str = "oxia> ";

        let mut rl = rustyline::DefaultEditor::new()?;
        let mut prev: Option<String> = None;
        loop {
            let depth = ctxts.len();
            let prompt = if depth == 0 {
                basic_prompt.to_string()
            } else {
                format!("[{depth}] {basic_prompt}")
            };

            let r = if let Some(prev) = prev.take() {
                rl.readline_with_initial(&prompt, (&prev, ""))
            } else {
                rl.readline(&prompt)
            };

            match r {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    let Some(args) = shlex::split(line) else {
                        eprintln!("Invalid command syntax: {line}");
                        prev = Some(line.to_string());
                        continue;
                    };

                    match ShellParser::try_parse_from(args) {
                        Ok(cli) => match cli.command {
                            ShellCommands::Exit | ShellCommands::Quit => {
                                if should_exit(&mut ctxts) {
                                    return Ok(());
                                }
                            }

                            ShellCommands::Command(Commands::Shell(_)) => {
                                let ctx = ctxts.last_mut().unwrap_or(ctx_arg).make_subcontext();
                                ctxts.push(ctx);
                            }

                            ShellCommands::Command(cmd) => {
                                let _ = rl.add_history_entry(line);
                                // let nctx = ctxts.len();
                                let ctx = ctxts.last_mut().unwrap_or(ctx_arg);
                                if let Err(e) = cmd.run(ctx).await {
                                    eprintln!("{e}");
                                }
                            }
                        },
                        Err(e) => {
                            trace!(?e);
                            match e.kind() {
                                ErrorKind::InvalidSubcommand => {
                                    ShellParser::command().print_long_help().unwrap();
                                }
                                ErrorKind::DisplayHelp => println!("{e}"),
                                _ => {
                                    prev = Some(line.to_string() + " ");
                                    eprintln!("{e}");
                                }
                            }
                        }
                    }
                }
                Err(ReadlineError::Eof) => {
                    if should_exit(&mut ctxts) {
                        return Ok(());
                    }
                }
                Err(ReadlineError::Interrupted) => println!("^C (Use 'exit' or 'quit' to exit"),
                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[async_trait::async_trait]
impl CommandRunnable for ShellCommand {
    async fn run(self, ctx: &mut crate::Context) -> anyhow::Result<()> {
        trace!(?self, ?ctx, "params");
        Self::run_shell(ctx).await
    }
}
