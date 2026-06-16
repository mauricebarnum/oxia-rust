# AGENTS.md – Project Guidance for AI Coding Assistants

This guide outlines rules, architecture, testing frameworks, and style constraints for AI coding assistants working in this experimental Rust Oxia client repository. Follow strictly for safe and efficient collaboration.

## Safety & Workflow Rules

- **Branches**: ALWAYS work on a dedicated task branch. Check out a new branch before making edits: `git checkout -b <prefix>-<task-name>`. Never modify or push directly to `main` or `origin/main`.
- **Isolation**: For major changes, draft code in a temporary directory (e.g., `./agent-temp/` or similar) first, review diffs, and confirm before writing to main repository files.
- **Git Safety**: Stash changes when appropriate to avoid conflicts. No raw commits or pushes without checking compatibility and verifying changes. Do not run destructive git operations.
- **Commit Annotations**: All commits created with assistance from an AI agent must include an annotation in the commit message body indicating AI assistance (e.g., `Created with assistance from <AssistantName>`).
- **Read-Only Paths**: NEVER modify `./ext/oxia/` (vendored Oxia mirror).
- **Work in Progress**: Always consider unmerged commits on the current branch. Run `git status` or check diffs relative to upstream tracking branches at the start of a session.

## Project Overview

Experimental Rust Oxia client with sharding, gRPC support, and notifications.

- Workspace crates: `common` (bindings), `client` (async API), `cmd` (CLI `oxia-cmd`), `oxia-bin-util` (server binary compiler).

## Build & Test Commands

| Task      | Command                                            | Notes                       |
| --------- | -------------------------------------------------- | --------------------------- |
| Build     | `cargo build --all-targets`                        | All targets                 |
| Tests     | `cargo nextest run`                                | Full; or `--package client` |
| Unit only | `cargo nextest run -E 'kind(lib)'`                 | Sandbox-safe; no sockets    |
| Lint      | `just lint`                                        | Strict                      |
| Format    | `just fmt`                                         | -                           |
| Security  | `cargo deny check`                                 | Licenses                    |
| Miri      | `cargo +nightly miri nextest run --package client` | UB detection                |

CI uses `--release --frozen`. Local commands can omit those flags.

## Workspace Structure

- `common`: gRPC protobuf bindings (`tonic-prost-build`).
- `client`: Async client with sharding and dispatch.
- `cmd`: CLI utility (`oxia-cmd`) implementing `get`, `put`, `delete`, `list`, `range`, `notifications`, and `shell`.
- `oxia-bin-util`: Builds vendored Go Oxia server for testing. Output binary is at `target/oxia/oxia` after build.

## Key Architecture

- **API**: `Client` is a concrete struct, not a trait object, with `put(key, value)` and `put_with_options(...)`. Uses zero-cost abstractions.
- **Sharding**: XXHASH3 routing; dynamic via `shard.rs`.
- **Pooling**: `pool.rs` per-shard gRPC; `ArcSwap` caches.
- **Retry/Timeouts**: `lib.rs::execute_with_retry` owns client-level retry orchestration. Keep the first attempt explicit, preserve stale-shard-map retry handling, and use `util::with_retry_timeout` for transient retries. Use `Client::execute_on_shard` for per-shard client operations instead of open-coding retry wrappers in public method internals.
- **Notifications**: Streaming key changes.
- **Protos**: Automatically generated from `./ext/oxia/common/proto`. No manual edits are allowed to generated files or protos.
- **Tests**: Spawn local servers via `client/tests/common.rs`.

## Security & Trust Boundaries

- **Public gRPC API**: Primary entry point for clients (`public_rpc_server.go`). Trust boundary for keys, values, and session management.
- **Internal Coordination API**: Server-to-server communication (`internal_rpc_server.go`). Critical for cluster integrity (replication, heartbeats).
- **CLI Arguments**: Entry point for user-supplied configuration (`cmd/src/main.rs`). Boundary for service addresses and timeouts.
- **Service Discovery**: The client receives node lists from the coordinator (`discovery.rs`). Boundary for potentially untrusted network metadata.
- **CI/CD Workflows**: Reusable workflows (`rust-build.yml`). Trust boundary for input-driven command execution.

## Concurrency & Synchronization (Hot Path: Lock-free)

1. `ArcSwap::load` to look up active shards/clients.
2. `RwLock::read` pool (only on misses).
   Guidelines: `ArcSwap` preferred over `RwLock`. Use `tokio::sync` primitives. Do not hold locks across `.await` points.

- **Async Traits**: Use `BoxFuture` desugaring (see `notification.rs`, `address.rs`), not the `async-trait` crate.

## Coding Style

- **Clippy**: Strict workspace Cargo.toml settings must be followed.
- **Imports**: Prefer unmerged `use` statements (one item per line). Do not merge imports (e.g. avoid `use std::{foo, bar};`).
- **Docs**: Terse, idiomatic Rust API guidelines; prefer `const fn` when possible.
- **Formatting**: Use `just fmt`, or run `rustfmt` with nightly; stable `cargo fmt` does not honor the repository's unstable import formatting settings.
- **Commit messages**: Wrap descriptive text at 72 characters
- **GitHub**: Submit changes via Draft PRs; push with `--force-with-lease`; always verify clippy and tests locally before pushing. Ignore local `main`.

## Dependencies

- Include minimal features when adding or managing dependencies.
- Do not add dependencies rejected by `cargo deny`.
- When making a module `pub`, update `allowed_external_types` in `client/Cargo.toml` for any newly exposed external types.
