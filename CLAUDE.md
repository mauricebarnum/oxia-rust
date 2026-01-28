# CLAUDE.md - Project Guidance for Claude Code

This CLAUDE.md guides Claude Code (`claude`) for this experimental Rust Oxia client repo. Follow strictly for safe, efficient collaboration. TITLE CLAUDE.md [file:43]

## Safety & Workflow Rules (New)

- **Branches**: ALWAYS prefix `claude-` (e.g., `claude-feature`). Checkout new feature branch before edits: `git checkout -b claude-task-name`. Never touch `main`/`origin/main`. [file:43]
- **Isolation**: Write to `./claude-temp/` first (create if needed: `mkdir claude-temp`). Propose diffs with `/diff`; confirm before main files. Read via `@file.rs` on-demand. [web:35]
- **Git Safety**: Pre-session: `git stash push -m "pre-claude"`. Stash changes: `git stash push`. No commits/pushes without `/confirm`. Post: `git stash pop` selectively. Block destructives via hooks. [web:33][conversation_history:24]
- **Tokens**: Use `/rewind` (Esc Esc) for undos. Limit tools (`ENABLE_TOOL_SEARCH=auto:5%`). Test incrementally; summarize with `/compact`. Monitor `/cost`. [web:10]
- **Read-Only**: NEVER modify `./extoxia/` (vendored Oxia mirror). [file:43]

## Project Overview

Experimental Rust Oxia client: sharding, gRPC, notifications. Workspace: common (bindings), client (async API), cmd (CLI `oxia-cmd`), oxia-bin-util (server binary). [file:43]

## Build & Test Commands

| Task     | Command                                                    | Notes                       |
| -------- | ---------------------------------------------------------- | --------------------------- |
| Build    | `cargo build --all-targets`                                | All targets                 |
| Tests    | `cargo nextest run`                                        | Full; or `--package client` |
| Lint     | `cargo clippy --all-targets --all-features -- -D warnings` | Strict                      |
| Format   | `cargo fmt` / `--check`                                    | -                           |
| Security | `cargo deny check`                                         | Licenses                    |
| Miri     | `+nightly cargo miri nextest run --package client`         | UB detection [file:43]      |

CI uses `--release --frozen`. Local: omit.

## Workspace Structure

- `common`: gRPC protos (tonic-prost-build).
- `client`: Async client (sharding, dispatch).
- `cmd`: `oxia-cmd` CLI (get/put/delete/list/range/notifications/shell).
- `oxia-bin-util`: Builds vendored Go Oxia server for tests. [file:43]

## Key Architecture

- **API**: `Client` struct; `put(key, value)` + `with_options()`. Zero-cost abstractions.
- **Sharding**: XXHASH3 routing; dynamic via `shard.rs`.
- **Pooling**: `pool.rs` per-shard gRPC; `ArcSwap` caches.
- **Notifications**: Streaming key changes.
- **Protos**: Auto from `./extoxia/common/proto`. No manual edits.
- **Tests**: Spawn servers via `client/tests/common.rs`. [file:43]

**Concurrency** (Hot Path: Lock-free):

1. `ArcSwap::load` shards/clients.
2. `RwLock::read` pool (misses).
   Guidelines: arcswap > RwLock; tokio-sync; no locks pre-`.await`. [file:43]

## Coding Style

- Clippy: All lints (exceptions documented).
- Docs: Terse; Rust API guidelines; `const fn`.
- Github: Draft PRs; `--force-with-lease`; tests/clippy pre-push. Ignore local `main`. [file:43]

## Efficiency Prompts

- "Plan in `./claude-temp/`; diff before apply."
- "Verify with tests; /cost check."
