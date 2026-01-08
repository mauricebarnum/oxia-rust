# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an experimental, incomplete Rust implementation of an [Oxia](https://github.com/oxia-db/oxia) client. Oxia is a scalable metadata store. The crates use "private" names (mauricebarnum-\*) and are unpublished.

## Build Commands

```bash
# Build all targets
cargo build --all-targets

# Run tests (uses cargo-nextest)
cargo nextest run

# Run a single test
cargo nextest run test_name

# Run tests for a specific package
cargo nextest run --package mauricebarnum-oxia-client

# Lint (strict - treats warnings as errors)
cargo clippy --no-deps --all-targets --all-features -- -D warnings

# Format
cargo fmt

# Format check
cargo fmt --check

# Dependency security/license check
cargo deny check

# Miri (undefined behavior detection) - client only
cargo +nightly miri nextest run --package mauricebarnum-oxia-client
```

CI uses `--release --frozen` flags. Local development typically omits these.

## Workspace Structure

- **common** (`mauricebarnum-oxia-common`): Low-level gRPC bindings generated from protobuf via tonic-prost-build
- **client** (`mauricebarnum-oxia-client`): Higher-level async client with sharding, dispatch, and ergonomic APIs
- **cmd** (`mauricebarnum-oxia-cmd`): CLI tool (`oxia-cmd`) with commands: get, put, delete, list, range-scan, range-delete, notifications, shell
- **oxia-bin-util**: Internal crate that builds the Go Oxia server binary for integration tests (sources vendored in `ext/oxia/`)

## Read-Only Directories

These directories should NEVER be modified:

- `ext/oxia/` - Mirror of a subset of <https://github.com/oxia-db/oxia> to build a standalone oxia server for testing. The source code is relevant for understanding how Oxia works

## Architecture Notes

### Client API Design

`Client` is a concrete struct (not a trait object) with generic parameters for flexibility:

```rust
pub async fn put(&self, key: impl Into<String>, value: impl Into<Bytes>) -> Result<PutResponse>
```

The `x` / `x_with_options` pattern provides zero-cost abstractions (e.g., `put()` and `put_with_options()`).

### Key Components

- **Sharding**: Uses XXHASH3 for key routing to shards
- **Channel pooling**: `pool.rs` manages gRPC connections per shard
- **Shard assignment**: `shard.rs` handles dynamic shard assignment from server
- **Notifications**: Streaming support for key change notifications

### Proto Compilation

The `common/build.rs` compiles protos from `ext/oxia/common/proto/`. Don't manually edit generated code.

### Integration Tests

Tests in `client/tests/` require the Oxia server binary, built automatically by `oxia-bin-util/build.rs` from vendored Go sources. The test harness (`client/tests/common.rs`) spawns servers and manages ports.

## Linting Configuration

Clippy is strict: all + pedantic lints are denied with exceptions for:

- `cast_possible_truncation`, `cast_precision_loss`
- `must_use_candidate`, `missing_errors_doc`

## New Files

Add Apache 2.0 license headers to new `.rs` files. Pre-commit hook handles this automatically using `.license-header.txt`.

## Github integration

- Always use the prefix `claude-` when creating a branch
- Use `--force-with-lease --force-if-includes` if a force push is required. Otherwise, fail and require a developer to do the push.
- Run clippy and tests before pushing
