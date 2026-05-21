# Oxia Rust Client - Project Memory

This document serves as the project memory for agent collaboration on the experimental Rust Oxia client repository. It consolidates information from the project README, agent instructions, security audit, and local agent session history.

## Project Overview & Current State
- **Purpose**: An experimental, incomplete Rust client implementation for [Oxia](https://github.com/oxia-db/oxia) to explore Rust and the Oxia ecosystem.
- **Crates Status**: Private, unpublished crates (`mauricebarnum-oxia-common`, `mauricebarnum-oxia-client`, `mauricebarnum-oxia-cmd`).
- **Code Maturity**: In development, subject to rapid change, minimal testing.
- **Git Workflow Constraints**:
  - ALWAYS prefix branches with `claude-` (e.g., `claude-feature`). Checkout a branch before editing: `git checkout -b claude-task-name`. Never commit or push directly to `main` or `origin/main`.
  - For isolation, test changes in `./claude-temp/` and propose diffs using `/diff` before writing to main files.
  - Stash changes using `git stash push -m "pre-claude"` at the start of a session and `git stash push` during development. Do not commit or push without explicit confirmation.
  - Annotate all commits created with AI assistance (e.g., including a line `Created with assistance from <AssistantName>` in the commit message body).
  - NEVER modify `./ext/oxia/` as it contains the upstream Go Oxia server mirror.

## Architecture Layout & Crate Structure
The repository is structured as a Cargo workspace containing the following crates:

1. **`common` (`mauricebarnum-oxia-common`)**:
   - Low-level gRPC client bindings generated using the `tonic` and `prost` build framework (`tonic-prost-build`).
   - Protobuf files are automatically read from the vendored Go server at `./ext/oxia/common/proto/client.proto`. Manual editing of protobufs or generated bindings is strictly forbidden.
2. **`client` (`mauricebarnum-oxia-client`)**:
   - Higher-level, ergonomic async client API wrapper.
   - Handles sharding, dispatch logic, and connection pooling while minimizing exposure of internal `tonic` structures or the async runtime.
   - Implements a concrete type `Client` rather than a trait interface (zero-cost abstraction) with generic `x` and `x_with_options` APIs.
3. **`cmd` (`mauricebarnum-oxia-cmd`)**:
   - The CLI client utility `oxia-cmd`.
   - Supports: `get`, `put`, `delete`, `list` (keys), `range` (scan/delete), and `notifications` commands.
4. **`oxia-bin-util`**:
   - Build utility that compiles the vendored Go Oxia server source code to provide a test server binary located at `target/oxia/oxia`.

### Core Architectural Patterns
- **Concurrency & State Cache**:
  - The hot path is lock-free. Use `ArcSwap::load` to retrieve active shard configuration and client connections.
  - Use `RwLock::read` on the pool only for connection cache misses.
  - General Rule: `ArcSwap` > `RwLock`. Use `tokio::sync` primitives. Do not hold locks across `.await` points.
- **Async Interfaces**:
  - Use `BoxFuture` desugaring for async traits (see `notification.rs`, `address.rs`) instead of the `async-trait` crate.
- **Coding Style**:
  - Strict linting is enforced via workspace Cargo.toml settings (pedantic, nursery, `clone_on_ref_ptr` warnings).
  - Unmerged `use` statements (one import per line) are preferred. No merged imports like `use std::{foo, bar};`.
  - When exposing new types in a public module, update `allowed_external_types` in `client/Cargo.toml`.

## Security Model & Trust Boundaries
A recent security audit identified key attack surfaces and vulnerability vectors:
- **Public gRPC API (`public_rpc_server.go`)**: Primary trust boundary for client requests, keys, values, and session management.
- **Internal Coordination API (`internal_rpc_server.go`)**: Critical boundary for replication, cluster health, and heartbeats.
- **CLI Arguments (`cmd/src/main.rs`)**: Trust boundary for configuration addresses and timeout parameters.
- **Service Discovery (`discovery.rs`)**: Boundary for processing untrusted network/node metadata received from the coordinator.

### Security Vulnerabilities & Remediation Status
1. **[MEDIUM] Authentication Bypass via Authority Spoofing** (`ext/oxia/oxiad/dataserver/public_rpc_server.go`):
   - *Risk*: `validateAuthority` uses `:authority` header which is easily spoofed.
   - *Remediation*: Require mTLS client verification or OIDC tokens for sensitive APIs.
2. **[MEDIUM] Denial of Service via HTTP/2 Infinite Loop** (`ext/oxia/cmd/go.mod`):
   - *Risk*: Dependency on `golang.org/x/net@v0.48.0` is vulnerable to `GO-2026-4918`.
   - *Remediation*: Upgrade `golang.org/x/net` to `v0.53.0` or later.
3. **[LOW] Panic in Heartbeat Background Task** (`client/src/shard.rs`):
   - *Risk*: `getrandom::fill(&mut seed).unwrap()` panicking under entropy exhaustion.
   - *Status*: **FIXED**. Proper error handling and fallback/retry logic implemented.
4. **[LOW] Insecure URL Construction for gRPC Clients** (`client/src/lib.rs`):
   - *Risk*: Directly concatenating `"http://"` with dynamic discovery address.
   - *Status*: **FIXED**. Dest URL validation implemented.
5. **[INFORMATIONAL] Non-Cryptographic Randomness for Coordinator Logic**:
   - *Risk*: Coordinator uses predictable `math/rand` for leader selection and load balancing.

## Outstanding Technical Constraints & Lessons Learned
Based on workspace configurations and local agent memory:
- **Oxia Cluster Readiness**:
  - The `oxia-readiness` gRPC health service transitions to `SERVING` as soon as it receives its *first* shard assignment. However, leader election may still be ongoing.
  - **Constraint**: A sleep of ~2s must be added after health probes settle before launching tests to ensure elections are completed.
- **Process Management**:
  - Always call `.wait()` on child processes after killing them (`child.kill().await`) to clean up zombie processes and release ports.
- **Nextest Filtering**:
  - Filter by test name using `test(foo::)`.
  - Filter by test file/binary using `binary(foo)`.
- **Clippy Type Cascades**:
  - Changing return types (e.g., `io::Result<()>` to `()`) can cascade to callers, especially concerning drop implementations. Watch out for `let _ =` patterns in drop implementations.
- **Shell Command Limitations**:
  - Fish shell does not support bash-style heredocs `$(cat <<'EOF')`. When committing or writing files, use `printf '...' > file && git commit -F file` instead.
- **Future Crates & CLI Enhancements (TODOs)**:
  - Add client robustness for transient errors: do not fail assignment processing on shard connection failure; implement reconnection logic.
  - Implement pretty formatting (JSON, raw, tabular) for `oxia-cmd`.
  - Add `shell` command with tab completion, as well as `create`/`stop`/`list` control commands.
  - Support secondary index write in the `put` command.
