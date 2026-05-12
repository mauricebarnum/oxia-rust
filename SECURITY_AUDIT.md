# Security Audit Report

## Executive Summary
The security audit of the `oxia-rust` and vendored `oxia` Go codebase identified **5 prioritized findings** ranging from Medium to Informational severity. The overall risk is assessed as **Moderate**, primarily due to potential authentication bypass vectors in the Go server and dependency vulnerabilities. The Rust client is generally robust but exhibits patterns of over-reliance on panics (`unwrap`/`expect`) in background tasks which could impact availability.

| Severity | Count |
| :--- | :--- |
| Critical | 0 |
| High | 0 |
| Medium | 2 |
| Low | 2 |
| Informational | 1 |

---

## Dependency Scan Output

### Rust (`cargo audit`)
```text
    Scanning Cargo.lock for vulnerabilities (243 crate dependencies)
    Success: No vulnerabilities found
```

### Go (`govulncheck`)
```text
Vulnerability #1: GO-2026-4918
    Infinite loop in HTTP/2 transport when given bad SETTINGS_MAX_FRAME_SIZE in
    net/http/internal/http2 in golang.org/x/net
  More info: https://pkg.go.dev/vuln/GO-2026-4918
  Module: golang.org/x/net
    Found in: golang.org/x/net@v0.48.0
    Fixed in: golang.org/x/net@v0.53.0
```

---

## Findings

### [MEDIUM] Authentication Bypass via Authority Spoofing
- **File:** `ext/oxia/oxiad/dataserver/public_rpc_server.go:92-101`
- **Category:** Broken Access Control
- **Description:** The `validateAuthority` method relies on the `:authority` pseudo-header (retrieved via `oxiadcommonrpc.GetAuthority`) to authorize requests. This header can often be manipulated by clients or improperly handled by transparent proxies/load balancers, potentially allowing a client to bypass authority-based restrictions if they can match an entry in the `assignmentDispatcher`'s authority list.
- **Exploit scenario:** An attacker connects to the Oxia server and provides a spoofed `:authority` header that matches a legitimate coordinator or shard authority, potentially gaining unauthorized access to internal RPC methods if TLS client authentication is not strictly enforced in tandem.
- **Remediation:** Do not rely solely on the `:authority` header for security decisions. Use mutual TLS (mTLS) with client certificate verification and OIDC tokens for all sensitive operations.

### [MEDIUM] Denial of Service via HTTP/2 Infinite Loop (Dependency)
- **File:** `ext/oxia/cmd/go.mod`
- **Category:** Dependency Vulnerability
- **Description:** The project depends on `golang.org/x/net@v0.48.0`, which is vulnerable to `GO-2026-4918`. A malicious peer can trigger an infinite loop in the HTTP/2 transport by sending a malformed `SETTINGS_MAX_FRAME_SIZE` frame.
- **Exploit scenario:** A malicious client or server sends a crafted HTTP/2 SETTINGS frame, causing the Oxia component to hang and consume 100% CPU.
- **Remediation:** Update `golang.org/x/net` to `v0.53.0` or later.

### [LOW] Availability Risk: Panic in Heartbeat Background Task
- **File:** `client/src/shard.rs:483-484`
- **Category:** Panics in production paths
- **Description:** The `start_heartbeat` task uses `getrandom::fill(&mut seed).unwrap()`. If the system entropy source is temporarily unavailable or restricted (e.g., in a heavily restricted container or during system exhaustion), the background heartbeat task will panic, leading to session loss and potential client instability.
- **Exploit scenario:** Under extreme system resource exhaustion, the heartbeat thread panics, causing the client to lose its Oxia session and abort all ephemeral keys.
- **Remediation:** Replace `unwrap()` with proper error handling that retries or logs a warning before gracefully degrading.

### [LOW] Insecure URL Construction for gRPC Clients
- **File:** `client/src/lib.rs:658-669`
- **Category:** Input Validation
- **Description:** The client constructs gRPC connection URLs by directly concatenating `"http://"` with a `dest` string received from the shard assignment map. There is no validation or sanitization of the `dest` string before it is passed to the gRPC connector.
- **Exploit scenario:** If the Oxia coordinator is compromised or a "Man-in-the-Middle" attacker can inject a malicious shard assignment, they could provide a `dest` string like `user:pass@malicious-host:6648`, potentially leading to credential leakage or connection to unauthorized endpoints.
- **Remediation:** Validate that the `dest` string matches an expected host:port format and does not contain unexpected URI components (like userinfo or path fragments).

### [INFORMATIONAL] Use of Non-Cryptographic Randomness for Logic
- **File:** `ext/oxia/oxiad/coordinator/selector/leader/lowerest_leaders_selector.go`
- **Category:** Insecure use of math/rand
- **Description:** The coordinator uses `math/rand` for leader selection and other distribution logic. While not directly a security vulnerability if used only for load balancing, it makes the system's behavior predictable.
- **Exploit scenario:** An attacker predicts the next leader selection sequence to target specific nodes for a coordinated attack.
- **Remediation:** Use `crypto/rand` for any logic that requires unpredictability, or at least seed `math/rand` with a high-entropy source.

---

## Attack Surface Map
- **Public gRPC API (`public_rpc_server.go`)**: Primary entry point for clients. Trust boundary for keys, values, and session management.
- **Internal Coordination API (`internal_rpc_server.go`)**: Used for server-to-server communication (replication, heartbeats). Critical boundary for cluster integrity.
- **CLI Arguments (`cmd/src/main.rs`)**: Trust boundary for configuration (service addresses, timeouts).
- **Service Discovery (`discovery.rs`)**: Boundary where the client receives potentially untrusted node lists from the coordinator.

---

## Notes & Assumptions
- **Vendored Code**: The audit included `ext/oxia/` as it is a critical part of the system's runtime behavior, even though it is technically a dependency.
- **Unsafe Code**: Suspect `unsafe` blocks in examples (`a.rs`, `e.rs`) were excluded as they are not part of the core library production path.
- **FFI**: No significant FFI boundaries were identified between Rust and Go; they communicate via gRPC (IPC).

---
*Audit completed on May 12, 2026.*
