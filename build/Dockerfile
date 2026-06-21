# Global Scope ARGs
ARG CARGO_HOME=/root/.cargo
ARG RUSTUP_HOME=/root/.rustup
ARG HOME=/root
ARG RUST_VERSION=1.96.0
ARG RUST_VERSION_EXTERNAL_TYPES=nightly-2026-03-20
ARG GO_VERSION=1.26.4

# ===== 1. Pull Official Images =====
FROM rust:${RUST_VERSION} AS rust-official
FROM golang:${GO_VERSION} AS go-official

# ===== 2. Toolchain & Tool Preparer =====
FROM rust-official AS toolchain-builder
ARG RUST_VERSION_EXTERNAL_TYPES

RUN rustup component add clippy && \
    rustup toolchain install --profile minimal --allow-downgrade nightly && \
    rustup component add --toolchain nightly miri rust-src && \
    cargo +nightly miri setup --print-sysroot

RUN rustup toolchain install "${RUST_VERSION_EXTERNAL_TYPES}" --profile minimal && \
    cargo +"${RUST_VERSION_EXTERNAL_TYPES}" install cargo-check-external-types

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y cargo-deny cargo-nextest sccache

# ===== 3. OS Base Stage =====
FROM ubuntu:24.04 AS os-base
ARG CARGO_HOME
ARG RUSTUP_HOME
ARG RUST_VERSION_EXTERNAL_TYPES
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y \
    build-essential ca-certificates curl git git-restore-mtime gnupg jq libssl-dev \
    pkg-config protobuf-compiler python3 python3-git python3-pip unzip zstd \
    && curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /__w "${CARGO_HOME}" "${RUSTUP_HOME}"

# ===== 4. Go Builder =====
FROM os-base AS go-builder
COPY --from=go-official /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:$PATH

# Bind mount ONLY the specific folder containing the Go module/vendored code
RUN --mount=type=bind,source=ext/oxia,dst=/src \
    --mount=type=cache,target=/tmp/gocache \
    GOPROXY=off GOCACHE=/tmp/gocache \
    go -C /src/cmd build -mod=vendor -o /tmp/oxia

# ===== 5. Code Base Context Staging =====
FROM os-base AS source-context
ARG REPO_NAME=oxia-rust
ARG CARGO_HOME
ARG RUSTUP_HOME
ARG HOME
ARG RUSTFLAGS
ARG RUST_VERSION_EXTERNAL_TYPES

ENV CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    RUSTFLAGS="${RUSTFLAGS}" \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:/usr/local/go/bin:$PATH

COPY --from=toolchain-builder /usr/local/cargo ${CARGO_HOME}
COPY --from=toolchain-builder /usr/local/rustup ${RUSTUP_HOME}
COPY --from=go-official /usr/local/go /usr/local/go
COPY --from=go-builder /tmp/oxia ${CARGO_HOME}/bin/oxia

WORKDIR /__w/${REPO_NAME}/${REPO_NAME}
# Use a cache mount for the cargo registry so downloads persist across builds!
RUN --mount=type=bind,source=.,target=/__w/oxia-rust/oxia-rust,ro \
    cargo fetch --locked && cargo +nightly fetch --locked

# ===== 5a. Parallel Builder: CI Profile =====
FROM source-context AS build-ci
RUN --mount=type=bind,source=.,target=/__w/oxia-rust/oxia-rust \
    cargo build --profile ci --all-targets --features metrics --frozen && \
    cargo clippy --profile ci --no-deps --all-targets --features metrics --frozen -- -D warnings

# ===== 5b. Parallel Builder: CI-Debug Profile =====
FROM source-context AS build-ci-debug
RUN --mount=type=bind,source=.,target=/__w/oxia-rust/oxia-rust \
    cargo build --profile ci-debug --all-targets --features metrics --frozen && \
    cargo clippy --profile ci-debug --no-deps --all-targets --features metrics --frozen -- -D warnings

# ===== 5c. Parallel Builder: Miri Targets =====
FROM source-context AS build-miri
RUN --mount=type=bind,source=.,target=/__w/oxia-rust/oxia-rust \
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
    cargo +nightly miri test  --package mauricebarnum-oxia-client  --features metrics  --frozen --no-run

# ===== 6. Final Assembly (The Fan-In) =====
FROM os-base AS final
ARG CARGO_HOME
ARG RUSTUP_HOME
ARG HOME
ARG RUSTFLAGS
ARG RUST_VERSION_EXTERNAL_TYPES

COPY --from=source-context ${CARGO_HOME} ${CARGO_HOME}
COPY --from=source-context ${RUSTUP_HOME} ${RUSTUP_HOME}
COPY --from=source-context /usr/local/go /usr/local/go
COPY --from=source-context ${CARGO_HOME}/bin/oxia ${CARGO_HOME}/bin/oxia

ENV CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    RUSTFLAGS="${RUSTFLAGS}" \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    RUST_VERSION_EXTERNAL_TYPES="${RUST_VERSION_EXTERNAL_TYPES}" \
    XDG_CACHE_HOME="${HOME}/.cache" \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:/usr/local/go/bin:$PATH

COPY build/cargo-config.toml ${CARGO_HOME}/config.toml

# Pull the independent target profile directories back together into the unified cache layout

COPY --from=build-ci       ${HOME}/deps/target/ ${HOME}/deps/target/
COPY --from=build-ci-debug ${HOME}/deps/target/ ${HOME}/deps/target/
COPY --from=build-miri     ${HOME}/deps/target/ ${HOME}/deps/target/
COPY --from=build-miri     ${HOME}/.cache/miri/ ${HOME}/.cache/miri/

RUN git config --global --add safe.directory /__w/oxia-rust/oxia-rust

WORKDIR ${HOME}
