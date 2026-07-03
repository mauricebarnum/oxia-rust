# Global Scope ARGs
ARG CARGO_HOME=/root/.cargo
ARG RUSTUP_HOME=/root/.rustup
ARG HOME=/root
ARG RUST_VERSION=1.96.1
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

# ===== 4. Go Builder =====
FROM os-base AS go-builder
COPY --from=go-official /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:$PATH

COPY ext/oxia /src
RUN --mount=type=cache,target=/tmp/gocache \
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
    RUSTC_WRAPPER="sccache" \
    SCCACHE_DIR="${HOME}/.cache/sccache" \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:/usr/local/go/bin:$PATH

COPY --from=toolchain-builder /usr/local/cargo ${CARGO_HOME}
COPY --from=toolchain-builder /usr/local/rustup ${RUSTUP_HOME}
COPY --from=go-official /usr/local/go /usr/local/go
COPY --from=go-builder /tmp/oxia ${CARGO_HOME}/bin/oxia

WORKDIR /__w/${REPO_NAME}/${REPO_NAME}

COPY . .
RUN cargo fetch --locked

# ===== 5a. Parallel Builder: CI Profile =====
FROM source-context AS build-ci
# Added sharing=locked to serialize access to the same cache mount safely if built concurrently
RUN --mount=type=cache,target=/root/.cache/sccache,sharing=locked \
    cargo build --profile ci --all-targets --features metrics --frozen && \
    cargo clippy --profile ci --no-deps --all-targets --features metrics --frozen -- -D warnings

# ===== 5b. Parallel Builder: CI-Debug Profile =====
FROM source-context AS build-ci-debug
# Added sharing=locked to prevent compiler-cache race mutations
RUN --mount=type=cache,target=/root/.cache/sccache,sharing=locked \
    cargo build --profile ci-debug --all-targets --features metrics --frozen && \
    cargo clippy --profile ci-debug --no-deps --all-targets --features metrics --frozen -- -D warnings

# ===== 5c. Parallel Builder: Miri Targets =====
FROM source-context AS build-miri
RUN RUSTC_WRAPPER= \
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
    cargo +nightly miri nextest run --package mauricebarnum-oxia-client --features metrics --frozen --no-run

# Keep Miri dependency and sysroot warmup, but do not ship local workspace
# artifacts. They are path-sensitive and must be rebuilt in the validation
# checkout that runs cargo-miri. Let Cargo remove every artifact associated
# with workspace packages, including build-script outputs and fingerprints.
FROM build-miri AS build-miri-final-artifacts
RUN cargo +nightly clean \
    --target-dir "${CARGO_TARGET_DIR}/miri" \
    --target "$(rustc +nightly -vV | sed -n 's/^host: //p')" \
    --workspace

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
    RUSTC_WRAPPER="sccache" \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    MIRI_SYSROOT=${HOME}/.cache/miri \
    RUST_VERSION_EXTERNAL_TYPES="${RUST_VERSION_EXTERNAL_TYPES}" \
    XDG_CACHE_HOME="${HOME}/.cache" \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:/usr/local/go/bin:$PATH

COPY build/cargo-config.toml ${CARGO_HOME}/config.toml

# FIX: Isolated graph paths. Instead of importing the shared parent directory,
# pull isolated explicit leaf-node profile subdirectories to eliminate graph collisions.
COPY --from=build-ci       /root/deps/target/ci/       /root/deps/target/ci/
COPY --from=build-ci-debug /root/deps/target/ci-debug/ /root/deps/target/ci-debug/
COPY --from=build-miri-final-artifacts /root/deps/target/miri/ /root/deps/target/miri/
COPY --from=build-miri-final-artifacts /root/.cache/miri/      /root/.cache/miri/

WORKDIR ${HOME}
