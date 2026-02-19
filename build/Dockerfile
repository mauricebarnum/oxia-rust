# TARGETARCH value will be set automatically

# Configuration, unlikely to change
ARG USER=runner
ARG HOME=/home/${USER}
ARG CARGO_HOME=${HOME}/.cargo
ARG RUSTUP_HOME=${HOME}/.rustup

# Versions
ARG RUST_VERSION=1.93.1
ARG GO_VERSION=1.26.0

# ===== Pull Official Images =====
FROM rust:${RUST_VERSION}-bookworm AS rust-official
FROM golang:${GO_VERSION}-bookworm AS go-official

# ===== Chef: shared base for planner and cook — guarantees same cargo-chef version =====
FROM rust-official AS chef
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y cargo-chef

# ===== Planner: generate cargo-chef recipe =====
# Copies only manifests so this layer (and the cook layer) are only invalidated
# when dependencies change, not when source files change.
FROM chef AS planner
WORKDIR /recipe
COPY Cargo.toml Cargo.lock ./
COPY common/Cargo.toml common/
COPY client/Cargo.toml client/
COPY cmd/Cargo.toml cmd/
COPY oxia-bin-util/Cargo.toml oxia-bin-util/
# cargo chef prepare uses `cargo metadata` which requires source files to exist
# for auto-discovered targets (anything without an explicit [lib]/[[bin]] section).
# Create empty stubs; cargo only checks existence at metadata time, not content.
RUN mkdir -p common/src client/src client/benches cmd/src oxia-bin-util/src \
    && touch common/src/lib.rs \
    client/src/lib.rs \
    client/benches/shard_lookup.rs \
    client/benches/channel_pool.rs \
    cmd/src/lib.rs cmd/src/main.rs \
    oxia-bin-util/src/lib.rs
RUN cargo chef prepare --recipe-path recipe.json

FROM ubuntu:24.04 AS build

# Re-declare ARGs needed in this stage
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME
ARG TARGETARCH

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ===== OS packages (long-lived layer), runs as root =====

RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    git \
    gnupg \
    jq \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    python3 \
    python3-pip \
    unzip \
    zstd \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# ===== Create non-root build user and switch =====
RUN useradd -ms /bin/bash ${USER}
USER ${USER}
WORKDIR /home/${USER}

# ===== Copy Rust from Official Image =====
# We copy the entire toolchain and cargo home
COPY --from=rust-official --chown=${USER}:${USER} /usr/local/cargo ${CARGO_HOME}
COPY --from=rust-official --chown=${USER}:${USER} /usr/local/rustup ${RUSTUP_HOME}

ENV \
    CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:$PATH

RUN rustup component add clippy
RUN rustup toolchain install --profile minimal --allow-downgrade nightly --component miri

COPY build/cargo-config.toml ${CARGO_HOME}/config.toml

# ===== Install cargo-check-external-types which requires a specific nightly, see
# ===== https://github.com/awslabs/cargo-check-external-types?tab=readme-ov-file#how-to-use
ARG CARGO_CHECK_EXTERNAL_TYPES_VERSION=nightly-2025-10-18
RUN rustup toolchain install ${CARGO_CHECK_EXTERNAL_TYPES_VERSION} --profile minimal && \
    cargo +${CARGO_CHECK_EXTERNAL_TYPES_VERSION} install cargo-check-external-types

# Copy cargo-chef from the shared chef stage (same version used by planner)
COPY --from=chef --chown=${USER}:${USER} /usr/local/cargo/bin/cargo-chef ${CARGO_HOME}/bin/cargo-chef

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y \
    cargo-deny \
    cargo-nextest \
    sccache

# ====== Cook dependencies with cargo-chef =====
# Pre-compile external dependencies into ${HOME}/deps/target so that CI runs
# (which bind-mount source at /src) find them via CARGO_TARGET_DIR and only
# need to compile workspace code.
# Two profiles are cooked so both debug and optimised CI builds hit the cache.
# Profile settings are read from ${CARGO_HOME}/config.toml (cargo-config.toml);
# they must match the workspace Cargo.toml profiles — see that file for details.
COPY --from=planner --chown=${USER}:${USER} /recipe/recipe.json ${HOME}/deps/recipe.json
WORKDIR ${HOME}/deps
# CARGO_INCREMENTAL=0 must match the final image ENV so that the metadata
# hash embedded in artifact filenames is identical between cook and CI.
# A mismatch causes cargo to ignore pre-built artifacts and recompile all deps.
RUN CARGO_INCREMENTAL=0 cargo chef cook --all-targets --recipe-path recipe.json && \
    CARGO_INCREMENTAL=0 cargo chef cook --profile ci --all-targets --recipe-path recipe.json
WORKDIR ${HOME}
ENV CARGO_TARGET_DIR=${HOME}/deps/target

# ===== Copy Go from Official Image =====
COPY --from=go-official /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:$PATH

# ====== Install oxia ======
# if this breaks, take a look at oxia-bin-util/build.rs
RUN --mount=type=bind,dst=/src GOPROXY=off GOCACHE=/tmp/gocache go -C /src/ext/oxia/cmd build -mod=vendor -o $HOME/go/bin/oxia && rm -rf "$GOCACHE"

# ===== ================== =====
# ===== Create final image =====
# ===== ================== =====

FROM scratch AS final

# Import defaults
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME

ENV \
    CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    CARGO_INCREMENTAL=0 \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:${HOME}/go/bin:/usr/local/go/bin:$PATH

COPY --from=build / /
