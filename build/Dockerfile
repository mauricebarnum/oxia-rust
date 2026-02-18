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

FROM ubuntu:24.04 AS build

# Re-declare ARGs needed in this stage
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME
ARG TARGETARCH
ARG SCCACHE_VERSION
ARG NEXTEST_VERSION

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Import defaults
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME

ARG TARGETARCH

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

ENV \
    CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    PATH=${RUSTUP_HOME}/bin:${CARGO_HOME}/bin:/home/${USER}/go/bin:$PATH

# ===== Copy Rust from Official Image =====
# We copy the entire toolchain and cargo home
COPY --from=rust-official --chown=${USER}:${USER} /usr/local/cargo ${CARGO_HOME}
COPY --from=rust-official --chown=${USER}:${USER} /usr/local/rustup ${RUSTUP_HOME}

# build time env: also add to final image
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

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y \
    cargo-deny \
    cargo-nextest \
    sccache

# ====== Fetch current dependencies =====
RUN --mount=type=bind,dst=/src cargo fetch --locked --manifest-path=/src/Cargo.toml

# ====== Prepopulate dependency caches =====
# Fetch downloads .crate files to $CARGO_HOME/registry/cache
# The build step extracts them to $CARGO_HOME/registry/src and clones git deps to $CARGO_HOME/git
# We use || true because the build will fail without all project files, but caches are populated
# Prepopulate both dev and release profiles since CI uses both
RUN --mount=type=bind,dst=/src \
    cd /src && \
    cargo fetch --locked && \
    (cargo check --frozen --all-targets 2>/dev/null || true) && \
    (cargo check --frozen --release --all-targets 2>/dev/null || true)

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
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:${HOME}/go/bin:/usr/local/go/bin:$PATH

COPY --from=build / /
