# Global Scope ARGs
ARG USER=runner
ARG HOME=/home/${USER}
ARG CARGO_HOME=${HOME}/.cargo
ARG RUSTUP_HOME=${HOME}/.rustup
ARG RUST_VERSION=1.95.0
ARG GO_VERSION=1.26.2

# ===== 1. Pull Official Images =====
FROM rust:${RUST_VERSION} AS rust-official
FROM golang:${GO_VERSION} AS go-official

# ===== 2. Toolchain & Tool Preparer =====
FROM rust-official AS toolchain-builder
# Do NOT override RUSTUP_HOME/CARGO_HOME here; use image defaults (/usr/local)
# to ensure the pre-installed stable toolchain is found by rustup.

RUN rustup component add clippy && \
    rustup toolchain install --profile minimal --allow-downgrade nightly && \
    rustup component add --toolchain nightly miri rust-src && \
    # Pre-build the miri sysroot in the default location
    cargo +nightly miri setup

ARG CARGO_CHECK_EXTERNAL_TYPES_VERSION=nightly-2025-10-18
RUN rustup toolchain install ${CARGO_CHECK_EXTERNAL_TYPES_VERSION} --profile minimal && \
    cargo +${CARGO_CHECK_EXTERNAL_TYPES_VERSION} install cargo-check-external-types

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y cargo-deny cargo-nextest sccache

# ===== 3. Chef & Planner =====
FROM rust-official AS chef
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y cargo-chef

FROM chef AS planner
WORKDIR /recipe
COPY Cargo.toml Cargo.lock ./
COPY common/Cargo.toml common/
COPY client/Cargo.toml client/
COPY cmd/Cargo.toml cmd/
COPY oxia-bin-util/Cargo.toml oxia-bin-util/

RUN for toml in */Cargo.toml; do \
    dir="${toml%/Cargo.toml}"; \
    mkdir -p "$dir/src"; \
    touch "$dir/src/lib.rs" "$dir/src/main.rs"; \
    done \
    && while ! cargo metadata --no-deps --format-version 1 >/dev/null 2>/tmp/meta-err; do \
    path=$(sed -n "s/.*failed to read \`\([^\`]*\)\`.*/\1/p" /tmp/meta-err | head -1); \
    [ -n "$path" ] || { cat /tmp/meta-err >&2; exit 1; }; \
    mkdir -p "$(dirname "$path")" && touch "$path"; \
    done \
    && cargo chef prepare --recipe-path recipe.json

# ===== 4. OS Base =====
FROM ubuntu:24.04 AS os-base
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && apt-get install -y \
    build-essential ca-certificates curl git gnupg jq libssl-dev \
    pkg-config protobuf-compiler python3 python3-pip unzip zstd \
    && curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash ${USER} && \
    mkdir -p /__w "${CARGO_HOME}" "${RUSTUP_HOME}" && \
    chown -R ${USER}:${USER} /__w "${CARGO_HOME}" "${RUSTUP_HOME}"

# ===== 5. Rust Cooker =====
FROM os-base AS rust-cooker
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME
ARG REPO_NAME=oxia-rust
USER ${USER}
WORKDIR ${HOME}

# MIGRATION: Copy from default image locations into runner's custom home
COPY --from=toolchain-builder --chown=${USER}:${USER} /usr/local/cargo ${CARGO_HOME}
COPY --from=toolchain-builder --chown=${USER}:${USER} /usr/local/rustup ${RUSTUP_HOME}
COPY --from=chef --chown=${USER}:${USER} /usr/local/cargo/bin/cargo-chef ${CARGO_HOME}/bin/cargo-chef

ENV CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:$PATH

COPY build/cargo-config.toml ${CARGO_HOME}/config.toml

COPY --from=planner --chown=${USER}:${USER} /recipe/recipe.json /tmp/recipe.json
RUN GH_WORKSPACE="/__w/${REPO_NAME}/${REPO_NAME}" && \
    mkdir -p "${GH_WORKSPACE}" && \
    cp /tmp/recipe.json "${GH_WORKSPACE}/recipe.json" && \
    cd "${GH_WORKSPACE}" && \
    CARGO_INCREMENTAL=0 cargo chef cook --profile ci-debug --all-targets --recipe-path recipe.json && \
    CARGO_INCREMENTAL=0 cargo chef cook --profile ci --all-targets --recipe-path recipe.json && \
    cargo +nightly fetch --locked

# ===== 6. Go Builder =====
FROM os-base AS go-builder
COPY --from=go-official /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:$PATH
RUN --mount=type=bind,dst=/src GOPROXY=off GOCACHE=/tmp/gocache \
    go -C /src/ext/oxia/cmd build -mod=vendor -o /tmp/oxia && rm -rf "$GOCACHE"

# ===== 7. Final Assembly =====
FROM os-base AS final
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME

# Redefine ENV for runtime persistence
ENV USER=${USER} \
    HOME=${HOME} \
    CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    CARGO_INCREMENTAL=0 \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Ensure ownership and artifacts (including Miri sysroot) are transferred
COPY --from=rust-cooker --chown=${USER}:${USER} ${CARGO_HOME} ${CARGO_HOME}
COPY --from=rust-cooker --chown=${USER}:${USER} ${RUSTUP_HOME} ${RUSTUP_HOME}
COPY --from=rust-cooker --chown=${USER}:${USER} ${HOME}/deps ${HOME}/deps
COPY --from=go-official /usr/local/go /usr/local/go
COPY --from=go-builder /tmp/oxia ${CARGO_HOME}/bin/oxia

USER ${USER}
WORKDIR ${HOME}
