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
# Seed the minimal conventional stubs, then let cargo metadata tell us what else
# is missing — each iteration unblocks one file until metadata succeeds.
# This is self-maintaining: adding new examples/tests/benches never requires
# updating this Dockerfile.
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
# /__w is the GitHub Actions workspace root (bind-mounted at runtime as
# -v /home/runner/work:/__w). Create it here so the cook step can populate
# /__w/<repo>/<repo> with cargo-chef stubs; the real source overlays it at runtime.
RUN useradd -ms /bin/bash ${USER} && mkdir -p /__w && chown ${USER}:${USER} /__w
USER ${USER}
WORKDIR /home/${USER}

# ===== Copy Rust from Official Image =====
# We copy the entire toolchain and cargo home
COPY --from=rust-official --chown=${USER}:${USER} /usr/local/cargo ${CARGO_HOME}
COPY --from=rust-official --chown=${USER}:${USER} /usr/local/rustup ${RUSTUP_HOME}

ENV \
    CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    CARGO_TARGET_DIR=${HOME}/deps/target \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:$PATH

RUN rustup component add clippy
RUN rustup toolchain install --profile minimal --allow-downgrade nightly && \
    rustup component add --toolchain nightly miri rust-src

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
# find them via CARGO_TARGET_DIR and only need to compile workspace code.
# Two profiles are cooked so both PR and branch CI builds hit the cache.
#   ci-debug: PR builds — dev opt-levels, line-tables-only debug info
#   ci:       branch builds — release opt-levels, optimised
# Profile settings are read from ${CARGO_HOME}/config.toml (cargo-config.toml);
# they must match the workspace Cargo.toml profiles — see that file for details.
# CARGO_INCREMENTAL=0 must match the final image ENV so that the metadata
# hash embedded in artifact filenames is identical between cook and CI.
# A mismatch causes cargo to ignore pre-built artifacts and recompile all deps.
#
# WORKDIR must match the GitHub Actions workspace path (/__w/<repo>/<repo>).
# Cargo encodes the workspace root into build-script OUT_DIR hashes; if the
# cook runs from a different directory, build-dependency fingerprints will not
# match and the entire build-dep graph (prost-build, tonic-build, sha2, etc.)
# will be recompiled on every CI run despite being pre-baked here.
# The real source is bind-mounted over this directory at runtime; cargo-chef's
# stub files are shadowed and the pre-built artifacts in ${HOME}/deps/target
# remain intact.
#
# REPO_NAME is passed at build time so GH_WORKSPACE can be derived without
# needing .git inside the build context. Pass via:
#   docker build --build-arg REPO_NAME=$(basename $(git remote get-url origin) .git) ...
# The CI workflow derives it from $GITHUB_REPOSITORY automatically.
ARG REPO_NAME=oxia-rust
COPY --from=planner --chown=${USER}:${USER} /recipe/recipe.json /tmp/recipe.json
RUN GH_WORKSPACE="/__w/${REPO_NAME}/${REPO_NAME}" && \
    mkdir -p "${GH_WORKSPACE}" && \
    cp /tmp/recipe.json "${GH_WORKSPACE}/recipe.json" && \
    cd "${GH_WORKSPACE}" && \
    CARGO_INCREMENTAL=0 cargo chef cook --profile ci-debug --all-targets --recipe-path recipe.json && \
    CARGO_INCREMENTAL=0 cargo chef cook --profile ci --all-targets --recipe-path recipe.json

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
