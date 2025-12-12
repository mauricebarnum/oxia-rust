# TARGETARCH value will be set automatically

# Configuration, unlikely to change
ARG USER=runner
ARG HOME=/home/${USER}
ARG CARGO_HOME=${HOME}/.cargo
ARG RUSTUP_HOME=${HOME}/.rustup

FROM ubuntu:24.04 AS build

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Import defaults
ARG USER
ARG HOME
ARG CARGO_HOME
ARG RUSTUP_HOME

# Versions
ARG RUST_VERSION=1.92.0
ARG GO_VERSION=go1.25.5
ARG SCCACHE_VERSION=v0.12.0
ARG NEXTEST_VERSION=0.9.114

ARG TARGETARCH
ARG GO_ARCH=${TARGETARCH}

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

# ===== Install Rust =====

# build time env: also add to final image
ENV \
    CARGO_HOME=${CARGO_HOME} \
    RUSTUP_HOME=${RUSTUP_HOME} \
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain ${RUST_VERSION} --profile minimal --component clippy -y

# ===== Install miri =====
RUN rustup toolchain install --profile minimal --allow-downgrade nightly --component miri

# ===== Install cargo-check-external-types which requires a specific nightly, see =====
# https://github.com/awslabs/cargo-check-external-types?tab=readme-ov-file#how-to-use
RUN rustup toolchain install nightly-2025-08-06 --profile minimal && \
    cargo +nightly-2025-08-06 install cargo-check-external-types

# ===== Install sccache =====
RUN case "${TARGETARCH}" in \
    "amd64") SCCACHE_ARCH="x86_64-unknown-linux-musl" ;; \
    "arm64") SCCACHE_ARCH="aarch64-unknown-linux-musl" ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/mozilla/sccache/releases/download/${SCCACHE_VERSION}/sccache-${SCCACHE_VERSION}-${SCCACHE_ARCH}.tar.gz" \
    | tar -xz -C /tmp && \
    mv "/tmp/sccache-${SCCACHE_VERSION}-${SCCACHE_ARCH}/sccache" "${CARGO_HOME}/bin/sccache" && \
    rm -rf /tmp/sccache-*

# ===== Install cargo-nextest =====
RUN case "${TARGETARCH}" in \
    "amd64") NEXTEST_ARCH="x86_64-unknown-linux-gnu" ;; \
    "arm64") NEXTEST_ARCH="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported architecture: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${NEXTEST_VERSION}/cargo-nextest-${NEXTEST_VERSION}-${NEXTEST_ARCH}.tar.gz" \
    | tar -xz -C /tmp && \
    mv "/tmp/cargo-nextest" "${CARGO_HOME}/bin/cargo-nextest" && \
    rm -rf /tmp/cargo-nextest*

# ===== Install cargo-deny =====
RUN cargo install cargo-deny

# ===== Install Go =====
RUN curl -fsSL "https://go.dev/dl/${GO_VERSION}.linux-${GO_ARCH}.tar.gz" | tar -C ${HOME} -xzf -

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
    PATH=${CARGO_HOME}/bin:${RUSTUP_HOME}/bin:${HOME}/go/bin:$PATH

COPY --from=build / /
