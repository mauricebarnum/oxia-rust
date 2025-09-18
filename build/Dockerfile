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
ARG RUST_VERSION=1.90
ARG GO_VERSION=go1.25.0

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

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain ${RUST_VERSION} -y

# ===== Install Cargo tools =====
RUN cargo install cargo-binstall && cargo binstall -y --no-track --disable-telemetry cargo-nextest

# ===== Install Go =====
RUN curl -fsSL "https://go.dev/dl/${GO_VERSION}.linux-${GO_ARCH}.tar.gz" | tar -C ${HOME} -xzf -

# ===== Install cargo-check-external-types which requires a specific nightly, see =====
# https://github.com/awslabs/cargo-check-external-types?tab=readme-ov-file#how-to-use
RUN rustup toolchain install nightly-2025-08-06 --profile minimal && \
    cargo +nightly-2025-08-06 install cargo-check-external-types

# ====== Fetch current dependencies =====
RUN --mount=type=bind,dst=/src cargo fetch --locked --manifest-path=/src/Cargo.toml

# ====== Install oxia ======
# if this breaks, take a look at oxia-bin-util/build.rs
RUN --mount=type=bind,dst=/src GOPROXY=off GOCACHE=/tmp/gocache go -C /src/oxia-bin-util/go build -mod=vendor -o $HOME/go/bin/oxia ./vendor/github.com/oxia-db/oxia/cmd && rm -rf "$GOCACHE"

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
