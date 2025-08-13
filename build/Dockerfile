FROM ubuntu:24.04

# ===== 1. OS packages (long-lived layer) =====
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    clang \
    cmake \
    git \
    curl \
    unzip \
    ca-certificates \
    protobuf-compiler \
    jq \
    python3 \
    python3-pip \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# ===== 2. Create non-root build user =====
RUN useradd -ms /bin/bash runner
USER runner
WORKDIR /home/runner

# ===== 3. Install Rust (latest 1.89.y patch) =====
ENV RUST_MINOR=1.89
RUN latest=$(curl -s https://api.github.com/repos/rust-lang/rust/releases \
      | jq -r '.[].tag_name' \
      | grep "^${RUST_MINOR}\." \
      | sort -Vr | head -n1) && \
    echo "Installing Rust $latest" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain "$latest" --profile minimal && \
    echo 'source $HOME/.cargo/env' >> ~/.bashrc
ENV PATH=/home/runner/.cargo/bin:$PATH

# ===== 4. Install Cargo tools =====
RUN . $HOME/.cargo/env && \
    cargo install sccache \
                  cargo-nextest \
                  cargo-edit \
                  cargo-outdated \
                  ripgrep \
                  fd-find

# ===== 5. Install Go (latest 1.24.x patch) =====
ENV GO_MINOR=1.24
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then GO_ARCH="amd64"; \
    elif [ "$ARCH" = "arm64" ]; then GO_ARCH="arm64"; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    LATEST=$(curl -s https://go.dev/dl/?mode=json \
      | jq -r '.[].version' \
      | grep "^go${GO_MINOR}\." \
      | sort -Vr | head -n1) && \
    echo "Installing Go $LATEST" && \
    curl -fsSL "https://go.dev/dl/${LATEST}.linux-${GO_ARCH}.tar.gz" \
      | tar -C /home/runner -xzf - && \
    mv /home/runner/go /home/runner/.go && \
    echo 'export PATH=$HOME/.go/bin:$PATH' >> ~/.bashrc
ENV PATH=/home/runner/.go/bin:$PATH

# ===== 6. Environment for sccache =====
ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/home/runner/.cache/sccache

# ===== --hack--. Add rust components left out of minimal install we'll need later.
# TODO: just switch profiles, this is a hack to re-use existing layers
RUN rustup component add clippy rustfmt

# ===== 7. Cacheable Cargo fetch layer =====
RUN --mount=type=bind,source=.,target=/tmp/src,readonly \
    . "$HOME/.cargo/env" && \
    cargo fetch --locked --manifest-path /tmp/src/Cargo.toml


# --hack-- set rust for future builds. get rid of this when we do the next big rebuild
RUN rustup default $(rustc --version | awk '{print $2}')
