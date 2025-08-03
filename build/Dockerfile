# Use Ubuntu 24.04 as base to match your current runner
FROM ubuntu:24.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV CARGO_TERM_COLOR=always
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:$PATH

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.88
RUN rustup component add clippy rustfmt

# Install Go 1.24 (architecture-aware)
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then GO_ARCH="amd64"; \
    elif [ "$ARCH" = "arm64" ]; then GO_ARCH="arm64"; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    LATEST=$(curl -s https://go.dev/VERSION?m=text | head -n1) && \
    curl -fsSL "https://go.dev/dl/${LATEST}.linux-${GO_ARCH}.tar.gz" | tar -xzC /usr/local
ENV PATH=/usr/local/go/bin:$PATH

# Install cargo-nextest
RUN cargo install cargo-nextest --locked

# Create non-root user for builds
RUN useradd -m -s /bin/bash runner
USER runner
WORKDIR /home/runner

# Verify installations
RUN rustc --version && \
    cargo --version && \
    go version && \
    protoc --version && \
    cargo nextest --version
