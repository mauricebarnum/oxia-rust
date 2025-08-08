# Multi-stage build for minimal runtime image
FROM ubuntu:24.04 AS builder

# Set environment variables for build stage
ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:/usr/local/go/bin:$PATH

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.89 \
    && rustup component add clippy rustfmt

# Install Go (architecture-aware)
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then GO_ARCH="amd64"; \
    elif [ "$ARCH" = "arm64" ]; then GO_ARCH="arm64"; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    LATEST=$(curl -s https://go.dev/VERSION?m=text | head -n1) && \
    curl -fsSL "https://go.dev/dl/${LATEST}.linux-${GO_ARCH}.tar.gz" | tar -xzC /usr/local

# Install Rust tools using binstall for speed
RUN cargo install cargo-binstall --locked \
    && cargo binstall sccache cargo-nextest --no-confirm --locked

# Clean cargo caches and temporary files
RUN rm -rf /usr/local/cargo/registry/cache \
    && rm -rf /usr/local/cargo/git/db \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*

# Final runtime stage
FROM ubuntu:24.04

# Create non-root user
RUN useradd -m -s /bin/bash runner

# Set environment variables for runtime
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:/usr/local/go/bin:$PATH
ENV CARGO_TERM_COLOR=always
ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies including protobuf
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    git \
    curl \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*

# Copy Rust installation from builder and set up environment
COPY --from=builder /usr/local/rustup /usr/local/rustup
COPY --from=builder /usr/local/cargo /usr/local/cargo

# Set up Rust environment properly - rustup is in cargo/bin
RUN /usr/local/cargo/bin/rustup default stable

# Copy Go installation from builder
COPY --from=builder /usr/local/go /usr/local/go

# Set up user directories and permissions
RUN mkdir -p /home/runner/.rustup /home/runner/.cargo \
    && chown -R runner:runner /home/runner \
    && chmod -R 755 /home/runner

# Verify installations work
RUN rustc --version && \
    cargo --version && \
    go version && \
    protoc --version && \
    cargo nextest --version && \
    sccache --version

# Switch to non-root user
USER runner
WORKDIR /home/runner
