default: build

build:
    cargo build --all-targets --all-features

build-all:
    cargo build --all-targets
    cargo build --all-targets --features metrics
    cargo build --all-targets --features go-metrics-compat

test:
    cargo nextest run --all-features

test-all:
    cargo nextest run
    cargo nextest run --features metrics
    cargo nextest run --features go-metrics-compat

check:
    cargo check --all-targets --all-features

lint: check
    cargo clippy --all-targets --all-features -- -D warnings
    cd client && cargo +nightly-2025-10-18 check-external-types

fmt:
    cargo +nightly fmt --all
