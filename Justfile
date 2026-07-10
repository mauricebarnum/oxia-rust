rust_version_external_types := env('RUST_VERSION_EXTERNAL_TYPES', 'nightly-2026-03-20')

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

lint:
    CARGO_BUILD_WARNINGS=deny cargo clippy --all-targets
    CARGO_BUILD_WARNINGS=deny cargo clippy --all-targets --all-features
    cd client && cargo +{{rust_version_external_types}} check-external-types --all-features

install-check-external-types:
    rustup toolchain install {{rust_version_external_types}} --profile minimal
    cargo +{{rust_version_external_types}} install cargo-check-external-types

fmt:
    cargo +nightly fmt --all
