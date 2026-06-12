rust_version_external_types := env('RUST_VERSION_EXTERNAL_TYPES', 'nightly-2026-03-20')

default: build

build:
    cargo build --all-targets --all-features

test:
    cargo nextest run

check:
    cargo check --all-targets --all-features

lint:
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-targets --all-features -- -D warnings
    cd client && cargo +{{rust_version_external_types}} check-external-types --all-features

install-check-external-types:
    rustup toolchain install {{rust_version_external_types}} --profile minimal
    cargo +{{rust_version_external_types}} install cargo-check-external-types

fmt:
    cargo fmt --all
