default: build

build:
    cargo build --all-targets --all-features

test:
    cargo nextest run

check:
    cargo check --all-targets --all-features

lint: check
    cargo clippy --all-targets --all-features -- -D warnings
    cd client && cargo +nightly-2025-10-18 check-external-types

fmt:
    cargo fmt --all
