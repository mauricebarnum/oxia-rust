default: build

build:
    cargo build --all-targets --all-features

test:
    cargo nextest run

check:
    cargo check --all-targets --all-features

lint:
    cargo clippy --all-targets --all-features -- -D warnings

fmt:
    cargo fmt --all
