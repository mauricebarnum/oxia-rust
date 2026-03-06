# ci.just — Validate docker-image.yml workflow changes locally
# Usage: just --justfile ci.just [recipe]
# Requires: act (>= 0.2.84), docker (OrbStack), gh CLI

set shell := ["bash", "-euo", "pipefail", "-c"]

GH_TOKEN    := `gh auth token 2>/dev/null || echo ""`
SHA         := `git rev-parse HEAD`
SHA_TAG     := "sha-" + SHA
GITHUB_REPO := `git remote get-url origin | sed 's|.*github.com[:/]||;s|\.git$||' | tr '[:upper:]' '[:lower:]'`
REPO_NAME   := `basename $(git remote get-url origin) .git`
REGISTRY    := "ghcr.io"
GHCR_IMAGE  := REGISTRY + "/" + GITHUB_REPO + "/rust-build"
LOCAL_IMAGE := "rust-build:local-test"

default:
    @just --justfile ci.just --list

# Run all local validations — no GHCR push or pull required.
# docker-build runs twice (smoke + validate) but is fast from cache.
local: check docker-smoke validate-local

# ── Level 1: Syntax & trigger validation (no credentials) ────────────────────

# List all jobs in docker-image.yml
list-docker:
    act --list -W .github/workflows/docker-image.yml

# List all jobs in rust.yml
list-rust:
    act --list -W .github/workflows/rust.yml

# Dry-run push event on docker-image.yml (checks job structure, no execution)
dry-docker-push:
    act -n push -W .github/workflows/docker-image.yml

# Dry-run PR event on docker-image.yml
dry-docker-pr:
    act -n pull_request -W .github/workflows/docker-image.yml

# Dry-run push event on rust.yml
dry-rust-push:
    act -n push -W .github/workflows/rust.yml

# Dry-run PR event on rust.yml
dry-rust-pr:
    act -n pull_request -W .github/workflows/rust.yml

# Run all dry-run checks (fast, no auth needed)
check: dry-docker-push dry-docker-pr dry-rust-push dry-rust-pr
    @echo "All dry-run checks passed"

# ── Level 3: Local Docker image build (no GHCR push needed) ─────────────────

# Build the image locally using same Dockerfile and args as CI
docker-build:
    docker build -f build/Dockerfile . \
        --build-arg REPO_NAME={{REPO_NAME}} \
        -t {{LOCAL_IMAGE}}

# Smoke-test the locally built image
docker-smoke: docker-build
    @echo "=== rustc ==="
    docker run --rm {{LOCAL_IMAGE}} rustc --version
    @echo "=== cargo nextest ==="
    docker run --rm {{LOCAL_IMAGE}} cargo nextest --version
    @echo "=== protoc ==="
    docker run --rm {{LOCAL_IMAGE}} protoc --version
    @echo "=== sccache ==="
    docker run --rm {{LOCAL_IMAGE}} sccache --version
    @echo "Smoke test passed"

# ── Level 2: Run act jobs (needs GHCR pull access) ───────────────────────────

# Run rust.yml on a PR event (uses ghcr latest image)
act-rust:
    act pull_request -W .github/workflows/rust.yml \
        -s GITHUB_TOKEN={{GH_TOKEN}}

# Run rust.yml on a push event (uses ghcr latest image)
act-rust-push:
    act push -W .github/workflows/rust.yml \
        -s GITHUB_TOKEN={{GH_TOKEN}}

# Run only the validate-image job using the SHA-tagged image in GHCR
# Note: act support for reusable workflows (workflow_call) is limited;
# this will attempt to run it but may fall back to running steps inline.
act-validate:
    act pull_request -W .github/workflows/docker-image.yml \
        -s GITHUB_TOKEN={{GH_TOKEN}} \
        -j validate-image

# ── Level 2 fallback: validate locally without pushing to GHCR ───────────────

# Build image locally then run the same steps validate-image would run.
# Mounts the workspace read-only; requires network for cargo fetch.
validate-local: docker-build
    docker run --rm \
        -v "$(pwd):/workspace:ro" \
        -w /workspace \
        {{LOCAL_IMAGE}} \
        bash -c "cargo deny check && cargo fetch --locked && cargo build --profile ci --frozen --all-targets && cargo clippy --profile ci --no-deps --all-targets --frozen -- -D warnings"

# ── Level 4 helpers: push SHA tag for end-to-end GitHub Actions test ─────────

# Print the SHA tag that would be used by CI for the current commit
print-sha-tag:
    @echo "{{GHCR_IMAGE}}:{{SHA_TAG}}"

# Push the locally built image to GHCR with the current SHA tag.
# After this, open a PR on a branch that touches build/Dockerfile to trigger
# the full docker-image.yml pipeline in GitHub Actions.
push-sha-tag: docker-build
    docker tag {{LOCAL_IMAGE}} {{GHCR_IMAGE}}:{{SHA_TAG}}
    docker push {{GHCR_IMAGE}}:{{SHA_TAG}}
    @echo "Pushed: {{GHCR_IMAGE}}:{{SHA_TAG}}"
    @echo "You can now trigger act-validate against this image, or open a PR."
