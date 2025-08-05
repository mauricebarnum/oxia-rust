#!/bin/bash

set -e

IMAGE_NAME="dev-tools"
TAG="test"

echo "🚀 Testing multi-arch build with OrbStack..."

# Clean up any existing test images
echo "🧹 Cleaning up existing test images..."
docker rmi -f ${IMAGE_NAME}:${TAG}-amd64 2>/dev/null || true
docker rmi -f ${IMAGE_NAME}:${TAG}-arm64 2>/dev/null || true

# Build and test ARM64 (native on Apple Silicon)
echo "🏗️  Building ARM64 (native)..."
docker buildx build --platform linux/arm64 -t ${IMAGE_NAME}:${TAG}-arm64 --load .

echo "✅ Testing ARM64 build..."
docker run --rm ${IMAGE_NAME}:${TAG}-arm64 sh -c "
    echo 'Architecture: \$(uname -m)'
    echo 'Rust: \$(rustc --version)'
    echo 'Go: \$(go version)'
    echo 'Protoc: \$(protoc --version)'
    echo 'Cargo nextest: \$(cargo nextest --version)'
    echo 'Sccache: \$(sccache --version)'
    echo '✅ ARM64 build working!'
"

# Build and test AMD64 (emulated)
echo "🏗️  Building AMD64 (emulated)..."
docker buildx build --platform linux/amd64 -t ${IMAGE_NAME}:${TAG}-amd64 --load .

echo "✅ Testing AMD64 build..."
docker run --rm ${IMAGE_NAME}:${TAG}-amd64 sh -c "
    echo 'Architecture: \$(uname -m)'
    echo 'Rust: \$(rustc --version)'
    echo 'Go: \$(go version)'
    echo 'Protoc: \$(protoc --version)'
    echo 'Cargo nextest: \$(cargo nextest --version)'
    echo 'Sccache: \$(sccache --version)'
    echo '✅ AMD64 build working!'
"

# Test multi-platform build (won't load locally, but validates the build process)
echo "🏗️  Testing multi-platform build process..."
docker buildx build --platform linux/amd64,linux/arm64 -t ${IMAGE_NAME}:${TAG}-multi .

echo "🎉 All tests passed! Both architectures build successfully."

# Show image sizes
echo "📊 Image sizes:"
docker images ${IMAGE_NAME} --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}"

# Clean up
echo "🧹 Cleaning up test images..."
docker rmi ${IMAGE_NAME}:${TAG}-amd64 ${IMAGE_NAME}:${TAG}-arm64 2>/dev/null || true

echo "✨ Multi-arch build test complete!"