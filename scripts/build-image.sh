#!/usr/bin/env bash
# Builds the SwiftCache image into the local Docker daemon. With colima/k3s using the
# docker runtime, images built here are immediately visible to the cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="${1:-swiftcache:$(git rev-parse --short HEAD 2>/dev/null || echo dev)$(git diff --quiet 2>/dev/null || echo -dirty)}"
DOCKER_BUILDKIT=1 docker build -t "$TAG" -t swiftcache:latest .
echo "$TAG"
