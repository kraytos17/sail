#!/usr/bin/env bash
set -euo pipefail

RUST_PROFILE="${RUST_PROFILE:-release}"
IMAGE_TAG="${IMAGE_TAG:-sail:latest}"
PLATFORM="${PLATFORM:-linux/amd64}"

docker buildx build \
  --platform "$PLATFORM" \
  --build-arg "RUST_PROFILE=$RUST_PROFILE" \
  --cache-from=type=gha \
  --cache-to=type=gha,mode=max \
  -t "$IMAGE_TAG" \
  .
