#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

PROFILE="dev"
PROFILE_EXPLICIT=false
TAG=""
RUST_VERSION="1.96.0"
PYSPARK_VERSION="4.2.0"
PYTHON_IMAGE="python:3.14-slim"
IMAGE=""
NO_CACHE=false
PLATFORM=""
PUSH=false
LOAD=""
CACHE_REF=""
BUILDER=""
PROGRESS="auto"
METADATA_FILE=""
DRY_RUN=false

usage() {
    cat <<EOF
Usage: $0 [options]

Build a Sail Docker image with BuildKit (docker buildx).

Options:
  -p, --profile <name>       Cargo profile: dev|test|release|bench (default: dev)
  -o, --optimized            Alias for --profile release
  -t, --tag <tag>            GitHub release tag; builds from the tag via
                             docker/release/Dockerfile (forces release profile)
      --rust-version <ver>   Rust toolchain version (default: ${RUST_VERSION})
      --pyspark-version <v>  pyspark[connect] version (default: ${PYSPARK_VERSION})
      --python-image <img>   Python base image (default: ${PYTHON_IMAGE})
      --image <name>         Output image name (default: sail:dev, sail:release, or sail:<tag>)
      --platform <plat>      Target platform(s), e.g. linux/amd64 or linux/amd64,linux/arm64
                             (multiple platforms require --push)
      --push                 Push the image to its registry (implies no --load)
      --load                 Load the image into the local Docker image store (default unless --push)
      --cache-ref <ref>      Registry ref for the build cache, e.g. user/sail:cache
                             (enables --cache-from and --cache-to type=registry,mode=max)
      --builder <name>       Buildx builder instance to use
                             (auto-created as 'sail-builder' with the docker-container driver
                             when --push or --cache-ref is used)
      --progress <mode>      Buildx progress mode: auto|plain|tty (default: ${PROGRESS})
      --metadata-file <path> Write build metadata (image digest) to <path> as JSON
      --no-cache             Disable Docker build cache
      --dry-run              Print the build command without running it
  -h, --help                 Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--profile)
            PROFILE="$2"
            PROFILE_EXPLICIT=true
            shift 2
            ;;
        -o|--optimized)
            PROFILE="release"
            PROFILE_EXPLICIT=true
            shift
            ;;
        -t|--tag)
            TAG="$2"
            shift 2
            ;;
        --rust-version)
            RUST_VERSION="$2"
            shift 2
            ;;
        --pyspark-version)
            PYSPARK_VERSION="$2"
            shift 2
            ;;
        --python-image)
            PYTHON_IMAGE="$2"
            shift 2
            ;;
        --image)
            IMAGE="$2"
            shift 2
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --load)
            LOAD=true
            shift
            ;;
        --cache-ref)
            CACHE_REF="$2"
            shift 2
            ;;
        --builder)
            BUILDER="$2"
            shift 2
            ;;
        --progress)
            PROGRESS="$2"
            shift 2
            ;;
        --metadata-file)
            METADATA_FILE="$2"
            shift 2
            ;;
        --no-cache)
            NO_CACHE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

case "${PROFILE}" in
    dev|test|release|bench) ;;
    *)
        echo "Error: invalid profile '${PROFILE}' (expected dev|test|release|bench)" >&2
        exit 1
        ;;
esac

case "${PROGRESS}" in
    auto|plain|tty) ;;
    *)
        echo "Error: invalid progress mode '${PROGRESS}' (expected auto|plain|tty)" >&2
        exit 1
        ;;
esac

if [[ -n "${TAG}" ]]; then
    if [[ "${PROFILE_EXPLICIT}" == true && "${PROFILE}" != "release" ]]; then
        echo "Error: --tag builds require the release profile, got '${PROFILE}'" >&2
        exit 1
    fi
    PROFILE="release"
fi

if [[ -z "${IMAGE}" ]]; then
    if [[ -n "${TAG}" ]]; then
        IMAGE="sail:${TAG}"
    else
        IMAGE="sail:${PROFILE}"
    fi
fi

if [[ -n "${PLATFORM}" && "${PLATFORM}" == *","* && "${PUSH}" != true ]]; then
    echo "Error: multi-platform builds require --push (the local image store is single-arch)" >&2
    exit 1
fi

DOCKERFILE="docker/dev/Dockerfile"
if [[ -n "${TAG}" ]]; then
    DOCKERFILE="docker/release/Dockerfile"
fi

# BuildKit availability: prefer `docker buildx build`; fall back to `docker build`.
if command -v docker buildx >/dev/null 2>&1; then
    USE_BUILDX=true
else
    USE_BUILDX=false
    echo "Warning: 'docker buildx' not found; falling back to 'docker build'." >&2
    if [[ "${PUSH}" == true || -n "${CACHE_REF}" ]]; then
        echo "Error: --push and --cache-ref require docker buildx (BuildKit)" >&2
        exit 1
    fi
fi

# Load into the local image store by default, unless pushing.
if [[ -z "${LOAD}" ]]; then
    LOAD=false
    if [[ "${PUSH}" != true ]]; then
        LOAD=true
    fi
fi

# Auto-create a dedicated docker-container builder for registry-backed workflows.
if [[ "${USE_BUILDX}" == true && ("${PUSH}" == true || -n "${CACHE_REF}") ]]; then
    if [[ -z "${BUILDER}" ]]; then
        BUILDER="sail-builder"
    fi
    if [[ "${DRY_RUN}" == true ]]; then
        echo "Would create buildx builder '${BUILDER}' (driver: docker-container) if missing"
    elif ! docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
        echo "Creating buildx builder '${BUILDER}' (driver: docker-container)"
        docker buildx create --name "${BUILDER}" --driver docker-container
    fi
fi

ARGS=(
    --build-arg "RUST_VERSION=${RUST_VERSION}"
    --build-arg "RUST_PROFILE=${PROFILE}"
    --build-arg "PYSPARK_VERSION=${PYSPARK_VERSION}"
    --build-arg "PYTHON_IMAGE=${PYTHON_IMAGE}"
    -t "${IMAGE}"
    -f "${DOCKERFILE}"
    --progress "${PROGRESS}"
)
if [[ -n "${TAG}" ]]; then
    ARGS+=(--build-arg "RELEASE_TAG=${TAG}")
fi
if [[ "${NO_CACHE}" == true ]]; then
    ARGS+=(--no-cache)
fi
if [[ -n "${PLATFORM}" ]]; then
    ARGS+=(--platform "${PLATFORM}")
fi
if [[ "${USE_BUILDX}" == true ]]; then
    if [[ "${PUSH}" == true ]]; then
        ARGS+=(--push)
    fi
    if [[ "${LOAD}" == true ]]; then
        ARGS+=(--load)
    fi
    if [[ -n "${CACHE_REF}" ]]; then
        ARGS+=(--cache-from "type=registry,ref=${CACHE_REF}")
        ARGS+=(--cache-to "type=registry,ref=${CACHE_REF},mode=max")
    fi
    if [[ -n "${BUILDER}" ]]; then
        ARGS+=(--builder "${BUILDER}")
    fi
    if [[ -n "${METADATA_FILE}" ]]; then
        ARGS+=(--metadata-file "${METADATA_FILE}")
    fi
    BUILD_CMD=(docker buildx build)
else
    BUILD_CMD=(docker build)
    if [[ "${LOAD}" == true ]]; then
        BUILD_CMD=(DOCKER_BUILDKIT=1 docker build)
    fi
fi

echo "Building Sail image: ${IMAGE}"
echo "  Dockerfile: ${DOCKERFILE}"
echo "  Profile:    ${PROFILE}"
[[ -n "${TAG}" ]] && echo "  Tag:        ${TAG}"
echo "  Rust:       ${RUST_VERSION}"
echo "  PySpark:    ${PYSPARK_VERSION}"
echo "  Python:     ${PYTHON_IMAGE}"
[[ -n "${PLATFORM}" ]] && echo "  Platform:   ${PLATFORM}"
[[ "${USE_BUILDX}" == true ]] && echo "  Builder:    ${BUILDER:-<default>}"
echo "  Output:     $([[ "${PUSH}" == true ]] && echo push || echo load)"
[[ -n "${CACHE_REF}" ]] && echo "  Cache:      ${CACHE_REF}"
echo

if [[ "${DRY_RUN}" == true ]]; then
    echo "${BUILD_CMD[*]} ${ARGS[*]} ."
    exit 0
fi

"${BUILD_CMD[@]}" "${ARGS[@]}" .

echo
echo "Built ${IMAGE}. Run the E2E test plan via stdin:"
echo "  docker run --rm -i ${IMAGE} spark run -f -"
