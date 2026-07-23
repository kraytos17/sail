# syntax=docker/dockerfile:1.9
ARG RUST_VERSION=1.97.0
ARG RUST_PROFILE=release
ARG PYSPARK_VERSION=4.1.1
ARG CARGO_BUILD_JOBS
ARG SCCACHE_CACHE_SIZE="20G"

FROM rust:${RUST_VERSION}-slim-trixie AS base
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-cache \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists \
    apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gcc libc6-dev git pkg-config \
        protobuf-compiler libprotobuf-dev mold && \
    rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt && \
    curl -L --proto '=https' --tlsv1.2 -sSf \
        https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall --no-confirm cargo-chef sccache

FROM python:3.14-slim-trixie AS python

FROM base AS planner
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

RUN find crates -type f ! -name 'Cargo.toml' -delete && \
    find crates -type d -empty -delete && \
    find crates -name Cargo.toml -execdir sh -c 'mkdir -p src && touch src/lib.rs src/main.rs' \;

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry \
    cargo chef prepare --recipe-path recipe.json

FROM base AS builder
ARG RUST_PROFILE
ARG CARGO_BUILD_JOBS
ARG SCCACHE_CACHE_SIZE

ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/root/.cache/sccache \
    SCCACHE_CACHE_SIZE=${SCCACHE_CACHE_SIZE} \
    CARGO_INCREMENTAL=0 \
    CARGO_NET_RETRY=3 \
    CARGO_HTTP_TIMEOUT=30 \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

WORKDIR /app

COPY --from=python /usr/local/ /usr/local/

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/root/.cache/sccache,id=sccache \
    --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git \
    --mount=type=cache,target=/app/target,id=cargo-target \
    JOBS=${CARGO_BUILD_JOBS:-$(nproc)} && \
    cargo chef cook --profile ${RUST_PROFILE} --recipe-path recipe.json \
        -p sail-cli --jobs "$JOBS"

COPY . .
RUN --mount=type=cache,target=/root/.cache/sccache,id=sccache \
    --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git \
    --mount=type=cache,target=/app/target,id=cargo-target \
    JOBS=${CARGO_BUILD_JOBS:-$(nproc)} && \
    cargo build -p sail-cli --profile ${RUST_PROFILE} --bins --jobs "$JOBS" && \
    install -m755 target/${RUST_PROFILE}/sail /usr/local/bin/sail && \
    sccache --show-stats

FROM python:3.14-slim-trixie AS runtime

ARG PYSPARK_VERSION

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-cache \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists \
    groupadd --system sail && \
    useradd --system --gid sail --no-create-home --shell /usr/sbin/nologin sail && \
    apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"

COPY --link --from=builder /usr/local/bin/sail /usr/local/bin/sail

USER sail
ENTRYPOINT ["/usr/local/bin/sail"]
