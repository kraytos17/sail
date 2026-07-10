ARG RUST_VERSION=1.95.0
ARG RUST_PROFILE=release
ARG RUSTFLAGS="-C link-arg=-fuse-ld=mold"
ARG PYSPARK_VERSION=4.1.1
ARG CARGO_BUILD_JOBS=0
ARG SCCACHE_CACHE_SIZE="20G"

FROM python:3.14-slim AS rust-base

ARG RUST_VERSION

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-cache \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gcc \
        libc6-dev \
        git \
        pkg-config \
        protobuf-compiler \
        libprotobuf-dev \
        mold

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y \
        --no-modify-path \
        --profile minimal \
        --default-toolchain ${RUST_VERSION}

RUN /root/.cargo/bin/rustup component add rustfmt

ENV PATH="/root/.cargo/bin:${PATH}"

RUN curl -L --proto '=https' --tlsv1.2 -sSf \
        https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh \
        | bash && \
    cargo binstall --no-confirm cargo-chef sccache

FROM rust-base AS planner

WORKDIR /app

COPY --link Cargo.toml Cargo.lock ./
COPY --link crates/ ./crates/

RUN find crates -type f ! -name 'Cargo.toml' -delete && \
    find crates -type d -empty -delete && \
    find crates -name Cargo.toml -execdir sh -c \
        'mkdir -p src && touch src/lib.rs src/main.rs' \;

RUN cargo chef prepare --recipe-path recipe.json

FROM rust-base AS builder

ARG RUST_PROFILE
ARG RUSTFLAGS
ARG CARGO_BUILD_JOBS
ARG SCCACHE_CACHE_SIZE

ENV RUSTFLAGS="${RUSTFLAGS}"
ENV RUSTC_WRAPPER=sccache
ENV SCCACHE_DIR=/root/.cache/sccache
ENV SCCACHE_CACHE_SIZE=${SCCACHE_CACHE_SIZE}
ENV CARGO_NET_RETRY=3
ENV CARGO_HTTP_TIMEOUT=30
ENV CARGO_INCREMENTAL=0
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV PYO3_PYTHON=/usr/local/bin/python3.14

WORKDIR /app

COPY --link --from=planner /app/recipe.json recipe.json

RUN --mount=type=cache,target=/root/.cache/sccache,id=sccache \
    --mount=type=cache,target=/root/.cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/root/.cargo/git,id=cargo-git \
    --mount=type=cache,target=/app/target,id=cargo-target \
    JOBS="${CARGO_BUILD_JOBS}"; \
    case "$JOBS" in ''|*[!0-9]*|0) JOBS=$(nproc) ;; esac; \
    cargo chef cook \
        --profile ${RUST_PROFILE} \
        --recipe-path recipe.json \
        --jobs "$JOBS" \
        -p sail-cli \
        --bins

RUN --mount=type=bind,source=.,target=/app,rw \
    --mount=type=cache,target=/root/.cache/sccache,id=sccache \
    --mount=type=cache,target=/root/.cargo/registry,id=cargo-registry \
    --mount=type=cache,target=/root/.cargo/git,id=cargo-git \
    --mount=type=cache,target=/app/target,id=cargo-target \
    JOBS="${CARGO_BUILD_JOBS}"; \
    case "$JOBS" in ''|*[!0-9]*|0) JOBS=$(nproc) ;; esac; \
    RUST_TARGET_SUBDIR=$(case "${RUST_PROFILE}" in \
        dev|test) echo "debug" ;; \
        release|bench) echo "release" ;; \
        *) echo "${RUST_PROFILE}" ;; \
    esac) && \
    cargo build \
        -p sail-cli \
        --profile ${RUST_PROFILE} \
        --bins \
        --jobs "$JOBS" && \
    install -m755 /app/target/${RUST_TARGET_SUBDIR}/sail /usr/local/bin/sail && \
    sccache --show-stats

FROM python:3.14-slim

ARG PYSPARK_VERSION

RUN groupadd --system sail && \
    useradd --system --gid sail --no-create-home --shell /usr/sbin/nologin sail

RUN python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"

COPY --link --from=builder /usr/local/bin/sail /usr/local/bin/sail

USER sail

ENTRYPOINT ["/usr/local/bin/sail"]
