ARG RELEASE_TAG
ARG RUST_VERSION=1.96.0
ARG RUST_PROFILE=release
ARG RUSTFLAGS=""
ARG PYSPARK_VERSION=4.2.0
ARG PYTHON_IMAGE=python:3.14-slim

FROM ${PYTHON_IMAGE} AS chef

ARG RUST_VERSION
ARG RUSTFLAGS

ENV RUSTFLAGS="${RUSTFLAGS}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        git \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init && \
    chmod a+x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --component rustfmt --default-toolchain ${RUST_VERSION}

ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install cargo-chef --locked

FROM chef AS source

ARG RELEASE_TAG

RUN test -n "${RELEASE_TAG}"

WORKDIR /app

RUN git clone --depth 1 --branch "${RELEASE_TAG}" https://github.com/lakehq/sail.git .

FROM chef AS planner

WORKDIR /app

# Only the manifests are needed to generate the recipe, so that the recipe
# layer stays cached while the source code changes.
COPY --from=source /app/Cargo.toml /app/Cargo.lock ./
COPY --from=source /app/crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

ARG RUST_PROFILE

WORKDIR /app

COPY --from=planner /app/recipe.json recipe.json

# Compile the dependencies once; this layer is only rebuilt when the recipe
# (i.e. the dependency manifests) changes.
RUN --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,target=/root/.cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    RUST_COOK_ARGS=$(case "${RUST_PROFILE}" in \
        release|bench) echo "--release" ;; \
        *) echo "" ;; \
    esac) && \
    cargo chef cook ${RUST_COOK_ARGS} --recipe-path recipe.json

COPY --from=source /app/. .

RUN --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,target=/root/.cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    RUST_TARGET_SUBDIR=$(case "${RUST_PROFILE}" in \
        dev|test) echo "debug" ;; \
        release|bench) echo "release" ;; \
        *) echo "${RUST_PROFILE}" ;; \
    esac) && \
    cargo build -p sail-cli --profile ${RUST_PROFILE} --bins --locked && \
    cp /app/target/${RUST_TARGET_SUBDIR}/sail /usr/local/bin

FROM ${PYTHON_IMAGE}

ARG PYSPARK_VERSION

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN python3 -m pip install --no-cache-dir "pyspark[connect]==${PYSPARK_VERSION}" && \
    groupadd --system --gid 10001 sail && \
    useradd --system --uid 10001 --gid sail --create-home --home-dir /home/sail sail

COPY --from=builder /usr/local/bin/sail /usr/local/bin

LABEL org.opencontainers.image.title="sail" \
      org.opencontainers.image.source="https://github.com/lakehq/sail"

USER sail
WORKDIR /home/sail

ENTRYPOINT ["/usr/local/bin/sail"]
