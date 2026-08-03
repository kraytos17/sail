# syntax=docker/dockerfile:1.26.0

FROM python:3.14-slim

ARG PYSAIL_VERSION
ARG PYSPARK_VERSION=4.1.1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN python3 -m pip install --no-cache-dir "pysail==${PYSAIL_VERSION}" && \
    python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}" && \
    groupadd --system --gid 10001 sail && \
    useradd --system --uid 10001 --gid sail --create-home --home-dir /home/sail sail

LABEL org.opencontainers.image.title="sail" \
      org.opencontainers.image.source="https://github.com/lakehq/sail"

USER sail
WORKDIR /home/sail

ENTRYPOINT ["/usr/local/bin/sail"]
