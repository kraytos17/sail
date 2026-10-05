ARG PYTHON_VERSION=3.14
ARG PYTHON_IMAGE=python:${PYTHON_VERSION}-slim-bookworm

FROM ${PYTHON_IMAGE}

ENV TZ=UTC
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata libreadline8 && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

ARG PYSAIL_VERSION
ARG PYSPARK_VERSION=4.2.0

RUN python3 -m pip install --no-cache-dir \
    "pysail==${PYSAIL_VERSION}" \
    "pyspark-client==${PYSPARK_VERSION}"

ENTRYPOINT ["/usr/local/bin/sail"]
