ARG PYTHON_VERSION=3.14
ARG PYTHON_IMAGE=python:${PYTHON_VERSION}-slim-bookworm

FROM ${PYTHON_IMAGE}

ARG PYSAIL_VERSION
ARG PYSPARK_VERSION=4.2.0

RUN python3 -m pip install --no-cache-dir \
    "pysail==${PYSAIL_VERSION}" \
    "pyspark-connect==${PYSPARK_VERSION}"

ENTRYPOINT ["/usr/local/bin/sail"]
