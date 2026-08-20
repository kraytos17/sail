# 08 — Config, Docker, K8s, Python Tests

> Non-Rust port surface: configuration keys, Docker build tooling (cargo-chef caching,
> non-root runtime user), a Flight-server Kubernetes manifest, a `buildx` helper script,
> and the Python integration tests that exercise the new features end-to-end.

Files:
- `crates/sail-common/src/config/application.rs` + `application.yaml` (+69)
- `docker/dev/Dockerfile` (+64), `docker/quickstart/Dockerfile` (+17), `docker/release/Dockerfile` (+75)
- `build.sh` (new)
- `k8s/sail.yaml` (+69)
- `python/pysail/tests/flight/conftest.py` (+29), `python/pysail/tests/flight/test_flight_heimdall.py` (new, 252)
- `python/pysail/tests/spark/catalog/iceberg_rest/test_commit.py` (+42)
- `.gitignore` (+small)

---

## 1. Configuration — `crates/sail-common/src/config/application.yaml` (+69)

New `cluster.*` keys (see also `07 §9`):

| key | type | default | description |
|---|---|---|---|
| `cluster.http2_keepalive_interval_secs` | number | `60` | HTTP/2 keep-alive ping interval on driver, worker, Spark Connect, and Flight servers |
| `cluster.http2_keepalive_timeout_secs` | number | `30` | ping-ack timeout; connection closed if no acknowledgement within the window |
| `cluster.worker_spawn_retry_strategy.type` | string | `fixed` | `fixed` / `exponential_backoff` (experimental) — used when re-spawning a worker whose pod failed to start |
| `cluster.worker_spawn_retry_strategy.fixed.max_count` | number | `1` | max re-spawn attempts (fixed) |
| `cluster.worker_spawn_retry_strategy.fixed.delay_secs` | number | `180` | delay between re-spawns (fixed) |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.max_count` | number | `3` | max re-spawn attempts (exponential) |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.initial_delay_secs` | number | `60` | initial delay (exponential) |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.max_delay_secs` | number | `180` | max delay (exponential) |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.factor` | number | `2` | delay multiplier per attempt |

`config/application.rs` adds the corresponding struct fields + typed env mapping
(`ClusterConfigEnv::HTTP2_KEEPALIVE_INTERVAL_SECS` / `HTTP2_KEEPALIVE_TIMEOUT_SECS`),
plus a **new non-cluster enum**:

```rust
pub enum IcebergRestAccessDelegation { VendedCredentials /* default */, None }
```

carried as an optional `access_delegation` field on the **`CatalogType::IcebergRest`
variant** in `sail-common/src/config/application.rs` (serde tag `iceberg-rest`,
`#[serde(skip_serializing_if = "Option::is_none")]`; **no default key in
`application.yaml`**). See `06 §2.1` for the wiring into the Iceberg REST catalog
provider.

### 1.1 Iceberg write-option change — `crates/sail-iceberg/data/options/iceberg.yaml`

The `write.compression-codec` option flips from **unsupported** to **supported**:
- description: `Override the Parquet compression codec for data file writes
  (snappy | zstd | gzip | lz4 | uncompressed | none).`
- default `"snappy"` (`parse_string`), `rust_type: String`
- additional layer `{ type: table_property, keys: [write.parquet.compression-codec],
  case_sensitive: true, parser: parse_string }` so `TBLPROPERTIES
  ('write.parquet.compression-codec'='zstd')` takes effect (see `03 §10`).

---

## 2. Docker

### 2.1 Shared approach across all three Dockerfiles

- `# syntax=docker/dockerfile:1.26.0` (BuildKit frontend pin).
- Parameterized base image: `ARG PYTHON_IMAGE=python:3.14-slim`.
- **cargo-chef** 3-stage build (`chef` → `planner` → `builder`):
  - `chef`: apt packages (`libclang-dev`, `libssl-dev`, `libprotobuf-dev`, `curl`, `git`,
    `pkg-config`) then `rm -rf /var/lib/apt/lists/*`; rustup `--profile minimal
    --component rustfmt`; `cargo install cargo-chef --locked`.
  - `planner`: `COPY Cargo.toml Cargo.lock crates ./` then `cargo chef prepare
    --recipe-path recipe.json` — the recipe layer stays cached while source changes.
  - `builder`: `COPY --from=planner /app/recipe.json`; dependency compilation cached in
    a locked RUN mount (`/root/.cargo/registry`, `/root/.cargo/git`, `/app/target`) with
    `RUST_COOK_ARGS` selecting `--release` for release/bench profiles.
- Release only: a `source` stage that `RUN test -n "${RELEASE_TAG}"` (builds from a tag).

### 2.2 `docker/quickstart/Dockerfile` (+17)

- `ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1`.
- pip installs of `pysail==${PYSAIL_VERSION}` and `pyspark-client==${PYSPARK_VERSION}`
  chained into one RUN layer.
- Non-root runtime user: `groupadd --system --gid 10001 sail` + `useradd --system --uid
  10001 --gid sail --create-home --home-dir /home/sail sail`.
- `LABEL org.opencontainers.image.title="sail"`, `source=https://github.com/lakehq/sail`.
- `USER sail`, `WORKDIR /home/sail`.

### 2.3 `docker/dev/Dockerfile` (+64)

- Same cargo-chef structure; `RUST_PROFILE` forwarded to `cargo build`; rustfmt component
  for fmt-checks in CI; apt cleanup in the same RUN.

### 2.4 `docker/release/Dockerfile` (+75)

- chef + source (requires `RELEASE_TAG`) + planner + builder; release builds pull the
  source from the `source` stage so the tag is the only input.

---

## 3. `build.sh` (new) — BuildKit `docker buildx` helper

Flags (from usage):
`-p/--profile <dev|test|release|bench>`, `-o/--optimized` (release alias),
`-t/--tag <tag>` (builds from `docker/release/Dockerfile`, forces release profile),
`--rust-version`, `--pyspark-version`, `--python-image`, `--image <name>`,
`--platform <plat>` (multi-arch requires `--push`), `--push`, `--load`,
`--cache-ref <ref>` (enables `--cache-from`/`--cache-to` `type=registry,mode=max`),
`--builder <name>` (auto-creates `sail-builder` with the `docker-container` driver when
push/cache-ref is used), `--progress`, `--metadata-file <path>` (writes image digest as
JSON), `--no-cache`, `--dry-run`.

Defaults: `RUST_VERSION=1.95.0`, `PYSPARK_VERSION=4.1.1`, `PYTHON_IMAGE=python:3.14-slim`.

---

## 4. Kubernetes — `k8s/sail.yaml` (+69)

Adds a second workload alongside the existing spark server:

- **Namespace** `sail` (already present).
- **`sail-flight-server` Deployment**: `replicas: 1` (a comment explains you cannot scale
  — each session is tied to a single pod), `serviceAccountName: sail-user`, container
  image `sail:latest`, `command: ["sail"] args: ["flight", "server", "--ip", "0.0.0.0",
  "--port", "32010"]`, `containerPort: 32010`, `imagePullPolicy: IfNotPresent`.
  Env:
  - `RUST_LOG=info`
  - `SAIL_MODE=kubernetes-cluster`
  - `SAIL_CLUSTER__DRIVER_LISTEN_HOST=0.0.0.0`
  - `SAIL_CLUSTER__DRIVER_EXTERNAL_HOST` ← `fieldRef status.podIP`
  - `SAIL_KUBERNETES__IMAGE=sail:latest`
  - `SAIL_KUBERNETES__NAMESPACE=sail`
  - `SAIL_KUBERNETES__DRIVER_POD_NAME` ← `fieldRef metadata.name`
  - `SAIL_KUBERNETES__WORKER_SERVICE_ACCOUNT_NAME=sail-user`
- **`sail-flight-service` Service** (ClusterIP) exposing port 32010.

---

## 5. Python tests

### 5.1 `python/pysail/tests/flight/conftest.py` (+29)

New module-scoped fixture `flight_catalog_uri(tmp_path_factory)`:
- Creates a temp Iceberg warehouse dir.
- Starts a real `FlightSqlServer` from `pysail.flight` (`ip="127.0.0.1", port=0`), which
  loads the default application config (Memory catalog with a `default` database).
- Reads `server.listening_address`; fails with `RuntimeError` if `None`.
- Yields `grpc://host:port` + the warehouse dir; stops the server in the `finally` block.
- Also suppresses ADBC autocommit warnings (Flight SQL can't disable autocommit).

### 5.2 `python/pysail/tests/flight/test_flight_heimdall.py` (new, 252 lines, 8 tests)

End-to-end heimdall tests over Flight SQL against the Memory catalog + `file://` Iceberg
warehouse:

| test | covers |
|---|---|
| `test_load_data_and_read_back` | LOAD DATA INPATH → SELECT |
| `test_load_data_overwrite_replaces_rows` | LOAD DATA OVERWRITE full-table replace |
| `test_refs_and_snapshots_metadata_tables` | `db.table.refs` / `db.table.snapshots` reads |
| `test_truncate_table` | TRUNCATE (empty-table no-op + non-empty) |
| `test_rollback_and_set_current_snapshot` | CALL rollback/set-current + output columns |
| `test_rollback_rejects_non_ancestor` | rollback ancestry validation |
| `test_expire_snapshots_returns_counts_and_removes_files` | expire_snapshots metadata commit + physical GC counts + files gone |
| `test_version_as_of` | `SELECT ... AS OF VERSION` against the reverted snapshot |

### 5.3 `python/pysail/tests/spark/catalog/iceberg_rest/test_commit.py` (+42)

`test_create_or_replace_table_as_select_replaces_rest_catalog_metadata`:
- First `CREATE OR REPLACE TABLE ... AS SELECT` creates the table (previously rejected as
  `"Replace table is not supported yet"`); asserts the metadata location (`_assert_uuid_metadata_location(_, 0)`).
- Second run replaces: new metadata version (1), `metadata-log` records the old location,
  `current-snapshot-id` set, data == `[(2, "b")]`.

---

## 6. `.gitignore`

Small additions for build artifacts generated by the new tooling.

---

## 7. Port notes

- The Dockerfile rewrite changes the build graph substantially (chef/planner/builder);
  if `feat/0.7` has since bumped Rust/PySpark versions or added crates, port the
  *structure* and re-apply current version pins.
- `k8s/sail.yaml` only adds the flight workload; do not duplicate the spark workload
  when merging.
- The Python tests depend on `pysail.flight.FlightSqlServer` and the new SQL features;
  they should be ported together with the SQL surface.
