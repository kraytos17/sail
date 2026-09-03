# Porting feat/0.7.0 → feat/0.7.1 — Doc 10: Config Surface, Docker, K8s, Python Tests & Build Tooling

> Part of the `docs/dev/port-v0.7.0/` inventory. Captures the non-crate delta on
> `feat/0.7.0` vs base `f0b137d6` that a port must not lose: the runnable test plan, image
> build tooling, deployment manifests and Python integration tests. Ground truth:
> `feat/0.7.0` tip `c07ad0c8`.

---

## 1. Scope

| File | Delta |
|---|---|
| `TEST_PLAN.md` | NEW — 722-line runnable PySpark-shell test plan for the whole v0.7.0 Iceberg surface |
| `build.sh` | NEW — 276-line `docker buildx` wrapper |
| `docker/dev/Dockerfile` | cargo-chef cache layering; `pyspark[connect]`; non-root `sail` user; OCI labels |
| `docker/quickstart/Dockerfile` | same runtime hardening for the `pysail` image |
| `docker/release/Dockerfile` | chef layering over a git-cloned release tag; same hardening |
| `k8s/kustomization.yaml`, `k8s/sail.yaml`, `k8s/test-volume-patch.yaml` | combined multi-protocol `sail-server` deployment |
| `docs/guide/deployment/kubernetes.md`, `README.md` | port-forward `service/sail 15002:15002`; `sc://localhost:15002` |
| `python/pysail/tests/flight/conftest.py`, `test_flight_heimdall.py` | Flight SQL heimdall-parity tests (new module fixture) |
| `python/pysail/tests/spark/catalog/iceberg_rest/test_commit.py` | CTAS `CREATE OR REPLACE` test |
| `Cargo.lock` | +9 lines (new deps for the crates above; `sail-cli` tonic/arrow-flight, `sail-iceberg` tokio/datafusion-datasource/sail-logical-plan/tempfile) |

---

## 2. `TEST_PLAN.md` (NEW)

Copy-paste PySpark-shell commands for a catalog-managed Iceberg environment (Polaris REST
`commit = IcebergRestCommit`, MinIO `s3://work/`, namespace `test1`). This is the manual
acceptance suite the branch was validated with and doubles as an executable spec:

- Setup, CREATE TABLE forms (basic / partitioned identity / `years`,`months` transforms /
  bucketed `CLUSTERED BY … INTO … BUCKETS` / partitioned+bucketed / `bucket(16, col)`).
- INSERT INTO (append), INSERT OVERWRITE (full + dynamic partition), `REPLACE WHERE`
  predicate overwrite.
- DELETE FROM (partition + non-partition predicates), TRUNCATE.
- UPDATE targeted rewrite incl. **count semantics** (reports rows *matched*, not rows
  written), multi-column, all-rows, no-match (count 0).
- Temp views; DESCRIBE TABLE / EXTENDED / VIEW / column-level; parser gotchas (bare
  `DESCRIBE` rejected, `DESCRIBE VIEW EXTENDED` order).
- ALTER TABLE on catalog-managed tables (RENAME TO, SET/UNSET TBLPROPERTIES incl.
  not-set and IF EXISTS semantics, ADD/DROP COLUMNS, DROP COLUMNS IF EXISTS no-op);
  SHOW TBLPROPERTIES (sorted, key lookup, empty for missing).
- MERGE INTO with UPDATE + INSERT clauses.
- End-to-end workflow mixing UPDATE/INSERT OVERWRITE/DELETE/ALTER/RENAME.
- **LOAD DATA** (§12): full `s3a://` URLs required; parquet fast path (footer register, no
  rewrite), glob/directory, OVERWRITE, CSV fallback rewrite, schema-mismatch fallback,
  unsupported LOCAL/PARTITION/bare-key, cross-bucket, mixed-format count summing,
  empty source (no-op snapshot), many-CSV parallel writers, compressed CSV
  (gzip/zstd from extension), partitioned tables (fast path disabled → rewrite).
- **CALL procedures** (§13): snapshot-history setup; `.refs`/`.snapshots` inspection;
  rollback_to_snapshot (ancestor rule), set_current_snapshot (snapshot_id xor ref),
  expire_snapshots (older_than TIMESTAMP / retain_last; six deleted counts; physical GC on
  the filesystem path); named args; error cases (non-system namespace, unknown procedure,
  nonexistent snapshot, view target).
- §14: exhaustive known-limitations list (parser gaps, flat nested columns, REST rejections,
  LOAD DATA v1 limits incl. missing typed bounds, catalog-managed vs filesystem ALTER, etc.).

Port note: this file is documentation-only but encodes expected behaviors/error strings that
the ported code must reproduce; several error-message assertions appear again in Python tests.

---

## 3. `build.sh` (NEW, executable)

BuildKit wrapper for Sail images (`docker buildx build`, fallback to `docker build`):
flags `-p/--profile` (dev|test|release|bench), `-o/--optimized`, `-t/--tag <release-tag>`
(forces `release` profile and `docker/release/Dockerfile` + `RELEASE_TAG` build-arg),
`--rust-version` (default 1.96.0), `--pyspark-version` (4.2.0), `--python-image`
(`python:3.14-slim`), `--image`, `--platform` (multi-arch requires `--push`), `--push`,
`--load`, `--cache-ref` (registry cache-from/to, `mode=max`; auto-creates a
`docker-container` builder `sail-builder`), `--builder`, `--progress`, `--metadata-file`,
`--no-cache`, `--dry-run`, `-h`. Build args passed: `RUST_VERSION`, `RUST_PROFILE`,
`PYSPARK_VERSION`, `PYTHON_IMAGE`, `RELEASE_TAG`. Ends with the E2E hint
`docker run --rm -i <img> spark run -f -`.

---

## 4. Dockerfiles

All three now: use `PYTHON_IMAGE` build-arg (`python:3.14-slim`), install
**`pyspark[connect]==…`** (was `pyspark-client`), add a non-root system user
`groupadd --system --gid 10001 sail` / `useradd --system --uid 10001 …`, `USER sail`,
`WORKDIR /home/sail`, `ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1`, OCI labels
(`org.opencontainers.image.title=sail`, `source=…lakehq/sail`).

- **dev/Dockerfile**: switched to **cargo-chef** staging — `chef` stage (apt deps incl.
  libprotobuf-dev etc. with `rm -rf /var/lib/apt/lists/*`; rustup `--profile minimal
  --component rustfmt`), `cargo install cargo-chef --locked`; `planner` stage runs
  `cargo chef prepare` from `Cargo.toml`/`Cargo.lock`/`crates`; `builder` stage `cargo chef
  cook` (recipe = manifest-only layer, caches registry/git/target mounts `sharing=locked`;
  `--release` only for release/bench profiles), then `COPY . .` and
  `cargo build -p sail-cli --profile … --bins --locked`, copying the binary.
- **release/Dockerfile**: adds a `source` stage that `git clone --depth 1 --branch
  RELEASE_TAG …lakehq/sail` (guarded by `test -n "${RELEASE_TAG}"`); planner/builder consume
  the cloned manifests/source.
- **quickstart/Dockerfile**: installs `pysail==PYSAIL_VERSION` then `pyspark[connect]==…`
  + the hardening (no Rust build).

---

## 5. Kubernetes (`k8s/`)

The deployment switches from a spark-connect-only `sail-spark-server` to the combined
multi-protocol **`sail-server`** (doc 02):

- Deployment `sail-server` (label `component: server`), replicas 1, container runs the new
  CLI verb:
  `sail server --ip 0.0.0.0 --spark-port 50051 --flight-port 32010 --mux-port 15002`
  (comment: one process serves both protocols off one shared session manager; port 15002
  multiplexes every Spark Connect client onto a single auto-generated canonical session).
- Ports: 15002 `spark-connect-mux`, 32010 `flight-sql` (50051 spark port still bound in the
  container but not exposed as a Service port).
- Worker-pool env for the shared canonical session: `SAIL_CLUSTER__WORKER_INITIAL_COUNT=4`,
  `SAIL_CLUSTER__WORKER_MAX_COUNT=4`, `SAIL_CLUSTER__WORKER_TASK_SLOTS=4`,
  `SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS=3600` (capacity rule `MAX_COUNT × TASK_SLOTS ≥
  peak concurrent tasks`).
- Service renamed `sail` (selector `component: server`) exposing 15002 + 32010.
- Role/RoleBinding renamed `sail-server`; `kustomization.yaml` test-volume patch target
  `name: sail-server`; `test-volume-patch.yaml` name updated.
- Docs updated: `kubectl -n sail port-forward service/sail 15002:15002`,
  `SPARK_REMOTE="sc://localhost:15002"` for `pyspark` and `pytest --pyargs pysail`;
  README quickstart reflects the same port.

Port note: **renames break compatibility** with anyone using the old `sail-spark-server`
service/manifests; the multi-port layout and canonical-session sizing env must be ported
together with doc 02's combined server.

---

## 6. Python tests

### 6.1 `flight/conftest.py`

New `flight_catalog_uri` module-scoped fixture: starts a `pysail.flight.FlightSqlServer`
(ip 127.0.0.1, port 0 = ephemeral) in the background on Sail's **default Memory catalog**,
creates an Iceberg warehouse dir under `tmp_path_factory`, yields `(grpc:// URI,
warehouse)`, always stops the server. Existing warning-suppression (`pytest_configure`)
remains.

### 6.2 `flight/test_flight_heimdall.py` (NEW, 252 LOC)

End-to-end Flight SQL tests of the heimdall surface over **local `file://` Iceberg tables**
(no external catalog/object store). Helpers build CSV files, `CREATE TABLE … USING iceberg
LOCATION 'file://…'`, `LOAD DATA INPATH`, snapshot introspection via
`<t>.snapshots`/`<t>.refs`. Tests:

- `test_load_data_and_read_back` — CSV load then read.
- `test_load_data_overwrite_replaces_rows` — OVERWRITE replaces rows.
- `test_refs_and_snapshots_metadata_tables` — `(main, branch)` in refs; current-snapshot /
  exists / parent-id query shapes.
- `test_truncate_table` — count 0 after TRUNCATE, append works afterwards.
- `test_rollback_and_set_current_snapshot` — CALL on `sail.system.*`, output row
  previous/current ids, refs updated.
- `test_rollback_rejects_non_ancestor` — `…not an ancestor of the current state`.
- `test_expire_snapshots_returns_counts_and_removes_files` — six-count row, fewer snapshots
  remain, retained data readable.
- `test_version_as_of` — `SELECT … VERSION AS OF <older>` sees only the older rows.

### 6.3 `spark/catalog/iceberg_rest/test_commit.py`

Adds `test_create_or_replace_table_as_select_replaces_rest_catalog_metadata`: two consecutive
`CREATE OR REPLACE TABLE … AS SELECT` against the REST catalog — first creates, second drops
+ recreates with a **new metadata location/version**; asserts the metadata-log records the
first location and current data reads back. (Previously the REST provider rejected
REPLACE — doc 06 §5.2.)

---

## 7. Port notes / risks

1. Non-code files carry naming/layout decisions (Service rename, port mapping, canonical
   session env) that must land *with* the code they reference — don't port in isolation.
2. `build.sh`/Dockerfiles embed toolchain versions (Rust 1.96.0, pyspark 4.2.0, python 3.14)
   — align with 0.7.1's own pinned versions rather than blindly copying.
3. `Cargo.lock` churn follows automatically from the crate dependency additions (doc 09
   Cargo.toml etc.).
4. The Python Flight tests require `pysail.flight.FlightSqlServer`'s
   `start(background=True)`/`listening_address` API — verify these exist on 0.7.1's `pysail`.
