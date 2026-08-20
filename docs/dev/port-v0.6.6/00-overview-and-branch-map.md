# Porting feat/v0.6.6 → feat/0.7 — Overview, Branch Map & Port Strategy

> This is the top-level document of a multi-file port inventory. It explains **exactly
> what is in `feat/v0.6.6` that is not in `feat/0.7`**, how the branches relate, commit
> by commit, and how to approach the port. The feature-level detail lives in the sibling
> files in this directory.
>
> Ground truth: the `feat/v0.6.6` working tree at commit **`b8804803`** (local ref
> `feat/v0.6.6` → `b8804803`; remote `kraytos17/feat/v0.6.6` → `b8804803`).

---

## 1. Branch topology — what actually has to be ported

> **Status: COMPLETE.** Phases 0–9 landed and verified on `feat/0.7.0`; Phase 10 **dropped**
> and Phase 11 **ported** (2026-08-18, rationale in `13-final-plan.md`). See the per-phase status in
> `13-final-plan.md`.

There are several `0.6.x`-looking refs; only one carries real work:

| Ref | HEAD | Relationship to `feat/0.7` | Port? |
|---|---|---|---|
| `feat/0.6.6` (local) | `68e73e02` | **Ancestor** of `feat/0.7` (merge-base is itself) | **Nothing to port** |
| `feat/sail-0.6.6` | `f090e646` | **Ancestor** of `feat/0.7` (merge-base is itself) | **Nothing to port** |
| `feat/v0.6.6` | `b8804803` | merge-base with `feat/0.7` = `f090e646`; 12 unique commits | **THE port surface** |
| `fix/iceberg-vend-credentials` | `8ff90862` | ancestor of `feat/0.7` | Nothing extra |
| `feat/iceberg-ops` | `3dc93e51` | temp work, ancestry via 68e73e02 | not part of the port |

So the entire port is the diff

```
git diff f090e646 b8804803        # 139 files changed, 21011 insertions(+), 477 deletions(-)
```

or equivalently the commit list `git log --oneline f090e646..b8804803`.

The cumulative code diff (excluding the `docs/dev/` planning docs) is ~14,800
insertions across 127 files.

---

## 2. The 12 commits on feat/v0.6.6 (oldest → newest)

Each commit is self-contained enough to be cherry-picked, but later commits build on
earlier ones and `feat/0.7` has diverged, so a manual re-port is required (see §4).

| Commit | Title | Functional content |
|---|---|---|
| `c96a5eb8` | rewrite fixes using Sail 0.6.6 | SQL analyzer statement rewrites (+649), Dockerfile rewrites, two big planning docs (`iceberg-row-level-ops-implementation-plan.md`, `sail-implementation-patterns.md`), `sail-session/src/catalog.rs` +4 |
| `64c04f41` | update impl | UPDATE: `sail-iceberg/logical/update.rs`, `sail-logical-plan/merge.rs` (`expand_update`, `UpdateExpansion`), `sail-plan/resolver/command/update.rs`, `physical_plan/scan_by_data_files_exec.rs` file-path column, `writer_exec.rs`/`writer_options.rs`, `table_format.rs` (+151) — 1790 insertions |
| `fafb8e50` | alter table impl | ALTER TABLE storage-level + catalog-level (`sail-iceberg/table_format.rs`, `sail-catalog-iceberg/provider.rs`, `sail-plan/resolver/command/mod.rs`, `sail-sql-analyzer/statement.rs`, parser AST + gold data) — 959 insertions |
| `48b63a14` | describe table cols, describe view | `sail-catalog-iceberg/provider.rs`, `sail-catalog/command.rs`, `sail-catalog/error.rs`, `sail-plan/resolver/command/mod.rs`, analyzer + parser — 410 insertions |
| `ad00db51` | load table natively via Sail (start impl) | LOAD DATA: `sail-iceberg/physical/load_classifier.rs`, `load_data_planner.rs`, `physical_plan/load_data_exec.rs`, `commit/commit_exec.rs` (+176), `sail-logical-plan/load_data.rs`, `sail-plan/resolver/command/load.rs` — 1948 insertions |
| `1456d174` | capture size also | size plumbing in `load_classifier.rs`, `load_data_planner.rs` (+96 each), `TEST_PLAN.md` — 180 insertions |
| `717f9c91` | iceberg snapshots + CALL stored procedures impl (WIP) | CALL parser/analyzer/AST/gold data, `sail-iceberg/physical/call_procedure_planner.rs`, `physical_plan/call_procedure_exec.rs`, `logical-plan/call_procedure.rs`, `resolver/command/call.rs`, metadata-table planning docs — 4763 insertions |
| `4983e685` | GC added for expire_snapshots | `physical_plan/expire_snapshots_gc.rs` (766 lines), `call_procedure_exec.rs` (+595), `call_procedure_planner.rs` (+18), `proto/codec.rs` (+117) — 1441 insertions |
| `c23cb52e` | temp WIP | heimdall flight docs + `k8s/sail.yaml`, `python/pysail/tests/flight/*`, iceberg write compression/parallelism plans, iceberg_rest `test_commit.py` — 2653 insertions |
| `b70f9c67` | worker issuance WIP | distributed exec: `driver/worker_pool/*` rewrite (+193 mod.rs), `task_assigner/state.rs`, `worker_manager/*`, `sail-server/retry.rs`, `proto` — 705 insertions |
| `d7ddf1d7` | task distribution is slightly more fair | `task_assigner/core.rs` (+376), `worker/peer_tracker/options.rs`, `worker_manager/kubernetes.rs`, `sail-flight/lib.rs`, `sail-iceberg/datasource/provider.rs` (+91), `sail-server/builder.rs` (+30) — 343 insertions |
| `881f63be` | add activity tracker to executor task context | `sail-common-datafusion/session/activity.rs`, `sail-spark-connect/executor.rs`, `service/plan_executor.rs` — 21 insertions |
| `b8804803` | add flight readiness gate | `sail-execution/driver/actor/handler.rs` (+12), `task_assigner/core.rs` (+44), `sail-session/session_manager/actor/handler.rs` (+276) — 297 insertions |

---

## 3. Functional surface grouped by subsystem

Every subsystem below has its own detailed document:

| Doc | Subsystem |
|---|---|
| `01-sql-frontend-parser-analyzer-spec.md` | `sail-sql-parser`, `sail-sql-analyzer`, `sail-common::spec` |
| `02-call-procedures.md` | `CALL <catalog>.system.*`, `CallProcedureExec`, expire GC |
| `03-row-level-operations.md` | UPDATE / DELETE / TRUNCATE / MERGE |
| `04-load-data.md` | `LOAD DATA INPATH` fast-register + rewrite |
| `05-metadata-tables.md` | `db.table.snapshots` / `db.table.refs` |
| `06-catalog-ddl-and-catalog-providers.md` | DESCRIBE / SHOW TBLPROPERTIES / ALTER TABLE + REST/memory/glue/hms/onelake providers |
| `07-distributed-execution.md` | worker pool accounting, readiness gate, spawn retry, activity tracker, session self-heal, HTTP/2 keepalive + RPC client hardening, codec |
| `08-config-docker-k8s-python.md` | config keys, Dockerfiles, `build.sh`, k8s manifest, Python tests |

## 3.1 Documentation deliverables added on the branch (also port candidates)

These 15 planning docs under `docs/dev/` plus `TEST_PLAN.md` were added between
`f090e646..b8804803` and exist **only** on `feat/v0.6.6`. They are a port surface of
their own (copy them over):

| Doc | Focus |
|---|---|
| `docs/dev/call-procedures-spec-deviations.md` | CALL grammar/argument deviations vs Apache Iceberg Spark |
| `docs/dev/expire-snapshots-file-gc-plan.md` | expire_snapshots physical file-GC design |
| `docs/dev/driver-worker-pool-accounting-plan.md` | worker-pool budget / idle-reap accounting (P0/P2) |
| `docs/dev/iceberg-row-level-ops-implementation-plan.md` | UPDATE/DELETE/MERGE design (largest doc) |
| `docs/dev/iceberg-metadata-tables-spark-analysis.md` | metadata-table parity analysis |
| `docs/dev/heimdall-parity-plan.md` | heimdall Spark-parity test plan |
| `docs/dev/heimdall-flight-sql-audit-plan.md` | Flight SQL audit |
| `docs/dev/heimdall-flight-sql-migration.md` | Flight SQL migration notes |
| `docs/dev/iceberg-catalog-replace-plan.md` | catalog REPLACE/CREATE OR REPLACE design |
| `docs/dev/iceberg-write-compression-plan.md` | parquet compression option design |
| `docs/dev/iceberg-write-parallelism-plan.md` | writer parallelism / repartition design |
| `docs/dev/sail-implementation-patterns.md` | general Sail codebase conventions |
| `TEST_PLAN.md` | runnable PySpark-shell test plan for the whole surface |

### 3.2 Mechanical change to apply repo-wide during the port

Adding the `SourceInfo` field `metadata_table: Option<IcebergMetadataTableType>` forces
`metadata_table: None` into **every** `SourceInfo` literal. Files that only change for
this reason (no other logic):

- `crates/sail-plan/src/resolver/command/delete.rs` (+1)
- `crates/sail-plan/src/resolver/command/delta.rs` (+2, two literals)
- `crates/sail-plan/src/resolver/command/write.rs` (+2, two literals)
- `crates/sail-data-source/src/formats/rate/mod.rs` (`metadata_table: _`)
- `crates/sail-data-source/src/formats/socket/mod.rs` (`metadata_table: _`)
- `crates/sail-data-source/src/listing/source.rs` (`metadata_table: _`)
- `crates/sail-iceberg/src/datasource/provider.rs` (`metadata_table: _` in `build_iceberg_provider`)
- `crates/sail-iceberg/src/table_format.rs` (`metadata_table: _` in `build_iceberg_provider`'s destructure)
- `crates/sail-delta-lake/src/table_format.rs` (+3 `metadata_table: _`)

Do not port these as isolated commits; fold them into the `05-metadata-tables` work.

---

## 4. Port strategy

### 4.1 What merges cleanly vs what needs manual work

Every `feat/v0.6.6` commit was checked for overlap with files that `feat/0.7` itself
changed since `f090e646`. Pure-additions can be copied near-verbatim; overlapping files
need a manual 3-way merge. Overlap counts per commit:

| Commit | Files also changed by feat/0.7 | Risk |
|---|---|---|
| `c96a5eb8` | 26 | HIGH (statement.rs, parser AST, docker files, session catalog) |
| `64c04f41` | 13 | HIGH (writer_exec, table_format, merge.rs, command/mod.rs) |
| `fafb8e50` | 11 | HIGH (statement.rs, AST, syntax.json, table_format) |
| `48b63a14` | 5 | MEDIUM (command.rs, provider.rs, AST) |
| `ad00db51` | 8 | MEDIUM (commit_exec, plan_builder, table_format) |
| `1456d174` | 0 | LOW (pure additions) |
| `717f9c91` | 22 | HIGH (statement.rs, AST, keywords, syntax.json, session manager) |
| `4983e685` | 2 | MEDIUM (codec.rs, call_procedure_exec) |
| `c23cb52e` | 7 | MEDIUM (test_commit.py, k8s, docs) |
| `b70f9c67` | 11 | MEDIUM (worker_pool, worker_manager) |
| `d7ddf1d7` | 18 | HIGH (task_assigner/core, kubernetes.rs, server/builder) |
| `881f63be` | 2 | LOW (executor.rs, plan_executor.rs) |
| `b8804803` | 2 | MEDIUM (session handler, task_assigner) |

### 4.2 Recommended order (bottom-up, so each layer compiles)

1. **Spec + parser + analyzer** (`sail-common::spec`, `sail-sql-parser`, `sail-sql-analyzer`) — pure AST/spec additions.
2. **Shared common-datafusion surface** (`SourceInfo.metadata_table`, `UpdateInfo`/`UpdateAssignment`, `MergeCapableSource`, `TableFormat` trait methods, `IcebergMetadataTableType`, `ActivityTracker`).
3. **Logical plans** (`sail-logical-plan`: `call_procedure.rs`, `load_data.rs`, `merge.rs`).
4. **Resolvers** (`sail-plan`: `command/call.rs`, `command/load.rs`, `command/update.rs`, `command/mod.rs`, `query/read.rs`, `command/catalog/table.rs`).
5. **Catalog commands + providers** (`sail-catalog/command.rs`, `error.rs`; Iceberg REST provider; memory rename; glue/hms/onelake stubs; `provider/options.rs`).
6. **Iceberg physical layer** (`sail-iceberg`: `table_format.rs`, `logical/update.rs`, `physical/*`, `physical_plan/planner/*`, `action_schema.rs`, `commit/commit_exec.rs`, `call_procedure_exec.rs`, `expire_snapshots_gc.rs`, `load_data_exec.rs`, `scan_by_data_files_exec.rs`, `writer_exec.rs`, `writer_options.rs`, `metadata_table.rs`, `operations/snapshot.rs`, spec snapshot/table_metadata accessors, `utils/metadata.rs`, `data/options/iceberg.yaml`, Cargo.toml).
7. **Distributed execution** (`sail-execution`: worker_pool, task_assigner, driver handler, worker_manager, codec + `physical.proto`; `sail-server`: builder + retry; `sail-session`: session manager handler; `sail-common-datafusion`: activity; `sail-spark-connect` + `sail-flight`: keepalive; `sail-common`: config).
8. **Docker / K8s / build.sh / Python tests.**

### 4.3 Key risks to verify during port

- `commit_exec.rs` merge is the most delicate: parent-manifest filtering, `Delete` /
  `Replace` / `Overwrite` operations, `reported_row_count`, stale-metadata-file conflict
  detection — all in one file with heavy `feat/0.7` churn.
- `statement.rs` (+784) and `sail-iceberg/table_format.rs` (+725) have the most
  three-way-merge surface.
- The `IcebergWriterExec` distribution change (single-partition → hash-partitioned for
  partitioned tables, plus `UnspecifiedDistribution` for unpartitioned) interacts with
  `feat/0.7`'s optimizer.
- `physical.proto` message numbers 55/56 (`iceberg_load_data_fast`, `call_procedure`)
  must not collide with any `feat/0.7` additions.

---

## 5. Pre-existing machinery the port depends on (do NOT port)

These already exist on both branches; the v0.6.6 code merely *uses* them:

- `crates/sail-iceberg/src/catalog_support/commit.rs` — `IcebergCatalogCommitCoordinator`,
  `IcebergCatalogCommitMode` (`Filesystem | MetadataLocationCas | CatalogCommit |
  CompatibilityCatalogCommit`), `CatalogCommitOutcome::Committed/NotSupported/Conflict`,
  `CatalogTableInfo`, `CatalogCommittedTable::metadata_location()`,
  `LakehouseCommitOutcome`. (`catalog_support/commit.rs` + `mod.rs` were **not** touched
  by the diff.)
- `LakehouseExecutionContext`, `LakehouseOperation` (`Maintenance` already existed),
  `CommitAuthority`, `ScanAuthority`, `TableKind`, `TableStatus`.
- `OptionLayer`, `PhysicalSinkMode`, `SinkMode`, `MergeCapableSource`'s Delta counterpart
  (`MERGE_FILE_COLUMN` const lives in `sail-common-datafusion::datasource`).
- `Table::load`, `find_latest_metadata_file`, metadata loaders, `StoreContext`,
  `Transaction`/`SnapshotProducer` (extended, see `03/10`), `Manifest`/`ManifestList`
  readers.

---

## 6. Test surface added

- Rust unit tests inside the new files (see per-feature docs): `call_procedure_exec.rs`
  (~20 tests), `expire_snapshots_gc.rs` (6 tokio tests), `load_classifier.rs`,
  `load_data_planner.rs`, `scan_by_data_files_exec.rs`, `action_schema.rs`,
  `worker_pool/mod.rs`, `task_assigner/core.rs`, `worker_manager/kubernetes.rs`,
  `session_manager/actor/handler.rs`, `command.rs` (SHOW TBLPROPERTIES).
- Integration: `crates/sail-catalog-iceberg/tests/rest_integration_test.rs` (alter
  rename/properties/add/drop columns, create-or-replace).
- Python: `python/pysail/tests/flight/test_flight_heimdall.py`,
  `python/pysail/tests/spark/catalog/iceberg_rest/test_commit.py`
  (`test_create_or_replace_table_as_select_replaces_rest_catalog_metadata`).
- Gold data: `crates/sail-sql-parser/tests/gold_data/syntax.json` (+95).
