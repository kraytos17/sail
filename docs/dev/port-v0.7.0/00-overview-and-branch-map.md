# Porting feat/0.7.0 → feat/0.7.1 — Overview, Branch Map & Port Inventory

> Top-level document of the `docs/dev/port-v0.7.0/` inventory: **exactly what is in
> `feat/0.7.0` that is not in `feat/0.7.1`**, commit by commit, and where the detail lives.
> Every feature/subsystem has its own file in this directory. Only the `feat/0.7.0` surface is
> inventoried here (the earlier `docs/dev/port-v0.6.6/` set and the older plan docs on the
> 0.7.0 branch are out of scope for this port).
>
> Ground truth: `feat/0.7.0` working tree at commit **`c07ad0c8`** (local `feat/0.7.0` =
> `kraytos17/feat/0.7.0`). Target: `feat/0.7.1` at `9544c925`.

---

## 1. Branch topology

| Ref | Relationship to the port |
|---|---|
| `feat/0.7.0` (source) | tip `c07ad0c8`; 11 unique commits over the shared base |
| `f0b137d6` (`chore: prepare v0.7.0`) | **merge-base** with `feat/0.7.1`; the shared ancestor |
| `feat/0.7.1` (target) | tip `9544c925` (`chore: prepare v0.7.1`); **82 commits ahead** of the merge base (all upstream post-0.7.0 work), 0 behind the source |

The entire portable surface is the diff

```
git diff f0b137d6 c07ad0c8        # 176 files, +28 257 / −747
git log --oneline f0b137d6..feat/0.7.0
```

(the 0.7.0 feature work was authored directly on the v0.7.0 prepare commit and never merged;
0.7.1 contains everything upstream did afterwards, so none of these 11 commits apply cleanly —
this is a **re-implementation port**, not a cherry-pick series.)

---

## 2. The 11 commits on feat/0.7.0 (oldest → newest)

| Commit | Title | Functional content |
|---|---|---|
| `ee78c38d` | Big bang refactor to Sail v0.7.0 | the bulk: iceberg row-level DELETE/UPDATE/MERGE planner + targeted rewrite (`physical_plan/planner/*`, `logical/update.rs`), `IcebergCommitExec` Delete/Replace/overwrite-partition/overwrite-predicate + reported count + stale-metadata conflict handling, `IcebergTableFormat::{create_deleter,create_updater,create_merger,call_procedure,retry_metadata_commit}`, metadata tables (`datasource/metadata_table.rs`, `IcebergMetadataTableType`, resolver read hook), procedures (`operations/procedure.rs`) + expire GC (`expire_snapshots_gc.rs`), LOAD DATA classifier/planner/fast-exec/`LoadDataNode`, writer hardening (`arrow_parquet`, `async_buffer`, `table_writer`, `writer_options`), file-path scan column, session/exec config & CLI pieces, catalog ALTER surface, parser/analyzer/spec (CALL/TRUNCATE/SHOW TBLPROPERTIES/ALTER COLUMNS), sql-analyzer/statement gold data, config/application yaml, docker/k8s/build, python heimdall flight tests, TEST_PLAN |
| `efcfca36` | some changes for LOAD path | `IcebergWriterExecOptions`/`writer_exec` LOAD fallback wiring (compression/`target_file_size_bytes` from resolved options), `table_writer` partition-values + size rolling, `arrow_parquet` writer-property builder |
| `b984b8bc` | WIP session fixing | `sail-session/src/session_config.rs` (`SessionConfigFactory`), worker session config parity, `sail-cli` `combo.rs`/`runner.rs` combined-server scaffolding, session-manager actor event additions |
| `46b73948` | revert hacky session ID fix; add multiplexed spark connect server | `sail-spark-connect/src/multiplexer.rs`, canonical session id + `config.server.session_id`, k8s multi-port manifest |
| `b72a956e` | WIP | worker pool `delete_worker` lifecycle (k8s pod delete), `running_worker_count`, idle scale-down gate, task-assigner `deactivate` rework, RPC peer tagging |
| `9e455322` | scalar subquery fix WIP | `job_graph/planner.rs` SubqueryIndex-set scalar-subquery tracking, stage `encoded_plan` caching, file-scan rewrite moved to plan time |
| `b603f9ff` | revert faulty fix | partial planner revert + codec predicate-drop guard |
| `49eba34b` | detect ScalarSubqueryExpr in ParquetSource page filter (remote exec) | `codec.rs` `contains_scalar_subquery_expr` + predicate drop on encode |
| `11d729f1` | some more fixes | config keys (`server.*`, `object_store.*`, cluster timeout/attempt defaults), `server_config.rs` tests, multiplexer refinements |
| `48dca759` | ALTER + empty-table DELETE on catalog-managed iceberg | REST catalog `update_table` ALTER arms; catalog-authority ALTER delegation; empty-table DELETE/TRUNCATE no-op; `CatalogObject::Column`; describe column |
| `c07ad0c8` | temp WIP | object-store registry S3 client tuning (`ObjectStoreConfig`), s3 region/client options |

---

## 3. Functional surface grouped by subsystem → doc

| Doc | Subsystem |
|---|---|
| `00` (this file) | overview / branch map / port strategy |
| `01-session-runtime-and-config.md` | `ServerConfig`/`ObjectStoreConfig`/`IcebergRestAccessDelegation` config; `SessionConfigFactory` (driver/worker parity); session idle-duration actor event; keepalive |
| `02-spark-connect-multiplexer.md` | `MultiplexedSparkConnectServer`, canonical session, `SailFlightSqlService::with_default_session`, CLI `server` (combined Spark Connect + Flight SQL) |
| `03-distributed-execution-worker-pool.md` | job-graph stage encoding + scalar-subquery indexing + file-scan rewrite; worker lifecycle/delete/prune; RPC peer tagging + keepalive; proto/codec (`iceberg_load_data_fast` #56, `file_path_column`) |
| `04-object-store-registry.md` | S3 client tuning via `ObjectStoreConfig` |
| `05-sql-frontend-parser-analyzer.md` | CALL / TRUNCATE / SHOW TBLPROPERTIES / DESCRIBE VIEW / ALTER TABLE ops in spec+parser+analyzer |
| `06-catalog-providers-and-ddl.md` | catalog command layer, REST provider ALTER + CREATE-OR-REPLACE + access delegation, DESCRIBE/SHOW TBLPROPERTIES/CALL dispatch, `TableFormat` contract |
| `07-iceberg-row-level-operations.md` | UPDATE/DELETE/TRUNCATE/MERGE targeted rewrite end-to-end (logical → planner → commit) |
| `08-iceberg-load-data-and-write-path.md` | LOAD DATA fast-register + fallback; data-file writer rolling/compression/options |
| `09-iceberg-procedures-metadata-tables-gc.md` | `CALL …system.{rollback_to_snapshot,set_current_snapshot,expire_snapshots}`, `db.table.{snapshots,refs}`, expire file GC |
| `10-config-docker-k8s-python-build.md` | TEST_PLAN, Dockerfiles, k8s, build.sh, Python tests |
| `11-gap-analysis-vs-0.7.1.md` | per-cluster 0.7.1 overlap, port order, strategic decisions |

---

## 3.1 Branch-local documentation (deliberately NOT ported as code)

The 0.7.0 diff also adds 29 files under `docs/dev/` (the older plan/design docs and the whole
`docs/dev/port-v0.6.6/` set) plus `docs/guide/deployment/kubernetes.md`. These are the branch's
own working docs, not port surface — the `port-v0.6.6/` set documents an earlier (completed)
port and is **out of scope per project instruction**. The design docs that are still useful as
reference for THIS port (they live on the source branch and can be read there during the
re-implementation) include: `docs/dev/iceberg-row-level-ops-implementation-plan.md`,
`docs/dev/call-procedures-spec-deviations.md`, `docs/dev/expire-snapshots-file-gc-plan.md`,
`docs/dev/iceberg-metadata-tables-spark-analysis.md`, `docs/dev/iceberg-write-compression-plan.md`,
`docs/dev/iceberg-write-parallelism-plan.md`, `docs/dev/iceberg-catalog-replace-plan.md`,
`docs/dev/driver-worker-pool-accounting-plan.md`, `docs/dev/actor-worker-scheduling-architecture.md`
and the heimdall notes. Copy/adapt them only if they should live on the target branch; they are
not compiled artifacts. The current inventory (`docs/dev/port-v0.7.0/`) is the authoritative
per-file port reference.

---

## 4. Mechanical changes to carry with their feature (do NOT port as isolated commits)

- `SourceInfo.metadata_table: Option<IcebergMetadataTableType>` — new field forcing
  `metadata_table: None`/`_` into every exhaustive `SourceInfo` destructure (rate/socket/
  listing formats, Delta, iceberg provider/table_format, resolvers delete/delta/merge/write/
  read). Fold into the metadata-tables work (docs 06/07/09).
- `Identifier`/`ObjectName` `PartialOrd`+`Ord` derives (spec) — pure compile surface for
  sorting needs.
- `CommitMeta`/`IcebergWriterExecOptions` extra fields (`touched_file_paths`,
  `overwrite_predicate`, `overwrite_partition_values`, `commit_operation`, `reported_row_count`)
  — ride along with docs 07/08.
- `metadata_files_for_version()` returning `(path, timestamp)` — ripples through
  `commit_exec.rs` and `table_format.rs` conflict handling.
- New default values in `application.yaml` (`task_stream_creation_timeout_secs` 120,
  `task_max_attempts` 5, keepalive 120 s) — deliberate behavior changes, adopt explicitly.

---

## 5. Port strategy & risks (full analysis in doc 11)

**Order (bottom-up):** 05 frontend → 01 config+session → 06 shared contracts + catalog →
04 object store / 03 execution additions → 08 write path → 07 row-level ops (decision) →
08 LOAD → 09 procedures/metadata/GC → 10 deployment/tests.

**Highest risks**
1. **07 row-level ops** — 0.7.1 ships its own Iceberg DELETE/MERGE (delete-writer/DV stack);
   0.7.0 replaces it with targeted rewrite. Requires a supersede-vs-gap decision (UPDATE is the
   unambiguous gap on 0.7.1).
2. **Commit-exec merge** — parent-manifest filtering, `Delete`/`Replace`, reported count,
   stale-metadata handling all live in one file that upstream churned heavily.
3. **Writer behavior change** (zstd default, 128 MB rolling, hash-partition requirement for
   partitioned writes) applies to every write on 0.7.1.
4. **Proto numbering** (`IcebergLoadDataFastExecNode` = 56, `file_path_column`) must not
   collide with 0.7.1 additions; codec predicate-drop depends on DataFusion scalar-subquery
   internals matching 0.7.1's pinned version.
5. **k8s rename + port layout** is externally visible — keep paired with the multiplexer.

---

## 6. Test surface added (with the source, for re-validation)

- Rust unit tests embedded in every new/changed module (docs list them per file): multiplexer
  registry/stamping; worker-pool pruning/counting; codec `iceberg_load_data_fast` round-trip;
  `commit_exec` reported-count merge; `action_schema` overwrite-field round-trips; metadata
  utils staleness; `session_config`/`worker` config-parity; `statement.rs`/`parser.rs`
  ALTER/CALL/TRUNCATE; REST catalog DDL wiremock tests; metadata-table + heimdall query tests;
  expire-GC object-store tests; load classifier/planner tests; writer rolling/props tests;
  `server_config` deserialization tests.
- Python: `python/pysail/tests/flight/test_flight_heimdall.py` (LOAD, refs/snapshots, TRUNCATE,
  rollback/set_current, expire, VERSION AS OF), `flight/conftest.py` fixture,
  `python/pysail/tests/spark/catalog/iceberg_rest/test_commit.py`
  (CREATE OR REPLACE CTAS on REST catalog).
- Gold data: `sail-sql-parser/tests/gold_data/syntax.json`, `sail-spark-connect/tests/gold_data/
  plan/ddl_alter_table.json`.
- Manual: `TEST_PLAN.md` (722-line PySpark-shell acceptance run).
