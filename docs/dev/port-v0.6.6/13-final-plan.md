# 13 — FINAL PLAN (Decision-Locked, MCP-Verified)

> **Status: COMPLETE.** All phases 0–9 landed and verified; Phase 10 **dropped**;
> Phase 11 **ported** (2026-08-18 — tooling tail, see below). The v0.6.6 → v0.7.0 port is done.
>
> Resolved + locked architecture decisions:
>
> 1. **CALL procedures → Track A** (`CatalogCommand::CallProcedure` + a new
>    `TableFormat::call_procedure` method). The v0.6.6 Track-B CALL machinery
>    (`CallProcedureNode`, `CallProcedureExec`, planner arm, driver placement, codec
>    node) is **NOT ported**.
> 2. **LOAD DATA → Track B** with a dedicated `LoadDataNode` (fast parquet-register
>    preserved). The INSERT-rewrite alternative is **rejected**.
>
> This document is the single execution blueprint. It consolidates and supersedes
> `11-refactor-plan.md` (phase order) and `12-architecture-and-track-decisions.md`
> (rationale) into one decision-locked plan. All symbol locations below were verified
> against the **`feat/0.7.0` = `tag/v0.7.0`** tree via the codebase graph (MCP) and
> source reads.

---

## 1. Verified anchors (graph-verified locations)

| Symbol | Where it lives (v0.7.0) | Verified |
|---|---|---|
| `CatalogCommand` (enum, serializable) | `crates/sail-catalog/src/command.rs:26` | yes |
| `CatalogCommandNode` (leaf logical node) | `crates/sail-plan/src/catalog.rs:21-26` | graph |
| `CatalogCommandExec` (leaf physical exec) | `crates/sail-physical-plan/src/catalog_command.rs:20-24` | graph |
| `TableFormat` trait (incl. `create_deleter`/`create_merger`, default `not_impl_err`) | `crates/sail-common-datafusion/src/datasource.rs:478-` | source |
| `TableFormatRegistry` | `crates/sail-common-datafusion/src/datasource.rs:625` | source |
| `OptionLayer` / `SourceInfo` / `SinkInfo` / `DeleteInfo` / `MergeInfo` | `crates/sail-common-datafusion/src/datasource.rs` (47 / 189 / 267 / 291 / 303) | source |
| `AlterTableOptions` | `crates/sail-catalog/src/provider/options.rs:139` | source |
| `TableFormatAlterTableOperation` | `crates/sail-common-datafusion/src/datasource.rs:451` | source |
| `RowLevelWriteNode` (+ `expand_merge`, `MergeCardinalityCheckNode`) | `crates/sail-logical-plan/src/merge.rs:143` | graph |
| `MergeCapableSource` trait (+ Delta impl) | `crates/sail-common-datafusion/src/datasource.rs`; `sail-delta-lake/.../table_source.rs` | source |
| `IcebergPhysicalPlanner` | `crates/sail-iceberg/src/physical/table_scan_planner.rs:15` | graph |
| `IcebergCatalogCommitCoordinator` / `IcebergCatalogCommitMode` / `CatalogCommitOutcome` | `crates/sail-iceberg/src/catalog_support/commit.rs:119 / 64 / 23` | graph |
| `IcebergWriterExec` / `IcebergCommitExec` / `IcebergScanByDataFilesExec` / `IcebergDiscoveryExec` / `IcebergManifestScanExec` / `IcebergDeleteApplyExec` | `crates/sail-iceberg/src/physical_plan/**` | source |
| resolver dispatcher `resolve_command_plan` | `crates/sail-plan/src/resolver/command/mod.rs:34-358` | graph |
| `ActivityTracker` | `crates/sail-common-datafusion/src/session/activity.rs:8` | source |
| `ServerBuilderOptions` (keepalive fields) | `crates/sail-server/src/builder.rs:15` | source |
| driver server (Track-independent) | `crates/sail-execution/src/driver/gateway.rs:130` | source |

---

## 2. The two tracks (recap)

**Track A — metadata commands (no data flow):**
```
resolver → CatalogCommandNode (leaf) → CatalogCommandExec (leaf)
        → CatalogCommand::execute(ctx, manager) → one RecordBatch
        → storage ops reach the format via TableFormatRegistry.get(format).<method>
          (precedents: alter_table, create_table_metadata)
```

**Track B — dataflow (rows move through executors):**
```
resolver → TableFormat::create_* (returns LogicalPlan)
        → real logical node (IcebergWriteNode / RowLevelWriteNode / LoadDataNode / provider TableScan)
        → format ExtensionPlanner::plan_extension → physical executor tree
        → writes funnel through the format commit exec
```

**Decision rule:** read-as-table or moves-rows ⇒ **Track B**; otherwise metadata
maintenance returning rows ⇒ **Track A**.

---

## 3. Master decision table (locked)

| # | Feature | Track | Locked decision |
|---|---|---|---|
| 1 | SHOW TBLPROPERTIES | A | `CatalogCommand::ShowTblProperties` + `ShowTblPropertiesRow` |
| 2 | DESCRIBE TABLE `<col>` | A | add `column: Option<String>` to `CatalogCommand::DescribeTable` |
| 3 | DESCRIBE VIEW | A | `DescribeItem::View` in parser/analyzer/spec |
| 4 | ALTER TABLE (rename / add-drop cols / column comment-nullability-position) | A | 6 variants × `spec::AlterTableOperation`, `AlterTableOptions`, `TableFormatAlterTableOperation` + converter + REST provider + `IcebergRestCommit` routing fix |
| 5 | **CALL procedures** | **A** | `CatalogCommand::CallProcedure` + `TableFormat::call_procedure` + expire-GC utility (NOT the v0.6.6 exec) |
| 6 | Metadata tables (`db.table.snapshots`/`refs`) | B | `SourceInfo.metadata_table` + `IcebergMetadataTableProvider` + resolver hook |
| 7 | Row-level UPDATE/DELETE/TRUNCATE/MERGE | B | `create_updater`/`create_deleter`/`create_merger` + planner module + commit machinery |
| 8 | **LOAD DATA** | **B (dedicated node)** | `LoadDataNode` + `plan_load_data` + `IcebergLoadDataFastExec` (fast-register kept) |
| 9 | Worker pool / readiness / spawn-retry / RPC / session / config | — | port deltas (Phase 10) |

---

## 4. Finalized phases

### Phase 0 — Dependencies
| File | Change |
|---|---|
| `crates/sail-iceberg/Cargo.toml` | `+ sail-logical-plan`, `+ datafusion-datasource` (workspace), `[dev-dependencies] tokio` |
| `crates/sail-catalog-memory/Cargo.toml` | `+ sail-common` |
| `crates/sail-logical-plan/Cargo.toml` | `+ serde` |
| `crates/sail-iceberg/src/options.rs` | confirm generated options include still works after dep add |

**Verify:** `cargo check -p sail-iceberg -p sail-catalog-memory -p sail-logical-plan`

### Phase 1 — Spec + parser + analyzer (SQL front-end)
| File | Add |
|---|---|
| `crates/sail-common/src/spec/plan.rs` | `CommandNode::CallProcedure{name,arguments}`, `CommandNode::ShowTblProperties{table,property_key}`, 6 `AlterTableOperation` variants, `ColumnDefinition`/`ColumnAlterationOption`/`ColumnPosition` |
| `crates/sail-sql-parser/data/keywords.txt` | `CALL` |
| `crates/sail-sql-parser/src/ast/statement.rs` | `Statement::Call`, `Statement::ShowTblProperties`, `Statement::TruncateTable`, `Statement::View` |
| `crates/sail-sql-parser/tests/gold_data/syntax.json` | golden cases |
| `crates/sail-sql-analyzer/src/statement.rs` | arms: `Call`→`CallProcedure`, `ShowTblProperties`, `TruncateTable`→`Delete{None}`, `DescribeItem::View`, `from_ast_alter_table_operation` (6 ops) |
| `crates/sail-sql-analyzer/src/parser.rs` | wire new statements |

**Verify:** `cargo test -p sail-sql-parser -p sail-sql-analyzer`

### Phase 2 — Common surface (`sail-common-datafusion`)
| File | Add |
|---|---|
| `crates/sail-common-datafusion/src/datasource.rs` | `SourceInfo.metadata_table: Option<IcebergMetadataTableType>`; `UpdateInfo`; `UpdateAssignment`; `TableFormat::create_updater` default; 6 `TableFormatAlterTableOperation` variants + `Display` labels; **`TableFormatProcedureOperation`** (for Track-A CALL) + default `call_procedure` method (`not_impl_err!`) |
| `crates/sail-common-datafusion/src/catalog/iceberg.rs` | `IcebergMetadataTableType{Snapshots,Refs}` + `from_name` + `Display` |
| `crates/sail-common-datafusion/src/catalog/mod.rs` | `pub use iceberg::IcebergMetadataTableType;` |
| mechanical `metadata_table: None` | `resolver/command/{delete,delta,write}.rs`, `data-source/formats/{rate,socket}/mod.rs`, `data-source/listing/source.rs`, `sail-iceberg/datasource/provider.rs`, `sail-iceberg/table_format.rs`, `sail-delta-lake/table_format.rs` |

**Verify:** `cargo check`; `cargo test -p sail-common-datafusion`

### Phase 3 — Logical plans (`sail-logical-plan`)
| File | Add |
|---|---|
| `crates/sail-logical-plan/src/load_data.rs` (new) | `LoadDataNode` (`UserDefinedLogicalNodeCore`, leaf, carries location/overwrite/target context) |
| `crates/sail-logical-plan/src/lib.rs` | `pub mod load_data;` |
| `crates/sail-logical-plan/src/merge.rs` | **only** `expand_update` + `UpdateExpansion` (rest already in v0.7.0) |
| **NOT created** | `call_procedure.rs` (CALL is Track A) |

**Verify:** `cargo test -p sail-logical-plan`

### Phase 4 — Resolvers (`sail-plan`)
| File | Add |
|---|---|
| `crates/sail-plan/src/resolver/command/load.rs` (new) | `resolve_command_load_data` → `LoadDataNode` (replaces `todo!` at `mod.rs:308`) |
| `crates/sail-plan/src/resolver/command/update.rs` (new) | `resolve_command_update` → format `create_updater` |
| `crates/sail-plan/src/resolver/command/call.rs` (new) | `resolve_command_call_procedure` → **builds `CatalogCommand::CallProcedure`** (arg scalar coercion) → `resolve_catalog_command` |
| `crates/sail-plan/src/resolver/command/mod.rs` | `mod load; mod update; mod call;` + dispatch arms (`CallProcedure`, `LoadData`, `Update`) |
| `crates/sail-plan/src/resolver/command/catalog/table.rs` | `resolve_catalog_alter_table` 6-op mapping |
| `crates/sail-plan/src/resolver/query/read.rs` | `try_resolve_iceberg_metadata_table` |

**Verify:** `cargo check`; `cargo test -p sail-plan`

### Phase 5 — Catalog commands + providers (Track A heavy) ✅ DONE
| File | Add |
|---|---|
| `crates/sail-catalog/src/command.rs` | `CatalogCommand::ShowTblProperties` + `ShowTblPropertiesRow`; `DescribeTable{column}`; **`CatalogCommand::CallProcedure{table,procedure:CallProcedureOptions}`** + `execute` (resolves lakehouse → `table_format.call_procedure`) + schema/serializer; 6-op `table_format_alter_operation` |
| `crates/sail-catalog/src/provider/options.rs` | `CallProcedureOptions` enum; 6 new `AlterTableOptions` variants + `AddColumn` |
| `crates/sail-catalog-iceberg/src/provider.rs` | REST `create_or_replace`/`replace` (drop+purge), `alter_table` (rename/properties/add/drop), `alter_table_properties` (reserved-key guard), `map_update_table_alter_error`, `IcebergRestCatalogOptions.access_delegation` |
| `crates/sail-catalog-iceberg/src/lib.rs` | `pub use sail_common::config::IcebergRestAccessDelegation;` |
| `crates/sail-common/src/config/application.rs` | `IcebergRestAccessDelegation{VendedCredentials,None}`; field on `CatalogType::IcebergRest` |
| `crates/sail-session/src/catalog.rs` | access_delegation wiring |
| `crates/sail-catalog-memory/src/provider.rs` | `RenameTable` arm |
| glue / hms / onelake / delta | 6-op stubs + labels |

Note: the planned `CommitAuthority::IcebergRestCommit` ALTER branch in `command.rs` was **not needed** — `AlterTable.execute` already routes through `TableFormat::alter_table` → REST provider (Phase 5e), so Iceberg ALTER works without a commit-authority branch (that authority is only for commits).

**Verify:** `cargo test -p sail-catalog-iceberg` (rest integration) — 26 unit tests pass; workspace compiles green.

### Phase 6 — Metadata tables (Track B) + snapshot accessors ✅ DONE
| File | Add |
|---|---|
| `crates/sail-iceberg/src/datasource/metadata_table.rs` (new) | `IcebergMetadataTableProvider` (+ snapshots/refs schemas, batches, heimdall-style unit tests) |
| `crates/sail-iceberg/src/datasource/mod.rs` | `pub mod metadata_table;` |
| `crates/sail-iceberg/src/table_format.rs` | `create_source` metadata branch → `build_iceberg_metadata_source` |
| `crates/sail-iceberg/src/spec/metadata/table_metadata.rs` | `TableMetadata::snapshot(id)` |
| `crates/sail-iceberg/src/spec/snapshots/snapshot.rs` | `SnapshotReference::{min_snapshots_to_keep,max_snapshot_age_ms,max_ref_age_ms}` |

**Verify:** `cargo check`; unit tests on provider — 84 tests pass; workspace compiles green.

### Phase 7 — Row-level ops + commit machinery (Track B, largest) ✅ 7.1/7.2/7.3/7.4 core DONE
(unchanged from `11-refactor-plan.md` Phase 7: `logical/update.rs`, `MergeCapableSource` impl, `scan_by_data_files_exec` file column + proto/codec, `physical_plan/planner/*`, `row_level_planner.rs`, `table_scan_planner.rs` arm, writer metrics/distribution/compression/commit-options, `action_schema`+`commit/mod`+`commit_exec` machinery, `operations/snapshot.rs` parent-entries + op strings, `utils/metadata.rs` stale-file helpers, `table_format.rs` `retry_metadata_commit` + `extract_partition_predicate_from_expr`, `plan_builder.rs` careful merge, `iceberg.yaml` compression flip)

**Landed:** `logical/update.rs` `expand_update_node`; `MergeCapableSource` on `IcebergTableSource` (file column); `scan_by_data_files_exec` `file_path_column` + proto/codec; `RowLevelWriteNode::new_update`; planner module (`planner/{mod,context,helpers,commit,op_delete,op_update,op_merge}`) + `row_level_planner.rs` + `table_scan_planner` `RowLevelWriteNode` dispatch + file-column scan routing; `commit_exec.rs` (`reported_row_count` + `new_with_reported_row_count`, `compute_untouched_manifest_entries` (format-version threaded), `filter_parent_manifest_entries[_by_values]`, Overwrite parent-entries + Delete/Replace arms, stale-metadata conflict checks); `IcebergCommitInfo`/`CommitMeta` touched/predicate/partition-values; `snapshot.rs` `parent_manifest_entries` builder + op-string→Operation summary; `utils/metadata.rs` stale-file helpers + `metadata_files_for_version` timestamps; writer_exec `commit_operation` override + `CommitMeta` population + `HashSet` partition-values extraction; compression (zstd default, `resolve_compression_codec` + `WriterConfig.writer_properties` + tests); **7.3 writer hardening**: `ExecutionPlanMetricsSet`/`MetricBuilder` (output_rows/output_bytes/elapsed_compute) + `required_input_distribution` (`HashPartitioned` on partition cols, else `UnspecifiedDistribution`); `IcebergTableFormat::create_deleter/create_updater/create_merger` + catalog-managed ALTER allows `RenameTable`; construct-only tests (deleter/updater).

**Deferred (7.4 remainder):** `plan_iceberg_write` `OverwriteIf`/`OverwritePartitions` + `extract_partition_predicate_from_expr` (INSERT ... REPLACE WHERE); `accumulate_action_batches` (→ Phase 9 LOAD DATA; v0.7.0 `merge_writer_commit_meta` is the current multi-batch idiom); `retry_metadata_commit` extraction **done**; `plan_builder.rs` repartition — **kept + documented deferral**: v0.6.6 removed `IcebergPlanBuilder::add_repartition_node` + `session` (relying on its optimizer's scan re-split); v0.7.0 keeps both by design — validate v0.7.0 write behavior before any removal.

**Final-completion fixes (post-audit):** provider `aggregate_statistics` empty-scan exact-0 stats (+2 tests); `arrow_primitive_to_iceberg` `is_utc_timezone` widening (`Etc/UTC` etc. → `Timestamptz`) + test; analyzer `parser.rs` TRUNCATE/CALL parse tests.

**Verify:** `cargo check`; `cargo test -p sail-iceberg` — 89 tests pass; workspace compiles green.

### Phase 8 — CALL execution backend (Track A, slim) ✅ DONE
| File | Add |
|---|---|
| `crates/sail-iceberg/src/table_format.rs` | `TableFormat::call_procedure` impl (dispatch on `TableFormatProcedureOperation`); extracted `retry_metadata_commit` (filesystem commit loop, also used by ALTER) |
| `crates/sail-iceberg/src/operations/procedure.rs` (new, pure fns) | `compute_procedure_updates`, `expire_snapshot_updates`, `retained_snapshot_ids`, `set_main_snapshot_ref`, `resolve_target_snapshot_id`, `is_current_ancestor`, `procedure_requirements`, `compute_procedure_output`, `validate_procedure_requirements`, `apply_procedure_updates`, `CallProcedureOutput` schema |
| `crates/sail-iceberg/src/operations/expire_snapshots_gc.rs` (new, utility) | `collect_files`, `diff_files`, `delete_files`, `expire_files_gc`, `FileKind`, `ExpireGcCounts` (+ 7 unit tests) |
| commits | **Filesystem path** via `retry_metadata_commit` loop (+ post-commit reload → `expire_files_gc` for expire_snapshots). **Catalog-managed** returns `NotSupported`: `call_procedure` only receives a `RuntimeEnv`, and catalog commits need session-level `CatalogManager` access (signature limitation; can be lifted later by extending the trait signature). |

**Audit fixes applied (post-review):** `alter_table_properties` refactored to use `retry_metadata_commit` (dedupes ~100 lines; adds stale-aware post-write conflict check); `DataFusionError::Execution` → let-else + `internal_err!`; procedure fns `pub(crate)`; dead serde derives dropped from `CallProcedureOutput`; `use` imports replace fully-qualified paths in `call_procedure`.

**NOT created (removed from v0.6.6):** `CallProcedureNode`, `CallProcedureExec`,
`IcebergPhysicalPlanner` CALL arm, `job_graph/planner.rs` CALL placement,
`CallProcedureExecNode` proto/codec.

**Verify:** `cargo test -p sail-iceberg` (procedure + GC unit tests) — 96 tests pass; workspace compiles green.

### Phase 9 — LOAD DATA (Track B, dedicated node) ✅ DONE
(unchanged from `11-refactor-plan.md` Phase 9: `operations/parquet_utils.rs`,
`aggregate_from_parquet_metadata_with_field_map`, `physical/load_classifier.rs`,
`physical/load_data_planner.rs`, `physical_plan/load_data_exec.rs`, module re-exports,
`table_scan_planner.rs` `LoadDataNode` arm, proto `IcebergLoadDataFastExecNode` **=56** +
codec, commit count-summing)

**Landed:** `operations/parquet_utils.rs` (`ParquetFooterInfo`, `read_parquet_footer`);
`aggregate_from_parquet_metadata_with_field_map` in `data_file_writer.rs`; `physical/load_classifier.rs`
(`classify_source_files` fast/fallback split, `resolve_source_files`, `split_glob`, `schema_matches`,
`build_data_file`, 7 tests); `physical/load_data_planner.rs` (`plan_load_data` fast + fallback-rewrite
branches, `group_by_format`, `build_fallback_scan`, `infer_source_compression`); `physical_plan/load_data_exec.rs`
(`IcebergLoadDataFastExec`); `table_scan_planner.rs` `LoadDataNode` dispatch; proto `IcebergLoadDataFastExecNode`
**=56** (55 taken) + codec encode/decode + round-trip test.

**Audit fixes (post-review):** removed dead `reported_row_count` on `IcebergLoadDataFastExec` (field/param/
accessor/proto field 7/planner `total_rows`+`fast_rows`/classifier `ClassifiedFiles.total_rows`); unified
`FileSource`/`FileGroup` imports to the `datafusion::datasource::physical_plan` facade; added the codec
round-trip test. Note: `accumulate_action_batches` commit count-summing deferred — v0.7.0's
`merge_writer_commit_meta` + the fast-exec action-schema row-count already sum correctly for the union path.

**Verify:** `cargo test -p sail-iceberg` — 104 tests pass; `cargo test -p sail-execution` codec round-trip passes; workspace green.

### Phase 10 — Distributed execution ❌ DROPPED (not ported)
(unchanged from `11-refactor-plan.md` Phase 10: worker pool accounting/readiness/spawn
retry, task-assigner `Pending` state, driver handler gate + `RetryWorkerSpawn` + idle
reap, `WorkerManager::delete_worker`, `rpc.rs` client hardening + `ServerMonitor::start(handle)`
+ io-runtime, `driver/gateway.rs` + `worker/actor/rpc.rs` keepalive wiring,
`ServerBuilderOptions::from_keepalive`/`From<&ClusterConfig>`, flight/spark entrypoints,
`ActivityTracker` executor wiring, session self-heal, config keys)

**Dropped after audit.** These are v0.6.6-cluster ops-hardening features (budget-accurate
worker provisioning, fleet-readiness barrier, spawn-retry backoff, idle-worker reap
protection, k8s pod deletion, session self-heal) written against v0.6.6's architecture,
which v0.7.0 has diverged from. v0.7.0's distributed execution (driver/gateway,
task_assigner, worker_pool, worker_manager/{k8s,local}, rpc) is **functional and complete
as-is**; `ActivityTracker` is already present. Porting would mean re-implementing each item
against v0.7.0's own shapes (~10 files; task_assigner alone +376 lines in v0.6.6) for
edge-case reliability benefit and real risk to a working distributed system. Revisit only
if a concrete production requirement emerges.

**Verify:** n/a (no code change).

### Phase 11 — Tooling + tests + docs ✅ PORTED (2026-08-18)

**Scope:** `docker/*` cargo-chef + non-root `sail` user + OCI labels,
`build.sh` (BuildKit `docker buildx` helper, defaults `RUST_VERSION=1.96.0` /
`PYSPARK_VERSION=4.2.0` / `PYTHON_IMAGE=python:3.14-slim`), `TEST_PLAN.md` (722-line
manual E2E plan), `k8s/sail.yaml` `sail-flight-server` Deployment +
`sail-flight-service` Service, `test_flight_heimdall.py` +
`flight_catalog_uri` fixture, `test_commit.py` CREATE OR REPLACE CTAS coverage;
update `00`/`09`/`11`/`12` status.

**Ported after audit (2026-08-18).** The tail of the v0.6.6 diff was originally skipped
(see note below). It is now fully ported:

- `build.sh` + `TEST_PLAN.md` added at repo root (verbatim, version defaults bumped to
  v0.7.0's toolchain: 1.96.0 / 4.2.0).
- `docker/{dev,quickstart,release}/Dockerfile` adopted v0.6.6's structure (cargo-chef
  layered caching via chef→planner→builder stages, non-root `sail` user uid/gid 10001,
  OCI labels, `--locked`, `RELEASE_TAG` git-source stage for release builds), keeping
  v0.7.0's version defaults. All three pass `docker build --check`.
- `k8s/sail.yaml` gained the `sail-flight-server` Deployment (replicas 1,
  serviceAccount `sail-user`, port 32010, k8s cluster-mode env) + `sail-flight-service`
  Service.
- `tests/flight/conftest.py` gained the module-scoped `flight_catalog_uri` fixture
  (Memory-catalog `FlightSqlServer` + tmp Iceberg warehouse); `test_flight_heimdall.py`
  added verbatim.
- `tests/spark/catalog/iceberg_rest/test_commit.py` gained the v0.6.6-only
  `test_create_or_replace_table_as_select_replaces_rest_catalog_metadata` (additive
  merge; v0.7.0's own tests kept).
- `.gitignore` `docs/dev/` entry **not** ported (v0.7.0 tracks those docs).

**Verify:** `bash -n build.sh` ✓; `docker build --check` on all three Dockerfiles ✓;
`python3 -m py_compile` on the three python files ✓; `kubectl apply --dry-run=client`
n/a (kubectl absent; manifest byte-identical to v0.6.6). The E2E python tests run
against external compose infra (Polaris/MinIO/seaweedfs) and are not part of unit suites.

---

## 5. Do-NOT-port list (v0.6.6 → v0.7.0)

| Do NOT port | Reason |
|---|---|
| `CallProcedureNode` (sail-logical-plan) | CALL is Track A |
| `CallProcedureExec` as `ExecutionPlan` | Track A command returns a batch |
| `IcebergPhysicalPlanner` CALL arm | Track A |
| `job_graph/planner.rs` CALL driver placement | `CatalogCommandExec` already driver-side + serialized |
| `CallProcedureExecNode` proto (=56) + codec arms | `CatalogCommand` already round-trips |
| LOAD DATA as INSERT-rewrite | rejected; dedicated node keeps fast-register |

---

## 6. Verification command summary

| Phase | Command |
|---|---|
| 0 | `cargo check -p sail-iceberg -p sail-catalog-memory -p sail-logical-plan` |
| 1 | `cargo test -p sail-sql-parser -p sail-sql-analyzer` |
| 2 | `cargo check` + `cargo test -p sail-common-datafusion` |
| 3 | `cargo test -p sail-logical-plan` |
| 4 | `cargo check` + `cargo test -p sail-plan` |
| 5 | `cargo test -p sail-catalog-iceberg` |
| 6–9 | `cargo check` + `cargo test -p sail-iceberg` |
| 10 | `cargo check` + `cargo test -p sail-execution -p sail-session` |
| 11 | targeted `pytest` |

---

## 7. PR breakdown

- PR-A: Phases 0–1 (deps + SQL front-end)
- PR-B: Phases 2–4 (common surface + logical + resolvers)
- PR-C: Phases 5–6 (catalog commands incl. **CALL as CatalogCommand** + metadata tables)
- PR-D: Phase 7 (row-level ops + commit machinery; split 7.1/7.2/7.3/7.4)
- PR-E: Phases 8–9 (**CALL backend** + **LOAD DATA**)
- PR-F: Phase 10 (distributed execution)
- PR-G: Phase 11 (tooling + tests + docs) ✅ ported

---

## 8. Decision log & glossary

**Locked decisions:**
- 2026-08-17 — **CALL → Track A**: `CatalogCommand::CallProcedure` + `TableFormat::call_procedure`; v0.6.6 CALL exec surface removed.
- 2026-08-17 — **LOAD DATA → Track B** with dedicated `LoadDataNode`; INSERT-rewrite rejected.
- 2026-08-17 — SHOW/DESCRIBE/ALTER → Track A; metadata tables + row-level ops → Track B.

**Glossary:** Track A = catalog-command path (leaf wrapper, one RecordBatch); Track B =
dataflow path (real logical node → ExtensionPlanner → executor tree);
`TableFormat::call_procedure` = new trait method enabling Track-A CALL;
`LoadDataNode` = Track-B logical node for LOAD DATA.
