# 11 — Bottom-Up Refactor Plan: Porting feat/v0.6.6 onto feat/0.7.0

> The execution blueprint for re-implementing everything captured in docs `01`–`08`
> (v0.6.6 inventory) on top of `feat/0.7.0` (= `tag/v0.7.0`, commit `f0b137d6`),
> accounting for how v0.7.0 has diverged (see `09-v070-gap-analysis.md`).
>
> Each phase is a **compilable, testable unit**. Verify with `cargo check -p <crate>`
> after editing and `cargo test -p <crate>` where tests exist; Python phases run a
> targeted `pytest` subset. Land every edit in its "logical home" per
> `docs/dev/sail-idioms-and-patterns.md` §19. Commit per phase.
>
> Reference trees: v0.7.0 working tree (this repo, `feat/0.7.0`) and the v0.6.6 tree at
> `b8804803` (extracted during analysis).

---

## 0. Guiding principles

1. **Bottom-up, dependency-first.** Never introduce a symbol that references a layer
   that does not exist yet. Spec → parser → analyzer → common → logical → resolver →
   catalog → iceberg physical → distributed → tooling.
2. **Reuse, don't rewrite.** v0.7.0 already ships a large shared surface (below). Port
   only the v0.6.6 delta; compose existing machinery.
3. **Match current structure, not v0.6.6 structure.** Where v0.7.0 moved or renamed
   files (e.g. driver server → `driver/gateway.rs`; no `conflict_checker.rs`), land the
   change in the v0.7.0 location.
4. **Each phase leaves the tree green.** No `todo!`-backed half-commits; new resolver
   arms are added only when their target exists.
5. **Follow the idioms checklist** (`sail-idioms-and-patterns.md` §20.2) before merging
   each phase.

### Already-present surface in v0.7.0 (reuse, never re-implement)

| Piece | Location |
|---|---|
| `TableFormat` trait (incl. `create_deleter`, `create_merger`, `infer_metadata`) | `crates/sail-common-datafusion/src/datasource.rs` |
| `TableFormatRegistry`, `OptionLayer`, `SourceInfo`/`SinkInfo`/`DeleteInfo`/`MergeInfo` | same |
| `RowLevelWriteNode`, `MergeExpansion`, `expand_merge`, `MergeCardinalityCheckNode`, insert-only fast append, source-metric branch | `crates/sail-logical-plan/src/merge.rs` |
| `MergeCapableSource` trait (+ Delta impl) | `sail-common-datafusion/src/datasource.rs`, `sail-delta-lake/.../table_source.rs` |
| `IcebergCatalogCommitCoordinator`, `IcebergCatalogCommitMode`, `CatalogCommitOutcome`, `CatalogTableInfo`, `CatalogCommittedTable::metadata_location` | `crates/sail-iceberg/src/catalog_support/commit.rs` |
| `ActivityTracker` (`track_activity`/`active_at`) | `crates/sail-common-datafusion/src/session/activity.rs` |
| `ServerBuilderOptions` (keepalive fields) | `crates/sail-server/src/builder.rs` |
| `IcebergManifestScanExec`, `IcebergDiscoveryExec`, `IcebergScanByDataFilesExec`, `IcebergWriterExec` (+ `output_partitions` props), `IcebergCommitExec` (Append/Overwrite), `IcebergDeleteApplyExec` | `crates/sail-iceberg/src/physical_plan/**` |
| Iceberg filesystem `alter_table_properties` (SET/UNSET) + conflict-error helper | `crates/sail-iceberg/src/table_format.rs` |
| `LOAD DATA` parser grammar + `spec::CommandNode::LoadData` | parser/spec (resolver is `todo!`) |
| `SnapshotRetention` struct fields | `spec/snapshots/snapshot.rs` |
| `MergeCardinalityCheckNode` physical exec | `crates/sail-physical-plan/src/merge_cardinality_check.rs` |
| `SessionExtension`/`SessionExtensionAccessor`, `LakehouseExecutionContext`, `CommitAuthority`/`ScanAuthority`/`LakehouseOperation` | `sail-common-datafusion` |

### Confirmed v0.7.0 layout facts that shape the phases

- `sail-iceberg/Cargo.toml` does **not** depend on `sail-logical-plan` or
  `datafusion-datasource`; must add (Phase 0).
- `sail-plan/src/resolver/command/` has `delete.rs`, `merge.rs`, `insert.rs`,
  `write*.rs`, `catalog/`, `show.rs`, ... but **no** `update.rs`, `call.rs`, `load.rs`.
- `sail-iceberg/src/logical/` = `mod.rs` + `table_source.rs` only (no `update.rs`).
- `sail-iceberg/src/physical/` = `mod.rs` + `table_scan_planner.rs` only.
- `sail-iceberg/src/physical_plan/` has `action_schema.rs`, `commit/` (only
  `commit_exec.rs` + `mod.rs` — **no `conflict_checker.rs`**), `delete_apply_exec.rs`,
  `discovery_exec.rs`, `manifest_scan_exec.rs`, `plan_builder.rs` (still has
  `add_repartition_node`), `scan_by_data_files_exec.rs`, `writer_exec.rs`,
  `writer_options.rs`.
- `sail-logical-plan/src/` has `merge.rs` (with `expand_merge`, **without**
  `expand_update`/`UpdateExpansion`), `monotonic_id.rs`, `barrier.rs`, etc. — no
  `call_procedure.rs`, no `load_data.rs`.
- Driver gRPC server lives in `crates/sail-execution/src/driver/gateway.rs` (line ~130,
  `ServerBuilder::new("sail_driver", Default::default())` via `tokio::spawn`); worker in
  `worker/actor/rpc.rs` (`serve(handle, addr)`, `Default::default()`).
- `WorkerManager` trait has `launch_worker` + `stop` only (no `delete_worker`).
- `rpc.rs`: `ServerMonitor::start(self, f)` (no handle), `ClientOptions { enable_tls,
  host, port }` (no `runtime`); no client keepalive constants.
- `task_assigner/state.rs` uses `WorkerResource::Inactive` (no `Pending` budget model).
- `IcebergWriterExec::required_input_distribution` → unconditional
  `UnspecifiedDistribution`; `iceberg.yaml` `compression_codec` `supported: false`.

---

## Phase 0 — Dependencies

**Goal:** the crate graph accepts the ported symbols.

| File | Change |
|---|---|
| `crates/sail-iceberg/Cargo.toml` | add `sail-logical-plan = { path = "../sail-logical-plan" }`, `datafusion-datasource = { workspace = true }`, `[dev-dependencies] tokio = { workspace = true }` |
| `crates/sail-catalog-memory/Cargo.toml` | add `sail-common = { path = "../sail-common" }` |
| `crates/sail-logical-plan/Cargo.toml` | add `serde = { workspace = true }` (`sail-function` already present) |

**Verify:** `cargo check -p sail-iceberg -p sail-catalog-memory -p sail-logical-plan`.

---

## Phase 1 — Spec + parser + analyzer (SQL front-end)

**Goal:** grammar and spec exist for every new statement/command; nothing downstream
references them yet. (Files: doc `01`.)

| File | Add |
|---|---|
| `crates/sail-common/src/spec/plan.rs` | `CommandNode::CallProcedure { name: ObjectName, arguments: Vec<(Option<Identifier>, Expr)> }`; `CommandNode::ShowTblProperties { table: ObjectName, property_key: Option<String> }`; `AlterTableOperation::{ RenameTable, AddColumns{items: Vec<ColumnDefinition>}, DropColumns{names, if_exists}, AlterColumnComment, AlterColumnNullability, AlterColumnPosition }`; new types `ColumnDefinition`, `ColumnAlterationOption`, `ColumnPosition` |
| `crates/sail-sql-parser/data/keywords.txt` | `CALL` |
| `crates/sail-sql-parser/src/ast/statement.rs` | `Statement::Call { name, arguments }` (via `compose` for `name => value`), `Statement::ShowTblProperties { table, property_key }`, `Statement::TruncateTable { name }`, `Statement::View { view, extended, name }` |
| `crates/sail-sql-parser/src/ast/expression.rs` | nothing new (function-arg list already supports named args) |
| `crates/sail-sql-parser/tests/gold_data/syntax.json` | golden cases for CALL / SHOW TBLPROPERTIES / TRUNCATE / DESCRIBE VIEW / new ALTER forms |
| `crates/sail-sql-analyzer/src/statement.rs` | arms in `from_ast_statement`: `Call`→`CommandNode::CallProcedure` (split named vs positional args); `ShowTblProperties`; `TruncateTable`→`CommandNode::Delete { condition: None }`; `DescribeItem::View`; `from_ast_alter_table_operation` for the six new ops + `from_ast_column_alteration_list` |
| `crates/sail-sql-analyzer/src/parser.rs` | wire the new statement kinds into analyzer parse entry points |

**Verify:** `cargo test -p sail-sql-parser -p sail-sql-analyzer` (gold-data tests).

---

## Phase 2 — Common surface (`sail-common-datafusion`)

**Goal:** shared types every downstream layer needs. (Docs `03`, `05`, `06`.)

| File | Add |
|---|---|
| `crates/sail-common-datafusion/src/datasource.rs` | `SourceInfo.metadata_table: Option<IcebergMetadataTableType>`; `UpdateInfo { table_name, path, target: Arc<LogicalPlan>, condition: Option<ExprWithSource>, assignments, lakehouse_table, options }`; `UpdateAssignment { column_path, expression }`; `TableFormat::create_updater` defaulted trait method (`not_impl_err!`); `TableFormatAlterTableOperation::{ RenameTable, AddColumns{columns}, DropColumns{names, if_exists}, AlterColumnComment, AlterColumnNullability, AlterColumnPosition }` + default `alter_table` arms + `Display` labels for all six |
| `crates/sail-common-datafusion/src/catalog/iceberg.rs` | `pub enum IcebergMetadataTableType { Snapshots, Refs }` + `from_name` + `Display` |
| `crates/sail-common-datafusion/src/catalog/mod.rs` | `pub use iceberg::IcebergMetadataTableType;` |
| mechanical `metadata_table: None` | `resolver/command/{delete,delta,write}.rs`, `data-source/formats/{rate,socket}/mod.rs`, `data-source/listing/source.rs`, `sail-iceberg/datasource/provider.rs`, `sail-iceberg/table_format.rs`, `sail-delta-lake/table_format.rs` |

**Verify:** `cargo check`; `cargo test -p sail-common-datafusion`.

---

## Phase 3 — Logical plans (`sail-logical-plan`)

**Goal:** extension nodes exist for CALL, LOAD DATA, and UPDATE expansion. (Docs `02`, `03`, `04`.)

| File | Add |
|---|---|
| `crates/sail-logical-plan/src/call_procedure.rs` (new) | `enum CallProcedure { RollbackToSnapshot{table,snapshot_id}, SetCurrentSnapshot{table,snapshot_id,ref}, ExpireSnapshots{table,older_than_ms,retain_last} }` + `CallProcedureNode` (`UserDefinedLogicalNodeCore`, leaf, carries target context) |
| `crates/sail-logical-plan/src/load_data.rs` (new) | `LoadDataNode` (`UserDefinedLogicalNodeCore`, leaf, carries location/overwrite/target context) |
| `crates/sail-logical-plan/src/lib.rs` | `pub mod call_procedure; pub mod load_data;` |
| `crates/sail-logical-plan/src/merge.rs` | **only** add `expand_update(info, path_column) -> UpdateExpansion` + `struct UpdateExpansion { write_plan, touched_files_plan, output_schema }` (everything else is already in v0.7.0) |

**Verify:** `cargo test -p sail-logical-plan` (merge tests must still pass).

---

## Phase 4 — Resolvers (`sail-plan`)

**Goal:** spec → DataFusion `LogicalPlan` for the new commands. (Docs `02`, `03`, `04`, `05`.)

| File | Add |
|---|---|
| `crates/sail-plan/src/resolver/command/call.rs` (new) | `resolve_command_call_procedure` + named/positional arg helpers + scalar coercions (`scalar_to_table_name/snapshot_id/i32/timestamp_ms`) |
| `crates/sail-plan/src/resolver/command/load.rs` (new) | `resolve_command_load_data` (rejects LOCAL + PARTITION; iceberg-only) |
| `crates/sail-plan/src/resolver/command/update.rs` (new) | `resolve_command_update` (target scan via `resolve_update_table_plan`, rename round-trip, assignment type-cast) |
| `crates/sail-plan/src/resolver/command/mod.rs` | `mod call; mod load; mod update;` + `CommandNode::CallProcedure` / `CommandNode::LoadData` (replaces `todo!`) / `CommandNode::Update` (replaces `todo!`) dispatch |
| `crates/sail-plan/src/resolver/command/catalog/table.rs` | `resolve_catalog_alter_table`: spec `AlterTableOperation` → `AlterTableOptions` for the six new ops |
| `crates/sail-plan/src/resolver/query/read.rs` | `try_resolve_iceberg_metadata_table` (trailing `refs`/`snapshots` segment → `SourceInfo.metadata_table`) |
| `crates/sail-plan/src/resolver/command/delete.rs` | add `metadata_table: None` (Phase 2) — no logic change |

**Verify:** `cargo check`; `cargo test -p sail-plan` if resolver tests exist.

---

## Phase 5 — Catalog commands + providers

**Goal:** catalog-level DDL works for all catalogs. (Doc `06`.)

| File | Add |
|---|---|
| `crates/sail-catalog/src/command.rs` | `CatalogCommand::ShowTblProperties { table, property_key }` + `ShowTblPropertiesRow` + execution (sorted, table-only); `DescribeTable { column: Option<String> }` single-column describe; `table_format_alter_operation` for six new ops; `CommitAuthority::IcebergRestCommit` short-circuit in ALTER routing |
| `crates/sail-catalog/src/error.rs` | `CatalogError::NotFound(CatalogObject::Column, ...)` path if needed |
| `crates/sail-catalog/src/provider/options.rs` | `AlterTableOptions::{ RenameTable, AddColumns, DropColumns, AlterColumnComment, AlterColumnNullability, AlterColumnPosition }` + `AddColumn` struct |
| `crates/sail-catalog-iceberg/src/provider.rs` | `CREATE OR REPLACE`/`REPLACE` (drop-then-recreate w/ purge; `Replace` on missing → NotFound); managed-vs-external location (`if is_external { location } else { None }`); `alter_table` (REST rename / properties / add columns / drop columns); `alter_table_properties` w/ reserved-key guard; `alter_table_add_columns`/`drop_columns`; `map_update_table_alter_error`; `IcebergRestCatalogOptions.access_delegation` |
| `crates/sail-catalog-iceberg/src/lib.rs` | `pub use sail_common::config::IcebergRestAccessDelegation;` |
| `crates/sail-common/src/config/application.rs` | `enum IcebergRestAccessDelegation { VendedCredentials /*default*/, None }`; field on `CatalogType::IcebergRest` |
| `crates/sail-session/src/catalog.rs` | destructure + pass `access_delegation` into `IcebergRestCatalogOptions` |
| `crates/sail-catalog-memory/src/provider.rs` | `AlterTableOptions::RenameTable` arm |
| `crates/sail-catalog-glue`, `sail-catalog-hms`, `sail-catalog-onelake` | `RenameTable` rejection + column-op stubs |
| `crates/sail-delta-lake/src/table_format.rs` | six new `TableFormatAlterTableOperation` arms (`RenameTable→Ok`, others → `not_impl_err`) + `Display` labels |
| `crates/sail-catalog-iceberg/tests/rest_integration_test.rs` | create-or-replace + alter tests |

**Verify:** `cargo test -p sail-catalog-iceberg` (rest integration tests).

---

## Phase 6 — Metadata tables + snapshot accessors

**Goal:** `db.table.snapshots` / `db.table.refs` readable. (Doc `05`.)

| File | Add |
|---|---|
| `crates/sail-iceberg/src/datasource/metadata_table.rs` (new) | `IcebergMetadataTableProvider` (`TableProvider`, single-batch materialization; `Snapshots` + `Refs` schemas/builders) |
| `crates/sail-iceberg/src/datasource/mod.rs` | `pub mod metadata_table;` |
| `crates/sail-iceberg/src/table_format.rs` | `create_source` branch when `info.metadata_table.is_some()` → `build_iceberg_metadata_source` |
| `crates/sail-iceberg/src/spec/metadata/table_metadata.rs` | `TableMetadata::snapshot(id) -> Option<&Snapshot>` |
| `crates/sail-iceberg/src/spec/snapshots/snapshot.rs` | `SnapshotReference::{ min_snapshots_to_keep, max_snapshot_age_ms, max_ref_age_ms }` accessors |

**Verify:** `cargo check`; unit tests on the provider.

---

## Phase 7 — Iceberg row-level ops + commit machinery (largest phase)

**Goal:** UPDATE / DELETE / TRUNCATE / MERGE on Iceberg with targeted rewrite. (Doc `03`.)

### 7.1 Source + scan
| File | Add |
|---|---|
| `crates/sail-iceberg/src/logical/table_source.rs` | implement `MergeCapableSource` for `IcebergTableSource` (`file_column`, `with_file_column`, `row_index_column_name`, `with_row_index_column`) |
| `crates/sail-iceberg/src/logical/update.rs` (new) | `expand_update_node(info)` (enable file column, `expand_update`, wrap in `RowLevelWriteNode::new_update`) |
| `crates/sail-iceberg/src/logical/mod.rs` | `pub mod update;` |
| `crates/sail-iceberg/src/physical_plan/scan_by_data_files_exec.rs` | `file_path_column` field + `new_with_file_path_column`; materialize via Parquet partition column using the exact manifest path string |
| `crates/sail-execution/proto/sail/plan/physical.proto` | `optional string file_path_column = 4;` on `IcebergScanByDataFilesExecNode` |
| `crates/sail-execution/src/proto/codec.rs` | encode/decode for `file_path_column` |

### 7.2 Planner module
| File | Add |
|---|---|
| `crates/sail-iceberg/src/physical/row_level_planner.rs` (new) | `plan_iceberg_row_level_write` dispatching Delete/Merge/Update to the planner module |
| `crates/sail-iceberg/src/physical_plan/planner/mod.rs` | re-export `PlannerContext`, `plan_delete`, `plan_merge`, `plan_update`, `assemble_iceberg_commit_plan` |
| `crates/sail-iceberg/src/physical_plan/planner/context.rs` | `PlannerContext { session, options, table_url, lakehouse_table, table }` + accessors + `object_store()` |
| `crates/sail-iceberg/src/physical_plan/planner/helpers.rs` | `collect_touched_file_paths` (runs on driver runtime), `build_targeted_writer_input` (RightAnti+Inner hash joins on file column), `strip_internal_columns` |
| `crates/sail-iceberg/src/physical_plan/planner/commit.rs` | `assemble_iceberg_commit_plan` (writer + optional remove-source → coalesce → commit exec) |
| `crates/sail-iceberg/src/physical_plan/planner/op_delete.rs` | `plan_delete` (TRUNCATE no-op for empty tables; conditional delete: ManifestScan→Discovery→Repartition→ScanByDataFiles→`NOT` filter→commit as `Operation::Delete`) |
| `crates/sail-iceberg/src/physical_plan/planner/op_update.rs` | `plan_update` (targeted rewrite → `Operation::Overwrite` + touched paths + matched row count) |
| `crates/sail-iceberg/src/physical_plan/planner/op_merge.rs` | `plan_merge` (insert-only → `Append`; else targeted rewrite → `Overwrite` + touched paths) |
| `crates/sail-iceberg/src/physical/table_scan_planner.rs` | `RowLevelWriteNode` (format `iceberg`) dispatch + file-column scan routing |

### 7.3 Writer + options
| File | Add |
|---|---|
| `crates/sail-iceberg/src/physical_plan/writer_options.rs` | `compression_codec`, `commit_operation: Option<Operation>`, `touched_file_paths`, `overwrite_predicate` |
| `crates/sail-iceberg/src/physical_plan/writer_exec.rs` | metrics (`ExecutionPlanMetricsSet`/`MetricBuilder`), `required_input_distribution` (`HashPartitioned` on partition cols, else `UnspecifiedDistribution`), `resolve_compression_codec`, overwrite partition-values extraction, `commit_operation` override, per-partition `execute` |
| `crates/sail-iceberg/data/options/iceberg.yaml` | flip `compression_codec` to `supported: true`, default `"snappy"`, add `write.parquet.compression-codec` table-property layer |

### 7.4 Action schema + commit exec
| File | Add |
|---|---|
| `crates/sail-iceberg/src/physical_plan/action_schema.rs` | `CommitMeta`/`CommitMetaAction`: `touched_file_paths`, `overwrite_predicate`, `overwrite_partition_values` (+ encode/decode round-trips) |
| `crates/sail-iceberg/src/physical_plan/commit/mod.rs` | `IcebergCommitInfo` gains the three fields (`serde skip_serializing_if` on options) |
| `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` | `reported_row_count`; `accumulate_action_batches`; `compute_untouched_manifest_entries`; `filter_parent_manifest_entries` (+by_values); `Operation::Delete`/`Replace` snapshot paths; stale-metadata-file-aware conflict checks |
| `crates/sail-iceberg/src/operations/snapshot.rs` | `parent_manifest_entries` + `with_parent_manifest_entries`; op-string → `Operation` (append/overwrite/delete/replace) |
| `crates/sail-iceberg/src/utils/metadata.rs` | `get_metadata_file_timestamp`, `is_stale_metadata_file` |
| `crates/sail-iceberg/src/table_format.rs` | `create_deleter`/`create_updater`/`create_merger` impls; extract `retry_metadata_commit`; `extract_partition_predicate_from_expr`; `reject_catalog_managed_iceberg_alter` allows `RenameTable`; `plan_iceberg_write` `OverwriteIf`/`OverwritePartitions` support + predicate validation |
| `crates/sail-iceberg/src/physical_plan/plan_builder.rs` | **careful merge:** v0.7.0 still has `add_repartition_node`; v0.6.6 removes it + wraps `CoalescePartitionsExec`. Validate against v0.7.0 optimizer with write tests before committing the removal |

**Verify:** `cargo check`; `cargo test -p sail-iceberg` (helpers/planner/commit/action-schema unit tests).

---

## Phase 8 — CALL stored procedures + expire GC

**Goal:** `rollback_to_snapshot` / `set_current_snapshot` / `expire_snapshots` incl.
physical GC. (Doc `02`.)

| File | Add |
|---|---|
| `crates/sail-iceberg/src/physical/call_procedure_planner.rs` (new) | `plan_call_procedure` (load table, compute updates/requirements/output, capture pre-commit metadata for expire) |
| `crates/sail-iceberg/src/physical_plan/call_procedure_exec.rs` (new) | `CallProcedureExec` (commits via `IcebergCatalogCommitMode`/`IcebergCatalogCommitCoordinator`; filesystem path via `retry_metadata_commit` + `validate_procedure_requirements`/`apply_procedure_updates`); `compute_procedure_updates`; `retained_snapshot_ids` retain-set algorithm; `CallProcedureOutput`; `procedure_requirements` |
| `crates/sail-iceberg/src/physical_plan/expire_snapshots_gc.rs` (new) | `collect_files`, `diff_files`, `delete_files`, `expire_files_gc`, `FileKind`, `ExpireGcCounts` |
| `crates/sail-iceberg/src/physical/mod.rs`, `physical_plan/mod.rs` | module + `pub use` re-exports |
| `crates/sail-iceberg/src/physical/table_scan_planner.rs` | `CallProcedureNode` dispatch |
| `crates/sail-execution/src/job_graph/planner.rs` | add `CallProcedureExec` to driver-stage detection + `is_driver_stage_plan` |
| `crates/sail-execution/proto/sail/plan/physical.proto` + `codec.rs` | `CallProcedureExecNode` (=56) + encode/decode + round-trip test |

**Verify:** `cargo test -p sail-iceberg` (exec unit tests); driver placement via explain/job-graph tests.

---

## Phase 9 — LOAD DATA

**Goal:** `LOAD DATA INPATH` native execution. (Doc `04`.)

| File | Add |
|---|---|
| `crates/sail-iceberg/src/operations/parquet_utils.rs` (new) | `ParquetFooterInfo`, `read_parquet_footer` |
| `crates/sail-iceberg/src/operations/write/base_writer/data_file_writer.rs` | `aggregate_from_parquet_metadata_with_field_map` |
| `crates/sail-iceberg/src/operations/mod.rs` | `pub mod parquet_utils;` |
| `crates/sail-iceberg/src/physical/load_classifier.rs` (new) | `classify_source_files`, `resolve_source_files`, `split_glob`, `schema_matches`, `build_data_file` |
| `crates/sail-iceberg/src/physical/load_data_planner.rs` (new) | `plan_load_data` (fast vs fallback branches, `group_by_format`, `build_fallback_scan`, `infer_source_compression`) |
| `crates/sail-iceberg/src/physical_plan/load_data_exec.rs` (new) | `IcebergLoadDataFastExec` |
| `crates/sail-iceberg/src/physical/mod.rs`, `physical_plan/mod.rs` | module + re-exports |
| `crates/sail-iceberg/src/physical/table_scan_planner.rs` | `LoadDataNode` dispatch |
| proto + codec | `IcebergLoadDataFastExecNode` (=55) + encode/decode + round-trip |

**Verify:** `cargo test -p sail-iceberg` (classifier/planner unit tests).

---

## Phase 10 — Distributed execution

**Goal:** worker-pool accounting, readiness gate, spawn retry, RPC hardening, session
health. (Doc `07`.)

| File | Add |
|---|---|
| `sail-execution/src/driver/worker_pool/mod.rs` | `spawn_retry_delays`/`spawn_retry_armed`, `reserve_worker_ids`, `next_spawn_retry_delay`, `has_pending_spawn_retry`, `fire_spawn_retry`, `reset_spawn_retry` |
| `sail-execution/src/driver/worker_pool/options.rs` | `http2_keepalive_*`, `spawn_retry_strategy`, `runtime` + `for_test` builders |
| `sail-execution/src/driver/task_assigner/state.rs` | repurpose `Inactive` → `Pending` |
| `sail-execution/src/driver/task_assigner/core.rs` | `pending/active/total_live_worker_count`, `request_workers` (head-of-queue), `request_initial_workers`, `add_pending_worker`/`activate_worker`/`deactivate_worker`/`track_worker_failed_to_start`, `is_task_queue_empty`, most-vacant-slot selection |
| `sail-execution/src/driver/task_assigner/options.rs` / `mod.rs` | `worker_initial_count`; remove `requested_worker_count` |
| `sail-execution/src/driver/actor/handler.rs` | fleet-readiness barrier, `spawn_initial_workers`/`spawn_workers`, `handle_probe_pending_worker` (delete pod + backoff), `handle_retry_worker_spawn`, idle-reap guards, scheduling-timeout guard |
| `sail-execution/src/driver/actor/{core,mod,event}.rs` | `RetryWorkerSpawn` event + dispatch; `worker_manager` field |
| `sail-execution/src/worker_manager/{mod,kubernetes,local,options}.rs` | `delete_worker(id)` on trait + k8s (deterministic pod name) + local (drop handle); keepalive opts |
| `sail-execution/src/rpc.rs` | `ServerMonitor::start(handle)`, `ClientOptions.runtime`, client endpoint hardening (connect_timeout 30s, tcp_keepalive 60s, http2 keepalive 30s/20s, keep_alive_while_idle), io-runtime spawn |
| `sail-execution/src/driver/gateway.rs` (v0.7.0 location) | `ServerBuilderOptions::from_keepalive` + `ServerMonitor::start(runtime.io(), ...)` |
| `sail-execution/src/worker/actor/rpc.rs` + `core.rs` | same keepalive/io-runtime wiring |
| `sail-server/src/builder.rs` | `ServerBuilderOptions::from_keepalive` + `From<&ClusterConfig>` |
| `sail-server/src/retry.rs` | `RetryStrategy::delay` → `pub` |
| `sail-flight/src/entrypoint.rs`, `sail-spark-connect/src/entrypoint.rs` | `ServerBuilderOptions::from(&config.cluster)` |
| `sail-spark-connect/src/executor.rs` + `service/plan_executor.rs` | pass `ActivityTracker`; `track_activity()` on every poll |
| `sail-session/src/session_manager/actor/handler.rs` | `create_session` extraction + stale-session recreate self-heal |
| `sail-common/src/config/application.yaml` + `application.rs` | `http2_keepalive_interval_secs` (60), `http2_keepalive_timeout_secs` (30), `worker_spawn_retry_strategy.*` + `ClusterConfigEnv` constants |

**Verify:** `cargo check`; `cargo test -p sail-execution -p sail-session` (unit tests for
worker pool, task assigner, session manager).

---

## Phase 11 — Docker / K8s / build.sh / Python tests + docs

**Goal:** tooling + end-to-end parity tests. (Doc `08`.)

| File | Change |
|---|---|
| `docker/dev/Dockerfile`, `docker/release/Dockerfile` | cargo-chef `chef/planner/builder` stages; `docker/quickstart/Dockerfile` non-root `sail` user + chained installs |
| `build.sh` (new) | buildx helper |
| `k8s/sail.yaml` | `sail-flight-server` Deployment + Service |
| `python/pysail/tests/flight/conftest.py` | `flight_catalog_uri` fixture |
| `python/pysail/tests/flight/test_flight_heimdall.py` (new) | LOAD DATA / metadata tables / TRUNCATE / rollback / expire / AS OF VERSION tests |
| `python/pysail/tests/spark/catalog/iceberg_rest/test_commit.py` | CREATE OR REPLACE test |
| `docs/dev/port-v0.6.6/` | update `00`/`09` to reflect completed phases |

**Verify:** targeted `pytest` for the flight + iceberg_rest subsets.

---

## Cross-cutting risks & watch-outs

1. **`plan_builder.rs` + writer distribution** (Phase 7.3): v0.7.0's
   `add_repartition_node` + `UnspecifiedDistribution` interplay with its optimizer is the
   riskiest correctness area. Port the v0.6.6 change behind write-behavior tests before
   removing the repartition.
2. **No `conflict_checker.rs` in v0.7.0** — v0.6.6's commit-conflict logic must be merged
   into the existing inline `commit_exec.rs` structure, not re-created as a separate file.
3. **`driver/gateway.rs` vs v0.6.6 `actor/rpc.rs`** — apply keepalive/io-runtime wiring to
   the v0.7.0 gateway.
4. **`IcebergDeleteApplyExec` exists in v0.7.0** — reuse it in row-level scans so tables
   with existing delete files are read correctly.
5. **REST-catalog routing** — add the `CommitAuthority::IcebergRestCommit` branch in
   `sail-catalog/src/command.rs` ALTER handling (v0.7.0 only routes non-lakehouse formats
   to `manager.alter_table`).
6. **`sail-iceberg` new deps** must land in Phase 0 or nothing compiles.
7. **Proto field numbers** — use 55/56 for the two new `NodeKind`s and confirm they do
   not collide with anything v0.7.0 added.
8. **Mechanical `metadata_table: None`** is folded into Phase 2, not scattered commits.

---

## Suggested commit/PR breakdown

- PR-A: Phases 0–1 (deps + SQL front-end)
- PR-B: Phases 2–4 (common surface + logical + resolvers)
- PR-C: Phases 5–6 (catalog + metadata tables)
- PR-D: Phase 7 (row-level ops + commit machinery) — split into 7.1/7.2/7.3/7.4 commits
- PR-E: Phases 8–9 (CALL + LOAD DATA)
- PR-F: Phase 10 (distributed execution)
- PR-G: Phase 11 (tooling + tests + docs)

Each PR keeps the tree green and is reviewable independently.
