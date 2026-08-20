# 09 — feat/0.7.0 Gap Analysis: What Already Exists vs What Must Be Ported

> This document is the bridge between the port inventory (`00`–`08`, which capture
> everything done on `feat/v0.6.6`) and the actual re-implementation on `feat/0.7`.
> For every feature in the inventory it states, with verification evidence, whether
> `feat/0.7.0` (= `tag/v0.7.0`) already implements it (**PRESENT**), has only part of it
> (**PARTIAL**), or has none of it (**ABSENT**), and what exactly must be ported vs
> reused.
>
> Scope note: the working tree audited is the `feat/0.7.0` branch checked out at
> `f0b137d6` (`chore: prepare v0.7.0 (#2342)`) — i.e. **exactly `tag/v0.7.0`**. All
> symbol/path evidence below is against that tree.

---

## 0. Executive summary

Of the ~14,800 code insertions of the `feat/v0.6.6` work:

- **~85% is ABSENT** from `v0.7.0` and must be re-implemented.
- **~15% is already satisfied** by upstream `v0.7.0` (shared MERGE machinery, catalog
  commit coordinator, activity tracker, server-builder options, Delta-side row-level
  infra) and should be **reused, not rewritten**.

The headline ABSENT items: Iceberg CALL stored procedures, Iceberg row-level
UPDATE/DELETE/TRUNCATE/MERGE, native LOAD DATA execution, Iceberg metadata tables,
SHOW TBLPROPERTIES / DESCRIBE TABLE `col` / DESCRIBE VIEW / TRUNCATE TABLE, ALTER TABLE
column ops + rename + add/drop columns, Iceberg REST catalog REPLACE / CREATE OR
REPLACE / alter_table, the entire worker-pool accounting + readiness-gate + spawn-retry
layer, RPC client hardening, the new cluster config keys, session self-heal, the codec
nodes, and the Docker/K8s/build.sh/Python-test surface.

The headline PARTIAL items (reuse the base, add the v0.6.6 delta): `IcebergCommitExec`
already exists but lacks the parent-manifest/operation/count machinery; the writer
already computes `output_partitions` but lacks metrics + hash distribution +
compression-codec support; `ServerBuilderOptions` exists with keepalive fields but no
cluster wiring; `ActivityTracker` exists but isn't wired into streaming; `merge.rs`
has Delta MERGE but lacks `expand_update`.

---

## 1. Methodology

1. Established the audited tree: `HEAD = f0b137d6`, `git describe` → `v0.7.0`, branch
   `feat/0.7.0`.
2. Confirmed the v0.6.6 work is NOT reachable: `git merge-base --is-ancestor b8804803
   HEAD` → **NO**.
3. For each feature family in docs `00`–`08`, searched the v0.7.0 tree for the defining
   symbols (graph-indexed; `search_graph`/`rg` fallback). Every claim below lists the
   evidence check used.
4. Classified each item PRESENT / PARTIAL / ABSENT and, for PARTIAL, split what exists
   (reuse) from what is new (port).

---

## 2. Baseline facts

| Fact | Value |
|---|---|
| Audited branch | `feat/0.7.0` |
| Audited commit | `f0b137d6` (`chore: prepare v0.7.0 (#2342)`, 2026-08-03) |
| Tag | `v0.7.0` (exact match) |
| v0.6.6 work HEAD | `b8804803` (`feat/v0.6.6`) |
| Is `b8804803` an ancestor of `feat/0.7`? | **No** |
| Diff baseline | `f090e646..b8804803` (139 files, ~21k insertions) |
| Code-only (excl. docs) | ~127 files, ~14.8k insertions |

---

## 3. Feature-by-feature gap matrix

### 3.1 A. SQL front-end (`01-sql-frontend-parser-analyzer-spec.md`)

#### `CALL` statement — **ABSENT**
- v0.7.0 `crates/sail-sql-parser/data/keywords.txt`: no `CALL`.
- v0.7.0 `src/ast/statement.rs`: no `Statement::Call`.
- v0.7.0 `crates/sail-common/src/spec/plan.rs`: no `CommandNode::CallProcedure`.
- v0.7.0 `gold_data/syntax.json`: no CALL cases.
- **Port**: keyword, AST variant, `from_ast_statement` lowering, `spec::CommandNode::CallProcedure`,
  gold-data entries (all from `01`).

#### `SHOW TBLPROPERTIES` — **ABSENT**
- No `Statement::ShowTblProperties` (parser), no `CommandNode::ShowTblProperties`
  (spec), no `CatalogCommand::ShowTblProperties` (`sail-catalog/src/command.rs`).
- **Port**: parser + analyzer + spec + catalog command + `ShowTblPropertiesRow`.

#### `TRUNCATE TABLE` — **ABSENT**
- v0.7.0 parser/AST: no `TruncateTable` statement (TRUNCATE is not modeled at all).
- **Port**: `Statement::TruncateTable` → lowered to `CommandNode::Delete { condition:
  None }`.

#### `DESCRIBE VIEW` — **ABSENT**
- v0.7.0 `sail-sql-parser/src/ast/statement.rs:1079` `DescribeItem` has `Query`,
  `Function`, `Catalog`, `Database`, `Table`, `TableExtended` — **no `View`**.
- **Port**: `DescribeItem::View`, analyzer lowering, spec handling.

#### `DESCRIBE TABLE <col>` — **ABSENT**
- v0.7.0 `CatalogCommand::DescribeTable { table, extended }` — **no `column` field**
  (`sail-catalog/src/command.rs:150-153`).
- **Port**: `column: Option<String>` on the command + `NotFound(CatalogObject::Column)`
  single-column describe.

#### ALTER TABLE column ops / rename / add-drop — **ABSENT**
- v0.7.0 `crates/sail-common/src/spec/plan.rs:1373` `AlterTableOperation`:
  `Unknown`, `SetTableProperties`, `UnsetTableProperties`, `AlterColumnType`,
  `AlterColumnDefault`, `AddCheckConstraint`. **Missing**: `RenameTable`, `AddColumns`,
  `DropColumns`, `AlterColumnComment`, `AlterColumnNullability`, `AlterColumnPosition`,
  plus the `ColumnDefinition` / `ColumnAlterationOption` / `ColumnPosition` types.
- v0.7.0 `crates/sail-catalog/src/provider/options.rs:139` `AlterTableOptions`: same six
  variants; **missing** the six new ones + `AddColumn`.
- v0.7.0 `crates/sail-common-datafusion/src/datasource.rs:451`
  `TableFormatAlterTableOperation`: `SetTableProperties`, `AlterColumnType`,
  `AlterColumnDefault`, `AddCheckConstraint`; **missing** `RenameTable`, `AddColumns`,
  `DropColumns`, `AlterColumnComment`, `AlterColumnNullability`, `AlterColumnPosition`.
- **Port**: spec types, catalog provider options, `TableFormatAlterTableOperation`
  variants + the `Display` labels (Delta too), analyzer `from_ast_alter_table_operation`
  mapping, resolver mapping (`resolve_catalog_alter_table`), catalog command dispatch,
  `table_format_alter_operation` mapping.

#### PARTIAL in this family
- `LOAD DATA` grammar + spec node **PRESENT** (see §3.4) — the resolver is a `todo!`.
- `DescribeItem` base enum present — only the `View` variant is new.
- `AlterTableOptions` / `AlterTableOperation` base enums present — only the six new
  variants are new.

### 3.2 B. Iceberg CALL stored procedures (`02-call-procedures.md`) — **ABSENT (whole)**

- `crates/sail-logical-plan/src/call_procedure.rs` — does not exist (no `CallProcedure`
  enum, no `CallProcedureNode`).
- `crates/sail-plan/src/resolver/command/call.rs` — does not exist.
- `crates/sail-iceberg/src/physical/call_procedure_planner.rs`,
  `physical_plan/call_procedure_exec.rs`, `physical_plan/expire_snapshots_gc.rs` — do
  not exist.
- `job_graph/planner.rs` driver placement for `CallProcedureExec` — absent.
- Codec/proto nodes (`CallProcedureExecNode`, field `pre_commit_metadata_json`) — absent.
- **Port**: the whole stack from `02`. **Reuse**: the shared commit machinery
  `IcebergCatalogCommitCoordinator` / `IcebergCatalogCommitMode` /
  `CatalogCommitOutcome` is **already in v0.7.0** at
  `crates/sail-iceberg/src/catalog_support/commit.rs` (see §4).

### 3.3 C. Iceberg row-level operations (`03-row-level-operations.md`)

#### Delta side — **PRESENT (reuse)**
- `crates/sail-common-datafusion/src/datasource.rs:523/529` — `TableFormat::create_deleter`
  / `create_merger` trait methods exist (Delta implements them).
- `MergeCapableSource` trait exists (`sail-common-datafusion/src/datasource.rs`, impl in
  `sail-delta-lake/src/logical/table_source.rs`).
- `crates/sail-logical-plan/src/merge.rs` has `RowLevelWriteNode` (line 143),
  `expand_merge` (line 428), `MergeCardinalityCheckNode` (line 47), insert-only fast
  append (`can_fast_append_insert_only` line 988), source-metric branch
  (`build_source_metric_plan` line 1233, `MERGE_SOURCE_METRIC_COLUMN`), and the Delta
  check-constraint / generated-column enforcement.

#### Iceberg side — **ABSENT**
- `crates/sail-iceberg/src/table_format.rs:622` — explicit
  `// TODO: Implement row-level DELETE/UPDATE/MERGE for this format.` There is no
  `create_merger`/`create_deleter`/`create_updater` in the Iceberg `TableFormat` impl.
- **New in v0.6.6 that v0.7.0 lacks:**
  - `TableFormat::create_updater` trait method + `UpdateInfo` + `UpdateAssignment`
    (neither struct exists anywhere in v0.7.0).
  - `expand_update` + `UpdateExpansion` (v0.7.0 `merge.rs` has `expand_merge` but not
    `expand_update`).
  - `crates/sail-iceberg/src/logical/update.rs` (whole file).
  - `physical/row_level_planner.rs`, `physical_plan/planner/{mod,context,helpers,commit,
    op_delete,op_update,op_merge}.rs` (whole module).
  - `IcebergTableSource` implements `MergeCapableSource` + `file_column` field.
  - `IcebergScanByDataFilesExec::new_with_file_path_column` (v0.7.0 `scan_by_data_files_exec.rs`
    has no `file_path_column`) + proto `file_path_column = 4` + codec.
  - `IcebergWriterExec`: metrics (`ExecutionPlanMetricsSet`, `output_rows`,
    `output_bytes`, `elapsed_compute`), `required_input_distribution`
    (`HashPartitioned` for partitioned tables; v0.7.0 returns `UnspecifiedDistribution`
    unconditionally at line 302), `resolve_compression_codec`, unique overwrite
    partition-values extraction, `commit_operation` override. (v0.7.0 already computes
    `output_partitions` in `compute_properties` — PARTIAL.)
  - `IcebergWriterExecOptions`: `compression_codec`, `commit_operation`,
    `touched_file_paths`, `overwrite_predicate`.
  - `CommitMeta` / `IcebergCommitInfo`: `touched_file_paths`, `overwrite_predicate`,
    `overwrite_partition_values` (v0.7.0 `action_schema.rs` / `commit/mod.rs` have none
    of these).
  - `IcebergCommitExec`: `reported_row_count`, `accumulate_action_batches`,
    `compute_untouched_manifest_entries`, `filter_parent_manifest_entries`,
    `filter_parent_manifest_entries_by_values`, `Operation::Delete` / `Operation::Replace`
    (v0.7.0 commit_exec has none of these).
  - `SnapshotProducer`: `parent_manifest_entries` + `with_parent_manifest_entries` +
    Delete/Replace operation strings (v0.7.0 `operations/snapshot.rs:83` still uses the
    `is_overwrite` boolean and only Append/Overwrite summaries).
  - `extract_partition_predicate_from_expr` (`table_format.rs`).
  - `is_stale_metadata_file` / `get_metadata_file_timestamp`
    (`crates/sail-iceberg/src/utils/metadata.rs` — absent in v0.7.0).
  - Compression: v0.7.0 `data/options/iceberg.yaml:568` has `compression_codec` but
    `supported: false` (the v0.6.6 change flips it to supported, default `snappy`,
    plus the `write.parquet.compression-codec` table-property layer).
  - `TableMetadata::snapshot(id)` method and `SnapshotReference` accessors
    (`min_snapshots_to_keep()` / `max_snapshot_age_ms()` / `max_ref_age_ms()`); v0.7.0
    has only the `SnapshotRetention` struct fields, no accessor methods.
  - `IcebergPlanBuilder` rewrite (drop lifetime/session, delete `add_repartition_node`,
    wrap `CoalescePartitionsExec`); v0.7.0 `plan_builder.rs` still has the old
    `IcebergPlanBuilder<'a>` with `add_repartition_node` (line 69/91).
- **Port**: the Iceberg side above. **Reuse**: `RowLevelWriteNode`, `expand_merge`,
  `MergeCardinalityCheckNode`, `MergeCapableSource` trait, `create_merger`/`create_deleter`
  trait methods, `MERGE_FILE_COLUMN`, Delta check-constraint/generated-column helpers.

### 3.4 D. LOAD DATA (`04-load-data.md`) — **PARTIAL → mostly ABSENT**

- **PRESENT**: parser `Statement::LoadData` (`ast/statement.rs:271`), spec
  `LoadData { local, location, table, overwrite, partition }`
  (`spec/plan.rs:503`), gold-data (`ddl_load_data.json`, `error_load_data.json`).
- **ABSENT**:
  - Resolver: `crates/sail-plan/src/resolver/command/mod.rs:308` still
    `CommandNode::LoadData { .. } => Err(PlanError::todo("CommandNode::LoadData"))`.
  - `LoadDataNode`, `resolve_command_load_data` (`command/load.rs`), `load_classifier.rs`,
    `load_data_planner.rs`, `load_data_exec.rs`, `operations/parquet_utils.rs`,
    `aggregate_from_parquet_metadata_with_field_map`
    (`data_file_writer.rs` — absent in v0.7.0), codec `IcebergLoadDataFastExecNode`
    (proto field `iceberg_load_data_fast = 55`), and the commit count-summing
    (`accumulate_action_batches`).
- **Port**: everything from `04` except the already-present grammar/spec node.

### 3.5 E. Iceberg metadata tables (`05-metadata-tables.md`) — **ABSENT (whole)**

- No `IcebergMetadataTableType` (`sail-common-datafusion/src/catalog/iceberg.rs`).
- No `SourceInfo.metadata_table` field (`datasource.rs`).
- No `IcebergMetadataTableProvider` / `datasource/metadata_table.rs`.
- No `try_resolve_iceberg_metadata_table` in `sail-plan/resolver/query/read.rs`.
- (The `metadata_table` matches in `commit_exec.rs` are the unrelated
  `catalog_metadata_table` / `catalog_registered_metadata_table` commit variables.)
- **Port**: the whole stack from `05`, including the **mechanical `metadata_table: None`
  addition** to every `SourceInfo` literal across ~9 files (see `00 §3.2`).

### 3.6 F. Catalog DDL + providers (`06-catalog-ddl-and-catalog-providers.md`)

#### Iceberg REST catalog — **mostly ABSENT**
- `create_table` REPLACE: v0.7.0 `provider.rs:1069` still
  `Err(CatalogError::NotSupported("Replace table is not supported yet"))` — the
  drop-then-recreate path (`CREATE OR REPLACE` / `REPLACE` with purge) is absent.
- `alter_table`: v0.7.0 `provider.rs:1224` is still `NotSupported("alter table in
  Iceberg catalog")` — `alter_table_properties`, `alter_table_add_columns`,
  `alter_table_drop_columns`, `RenameTable` (REST `rename_table`), and
  `map_update_table_alter_error` are all absent.
- `IcebergRestCatalogOptions`: v0.7.0 has `credentials` + `properties` only — the
  `access_delegation: IcebergRestAccessDelegation` field is absent.
- `IcebergRestAccessDelegation` enum (`VendedCredentials`/`None`): **absent** from
  `sail-common/src/config/application.rs`. (v0.7.0's `provider.rs:247` has an unrelated
  `access_delegation: Option<&str>` **REST query param** for `load_table` — do not
  confuse the two.)
- Catalog command routing: v0.7.0 `command.rs:479` routes `manager.alter_table` only for
  **non-lakehouse formats**; the `CommitAuthority::IcebergRestCommit` short-circuit is
  absent.
- `sail-session/src/catalog.rs` access_delegation wiring: absent.
- Managed-vs-external location forwarding (`location: if is_external ...`): absent.

#### PARTIAL in this family
- `alter_table` routing structure in `sail-catalog/src/command.rs` exists (catalog
  path for non-lakehouse formats).
- Iceberg **filesystem** `alter_table` + `alter_table_properties` (SET/UNSET TBLPROPERTIES)
  exist in `table_format.rs:262/499` — the `retry_metadata_commit` extraction, stale-file
  conflict helpers, column ops, and `RenameTable → Ok(())` are new.

#### Memory / Glue / HMS / OneLake / Delta
- `sail-catalog-memory/src/provider.rs` `RenameTable` support: **ABSENT** (v0.6.6 added
  it).
- Glue/HMS `RenameTable` rejections + column-op stubs: **ABSENT**.
- Delta `alter_table` handling of the six new `TableFormatAlterTableOperation` variants
  + `Display` labels: **ABSENT**.
- `sail-catalog-memory/Cargo.toml` `sail-common` dep: **ABSENT**.
- `sail-iceberg/Cargo.toml` `sail-logical-plan` + `datafusion-datasource` + tokio
  dev-dep: **ABSENT** (v0.7.0's `sail-iceberg/Cargo.toml` has none).

### 3.7 G. Distributed execution (`07-distributed-execution.md`)

#### ABSENT in v0.7.0
- `WorkerPool` spawn-retry state (`spawn_retry_delays`, `spawn_retry_armed`),
  `reserve_worker_ids`, `next_spawn_retry_delay`, `has_pending_spawn_retry`,
  `fire_spawn_retry`, `reset_spawn_retry` — `worker_pool/mod.rs`.
- `WorkerPoolOptions` `http2_keepalive_*` / `spawn_retry_strategy` / `runtime`.
- `TaskAssigner`: `pending_worker_count` / `active_worker_count` /
  `total_live_worker_count`, `request_initial_workers`, `add_pending_worker`,
  `activate_worker`, `deactivate_worker`, `track_worker_failed_to_start`,
  `is_task_queue_empty`, head-of-queue `request_workers`, most-vacant-slot selection.
  v0.7.0 `task_assigner/state.rs:95` still has `WorkerResource::Inactive` (not `Pending`).
- `WorkerResource::Inactive` → `Pending` repurposing, `TaskAssignerOptions.worker_initial_count`,
  removal of `requested_worker_count`.
- Driver actor: fleet-readiness barrier, `spawn_initial_workers`/`spawn_workers`,
  `DriverEvent::RetryWorkerSpawn`, `handle_retry_worker_spawn`, idle-reap guards,
  scheduling-timeout guard for pending spawn retry.
- `WorkerManager::delete_worker(id)` on k8s (deterministic pod name) + local (drop
  handle).
- RPC layer: `ServerMonitor::start(handle)`, `ClientOptions.runtime`,
  client endpoint hardening (`CLIENT_CONNECT_TIMEOUT=30s`, `tcp_keepalive=60s`,
  `http2_keep_alive_interval=30s`, `keep_alive_timeout=20s`, `keep_alive_while_idle`),
  io-runtime spawn of the connect task. v0.7.0 `sail-execution/src/rpc.rs` has none of
  these.
- Driver/worker server wiring: in v0.7.0 the **driver** server lives in
  `crates/sail-execution/src/driver/gateway.rs` (line 130,
  `ServerBuilder::new("sail_driver", Default::default())`, spawned via plain
  `tokio::spawn`) and the **worker** server in `crates/sail-execution/src/worker/actor/rpc.rs`
  (line 72, `ServerBuilder::new("sail_worker", Default::default())`, `serve(handle, addr)`
  takes no options). **Note the layout differs from the v0.6.6 tree**, which used
  `driver/actor/rpc.rs` + `ServerMonitor`; the v0.6.6 change (build from
  `ServerBuilderOptions` + start the server on the io runtime) must be adapted to the
  v0.7.0 `DriverGateway` / worker `serve` locations.
- `sail-flight` / `sail-spark-connect` entrypoints using
  `ServerBuilderOptions::from(&config.cluster)`.
- Session self-heal (`create_session` reuse/recreate stale sessions) — v0.7.0
  `session_manager/actor/handler.rs` has no `create_session` extraction.
- Config keys `cluster.http2_keepalive_interval_secs`, `cluster.http2_keepalive_timeout_secs`,
  `cluster.worker_spawn_retry_strategy.*` — absent from `application.yaml`.
- Codec/proto: `IcebergLoadDataFastExecNode` (=55), `CallProcedureExecNode` (=56),
  `IcebergScanByDataFilesExecNode.file_path_column` (=4) — absent.

#### PARTIAL in v0.7.0 (reuse the base, port the delta)
- **`ActivityTracker`** — PRESENT at `sail-common-datafusion/src/session/activity.rs`
  (`track_activity`/`active_at`), used by the session manager; the v0.6.6 delta is only
  the **spark-connect streaming-executor wiring** (`ExecutorTaskContext::new(stream,
  interval, tracker)` + `track_activity()` on every poll). `executor.rs` /
  `plan_executor.rs` in v0.7.0 do not reference it.
- **`ServerBuilderOptions`** — PRESENT at `sail-server/src/builder.rs` with
  `http2_keepalive_interval` / `http2_keepalive_timeout` fields (plus `nodelay`,
  `keepalive`, `http2_adaptive_window`). Missing: `from_keepalive` /
  `From<&ClusterConfig>` and all call-site wiring.
- **Iceberg writer `output_partitions`** plan properties — PRESENT
  (`writer_exec.rs:111-131` `compute_properties(schema, output_partitions)`). The
  metrics, hash-distribution, and compression-codec support are new.

### 3.8 H. Config / Docker / K8s / Python (`08-config-docker-k8s-python.md`) — **mostly ABSENT**

- Docker: v0.7.0 `docker/dev/Dockerfile` / `docker/release/Dockerfile` have **no
  cargo-chef** stages; `docker/quickstart/Dockerfile` has no non-root user / chained
  installs. The whole v0.6.6 Dockerfile rewrite is ABSENT.
- `build.sh` — ABSENT.
- `k8s/sail.yaml` — no `sail-flight-server` Deployment/Service.
- Python: `python/pysail/tests/flight/test_flight_heimdall.py` — ABSENT (only
  `test_flight.py` exists); `conftest.py` has no `flight_catalog_uri` fixture;
  `iceberg_rest/test_commit.py` lacks the CREATE OR REPLACE test.
- `.gitignore` additions — ABSENT.
- `IcebergRestAccessDelegation` config enum — ABSENT (see §3.6).

---

## 4. Reusable infrastructure already in v0.7.0 (do NOT port)

| Piece | Location in v0.7.0 | Reused by |
|---|---|---|
| `IcebergCatalogCommitCoordinator`, `IcebergCatalogCommitMode` (`Filesystem`/`MetadataLocationCas`/`CatalogCommit`/`CompatibilityCatalogCommit`), `CatalogCommitOutcome`, `CatalogTableInfo`, `CatalogCommittedTable::metadata_location` | `crates/sail-iceberg/src/catalog_support/commit.rs` | `CallProcedureExec` (02) |
| `TableFormat::create_deleter` / `create_merger` trait methods | `sail-common-datafusion/src/datasource.rs:523/529` | Iceberg `create_*` impls (03) |
| `MergeCapableSource` trait (`file_column_name`, `with_file_column`, `row_index_column_name`, `with_row_index_column`) | `sail-common-datafusion/src/datasource.rs` | `IcebergTableSource` impl (03) |
| `RowLevelWriteNode`, `expand_merge`, `MergeCardinalityCheckNode`, insert-only fast append, source-metric branch, `MERGE_SOURCE_METRIC_COLUMN`, `MergeIntoOptions`/`MergeInfo` | `crates/sail-logical-plan/src/merge.rs` | Iceberg row-level ops (03) |
| Delta check-constraint + generated-column enforcement (`apply_delta_check_constraint_filter`, `RaiseError`) | `sail-logical-plan/src/merge.rs`, `sail-function` | MERGE/UPDATE expansion (03) |
| `ActivityTracker` (`track_activity`/`active_at`) | `sail-common-datafusion/src/session/activity.rs` | spark-connect streaming keep-alive (07) |
| `ServerBuilderOptions` (keepalive fields) | `crates/sail-server/src/builder.rs` | keepalive wiring for driver/worker/flight (07) |
| Iceberg `alter_table_properties` (filesystem SET/UNSET) + conflict-error helper | `table_format.rs:499/1044` | `retry_metadata_commit` extraction (06/03) |
| Iceberg writer `output_partitions` plan properties | `writer_exec.rs:111-131` | writer distribution (03) |
| `LOAD DATA` parser + spec node | `sail-sql-parser`/`sail-common::spec` | native LOAD DATA resolver/physical (04) |
| `SnapshotRetention` struct fields | `spec/snapshots/snapshot.rs:128-140` | accessor methods (05) |
| `MergeCardinalityCheckNode` physical exec | `crates/sail-physical-plan/src/merge_cardinality_check.rs` (present) | MERGE (03) |

---

## 5. Revised port checklist (v0.7.0-aware)

1. **Spec + parser + analyzer** (`01`): add `CALL`, `SHOW TBLPROPERTIES`, `TRUNCATE
   TABLE`, `DescribeItem::View`, `DescribeTable{column}`, six ALTER ops + helper types,
   `CommandNode::CallProcedure`/`ShowTblProperties`. (LOAD DATA grammar already there.)
2. **Common surface**: `SourceInfo.metadata_table` + mechanical `None` updates;
   `UpdateInfo`/`UpdateAssignment` + `TableFormat::create_updater`;
   `IcebergMetadataTableType`.
3. **Logical plans**: `call_procedure.rs`, `load_data.rs`; `merge.rs` → add
   `expand_update`/`UpdateExpansion` only (rest is present).
4. **Resolvers**: `command/call.rs`, `command/load.rs` (un-todo), `command/update.rs`,
   `query/read.rs` metadata hook, `command/mod.rs` dispatch, `catalog/table.rs` mapping.
5. **Catalog**: `ShowTblProperties`, `DescribeTable{column}`, Iceberg-REST
   REPLACE/alter/rename/access-delegation, memory rename, glue/hms/onelake/delta stubs.
6. **Iceberg physical** (03/04/05/02): planner module, row-level + load-data + CALL
   execs, expire GC, commit-exec machinery, writer changes, snapshot producer,
   metadata-table provider, codec nodes.
7. **Distributed exec** (07): worker-pool accounting/readiness/spawn-retry, RPC client
   hardening + io-runtime, server keepalive wiring, activity-tracker executor wiring,
   session self-heal, config keys. **Layout caveat**: the driver server is at
   `driver/gateway.rs` (not v0.6.6's `driver/actor/rpc.rs`), so the keepalive/io-runtime
   wiring must be applied to the v0.7.0 `DriverGateway` and the worker `serve` in
   `worker/actor/rpc.rs`.
8. **Docker / K8s / build.sh / Python tests** (08).
9. **Deps**: `sail-iceberg` += `sail-logical-plan`, `datafusion-datasource`, tokio
   dev-dep; `sail-catalog-memory` += `sail-common`; `sail-logical-plan` += `serde`
   (v0.7.0 already has `sail-function`).

---

## 6. Key verification evidence (grep/anchor points, v0.7.0 tree)

- `sail-iceberg/src/table_format.rs:622` — `TODO: Implement row-level DELETE/UPDATE/MERGE`
  (Iceberg row-level ops absent).
- `sail-plan/src/resolver/command/mod.rs:308` — `CommandNode::LoadData { .. } =>
  Err(PlanError::todo(...))` (LOAD DATA unimplemented).
- `sail-catalog-iceberg/src/provider.rs:1069` — `Replace table is not supported yet`;
  `:1224` — `alter table in Iceberg catalog` (REST replace/alter absent).
- `sail-execution/src/driver/task_assigner/state.rs:95` — `WorkerResource::Inactive`
  (accounting rewrite absent).
- `sail-iceberg/src/operations/snapshot.rs:83` — `is_overwrite` boolean, Append/Overwrite
  only (producer extension absent).
- `sail-iceberg/src/physical_plan/writer_exec.rs:302` — unconditional
  `UnspecifiedDistribution` (hash distribution absent); `:111-131` — `output_partitions`
  already present.
- `sail-iceberg/data/options/iceberg.yaml:568-573` — `compression_codec` `supported: false`
  (support flip absent).
- `sail-common-datafusion/src/session/activity.rs:8/26/35` — `ActivityTracker` present;
  `sail-spark-connect/src/executor.rs` — not referenced (wiring absent).
- `sail-server/src/builder.rs:15-21` — `ServerBuilderOptions` keepalive fields present;
  no `from_keepalive`.
- `sail-execution/src/driver/gateway.rs:130` — `ServerBuilder::new("sail_driver",
  Default::default())` via `tokio::spawn`; `sail-execution/src/worker/actor/rpc.rs:72` —
  `ServerBuilder::new("sail_worker", Default::default())`, `serve(handle, addr)` takes no
  options (keepalive/io-runtime wiring absent; v0.7.0 layout differs from v0.6.6's
  `driver/actor/rpc.rs`).
- `sail-execution/src/worker_manager/mod.rs:13/27` — `launch_worker` + `stop` only, no
  `delete_worker` (per-worker k8s/local deletion absent).
- `sail-execution/src/rpc.rs:40-47` — `ServerMonitor::start(self, f)` no runtime handle;
  `:74-78` — `ClientOptions { enable_tls, host, port }` no `runtime` (client hardening
  absent).
- `sail-common/src/spec/plan.rs:1373` / `sail-catalog/src/provider/options.rs:139` /
  `sail-common-datafusion/src/datasource.rs:451` — limited ALTER enums (new variants
  absent).
- `crates/sail-iceberg/src/catalog_support/commit.rs:23/64` — `CatalogCommitOutcome`,
  `IcebergCatalogCommitMode` present (reuse).
