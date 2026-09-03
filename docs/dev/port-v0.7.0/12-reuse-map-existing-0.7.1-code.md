# Reuse Map — What feat/0.7.1 Already Has (per Port Cluster)

> Part of the `docs/dev/port-v0.7.0/` inventory. This is the **target-branch companion** to
> `11-gap-analysis-vs-0.7.1.md`. While docs 00–10 inventory the *source* (`feat/0.7.0`), this
> file records, cluster by cluster, exactly what **`feat/0.7.1` already implements**, where it
> lives, and the additive-edit guidance for the port — so new code reuses 0.7.1's own patterns
> and is not duplicative or destructive.
>
> Evidence basis: fresh full codebase index (project `sail`, `feat/0.7.1` @ `9544c925`,
> 31 682 nodes) + source sweeps + spot-checked `file:line` anchors. Where the target lacks a
> feature the statement is explicit ("ABSENT"), so porting can proceed deliberately.
> Ground-truth source branch remains available at `/home/soumilk/sail-0.7.0` (`c07ad0c8`).

---

## 1. How to use this document

For each port cluster: (a) reuse the listed existing machinery/idioms instead of re-introducing
it; (b) only add what the "port delta" column says; (c) mind the traps in §8 (module moves,
generated code, lint rules). Every cluster doc (01–10) from the source inventory tells you
*what* was implemented on 0.7.0; this file tells you *where it fits on 0.7.1*.

---

## 2. Session, runtime & config (source doc 01)

**Target state (0.7.1)**
- `AppConfig` (sail-common `crates/sail-common/src/config/application.rs:23`) sections today:
  `mode, runtime, cluster, execution, kubernetes, parquet, catalog, optimizer, spark, flight,
  python, telemetry, internal`. **No `server` section, no `object_store` section.** Config is a
  flat `application.yaml` item list (`crates/sail-common/src/config/application.yaml`), loaded
  via figment with `SAIL_`-prefixed env, `__`→`.` (`application.rs:74-83`); typed env consts via
  `define_cluster_config_env!`/`ClusterConfigEnv` (`application.rs:900-934`). Catalog config:
  `CatalogType` internally-tagged enum `application.rs:740-834` (IcebergRest has
  `uri/warehouse/prefix/namespace_separator/oauth/bearer/…` **no `access_delegation`**).
- Server session config lives inline in `ServerSessionFactory::create_session_config`
  (`crates/sail-session/src/session_factory/server.rs:104`) with the three appliers
  `apply_execution_config` (:166), `apply_optimizer_config` (:181), `apply_execution_parquet_config`
  (:194). **Workers do NOT apply these** (`WorkerSessionFactory` `crates/sail-session/src/session_factory/worker.rs:14`,
  config = `SessionConfig::default()` + DeltaTableCache + RepartitionBufferConfig) — this is the
  exact parity gap the 0.7.0 `SessionConfigFactory` closed.
- Session manager: `crates/sail-session/src/session_manager/mod.rs` (`SessionManager::try_new`,
  `get_or_create_session_context` :47, free ctor `create_session_manager` :87), actor
  `session_manager/actor/{mod.rs,core.rs,handler.rs,message.rs,options.rs,session.rs}`;
  messages are `SessionManagerMessage` (**no `SessionManagerEvent`**) and include
  `GetOrCreateSession/ProbeIdleSession/DeleteSession/SetSessionFailure/GetDriver/Shutdown`;
  idle cleanup via `ProbeIdleSession` + `ActivityTracker` (`handler.rs:165-192`);
  `ServerSessionState { Running, Deleted, Failed }` (`session.rs:8`). Session timeouts default:
  spark 900 s, flight 3600 s (yaml).
- `ActivityTracker` extension exists (`sail-common-datafusion/src/session/activity.rs:44`).
- Keepalive: `ServerBuilderOptions` (`crates/sail-common/src/server/builder.rs:16-35`) defaults
  `http2_keepalive_interval 1 min`, `http2_keepalive_timeout 10 s`. **No `server.*` config key.**

**Port delta (additive)**: `ServerConfig`/`ObjectStoreConfig`/`IcebergRestAccessDelegation`
types + `application.yaml` keys; refactor the three inline appliers on `server.rs` into a shared
`SessionConfigFactory` and apply it in `worker.rs` too (include the
`enable_file_stream_work_stealing=false` only after confirming 0.7.1's worker-side decode +
file-scan rewrite semantics — see §4); optional `SessionIdleDuration` actor message if the
multiplexer teardown needs cross-protocol idle introspection.

**Reuse**: figment config plumbing, serde `deny_unknown_fields` + alias idioms, the existing
mutator extension point `ServerSessionMutator` (`session_factory/server.rs:39`), cluster/session
timeout constants.

---

## 3. Spark Connect multiplexer + combined server (source doc 02)

**Target state (0.7.1)**
- `SparkConnectServer` (`crates/sail-spark-connect/src/server.rs:27`) maps each request's
  `session_id` 1:1 → `session_manager.get_or_create_session_context(session_id, user_id)`;
  `server_side_session_id` is always the same id. **No canonical/multiplexed session.** TODO at
  server.rs:117. `release_session` → `delete_session`; `allow_reconnect` unsupported; several
  RPCs unimplemented (`clone_session/get_status/fetch_error_details`, `server.rs:471-495`).
- `add_artifacts`: consistency check `session ID must be consistent`
  (`server.rs:317-319`), handler `service::handle_add_artifacts`
  (`crates/sail-spark-connect/src/service/artifact_manager.rs:12`) is a `SparkError::todo`
  stub in 0.7.1.
- Spark Connect service registration + gzip/zstd + reflection
  `crates/sail-spark-connect/src/entrypoint.rs:30-41`; `SparkError`/`SparkResult`
  (`src/error.rs`), `SparkThrowable` → gRPC `Status` with `ErrorDetails` + 2500-byte truncation.
- Flight: `SailFlightSqlService` (`crates/sail-flight/src/service.rs:33`) with fixed
  `DEFAULT_SESSION_ID = "flight-default"`/`DEFAULT_USER_ID` (:55-56); `new(session_manager)`;
  **no `with_default_session`**. Flight entrypoint uses `ServerBuilderOptions::default()` with no
  reflection/compression (`crates/sail-flight/src/entrypoint.rs:29-31`).
- CLI: subcommands `Spark|Flight|Worker` only (`crates/sail-cli/src/runner.rs:19-27`); spark
  server launch helper `crates/sail-cli/src/spark/server.rs:70` (`with_spark_connect_server`).

**Port delta (additive)**: `sail-spark-connect/src/multiplexer.rs`, canonical-session id
resolution (config or UUID), `SailFlightSqlService::with_default_session`, the combined
`sail-cli` `server` verb + `combo.rs` running both protocols off ONE shared session manager, the
new `server.session_id` config key.

**Reuse**: existing service-registration/compression helpers, `SparkError`, the shared
`create_spark_session_manager`/`create_flight_session_manager`, and the entrypoint shutdown
pattern (`session_manager.shutdown()`, system join).

---

## 4. Distributed execution — worker pool / job graph / RPC (source doc 03)

**Target state (0.7.1)**
- Actor system lives in `sail-common` (**no `sail-server` crate on 0.7.1**):
  `crates/sail-common/src/actor.rs` — `Actor`/`ActorAction`, `ActorContext` (`send`/`send_with_delay`/`spawn`),
  `ActorSystem`, `ActorHandle`; oneshot-reply idiom throughout. Server building:
  `crates/sail-common/src/server/builder.rs` (`ServerBuilder`/`ServerBuilderOptions`, health +
  reflection + keepalive; `.add_service(service, Some(r#gen::FILE_DESCRIPTOR_SET))`).
- Worker pool `crates/sail-execution/src/driver/worker_pool/{mod.rs,core.rs,state.rs,options.rs}`:
  states `Pending|Running|Completed|Failed`; **no `running_worker_count`, no
  `prune_terminal_workers`, no `delete_worker`, no idle scale-down floor** (terminal descriptors
  accumulate; idle stop condition is `is_worker_idle && last_update <= instant`,
  `driver/actor/handler.rs:101-121`; `worker_initial_count` only seeds startup,
  `handler.rs:41-47`).
- Task assigner `crates/sail-execution/src/driver/task_assigner/`: `WorkerResource` has
  `Active{task_slots,local_streams}` and **`Inactive` unit tombstone**; `deactivate_worker`
  sets Inactive (`core.rs:77`).
- `WorkerManager` trait = `launch_worker` + `stop` only (`crates/sail-execution/src/worker_manager/mod.rs:13`);
  k8s `stop` = delete_collection on label (`kubernetes.rs:418`).
- Job graph `crates/sail-execution/src/job_graph/{mod.rs,planner.rs}`: `Stage { inputs, plan,
  group, mode, distribution, placement }` — **no cached `encoded_plan`**; job scheduler encodes
  the stage plan per task attempt (`driver/job_scheduler/core.rs:655`). Scalar-subquery tracking
  is **boolean flags** (`planner.rs:200-227`, explicit TODO at :202-203). **File-scan rewrite is
  at task-run time**, not plan time: `crates/sail-execution/src/task_runner/actor/handler.rs:381`
  (`rewrite_file_scans` :408-438, sets preserve_order + schema-evolution expr adapter).
- `RemoteExecutionCodec` (`crates/sail-execution/src/proto/codec.rs:321`); proto oneof
  `ExtendedPhysicalPlanNode.NodeKind` 1–58 (`crates/sail-execution/proto/sail/plan/physical.proto`).
  Iceberg scan-by-data-files encoded at codec.rs:1492/2424; **no scalar-subquery handling in the
  codec**, **no `file_path_column`**, **no `iceberg_load_data_fast` node**.
- RPC clients `crates/sail-execution/src/rpc.rs`: `ClientOptions { enable_tls, host, port }`
  (**no `peer`/keepalive/timeouts**); `impl_client_builder!` macro; `ClientHandle` lazy connect.
  No `rpc_error` helper; errors = `ExecutionError::{TonicTransportError,TonicStatusError}`.
- `cluster.*` config already has `worker_initial_count` (default 4) / `worker_max_count` (0) /
  `worker_max_idle_time_secs` (60) / `task_launch_timeout_secs` (120) / `task_max_attempts` (3)
  — note 0.7.1 still uses task_max_attempts **3** and launch timeout **120** (source 0.7.0 moved
  attempts to 5 / stream creation to 120 — adopt only if desired).

**Port delta (additive)**: proto additions (`IcebergLoadDataFastExecNode` and
`file_path_column` — pick free oneof numbers > 58); codec arms + JSON round-trips; job-graph
scalar-subquery `SubqueryIndex`-set tracking; optional lazy per-stage plan encode;
moving/duplicating `rewrite_file_scans` to plan time (careful: it already runs at task time on
0.7.1 — decide ownership); `WorkerManager::delete_worker` default + k8s pod-delete; worker-pool
prune/running-count/idle floor; `rpc_error`/peer tagging + client keepalive.

**Reuse**: the actor framework, `ServerBuilder`, `ExecutionError` conversion surface, the
existing `rewrite_file_scans` implementation (move rather than reinvent), and
`IcebergScanByDataFilesExec` codec arms.

---

## 5. Object-store registry (source doc 04)

**Target state (0.7.1)**
`crates/sail-object-store/src/registry.rs` — `DynamicObjectStoreRegistry` (dashmap keyed by
scheme/authority/session fingerprint) with `register_session_store`,
`get_store_with_session`, lazy `RuntimeAwareObjectStore`/`get_dynamic_object_store`
(s3/gcp/azure/http/local/memory/hdfs/hf); installed in `RuntimeEnvFactory::create`
(`crates/sail-session/src/runtime.rs:48-58`). `s3.rs` builds the S3 store from env + AWS SDK
credentials; **no `ObjectStoreConfig`**; client options are crate defaults.

**Port delta**: `ObjectStoreConfig` type + `new_with_config` ctor +
`client_options_from_config` in `s3.rs` + the `object_store.*` yaml keys + wiring in
`RuntimeEnvFactory`. Purely additive.

---

## 6. SQL frontend — parser/analyzer/spec (source doc 05)

**Target state (0.7.1)**
- Parser AST `crates/sail-sql-parser/src/ast/statement.rs`: statements include `Update`,
  `Delete`, `LoadData`, `MergeInto`, `AlterTable`, `Describe`, … **ABSENT**: `TRUNCATE TABLE`
  (keyword exists in `data/keywords.txt:330` but unused), `CALL`, `SHOW TBLPROPERTIES`,
  `DESCRIBE VIEW`. `DescribeItem::Table` carries `column` (parser OK);
  `DescribeItem` has **no View variant**. `AlterTableOperation`/`AlterColumnOperation` grammar
  already has rename/add/drop columns, comment/set-not-null/drop-not-null/position/type/default
  (`ast/statement.rs:728-901`). Grammar gold `tests/gold_data/syntax.json`.
- Analyzer `crates/sail-sql-analyzer/src/statement.rs`: `from_ast_statement` (:103);
  `from_ast_alter_table_operation` (:2230) maps only properties/type/default/check-constraint;
  everything else incl. `AddColumns/ReplaceColumns` (validated then) → spec `Unknown`
  (:2293-2299); UPDATE/DELETE/MERGE → spec `CommandNode::Update/Delete/MergeInto`; LoadData →
  `CommandNode::LoadData`. Spec `plan.rs` `CommandNode` list **has no** `ShowTblProperties`,
  `CallProcedure`, `TruncateTable`, `DescribeView`. Spec `AlterTableOperation`
  (`spec/plan.rs:1371-1395`) = `Unknown/SetTableProperties/UnsetTableProperties/AlterColumnType/
  AlterColumnDefault/AddCheckConstraint` (TODO comment at :1394).
- Spark-connect plan gold files (`crates/sail-spark-connect/tests/gold_data/plan/*.json`)
  currently gold untranslated ALTER ops as `"operation": "unknown"` (`ddl_alter_table.json`).

**Port delta**: keywords (`CALL`, `TRUNCATE` reuse), AST statements/variants, `DescribeItem::View`,
analyzer conversions, spec `CommandNode`/`AlterTableOperation` variants + `ColumnDefinition`/
`ColumnPosition` helpers, `Identifier`/`ObjectName` `Ord` derives, gold-data regeneration.

**Reuse**: parser grammar mechanics, `from_ast_*` conventions, `Expression`/`DataType` spec,
gold-data test harness (`SAIL_UPDATE_GOLD_DATA=1`, `crates/sail-common/src/tests.rs`).

---

## 7. Catalog providers & DDL (source doc 06)

**Target state (0.7.1)**
- `CatalogManager`/`CatalogProvider`/`CatalogCommand`: `crates/sail-catalog/src/manager/mod.rs`,
  `provider/mod.rs`, `command.rs`. `CatalogCommand` variants (`command.rs:27-158`) include
  `AlterTable`/`DescribeTable{table,extended}`/… **no** `ShowTblProperties`, `CallProcedure`.
  `AlterTableOptions` (`provider/options.rs:137-159`) = `Set/UnsetTableProperties`,
  `AlterColumnType`, `AlterColumnDefault`, `AddCheckConstraint` — **no** Rename/Add/Drop/Column-
  comment/nullability/position, no `AddColumn`, no `CallProcedureOptions`.
- `CatalogCommand::AlterTable` execute (`command.rs:441-519`): storage-first
  (`TableFormat::alter_table` with `TableFormatAlterTableOperation` = SetProps/ColumnType/
  ColumnDefault/AddCheckConstraint only, `datasource.rs:456-480`) then `catalog_sync_alter_options`
  + `manager.alter_table`. Memory catalog implements replace (remove+recreate,
  `catalog-memory/src/provider.rs:199-211`); HMS/Glue/Unity reject or no-op replace;
  `CatalogError`/`CatalogObject` (error.rs) has no `Column` variant.
- Iceberg REST provider `crates/sail-catalog-iceberg/src/provider.rs`: `create_table` :1119
  (**`is_replace()` → `NotSupported("Replace table is not supported yet")`** :1154-1158),
  `get_table` :1235, `drop_table` :1279, **`alter_table` = `NotSupported`** :1309-1318,
  `commit_lakehouse_table` :1320-1432 (already serializes `TableRequirement`/`TableUpdate` JSON →
  `client.update_table` in `with_auth_retry`; maps 404/409/401/403/429 → CatalogError),
  `begin_table_access` :1434-1484 (vended-credentials header). Generated REST client already has
  `update_table`, `rename_table`, and `TableUpdate` actions incl. `AddSchema/SetCurrentSchema/
  SetProperties/RemoveProperties/SetSnapshotRef/RemoveSnapshots/RemoveSnapshotRef` +
  `TableRequirement` incl. `AssertRefSnapshotId`. `IcebergRestCatalogOptions` (provider.rs:120)
  has **no `access_delegation` field**.
- Lakehouse glue `crates/sail-catalog/src/lakehouse.rs`: `LakehouseExecutionContext`,
  `CommitAuthority`, `resolved_lakehouse_authority` (:352-392, Iceberg REST →
  CatalogAuthoritative{IcebergRest, IcebergRestCommit}), `LakehouseOperation::{…,Alter,Maintenance,…}`.
- RESOLVER: `resolve_delta_alter_table_or_catalog` (`crates/sail-plan/src/resolver/command/delta.rs:44`)
  and `resolve_catalog_alter_table` (`resolver/command/catalog/table.rs:464`) handle only the
  four existing ops; `DescribeTable` resolver rejects column (`mod.rs:337-339`).

**Port delta**: `AlterTableOptions`/`AddColumn`/`CallProcedureOptions` additions; REST provider
`alter_table` via `update_table` (rename/set+unset/add/drop cols) + CREATE-OR-REPLACE (drop w/
purge then create) + `access_delegation`; catalog command variants + row structs; resolver
mappings; `CatalogObject::Column`; provider `NotSupported` polish for HMS/Glue; shared
`TableFormatAlterTableOperation`/`TableFormatProcedureOperation`/`TableFormat::create_updater`/
`call_procedure` contract additions.

**Reuse**: `CatalogCommand` execute + row-display pattern (`display.bools()`, `ArrowSerializer`),
`with_auth_retry`, `commit_lakehouse_table` update_table path, `resolved_catalog_config`,
generated `r#gen` TableUpdate/TableRequirement, `catalog_sync_alter_options`,
Memory's replace semantics as the reference behavior.

---

## 8. Iceberg row-level ops (source doc 07) — the overlap cluster

**Target state (0.7.1) — different design (merge-on-read), do not blindly overwrite.**
- `IcebergTableFormat` implements **`create_deleter` (`crates/sail-iceberg/src/table_format.rs:152`)
  and `create_merger` (:209)**; **no `create_updater`**; explicit TODO at `table_format.rs:697`
  ("Add row-level UPDATE and configurable COW/MOR strategy selection"). Trait default methods in
  `sail-common-datafusion/src/datasource.rs:484-626`; **no `create_updater` on the trait**.
- Row-level physical planning `crates/sail-iceberg/src/physical/row_level_planner.rs:27`
  (`plan_iceberg_row_level_write`), rejects `Update` (:33-37, `not_impl_err`); mode gate
  `ensure_current_row_level_mode` (:171) allows only `merge-on-read`.
  `plan_iceberg_delete` (:99) → **equality-delete** writer
  (`physical_plan/equality_delete_writer_exec.rs:33`) committed as `RowDelta`;
  `plan_iceberg_merge` (:40) → **position-delete + append** via
  `IcebergWriterExec::new_merge`, `IcebergMergeRowProjection` (`merge_row_projection.rs`),
  `PositionDeleteAccumulator` (`position_delete_writer.rs`), `IcebergMergeMetadataExec`
  (`merge_metadata_exec.rs`, row-index + file-path metadata), commit via
  `IcebergCommitExec` `RowDelta` with `expected_snapshot_id` guard.
- Logical merge `crates/sail-iceberg/src/logical/merge.rs:23` `expand_merge_node` (adds
  `__sail_file_path`, `__sail_iceberg_partition_spec_id`, `__sail_iceberg_partition` metadata);
  `RowLevelWriteNode::new_merge/new_delete` exist (`crates/sail-logical-plan/src/merge.rs:176/214`),
  **no `new_update`**; `expand_merge` (`merge.rs:505`) has `row_index_delete_plan` + touched-file
  slots (Delta uses them; Iceberg passes an empty touched plan placeholder).
- `IcebergCommitExec` (`crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs:132`):
  consumes Arrow action batches (`ActionRow` add|delete|commit_meta) from one partition;
  `SnapshotUpdateKind { FastAppend, FullOverwrite, RowDelta }` (`operations/snapshot.rs:269`);
  schema/spec-required validation, `expected_snapshot_requirement`, orphan-task-file cleanup,
  `MAX_COMMIT_RETRIES=5`; **no `touched_file_paths`/`overwrite_predicate`/
  `overwrite_partition_values`/`reported_row_count` anywhere**.
- `IcebergScanByDataFilesExec` (`physical_plan/scan_by_data_files_exec.rs:208`) has **no**
  `file_path_column`; file-path/row-index metadata on the *provider scan* path instead
  (`IcebergTableProvider::with_file_column/with_row_index_column`, `datasource/provider.rs:197`,
  `RowLevelMetadataColumns`). Delete application at read:
  `IcebergDeleteApplyExec` (`delete_apply_exec.rs:37`). Scan statistics empty → `new_unknown`
  (`provider.rs:657`).
- Delta: DELETE/MERGE both implemented with COW (eager) + MOR (DV) strategies
  (`sail-delta-lake/src/physical_plan/planner/row_level.rs`, `dv_writer_exec.rs`); Delta UPDATE
  unimplemented too.

**Port delta**: (1) Iceberg **UPDATE** — genuinely absent on 0.7.1: add trait `create_updater`,
`UpdateInfo`/`UpdateAssignment`, logical `expand_update`, `physical_plan/planner/*` UPDATE arm
or a `plan_iceberg_update`, `RowLevelWriteNode::new_update`; (2) optionally the COW targeted
rewrite machinery — **do NOT replace 0.7.1's delete writers** without a deliberate decision
(multiple upstream commits shipped the MOR stack; see `11-gap-analysis-vs-0.7.1.md §3`);
(3) empty-table DELETE/TRUNCATE no-op; (4) commit-exec `reported_row_count` semantics if UPDATE
needs predicate-row counts.

**Reuse**: `MergeCapableSource`/metadata-column plumbing, `SnapshotProducer`,
`IcebergCommitExec` action-batch protocol, `IcebergWriterExec` internals, expected-snapshot
guards, `IcebergTableWriter`/`DataFileWriter` for COW file production, delta's COW
`planner/op_delete/op_merge` as a structural template (they already do targeted rewrite against
`touched_files_plan`).

---

## 9. Iceberg LOAD DATA & write path (source doc 08)

**Target state (0.7.1)**
- **LOAD DATA: ABSENT** end-to-end (no `LoadDataNode`, no `IcebergLoadDataFastExec`, no
  classifier; `CommandNode::LoadData` is resolver `todo`). `LoadData` SQL parses/analyzes fine.
- Write path: `IcebergWriterExec`/`IcebergWriterExecOptions`
  (`crates/sail-iceberg/src/physical_plan/writer_exec.rs:57`, `writer_options.rs:43`) — fields
  merge/overwrite_schema, write paths, table_properties, lakehouse_table, variant shredding;
  **no compression/target-size/commit_operation/touched/overwrite fields**. Writer requires
  hash distribution for MERGE only (`required_input_distribution`, `writer_exec.rs:253`),
  otherwise unspecified; IcebergPlanBuilder repartitions (RoundRobin 4 unpartitioned / Hash 4
  partitioned) and supports FastAppend/FullOverwrite only (`plan_builder.rs:153-169`);
  `OverwriteIf`/`OverwritePartitions` not implemented. `WriterConfig`
  (`operations/write/config.rs:24-35`) hard-codes `WriterProperties::default()` (parquet
  SNAPPY) + `target_file_size 134_217_728` and **no file-size rollover** (one file per
  partition per task; `IcebergTableWriter::close` flushes all).
  `ArrowParquetWriter` = `AsyncArrowWriter<Vec<u8>>` (full buffering; **no AsyncShareableBuffer
  in iceberg**). Options yaml: `compression-codec`, `target-file-size-bytes`,
  `write.target-file-size-bytes` etc. are `supported: false` (no generated fields).
- Delta is the in-repo model for rolling writers: `sail-delta-lake` `PartitionWriter` rolls at
  `target_file_size` (`writer/mod.rs:477`), `AsyncArrowWriter<AsyncShareableBuffer>`
  (`writer/async_buffer.rs`), supported `target_file_size`/`write_batch_size` options.

**Port delta**: LOAD DATA = `LoadDataNode` (logical) + resolver + `physical/load_classifier.rs` +
`load_data_planner.rs` + `IcebergLoadDataFastExec` + proto/codec node + table-format layer glue;
writer upgrades = make `compression_codec`/`target_file_size_bytes` supported in
`iceberg.yaml`, add the exec-options fields + `build_writer_properties` + rolling in
`IcebergTableWriter` + `AsyncShareableBuffer` copy + hash distribution for partitioned writes.

**Reuse**: option-resolution machinery (see doc 13 §3), `split_iceberg_write_options_and_table_properties`
(`table_format.rs:1091`), `metadata_location_from_options`/`catalog_managed_iceberg_from_options`,
`prepare_iceberg_write_context`/`IcebergPlanBuilder`, `IcebergTableFormat::create_table_metadata`
bootstrap / `replace_empty_table_metadata`, delta's rolling writer + async buffer as templates,
`IcebergCommitExec` bootstrap/conflict paths for the fast-register commit.

---

## 10. Procedures / metadata tables / GC (source doc 09)

**Target state (0.7.1)**
- **ABSENT**: no `CallProcedureOptions`, no CALL grammar/resolver, no `.snapshots`/`.refs`
  metadata tables, no `expire_snapshots` GC, no `IcebergMetadataTableType`,
  no `SourceInfo.metadata_table` (the shared `SourceInfo` at
  `sail-common-datafusion/src/datasource.rs:189-208` has no such field).
- Available building blocks: `Snapshot`/`SnapshotReference`/`SnapshotRetention`/
  `SnapshotLog`/`MetadataLog`/`refs` in the vendored `sail-iceberg/src/spec/snapshots/*` and
  `table/mod.rs` (snapshot selection :141-232); REST `load_table(…, snapshots=refs|all)` query
  supported by the generated client but unused (provider.rs:285 passes None);
  commit machinery (`SnapshotProducer`, `Transaction`, `retry`-style commits with
  `metadata_files_for_version` + version-hint); catalog `commit_lakehouse_table` +
  generated `TableUpdate` actions incl. `RemoveSnapshots/RemoveSnapshotRef/SetSnapshotRef` +
  `AssertRefSnapshotId`.

**Port delta**: everything — but hang it on: new `TableFormatProcedureOperation` +
`TableFormat::call_procedure` default + `CallProcedureOutput`; catalog `CallProcedure` command
+ `CallProcedureOptions`; resolver `resolve_command_call_procedure`; iceberg procedure
updates/retain-set + `expire_files_gc`; `IcebergMetadataTableType` + `SourceInfo.metadata_table`
(**ripples through every `SourceInfo` destructure**) + resolver named-read hook +
`IcebergMetadataTableProvider`; wire `snapshots=refs/all` on REST loads.

**Reuse**: spec snapshot/ref model, `load_manifest_list`/`load_manifest` readers, REST
`TableUpdate`/`TableRequirement` set, commit + orphan-cleanup helpers.

---

## 11. Config/docker/k8s/python/build (source doc 10)

**Target state (0.7.1)**
- Docker: chef-less `docker/dev|release/Dockerfile`, `pyspark-client` pip, root user;
  quickstart installs `pysail` + `pyspark-client`; k8s manifest is the old single-protocol
  `sail-spark-server` (port 50051, `sail spark server`); no `build.sh`; README/guide forward
  `50051`.
- Python tests on target: `python/pysail/tests/flight/test_flight.py` (FlightSqlServer + adbc);
  `tests/spark/catalog/iceberg_rest/` uses docker `apache/iceberg-rest-fixture` + MinIO
  (`python/pysail/testing/containers/iceberg_rest.py`); `conftest.py` (root + spark) uses
  `yamlsnapshot` syrupy plugin, `-m integration`, `SPARK_REMOTE`; doctests via pytest; no
  heimdall naming on this branch.

**Port delta**: Dockerfiles (chef layering, `pyspark[connect]`, non-root sail user, labels),
`build.sh`, k8s `sail-server` combined manifest + `15002`/`32010` service + worker-pool env,
kubernetes guide/README port updates, python flight conftest fixture + heimdall tests,
REST `test_commit.py` CREATE-OR-REPLACE test, TEST_PLAN.md.

---

## 12. Traps & cautions for additive, non-destructive edits

1. **`sail-server` crate does not exist on 0.7.1** — actor + server infra moved into
   `sail_common::{actor,server}`. Never reference `sail_server::*` in new code.
2. **Generated code**: options (`data/options/*.yaml` → OUT_DIR via
   `sail-build-scripts/src/data_source/options.rs`, only `supported: true` scopes generate
   fields), proto (`build.rs` + `r#gen` modules + `FILE_DESCRIPTOR_SET`), keywords
   (`data/keywords.txt`), system catalog, function metadata, metrics. Change the YAML/spec
   **and** rebuild; don't hand-edit OUT_DIR artifacts. `physical.proto` oneof numbers must not
   collide (currently up to 58).
3. **Lints**: workspace denies `unwrap_used`/`expect_used`/`panic`/`dbg_macro`/`todo`; use
   `#[expect(clippy::…)]`; new files should NOT carry Apache license headers (only vendored
   files do). rustfmt group_imports.
4. **No license header on new files**; keep headers only when carrying over vendored files.
5. **Error propagation**: keep `Result<_, String>` inside writer internals, convert at the exec
   boundary (`DataFusionError::Execution`); implement `From<MyError> for DataFusionError` once;
   per-crate `XError`/`XResult` + tiny ctors; explicit `From` arms for sibling-crate errors.
6. **Row-level ops**: 0.7.1 is deliberately MOR (delete writers, DV). Re-landing 0.7.0's COW
   targeted rewrite requires a conscious supersede decision; port UPDATE and the commit-exec
   count semantics as additive gaps first.
7. **File-scan rewrite already runs at task time on 0.7.1** (`task_runner/actor/handler.rs:381`).
   If the plan-time variant is ported, remove/reconcile the task-runner copy to avoid double work.
8. **`metadata_files_for_version` returns paths (not timestamps) on 0.7.1** — the timestamp
   return type + stale-file helpers are a source-branch change that ripples through the commit
   loops; port them consciously.
9. Config defaults on target differ from the source branch's tuned values
   (`task_max_attempts` 3 vs 5; launch timeout 120 vs stream-creation 120 etc.) — adopt the
   source values only when they matter to the ported feature.
