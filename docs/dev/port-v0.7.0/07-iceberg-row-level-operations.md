# Porting feat/0.7.0 → feat/0.7.1 — Doc 07: Iceberg Row-Level Operations (UPDATE / DELETE / TRUNCATE / MERGE)

> Part of the `docs/dev/port-v0.7.0/` inventory. This is the largest cluster on
> `feat/0.7.0`. It documents the **targeted-rewrite row-level operations** implemented for
> Iceberg on top of base `f0b137d6`: the shared `UpdateInfo`/`DeleteInfo` contracts, the
> `IcebergTableFormat::create_deleter/updater/merger` entry points, the logical UPDATE
> expansion, the new `physical_plan/planner/` module (`context.rs`, `helpers.rs`,
> `commit.rs`, `op_delete.rs`, `op_update.rs`, `op_merge.rs`), the `IcebergCommitExec`
> extension (reported row count, parent-manifest filtering, Delete/Replace operations,
> stale-metadata-file handling, empty-table DELETE), the `__sail_file_path` column end to
> end, and the SQL-frontend/resolver wiring (UPDATE statement; TRUNCATE ⇒ DELETE; metadata
> table read detection). Sibling docs: 05 (spec/parser/analyzer), 06 (catalog DDL & commit
> authority), 08 (writer + LOAD), 09 (procedures/GC/metadata tables — shares the commit exec
> and file-path scan work).
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`.

---

## 1. Scope

| File | Delta |
|---|---|
| `sail-common-datafusion/src/datasource.rs` | `UpdateInfo`, `UpdateAssignment`; `TableFormat::create_updater` default; `MERGE_FILE_COLUMN` reuse |
| `sail-iceberg/src/table_format.rs` | `create_deleter`, `create_updater`, `create_merger`; `call_procedure`; `retry_metadata_commit`; `alter_table_properties` refactor; catalog-managed ALTER rejection relaxation (RenameTable) |
| `sail-iceberg/src/logical/update.rs` | NEW — `expand_update_node`, `ensure_update_metadata_columns`, `try_enable_metadata_column` |
| `sail-iceberg/src/logical/table_source.rs` | `IcebergTableSource` implements `MergeCapableSource` (`file_column`) |
| `sail-iceberg/src/physical/row_level_planner.rs` | NEW — dispatch DELETE/MERGE/UPDATE → planner ops |
| `sail-iceberg/src/physical/table_scan_planner.rs` | `plan_extension` handles `RowLevelWriteNode` + `LoadDataNode`; `plan_table_scan` file-column route |
| `sail-iceberg/src/physical_plan/planner/{mod,context,helpers,commit,op_delete,op_merge,op_update}.rs` | NEW planner module |
| `sail-iceberg/src/physical_plan/commit/{commit_exec.rs, mod.rs}` | `touched_file_paths`/`overwrite_predicate`/`overwrite_partition_values`; `reported_row_count`; parent-manifest filters; `Operation::Delete`/`Replace`; stale-metadata-file conflict handling; empty-table DELETE; tests |
| `sail-iceberg/src/physical_plan/action_schema.rs` | `CommitMeta`/`CommitMetaAction` new fields; encode/decode; tests |
| `sail-iceberg/src/physical_plan/scan_by_data_files_exec.rs` | `file_path_column` (synthetic partition col carrying the exact manifest path) |
| `sail-iceberg/src/physical_plan/mod.rs`, `lib.rs`, `logical/mod.rs`, `datasource/mod.rs`, `properties.rs` | module wiring; `is_reserved_iceberg_table_property` pub |
| `sail-iceberg/src/datasource/provider.rs` | empty-scan stats exact `0` (COUNT(*) fold), +tests |
| `sail-iceberg/src/datasource/type_converter.rs` | `is_utc_timezone` (Etc/UTC etc.) |
| `sail-iceberg/src/spec/...` | `TableMetadata::snapshot(id)`; `SnapshotReference::{min,max}` retention accessors (GC, doc 09) |
| `sail-iceberg/src/utils/metadata.rs` | `metadata_files_for_version` → `(path, ts)`; `is_stale_metadata_file`, `get_metadata_file_timestamp` + tests |
| `sail-logical-plan/src/merge.rs` | `RowLevelWriteNode::new_update`, `UpdateExpansion`, `expand_update` |
| `sail-plan/src/resolver/command/{mod,update.rs,read.rs,delete.rs,delta.rs,merge.rs,write.rs,catalog/table.rs}` | UPDATE resolver (new file); metadata-table read detection; ALTER option mapping; `metadata_table: None` destructures |
| `sail-sql-analyzer`/parser/spec | TRUNCATE⇒DELETE, UPDATE command (doc 05) |

---

## 2. Architecture: targeted rewrite

Model: each row of the Iceberg **target scan** is tagged with its source file path in a
synthetic column `__sail_file_path` (`MERGE_FILE_COLUMN`, `sail_common_datafusion`). Row-level
operations then:

1. run the target scan with the file-path column exposed
   (`MergeCapableSource::with_file_column` → scan reads manifest → discovery →
   `IcebergScanByDataFilesExec` which materializes the path as a partition column);
2. compute the **touched files** = distinct files containing rows matching the operation
   condition (executed eagerly at plan time from a `touched_files_plan`);
3. build the **write input**: untouched rows (anti-joined away from the touched set) + the
   rewritten/touched rows, then strip internal columns;
4. commit through `IcebergCommitExec` with `touched_file_paths` so the commit keeps
   non-touched parent manifests and replaces only touched ones; `DELETE`/`TRUNCATE` commit
   `Operation::Delete`; UPDATE/MERGE-with-rewrite commit `Operation::Overwrite`.

Iceberg does **not** use row-index/DV metadata columns for these ops (`with_row_index_column`
returns `self.clone()`); a future DELETE-by-DV extension is noted.

---

## 3. Shared contracts (`sail-common-datafusion/src/datasource.rs`)

```rust
pub struct UpdateInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub target: Arc<LogicalPlan>,          // resolved logical target scan (real column names)
    pub condition: Option<ExprWithSource>,
    pub assignments: Vec<UpdateAssignment>, // SET col = expr (only len-1 column_paths honored today)
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub options: Vec<OptionLayer>,
}
pub struct UpdateAssignment { pub column_path: Vec<String>, pub expression: Expr }

// trait default:
async fn create_updater(&self, ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan>;
```

`DeleteInfo` and `MergeInfo` pre-exist on the base. `TableFormat::create_deleter` already
existed as a trait method; Iceberg now implements it.

---

## 4. `IcebergTableFormat` entry points (`sail-iceberg/src/table_format.rs`)

### 4.1 `create_deleter`

Builds a `RowLevelWriteNode::new_delete(EmptyRelation, empty DFSchema, condition, "iceberg",
path, table_name, options, lakehouse_table)` wrapped in a `LogicalPlan::Extension`. The empty
relation is only a placeholder — the physical planner (plan_delete) does the real work. Comment
notes row-level DELETE/UPDATE/MERGE use targeted rewrite; low-level delete-artifact sinks
(deletion vectors) are a future extension.

### 4.2 `create_updater`

`crate::logical::update::expand_update_node(info)` (see §6).

### 4.3 `create_merger`

`expand_merge(info, MERGE_FILE_COLUMN, None)` → `RowLevelWriteNode::new_merge(raw_target,
raw_source, raw_input_schema, write_plan, touched_files_plan,
expansion.deletion_vector_plan.map(Arc::new), expansion.options, expansion.output_schema)`.
(`RowLevelWriteNode::new_merge` existed; `expand_merge`'s third arg is the DV-plan slot, `None`
for Iceberg.)

### 4.4 `call_procedure`

Iceberg procedure execution (filesystem-commit only — catalog-managed commits return
`not_impl_err!` because `call_procedure` receives only a runtime env, no session/catalog
access). Details in doc 09 §3.

### 4.5 `retry_metadata_commit` + `alter_table_properties` refactor

`retry_metadata_commit(object_store, store_ctx, table_url, initial_latest_meta,
check_post_write, mutate)` is a generalized version of the old
`alter_table_properties` commit loop: re-read latest metadata (attempt 1 reuses the passed
`initial_latest_meta`), apply `mutate(&mut TableMetadata)`, write next monotonic version, and
on version collisions distinguish **real concurrent writes** from **stale leftover files**
using `get_metadata_file_timestamp(current) + is_stale_metadata_file(candidate, current)`.
`alter_table_properties` now calls it with `check_post_write = true` and the property-change
closure; `call_procedure` uses it with `check_post_write = true` and
validate/apply closures. Stale handling fixes DROP+CREATE reuse of version numbers.

### 4.6 Catalog-managed ALTER rejection relaxed

`reject_catalog_managed_iceberg_alter(lakehouse_table, operation)` now allows `RenameTable`
for non-filesystem authority: Iceberg has no storage-level rename metadata update, so
catalog-managed tables rename purely through the catalog (REST `rename_table`, doc 06).

---

## 5. Logical UPDATE expansion (`logical/update.rs`, NEW)

### 5.1 `IcebergTableSource` becomes `MergeCapableSource`

`table_source.rs`:

- new field `file_column: Option<String>`;
- `schema()` appends a `Utf8` nullable column named `file_column` when set (unless the base
  schema already has that name);
- `supports_filters_pushdown` returns `Unsupported` for every filter when a file column is
  set (row-level scan path reads all files and applies no filters itself; predicates stay
  above the scan — mirrors the metadata-as-data read path);
- `MergeCapableSource` impl:
  - `file_column_name() -> Option<&str>`,
  - `with_file_column(name)` → clone with `file_column = Some(name)`,
  - `row_index_column_name() -> None`,
  - `with_row_index_column(name)` → returns `self.clone()` (no-op, documented).

### 5.2 `expand_update_node(info: UpdateInfo) -> Result<LogicalPlan>`

1. `target_plan = ensure_update_metadata_columns(info.target.clone())` — see below.
2. If the file column is not present in the resulting schema, project all existing columns
   plus `__sail_file_path` (aliased as itself).
3. Stash the raw (pre-file-column) target/input-schema/location/table_name/options/
   lakehouse/condition, then call `expand_update(info-with-enhanced-target,
   MERGE_FILE_COLUMN)` from `sail_logical_plan::merge`.
4. Build `RowLevelWriteNode::new_update(raw_target, raw_input_schema, write_plan,
   touched_files_plan, condition, "iceberg", location, table_name, options, lakehouse_table)`.
5. Return `LogicalPlan::Extension`.

`ensure_update_metadata_columns(plan)` walks up (`transform_up`):
- a `TableScan` whose source is an `IcebergTableSource` without a file column →
  `try_enable_metadata_column` produces the new source + schema; the scan projection (or a
  full-range projection) is extended to include the file-column index and a fresh
  `TableScan::try_new` is emitted;
- a `Projection` whose input has the file column but whose expr does not reference it →
  append `__sail_file_path` (column or alias) to the projection.

`try_enable_metadata_column` downcasts to `IcebergTableSource`, returns `None` if a file
column is already configured, else `with_file_column(MERGE_FILE_COLUMN)` + its schema.

---

## 6. `expand_update` (`sail-logical-plan/src/merge.rs`)

New `UpdateExpansion { write_plan, touched_files_plan, output_schema (empty DFSchema) }`.

`expand_update(info: UpdateInfo, path_column: &str)`:
- assignments are indexed by single-part column path only; nested (multi-part) assignments are
  dropped with a `trace!` (no nested-column UPDATE support yet);
- **write_plan**: for each target field (skipping the path column), project either the current
  column or `CASE WHEN condition THEN assignment ELSE current END` (when conditioned) / the raw
  assignment (unconditioned), each aliased to the field name; finally append the path column;
- **touched_files_plan**: `target → filter(condition) → project(path_column)` (or plain
  project when no condition);
- the `RowLevelWriteNode::new_update` ctor fills a node with
  `command: Update`, `write_plan`, `touched_files_plan`, `condition`, and empty merge slots.

---

## 7. Physical planner module (`physical_plan/planner/*`, NEW)

### 7.1 `context.rs` — `PlannerContext<'a>`

```rust
pub struct PlannerContext<'a> { session, options: IcebergWriteOptions, table_url: Url,
    lakehouse_table: Option<LakehouseExecutionContext>, table: Table }
PlannerContext::new(session, options, table_url, lakehouse_table, metadata_location,
                    catalog_managed_table) -> Result<Self>
```

Loads `Table::load_with_metadata_location(session, table_url, metadata_location)` where
`metadata_location` is only honored for catalog-managed tables (`catalog_managed_table.then_some
(metadata_location).flatten()`); path tables fall back to the storage scan. In-code rationale:
loading from storage while committing via the catalog location would let the planner and the
commit exec disagree on the current state (e.g. DELETE/TRUNCATE against a stale snapshot).
Accessors + a reserved `object_store()` helper (unused today).

### 7.2 `helpers.rs`

- `collect_touched_file_paths(session, touched_files_plan) -> (Vec<String>, u64)` — executes
  the touched plan at **plan time** on a `TaskContext` built from the session config + runtime
  env; sums pre-dedup row count (`matched_row_count`), collects distinct non-empty `Utf8`
  column-0 values, returns sorted paths. The doc comment is explicit: must be awaited inside
  the tokio runtime (the plan contains `RepartitionExec`/parquet `DataSourceExec` that spawn
  tokio tasks; `futures::executor::block_on` would deadlock).
- `build_targeted_writer_input(write_plan, touched_file_paths) -> (untouched_rows,
  touched_rows)` — requires the write plan to carry `MERGE_FILE_COLUMN`; filters out insert
  rows (`IsNotNullExpr` on the file column), builds a one-column in-memory touched-path source
  (`touched_paths_source`), then:
  - untouched = `RightAnti` hash join (touched × non-insert on file path), full projection;
  - touched = `Inner` join, projecting only the right-side (write-plan) columns.
  Both `CollectLeft` partition mode, `NullEquality::NullEqualsNothing`.
- `strip_internal_columns(input, table_schema)` — projects only the input columns whose names
  exist in the table schema (drops `__sail_file_path`, operation-type, etc.).

### 7.3 `commit.rs` — `assemble_iceberg_commit_plan`

```rust
assemble_iceberg_commit_plan(ctx, writer_input, remove_source /* None today */,
    output_schema, operation, touched_file_paths, reported_row_count) -> Result<Arc<dyn ExecutionPlan>>
```

- partition columns from `table.metadata().default_partition_spec()`;
- `IcebergWriterExecOptions` from ctx options + `commit_operation = Some(op)` +
  `lakehouse_table` + `touched_file_paths`;
- builds `IcebergWriterExec(writer_input, url, partition_columns, PhysicalSinkMode::Append,
  true, options, Some(output_schema))` (optionally unioned with `remove_source`);
- wraps in `CoalescePartitionsExec` → `IcebergCommitExec::new_with_reported_row_count(...)`.

### 7.4 `op_delete.rs` — `plan_delete(ctx, condition)`

- **No current snapshot** (created-but-never-written table, metadata-only) → `noop_delete_plan`
  (an `EmptyExec` feeding a plain `IcebergCommitExec`; commit exec short-circuits to `count=0`).
  Tested for both TRUNCATE and conditional DELETE.
- **TRUNCATE** (`condition.is_none()`): `EmptyExec` with the table's Arrow schema →
  `assemble_iceberg_commit_plan(... Operation::Delete, touched=[], reported=None)`, producing
  an empty-snapshot commit that drops all rows.
- **Conditional DELETE**: manifest scan → discovery → repartition round-robin
  (`target_partitions`) → `IcebergScanByDataFilesExec` → `FilterExec(Not(condition))` (keep
  survivors) → commit with `Operation::Delete`, `touched=[]` (DELETE is a full replacement —
  new files replace all parent manifests), reported count from rows that matched.
- Physical condition built via `session.create_physical_expr`.

### 7.5 `op_update.rs` — `plan_update(ctx, write_plan, touched_files_plan)`

- Collect touched paths + matched_row_count (await plan-time).
- **No matched files** → rewrite all rows unchanged as a full replacement
  (`Operation::Overwrite`, `touched=[], reported=Some(0)`).
- Else `build_targeted_writer_input` → union untouched + touched → `strip_internal_columns` →
  `assemble_iceberg_commit_plan(... Operation::Overwrite, touched_file_paths,
  reported_row_count = matched_row_count)`. So UPDATE reports the number of rows matching the
  predicate, not rows written.

### 7.6 `op_merge.rs` — `plan_merge(ctx, write_plan, touched_files_plan, is_insert_only)`

- `is_insert_only` = no matched and no not-matched-by-source clauses (computed in the row-level
  planner).
- **Insert-only or no touched files** → strip internals, full `Operation::Append` commit
  (all rows new).
- Otherwise same targeted-rewrite machinery as UPDATE, committing `Operation::Overwrite` with
  touched paths (reported count `None` — uses written rows).

### 7.7 Dispatch — `row_level_planner.rs` + `table_scan_planner.rs`

`plan_iceberg_row_level_write(session_state, planner, node)` resolves write options,
`parse_table_url`, reads `metadata_location_from_options`/`catalog_managed_iceberg_from_options`,
builds a `PlannerContext`, and dispatches on `node.command()` (`Delete`→plan_delete with
condition; `Merge`/`Update`→ physicalize `write_plan` + `touched_files_plan` via the passed
`planner` and call plan_merge/plan_update). MERGE requires both plans.

`IcebergPhysicalPlanner::plan_extension` now downcasts in order: `IcebergWriteNode`
(unchanged path) → `RowLevelWriteNode` when `target_format() == "iceberg"` →
`LoadDataNode` when `target_format() == "iceberg"` (doc 08). `plan_table_scan` adds a branch:
when the source has a file column, it routes away from the `DataSourceExec` provider scan to
`IcebergManifestScanExec → IcebergDiscoveryExec → IcebergScanByDataFilesExec::new_with_file_path_column`
(returns `EmptyExec` of the provider schema when there is no current snapshot), then applies
the scan projection as a `ProjectionExec` above the chain (the chain reads all columns and
filters stay above because the source reports them unsupported).

---

## 8. File-path column plumbing (`scan_by_data_files_exec.rs`)

- `IcebergScanByDataFilesExec` gains `file_path_column: Option<String>` and
  `new_with_file_path_column(input, table_url, output_schema, Option<String>)` (plus the
  plain `new` delegating with `None`). When set, the **output schema** appends the Utf8 column.
- The state machine materializes it as a **partition value** on each `PartitionedFile`:
  `ScalarValue::Utf8(Some(raw_path))` — the **exact manifest path** (`data_file.file_path()`),
  not the re-resolved absolute object path, because row-level ops compare this value against
  manifest paths when deciding which files to rewrite.
- The Parquet file source is built via `TableSchema::new(file_schema_without_path_col, [Utf8
  path field])` so the path arrives as a synthetic partition column; `file_path_column()` is
  exposed for the codec (doc 03 §2.4 proto field `file_path_column = 4`).

---

## 9. `IcebergCommitExec` extensions

### 9.1 Fields / actions

- Exec: `reported_row_count: Option<u64>` + `new_with_reported_row_count` (plain `new`
  keeps `None`). Every place that emitted `commit_info.row_count` now emits
  `reported_row_count.unwrap_or(commit_info.row_count)` — "rows affected" wins.
- `IcebergCommitInfo` (commit/mod.rs) + `CommitMeta`/`CommitMetaAction` (action_schema.rs):
  `touched_file_paths: Vec<String>`, `overwrite_predicate: Option<String>` (JSON
  `Vec<(String,String)>` partition equality pairs for `INSERT … REPLACE WHERE`),
  `overwrite_partition_values: Option<String>` (JSON `Vec<Vec<String>>` for dynamic partition
  overwrite). All serde round-tripped (skip when empty/None) with new unit tests.

### 9.2 Operation handling (inside the commit loop)

`match commit_info.operation` now handles:
- `Overwrite` — computes **parent manifest entries to keep**:
  1. `overwrite_predicate` present → `filter_parent_manifest_entries(...)` (keep manifests
     whose partition bounds do not match the predicate; conservative when no bounds/no summary);
  2. else `overwrite_partition_values` present →
     `filter_parent_manifest_entries_by_values(...)` (keep manifests whose bounds do not overlap
     any written partition value);
  3. else `touched_file_paths` non-empty → `compute_untouched_manifest_entries(...)` (keep
     non-data manifests and data manifests with no touched file; on any read/parse error keep
     the manifest, logging a warning);
  4. else `vec![]` (full replacement).
  Passed via `SnapshotProducer::with_parent_manifest_entries(Some(parent_entries))`.
- `Delete` — new arm: `SnapshotProducer` with the incoming (empty for TRUNCATE or survivor)
  files and **no parent manifests** (`Some(vec![])`), operation label "delete".
- `Replace` — new arm: full replacement, operation label "replace" (used by fast LOAD
  overwrite — doc 08).
- (the previous catch-all `_ => NotImplemented` is gone).

### 9.3 Conflict / stale-metadata handling

`metadata_files_for_version` now returns `Vec<(String, DateTime<Utc>)>`. All "existing files
for next version" checks compare with `is_stale_metadata_file(candidate_ts,
current_metadata_ts)` before treating them as a conflict: a higher-version file older than the
current metadata is stale (leftover from a previous DROP+CREATE) and is ignored rather than
triggering retries/failures. Same for the post-write conflict check (which also deletes the
just-written file when a real concurrent write won).

### 9.4 Empty-table DELETE

Before the "metadata exists but no current snapshot → bootstrap first snapshot" branch, an
extra short-circuit: if `maybe_snapshot.is_none()` **and** `operation == Operation::Delete`,
return a single `count = 0` batch without committing. This lets an empty created-but-never-
written table TRUNCATE cleanly even when planner and commit exec resolved different metadata
files (catalog `metadata-location` vs storage scan) — the catalog-managed empty-table DELETE
case (commit `48dca759`).

### 9.5 Unit tests (commit_exec)

`count_sums_multiple_commit_meta`, `count_single_commit_meta`,
`count_reported_row_count_overrides`, `count_zero_for_no_commit_meta`,
`merge_rejects_inconsistent_commit_meta`.

---

## 10. Scan statistics: empty scans report exact 0

`IcebergTableProvider::aggregate_statistics(&[])` returns exact `num_rows = 0`,
`total_byte_size = 0` and per-column `null_count Exact(0)` with absent bounds (previously
`Statistics::new_unknown`). Rationale: lets DataFusion's `AggregateStatistics` fold `COUNT(*)`
to `0` without scanning (no worker spawn for a no-op read), mirroring delta/`EmptyExec`.
Tests: `aggregate_statistics_empty_data_files_reports_zero_rows`,
`aggregate_statistics_non_empty_data_files_are_aggregated`.

---

## 11. Frontend / resolver wiring

### 11.1 UPDATE statement

- `sail-sql-analyzer` maps `Statement::Update` (existing AST) to
  `spec::CommandNode::Update` (doc 05) — before it errored as TODO.
- `sail-plan/src/resolver/command/mod.rs`: `CommandNode::Update` → new
  `resolve_command_update`; `CommandNode::LoadData` → new `resolve_command_load_data`;
  `CommandNode::CallProcedure` → new `resolve_command_call_procedure`; DESCRIBE TABLE now
  accepts `column` and forwards `column.map(join("."))`.
- `resolver/command/update.rs` (NEW, 228 LOC):
  - rejects views and tables without a location;
  - for DELTA with no catalog columns, infers schema from the source (`create_source`);
  - resolves condition and each assignment via the **field-id-renamed** schema, then undoes the
    rename (`expression_before_rename(.., true)`) so the format layer sees real column names;
  - casts each assignment to the target column's Arrow type
    (`SET score = 100.0` on a DOUBLE column yields Float64, not Decimal);
  - the target plan is a resolved `Read NamedTable` scan with the field-id renames undone
    (`rename_logical_plan` back to real names — mirrors `resolve_write_input`);
  - produces `UpdateInfo { table_name, path, target, condition, assignments, lakehouse_table
    (Write op), options: [TablePropertyList { properties }] }` and calls
    `registry.get(format)?.create_updater(...)`.

### 11.2 TRUNCATE

`TRUNCATE TABLE` parses to a dedicated statement (doc 05) and is converted to
`Delete { condition: None }`; everything downstream is the empty-condition DELETE path (§7.4).

### 11.3 `metadata_table: None` destructure ripple

The `SourceInfo.metadata_table` field (doc 06 §4.1) forces `metadata_table: None` into
resolvers: `delete.rs`, `delta.rs` (×2), `merge.rs`, `write.rs` (×2), `read.rs`
(×2 literal sites + new metadata-table arm), and iceberg `table_format.rs`
`build_iceberg_provider`. Fold these into the metadata-tables work (doc 09).

### 11.4 Metadata-table read detection (`resolver/query/read.rs`)

`resolve_query_read_named_table` first calls `try_resolve_iceberg_metadata_table(&name, state)`:
if the last name part is `refs`/`snapshots` (case-insensitive `IcebergMetadataTableType::from_name`),
the base prefix (≥ `db.table`) resolves to an Iceberg table with a location, then it builds a
`SourceInfo { paths: [location], lakehouse_table: Some(Read), options: [TablePropertyList], metadata_table: Some(type) }`
and asks the iceberg `TableFormat` to `create_source`, finishing through
`resolve_table_source_with_rename`. Non-matching names fall through to normal resolution.

---

## 12. Misc iceberg deltas on this path

- `datasource/type_converter.rs`: `is_utc_timezone` now treats `Etc/UTC`, `Etc/GMT`, `Etc/GMT±0`,
  `Etc/Zulu` as UTC for timestamptz conversions (new test for `Etc/UTC`).
- `properties.rs`: `is_reserved_iceberg_table_property` made `pub` (used by the REST provider,
  doc 06).
- `table_format.rs`: `create_deleter_builds_delete_node` and
  `create_updater_builds_update_node` tests.
- `IcebergWriteNode`/existing `IcebergWriterExecOptions.commit_operation` field now consumed by
  `assemble_iceberg_commit_plan`.

---

## 13. Port notes / risks

1. **This cluster depends on row-level infra that 0.7.1 may already have partially** (DELETE/
   MERGE existed upstream at v0.7.0/v0.7.1 in some form). Before porting, compare 0.7.1's
   `create_deleter`/`IcebergDeleteApplyExec`/deletion-vector design: this branch **replaced**
   the flow with targeted rewrite + full/partial parent-manifest replacement. Merging both
   designs requires choosing one path or reconciling `RowLevelWriteNode` shape.
2. `RowLevelWriteNode::new_merge/new_delete/new_update` and `MergeCapableSource` must exist on
   0.7.1 (they partly come from upstream shared-datafusion; verify signatures incl. the
   `deletion_vector_plan` param of `new_merge`).
3. **Plan-time execution of the touched plan** (`collect_touched_file_paths`) executes
   real work during physical planning; keep the tokio-runtime constraint comment in mind when
   integrating with 0.7.1's planning flow.
4. Commit-exec fields (`touched_file_paths`, `overwrite_predicate`,
   `overwrite_partition_values`, `reported_row_count`) + the new `Operation::Delete`/`Replace`
   arms are load-bearing for UPDATE/DELETE/MERGE/LOAD and for stale-file conflict handling;
   the `metadata_files_for_version` signature change ripples through commit_exec and
   table_format.
5. The `file_path_column` proto field (`= 4`) must match 0.7.1's `physical.proto` layout
   (doc 03 §2.4).
