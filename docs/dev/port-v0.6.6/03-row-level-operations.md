# 03 — Iceberg Row-Level Operations: UPDATE, DELETE/TRUNCATE, MERGE

> Full implementation of row-level writes for Iceberg on `feat/v0.6.6`, using a
> **targeted-rewrite** strategy: only files containing rows that changed are rewritten;
> untouched files stay referenced by the parent manifests.

Files:
- `crates/sail-logical-plan/src/merge.rs` (+163, has the bulk of the expansion logic)
- `crates/sail-iceberg/src/logical/update.rs` (new, 172)
- `crates/sail-iceberg/src/physical/row_level_planner.rs` (new, 94)
- `crates/sail-iceberg/src/physical_plan/planner/{mod,context,helpers,commit,op_delete,op_update,op_merge}.rs` (new module)
- `crates/sail-iceberg/src/physical_plan/scan_by_data_files_exec.rs` (+288)
- `crates/sail-iceberg/src/physical_plan/writer_exec.rs` (+138), `writer_options.rs` (+19)
- `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` (+604)
- `crates/sail-iceberg/src/physical_plan/action_schema.rs` (+106)
- `crates/sail-iceberg/src/operations/snapshot.rs` (+48)
- `crates/sail-plan/src/resolver/command/update.rs` (new, 228)
- `crates/sail-common-datafusion/src/datasource.rs` (+~120) — `UpdateInfo`, `UpdateAssignment`, `MergeCapableSource`
- `crates/sail-iceberg/src/logical/table_source.rs` (+55), `src/datasource/type_converter.rs` (+32)
- `crates/sail-iceberg/src/table_format.rs` (+725; `create_deleter`/`create_updater`/`create_merger`)

---

## 1. Shared types — `sail-common-datafusion/src/datasource.rs`

```rust
pub struct UpdateInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub target: Arc<LogicalPlan>,          // resolved logical target scan
    pub condition: Option<ExprWithSource>,
    pub assignments: Vec<UpdateAssignment>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub options: Vec<OptionLayer>,
}

pub struct UpdateAssignment {
    pub column_path: Vec<String>,
    pub expression: Expr,
}
```

`TableFormat` trait gains (with `not_impl_err!` defaults):

```rust
async fn create_updater(&self, ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan>
async fn create_deleter(&self, ctx: &dyn Session, info: DeleteInfo) -> Result<LogicalPlan>
```

`TableFormatAlterTableOperation` gains `RenameTable`, `AddColumns`, `DropColumns`,
`AlterColumnComment`, `AlterColumnNullability`, `AlterColumnPosition` (covered in `06`).

`RowLevelCommand` enum already existed (`Delete | Merge | Update`) — re-exported into
`sail-logical-plan::merge`.

---

## 2. `MergeCapableSource` + the file-path column

`crates/sail-iceberg/src/logical/table_source.rs` — `IcebergTableSource` now implements:

```rust
impl MergeCapableSource for IcebergTableSource {
    fn file_column_name(&self) -> Option<&str> { self.file_column.as_deref() }
    fn with_file_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        // adds the synthetic file-path column to the source schema
        // (errors if a base column already has that name)
    }
    fn row_index_column_name(&self) -> Option<&str> { None }
    fn with_row_index_column(&self, _name) -> Result<Arc<dyn TableSource>> { ... unchanged }
}
```

`file_column: Option<String>` field on the struct, default `None`. The synthetic column
is materialized by `IcebergScanByDataFilesExec` (see §6) as a Parquet partition column.

---

## 3. UPDATE logical expansion

### 3.1 `crates/sail-iceberg/src/logical/update.rs` — `expand_update_node(info)`

1. `ensure_update_metadata_columns(info.target)` — `transform_up` over the target plan:
   - On `LogicalPlan::TableScan`, if the source is an `IcebergTableSource` without a file
     column, call `try_enable_metadata_column` (downcast → `with_file_column(MERGE_FILE_COLUMN)`
     → new schema), then rebuild the `TableScan::try_new` with the file-column index
     appended to the projection (materializing the projection when `None`).
   - On `LogicalPlan::Projection`, if the input schema has `MERGE_FILE_COLUMN` but the
     projection drops it, append `Column(MERGE_FILE_COLUMN) as MERGE_FILE_COLUMN`.
2. Ensure the target plan's schema carries `MERGE_FILE_COLUMN`; if not, add a projection
   that selects all columns + the file column.
3. `expand_update(info, MERGE_FILE_COLUMN)` (in `sail-logical-plan::merge`).
4. Build `RowLevelWriteNode::new_update(raw_target, raw_input_schema,
   expansion.write_plan, expansion.touched_files_plan, condition, assignments,
   "iceberg", location, table_name, options, lakehouse_table)`.

### 3.2 `sail-logical-plan/src/merge.rs` — `expand_update(info, path_column)`

Returns `UpdateExpansion { write_plan, touched_files_plan, output_schema: empty }`.

- `assignment_by_name` from `UpdateAssignment`s (only single-part column paths; nested
  assignments are traced and ignored).
- `write_plan`: for each target column (skipping the path column), the column is either
  kept as-is or, when assigned, rewritten as
  `CASE WHEN condition THEN assignment ELSE current END` (or just `assignment` when no
  condition); the file path column is projected through at the end.
- `touched_files_plan` = `target scan -> filter(condition) -> project(file_path)`.

---

## 4. MERGE logical expansion — `expand_merge(info, path_column, row_index_column)`

Returns `MergeExpansion { write_plan, touched_files_plan, deletion_vector_plan,
output_schema, options }`.

### 4.1 Column normalization

- `desired_target_names` / `desired_source_names`: prefer the real field names captured
  at resolution time (`options.resolved_target_field_names` /
  `resolved_source_field_names`); fall back to `recover_field_names` heuristic, then to
  the resolved schemas' field names.
- Target projected with `Column(field) AS desired`; the path column (and row-index
  column when present) are force-appended.
- Source columns are projected with a stable `__sail_src_<name>` prefix to avoid
  duplicate unqualified names after the full outer join.
- `target_rename_map` / `source_rename_map`; every ON condition, join key, residual /
  target-only predicate, matched clause, not-matched-by-source clause, not-matched-by-
  target clause, generated-column expr and Delta check-constraint expr is rewritten
  through them (`rewrite_merge_columns`). `options` is normalized in place via
  `normalize_target_column_names`.

### 4.2 Cardinality check

- `should_check_cardinality = should_check_cardinality(matched_clauses)`; disabled when
  `source_is_unique_on_merge_join_keys(&source_plan, &join_key_pairs)`.
- When enabled, a `MonotonicIdNode` adds `TARGET_ROW_ID_COLUMN` (`__sail_merge_target_row_id`)
  to the target before the join; after the join a `MergeCardinalityCheckNode` wraps the
  result and detects 1:N violations.

### 4.3 `MergeCardinalityCheckNode`

`UserDefinedLogicalNodeCore` (`name() = "MergeCardinalityCheck"`) with fields
`target_row_id_col`, `target_present_col`, `source_present_col`; schema = input schema;
one child.

### 4.4 Insert-only fast append (`can_fast_append_insert_only`)

Enabled only when: no `matched_clauses`, no `not_matched_by_source_clauses`, ≥1
`not_matched_by_target_clause`, and **no** NOT-MATCHED clause's condition or INSERT
values reference a target column. When enabled:
- `insert_rows` = `source LEFT ANTI JOIN target` (on join keys + residual filter).
- `insert_operation` = `CASE WHEN insert_only_insert_filter(...) THEN Insert ELSE Noop END`.
- `insert_projected` = `build_insert_only_projection` (per target column, the first
  matching clause's value, defaulting to NULL; unsupported columns NULL; `Noop` rows
  kept for metrics) + `apply_generation_projection`.
- `noop_rows` = `source LEFT SEMI JOIN target` → `build_insert_only_noop_projection`.
- `projected` = `insert_projected UNION noop_projected` →
  `apply_delta_check_constraint_filter` (with the row-level op filter).
- `touched_plan = LogicalPlanBuilder::empty(false)` → physical path becomes a pure
  **Append** (no file rewrites).

### 4.5 Default expansion (`build_default_merge_expansion`)

- Augment target with `TARGET_PRESENT_COLUMN` (and path column null placeholder if
  absent) and source with `SOURCE_PRESENT_COLUMN`, both `lit(true)`.
- `join = target FULL OUTER JOIN source` (join keys, residual filter, `NullEqualsNothing`).
- Optional `MergeCardinalityCheckNode`.
- Predicates: `matched = target_present AND source_present`;
  `not_matched_by_source = target_present AND NOT source_present`;
  `not_matched_by_target = NOT target_present AND source_present`.
- `delete_pred` / `insert_pred` accumulated by OR-ing each clause's (predicate AND
  clause-condition); `delete_expr`, `insert_expr`, `active_expr = target_present OR
  insert_expr`.
- `filtered = join -> filter(active_expr)`.
- `projected = filtered -> project(build_merge_projection(...))` →
  `apply_generation_projection` (generated columns: INSERT rows with a user value that
  mismatches the expression → `RaiseError` with `[DELTA_GENERATED_COLUMNS_VALUE_MISMATCH]`;
  UPDATE rows silently recompute) → **union with the source-metric branch**.
- Source-metric branch: `source_plan -> aggregate(count(*)) as MERGE_SOURCE_METRIC_COLUMN`
  → projection of nulls for all target columns + the path column; this preserves the
  "source rows seen" count even when targeted rewrite drops matched-but-unchanged rows.
- `apply_delta_check_constraint_filter` with `row_level_data_operation_expr()`.
- `touched_plan` = `join -> filter(rewrite_filter) -> aggregate(distinct path) ->
  project(path)` where `rewrite_filter` covers matched clauses that rewrite (UPDATE/UPDATE SET)
  and not-matched-by-source UPDATEs.
- `deletion_vector_plan` = `join -> filter(delete_expr) -> project(path, row_index)` when
  a row-index column is provided (Iceberg passes `None` in v1; used by Delta).

---

## 5. `RowLevelWriteNode`

Unified extension node (`name() = "RowLevelWrite"`) carrying everything the physical
planner needs:

```rust
pub struct RowLevelWriteNode {
    command: RowLevelCommand,                 // Delete | Merge | Update
    raw_target: Arc<LogicalPlan>,
    raw_source: Option<Arc<LogicalPlan>>,
    raw_input_schema: DFSchemaRef,
    write_plan: Option<Arc<LogicalPlan>>,     // MERGE/UPDATE expanded write plan
    touched_files_plan: Option<Arc<LogicalPlan>>,
    deletion_vector_plan: Option<Arc<LogicalPlan>>,
    condition: Option<ExprWithSource>,        // DELETE/UPDATE
    assignments: Option<Vec<UpdateAssignment>>, // UPDATE
    merge_options: Option<MergeIntoOptions>,
    target_format: String,
    target_location: String,
    target_table_name: Vec<String>,
    target_partition_by: Vec<String>,
    target_options: Vec<OptionLayer>,
    target_lakehouse_table: Option<LakehouseExecutionContext>,
    with_schema_evolution: bool,
    schema: DFSchemaRef,
}
```

Constructors: `new_merge(...)`, `new_delete(...)`, `new_update(...)`. Accessors for all
fields. `inputs()` exposes write_plan + touched_files_plan + deletion_vector_plan.
`fmt_for_explain` prints command + target + format, plus condition / assignments count /
MERGE clause counts.

---

## 6. Physical scan — `scan_by_data_files_exec.rs` file-path column

- `IcebergScanByDataFilesExec` gains `file_path_column: Option<String>`.
- `new(...)` delegates to `new_with_file_path_column(input, table_url, output_schema, None)`.
- With a file column, the **output schema appends** `Field(file_column, Utf8, true)`.
- `ScanByDataFilesState` materializes the column via the Parquet scan partition columns:
  - `partition_values = [ScalarValue::Utf8(Some(raw_path))]` — **the EXACT manifest
    string** (`data_file.file_path()`), not the re-resolved object path, because
    row-level ops compare this value against manifest paths to decide which files to
    rewrite.
  - the Parquet file schema is the user-data schema with the file column **removed**,
    and a `TableSchema` adds it as a synthetic partition column.
- Tests: `file_path_column_is_materialized_per_file` (writes two parquet files, checks
  each row is tagged with its file's path).

### 6.1 Codec — `IcebergScanByDataFilesExecNode.file_path_column`

`crates/sail-execution/proto/sail/plan/physical.proto` adds
`optional string file_path_column = 4;` to `IcebergScanByDataFilesExecNode`;
`crates/sail-execution/src/proto/codec.rs` encodes `scan_by_files.file_path_column().clone()`
and decodes back via `IcebergScanByDataFilesExec::new_with_file_path_column(...)`
(part of the row-level-write remote-execution path).

---

## 7. Physical planner dispatch — `physical/table_scan_planner.rs`

`IcebergPhysicalPlanner`:
- `plan_extension`: `IcebergWriteNode` → `plan_iceberg_write`; **`RowLevelWriteNode`
  (format `iceberg`) → `plan_iceberg_row_level_write`**; `LoadDataNode` →
  `plan_load_data`; `CallProcedureNode` → `plan_call_procedure`; else `Ok(None)`.
- `plan_table_scan`: when `source.file_column_name()` is set, routes to the
  `IcebergManifestScanExec → IcebergDiscoveryExec → IcebergScanByDataFilesExec::new_with_file_path_column`
  chain (instead of the provider scan) and applies the scan projection above it. Empty
  table (no current snapshot) → `EmptyExec`.

---

## 8. `physical/row_level_planner.rs` — `plan_iceberg_row_level_write`

1. `IcebergWriteOptions::resolve(session_state, node.target_options())`.
2. `IcebergTableFormat::parse_table_url([target_location])`.
3. `PlannerContext::new(session, options, table_url, lakehouse_table)` (loads the table).
4. Dispatch on `node.command()`:
   - `Delete` → `planner::plan_delete(&ctx, node.condition().cloned())`.
   - `Merge` → physical-plan `write_plan` + `touched_files_plan` via the caller's
     `PhysicalPlanner`; `is_insert_only` from `merge_options` (no matched /
     not-matched-by-source clauses); → `planner::plan_merge(...)`.
   - `Update` → physical-plan both plans → `planner::plan_update(...)`.

---

## 9. The `physical_plan/planner/` module

### 9.1 `context.rs` — `PlannerContext`

Bundles `session`, `options: IcebergWriteOptions`, `table_url: Url`,
`lakehouse_table`, `table: Table` (loaded). Accessors + `object_store()` (from the
runtime-env object-store registry).

### 9.2 `helpers.rs`

- **`collect_touched_file_paths(session, touched_files_plan) -> (Vec<String>, u64)`**:
  builds a `TaskContext` from the session config + runtime env, executes partition 0,
  collects batches, sums row count (`matched_row_count`), extracts the single Utf8
  column, dedups into a sorted `HashSet`. **Must be awaited inside the driver's tokio
  runtime** (the touched plan contains `RepartitionExec` + parquet `DataSourceExec`
  which spawn tokio tasks; `block_on` would park a runtime worker and deadlock).
- **`touched_paths_source(paths)`**: one-column (`MERGE_FILE_COLUMN`, Utf8) in-memory
  `DataSourceExec`.
- **`build_targeted_writer_input(write_plan, touched_paths) -> (untouched_rows,
  touched_rows)`**:
  - `file_path_idx` = index of `MERGE_FILE_COLUMN` in the write plan;
  - `non_insert = write_plan -> filter(file_path IS NOT NULL)`;
  - `untouched_rows` = `RightAnti` hash join (touched_source × non_insert on the file
    column), then project all columns;
  - `touched_rows` = `Inner` hash join, then project **only the right-side (write plan)
    columns** (`left_width..`);
  - both joins `PartitionMode::CollectLeft`, `NullEquality::NullEqualsNothing`.
- **`strip_internal_columns(input, table_schema)`**: projection of only columns whose
  name matches a `table_schema` field (drops `__sail_*` internals).

### 9.3 `commit.rs` — `assemble_iceberg_commit_plan`

```rust
assemble_iceberg_commit_plan(ctx, writer_input, remove_source, output_schema,
    operation, touched_file_paths, reported_row_count) -> Arc<dyn ExecutionPlan>
```

- partition columns from the table's default partition spec
  (`catalog_partition_field_from_iceberg`).
- `IcebergWriterExecOptions::from(ctx.options())` + `commit_operation = Some(operation)` +
  `lakehouse_table` + `touched_file_paths`.
- `IcebergWriterExec::new(writer_input, table_url, partition_columns,
  PhysicalSinkMode::Append, true, options, Some(output_schema))`.
- optional `remove_source` unioned; `CoalescePartitionsExec`; `IcebergCommitExec` with
  the `reported_row_count`.

### 9.4 `op_delete.rs` — `plan_delete(ctx, condition)`

- **TRUNCATE** (`condition.is_none()`):
  - empty table (no current snapshot) → `noop_delete_plan`: `EmptyExec` feeding
    `IcebergCommitExec` (no commit meta + no data files → a single `count = 0` batch,
    no table-state mutation). Matches Spark/Iceberg.
  - non-empty → full replacement: `EmptyExec` (arrow schema) →
    `assemble_iceberg_commit_plan(..., Operation::Delete, vec![], None)`.
- **Conditional DELETE**: empty table (no current snapshot) → `noop_delete_plan`
  (0-row no-op, same as TRUNCATE; no table-state mutation); otherwise builds
  `IcebergManifestScanExec → IcebergDiscoveryExec → RepartitionExec(RoundRobinBatch,
  target_partitions) → IcebergScanByDataFilesExec`, `physical_condition =
  create_physical_expr(condition, df_schema)`, `survivors = FilterExec(NOT condition)`,
  commits `Operation::Delete`, `touched_file_paths = []`, `reported_row_count = None`.
- Tests: `noop_delete_plan_reports_zero_count`,
  `conditional_delete_on_snapshotless_table_is_noop`.

### 9.5 `op_update.rs` — `plan_update(ctx, write_plan, touched_files_plan)`

- `collect_touched_file_paths` → `(touched, matched_row_count)`.
- No touched files → `strip_internal_columns(write_plan, arrow_schema)` →
  `assemble_iceberg_commit_plan(..., Operation::Overwrite, vec![], Some(0))`.
- Else `build_targeted_writer_input` → `UnionExec(untouched, touched)` →
  `strip_internal_columns` → commit `Operation::Overwrite` with
  `touched_file_paths` and `Some(matched_row_count)`.

### 9.6 `op_merge.rs` — `plan_merge(ctx, write_plan, touched_files_plan, is_insert_only)`

- `is_insert_only || touched.is_empty()` → strip internals →
  `assemble_iceberg_commit_plan(..., Operation::Append, vec![], None)`.
- else `build_targeted_writer_input` → union → strip → commit `Operation::Overwrite`
  with touched paths.

---

## 10. Writer changes — `writer_exec.rs` / `writer_options.rs`

`IcebergWriterExecOptions` gains:
- `compression_codec: String` (default `"snappy"`),
- `commit_operation: Option<crate::spec::Operation>`,
- `touched_file_paths: Vec<String>`,
- `overwrite_predicate: Option<String>` (JSON `Vec<(String,String)>`).

**Compression option wiring** — `crates/sail-iceberg/data/options/iceberg.yaml` changes
the `write.compression-codec` option from **unsupported** to **supported**:
- `description: Override the Parquet compression codec for data file writes
  (snappy | zstd | gzip | lz4 | uncompressed | none).`
- `default: { value: "snappy", parser: parse_string }`, `rust_type: String`
- additional option layer: `{ type: table_property, keys:
  [write.parquet.compression-codec], case_sensitive: true, parser: parse_string }`
  (so `TBLPROPERTIES ('write.parquet.compression-codec'='zstd')` works too).

`IcebergWriterExec`:
- `compute_properties` now takes `output_partitions = input.output_partitioning().partition_count().max(1)` (was fixed 1).
- `required_input_distribution`: `UnspecifiedDistribution` when no partition columns
  (upstream repartitioning controls file counts); else `HashPartitioned` on the partition
  columns so each task writes its partitions without opening many writers.
- `execute(partition, ctx)`: runs per input partition (not just partition 0); `partition
  >= input_partitions` → error.
- Metrics: `output_rows`, `output_bytes`, `elapsed_compute` via `ExecutionPlanMetricsSet`.
- `PhysicalSinkMode::OverwriteIf`/`OverwritePartitions` no longer error.
- **Compression**: `resolve_compression_codec` (`snappy|zstd|gzip|lz4|brotli|none|uncompressed`)
  → `WriterProperties`.
- **Overwrite partition values**: for `OverwritePartitions`, collects the unique
  partition-value tuples from the written `DataFile`s
  (`Some(lit)` formatted via `Debug`, `None` → `"__NULL__"`), serializes
  `Vec<Vec<String>>` → `overwrite_partition_values` in the `CommitMeta`.
- **Operation override**: `operation = options.commit_operation.unwrap_or(Overwrite for
  Overwrite/OverwriteIf/OverwritePartitions, else Append)`.
- CommitMeta now carries `touched_file_paths`, `overwrite_predicate`,
  `overwrite_partition_values`.

---

## 11. Commit exec — `commit/commit_exec.rs`

### 11.1 `reported_row_count` + `accumulate_action_batches`

- `IcebergCommitExec` gains `reported_row_count: Option<u64>` (constructor arg,
  propagated through `with_new_children`).
- `accumulate_action_batches(&[RecordBatch]) -> (Vec<DataFile>, u64, Option<CommitMeta>)`:
  sums `row_count` across every commit-meta batch (LOAD DATA unions fast + rewrite
  writers), last meta wins for other fields.
- The returned `count` column = `reported_row_count.unwrap_or(total_written_rows)`
  (UPDATE reports matched-row count; INSERT/LOAD report rows written).

### 11.2 Parent-manifest filtering for `Overwrite`

In the Overwrite branch the commit picks parent manifests to keep:

1. `commit_info.overwrite_predicate` (predicate overwrite / `REPLACE WHERE`) →
   `filter_parent_manifest_entries` — keeps manifests whose partition bounds do **not**
   match the predicate (missing summary ⇒ keep).
2. `commit_info.overwrite_partition_values` (dynamic partition overwrite) →
   `filter_parent_manifest_entries_by_values` — keeps manifests whose bounds do not
   overlap any written partition tuple.
3. `!commit_info.touched_file_paths.is_empty()` (row-level ops) →
   `compute_untouched_manifest_entries` — loads each parent manifest, keeps it only if
   none of its data-file paths is touched; non-data manifests always kept; load/parse
   failures warn-and-keep. On error → **fall back to full replacement** (empty entries).
4. else → full replacement (`vec![]`).

`SnapshotProducer` is then built with `.with_parent_manifest_entries(Some(parent_entries))`
(see §13).

### 11.3 New operations: `Delete`, `Replace`

Previously only `Overwrite` was implemented (`_ => NotImplemented`). Now:

- `Operation::Delete` → `SnapshotProducer` with `with_parent_manifest_entries(Some(vec![]))`,
  producer op tag `"delete"`.
- `Operation::Replace` → same with tag `"replace"`.
- `Overwrite` → parent-entries-based (above).

### 11.4 Stale-metadata-file conflict detection

Both the pre-write version check and the post-write conflict check now use
`get_metadata_file_timestamp` + `is_stale_metadata_file` so leftover files from a
crashed attempt don't count as real concurrent writers. Only real conflicts consume
retries (`MAX_COMMIT_RETRIES = 5`).

### 11.5 `extract_partition_predicate_from_expr` (`table_format.rs`)

`pub(crate) fn extract_partition_predicate_from_expr(expr: &Expr) -> Option<Vec<(String,
String)>>` — extracts partition-column equality pairs for `INSERT ... REPLACE WHERE`:
- `col = literal` (either operand order) → `Some(vec![(col.name, scalar.to_string())])`;
- `a AND b` → concatenates the results of both sides (left-biased union; a failing side
  is skipped, `None ∧ None` → `None`);
- anything else (non-eq operators, non-literal rhs) → `None`.

Used by `plan_iceberg_write` to (a) validate the predicate only references partition
columns (error otherwise: `"Cannot use REPLACE WHERE with non-partition predicate:
column '{col}' is not a partition column"` / `"INSERT ... REPLACE WHERE predicate must be
equality predicates on partition columns"`) and (b) serialize it into
`options.overwrite_predicate`. Tests: `extract_partition_predicate_simple_eq` and the
AND/other-op cases.

Tests: count-summing for LOAD DATA union, single writer, override.

---

## 12. `action_schema.rs` — CommitMeta extensions

```rust
pub struct CommitMeta {
    ...
    pub touched_file_paths: Vec<String>,
    pub overwrite_predicate: Option<String>,
    pub overwrite_partition_values: Option<String>,
}
```

`CommitMetaAction` gains the three `*_json` string fields; `encode_commit_meta` /
`decode_actions_and_meta_from_batch` round-trip them. Tests for predicate and
partition-values round-trips and absence.

### 12.1 `IcebergCommitInfo` (`physical_plan/commit/mod.rs`)

The serialized writer→commit struct `IcebergCommitInfo` gains the same three fields
(`touched_file_paths: Vec<String>`, `overwrite_predicate: Option<String>`,
`overwrite_partition_values: Option<String>`). The two `Option` fields use
`#[serde(skip_serializing_if = "Option::is_none")]` (absent keys are not serialized).
`IcebergCommitExec` reads them out of the decoded `commit_info` in the commit branch
(see §11.2).

---

## 13. `operations/snapshot.rs` — `SnapshotProducer.parent_manifest_entries`

```rust
pub parent_manifest_entries: Option<Vec<ManifestFile>>,
pub fn with_parent_manifest_entries(mut self, entries: Option<Vec<ManifestFile>>) -> Self
```

- `Some(vec![])` → full replacement (no inherited parent manifests) — used by Overwrite,
  Delete, Replace.
- `Some(entries)` → partial replacement — targeted rewrite.
- `None` → legacy: load parent for Append, skip for Overwrite.

`commit(self, op)`:
- `op_type` derived from `op.operation()` string:
  `"append"→Append, "overwrite"→Overwrite, "delete"→Delete, "replace"→Replace` (fallback
  Append) — so the snapshot `summary.operation` is correct for every operation.
- Parent manifest resolution honors the override; Append + not bootstrap + non-empty
  parent manifest list loads the parent entries as before.

### 13.1 `IcebergPlanBuilder` rewrite (`physical_plan/plan_builder.rs`)

`IcebergPlanBuilder<'a>` → `IcebergPlanBuilder` (lifetime and `session` field removed;
`new(...)` no longer takes a session). Two behavioral changes:

- **`add_repartition_node` removed** — the builder no longer injects a `RepartitionExec`
  (`RoundRobinBatch(4)` for unpartitioned, `Hash(exprs, 4)` for partitioned). File counts
  / small-file behavior are now controlled by upstream repartitioning +
  `IcebergWriterExec::required_input_distribution` (§10).
- **`CoalescePartitionsExec` wrap** — in `add_writer_node`, the input is wrapped in
  `CoalescePartitionsExec` so **every** writer partition's action batch reaches the
  single-partition `IcebergCommitExec` (which is required because the commit gathers all
  writer action batches via `accumulate_action_batches`, §11.1).

The `plan_iceberg_write` call site in `table_format.rs` drops the trailing `ctx`
argument it previously passed to `IcebergPlanBuilder::new`.

---

## 14. Resolver — `sail-plan/src/resolver/command/update.rs`

`resolve_command_update(table, table_alias, assignments, condition, state)`:
1. `get_table_info_for_update(&table_status, &table_name)`:
   - must be a table (not a view); location required.
   - `lakehouse_table = resolve_lakehouse_table_context(table_name,
     LakehouseOperation::Read, Some(format), vec![])`, then
     `.for_operation(LakehouseOperation::Write)`.
   - schema: from catalog columns (Arrow schema of `c.field()`), or for DELTA with empty
     columns, inferred from the data source via `TableFormatRegistry::get(format).create_source`.
2. `state.register_fields(schema.fields())` → `field_ids`; build the renamed resolution
   schema (`rename_schema` / `to_dfschema_ref`).
3. `resolve_update_table_plan(table, state)`: builds a `spec::ReadNamedTable` query,
   resolves it (aliases every column to opaque ids via read.rs's `rename_logical_plan`),
   then **undoes** the aliasing with the real field names
   (`Self::get_field_names(plan.schema(), state)` → `rename_logical_plan`).
4. Condition resolved against the renamed schema, then
   `expression_before_rename(&resolved, &field_ids, &original_arrow_schema, true)`.
5. Each assignment value resolved + renamed; **cast to the target column's Arrow type**
   via `datafusion_expr::cast(value, target_type)` so the write schema matches the table
   (`SET score = 100.0` on DOUBLE → Float64, not Decimal).
6. `UpdateInfo` → `registry.get(&format)?.create_updater(state, update_info)`.

`TableInfo { location, format, schema, properties, lakehouse_table }` private struct.

`CommandNode::Update` previously `Err(PlanError::todo("CommandNode::Update"))` — now
resolved (see `00`, `01`).

---

## 15. Wiring summary

- `sail-logical-plan/src/lib.rs`: merge module re-exports.
- `sail-iceberg/src/logical/mod.rs`: `pub mod update;`
- `sail-iceberg/src/physical/mod.rs`: `pub mod row_level_planner;`
- `sail-iceberg/src/physical_plan/mod.rs`: `pub mod planner;` +
  `pub use planner::{plan_delete, plan_merge, PlannerContext};`
- `sail-iceberg/src/table_format.rs`: `create_deleter`/`create_updater`/`create_merger`
  impls (see `table_format.rs` diff); `MERGE_FILE_COLUMN` const used throughout.
- `sail-common-datafusion/src/datasource.rs`: `UpdateInfo`, `UpdateAssignment`,
  `TableFormat::create_updater`/`create_deleter`.

---

## 16. Behavior contracts to preserve during port

- DELETE/TRUNCATE produce a **full-replacement** snapshot (`Operation::Delete`); TRUNCATE
  on an empty table is a successful no-op with `count = 0`.
- UPDATE and MERGE (with matched clauses) use **targeted rewrite**: only touched files
  are rewritten; untouched parent manifests are preserved via
  `compute_untouched_manifest_entries`; the `count` column reports matched rows for
  UPDATE and rows written otherwise.
- Insert-only MERGE bypasses file rewrite entirely (`Operation::Append`).
- The file-path column must carry the **exact manifest path string** so joins against
  touched paths match.
- `collect_touched_file_paths` must run inside a tokio runtime.
- Generated-column / check-constraint semantics reuse the existing Delta machinery
  (shared, do not reinvent).
