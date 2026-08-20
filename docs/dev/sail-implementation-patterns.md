# Sail Implementation Patterns

> **Purpose:** This document defines the canonical patterns used in Sail for implementing
> table format features (Apache Iceberg, Delta Lake). Use this as the reference when
> implementing new features or refactoring existing code.
>
> Generated from deep analysis of the `main` branch Delta Lake reference implementation
> and the `feat/iceberg-ops` branch Iceberg implementation.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [The `TableFormat` Trait](#2-the-tableformat-trait)
3. [Logical Plan Nodes](#3-logical-plan-nodes)
4. [Physical Planning (ExtensionPlanner)](#4-physical-planning-extensionplanner)
5. [DELETE — Full Reference Pattern](#5-delete--full-reference-pattern)
6. [UPDATE — Full Reference Pattern](#6-update--full-reference-pattern)
7. [MERGE — Full Reference Pattern](#7-merge--full-reference-pattern)
8. [TRUNCATE Pattern](#8-truncate-pattern)
9. [INSERT / Overwrite / Predicate Overwrite](#9-insert--overwrite--predicate-overwrite)
10. [The Commit Pipeline](#10-the-commit-pipeline)
11. [ALTER TABLE — Full Pipeline](#11-alter-table--full-pipeline)
12. [Branch & Tag Creation Pattern](#12-branch--tag-creation-pattern)
13. [Physical Executor Implementation](#13-physical-executor-implementation)
14. [Schema Evolution Integration](#14-schema-evolution-integration)
15. [Session & Extension Registration](#15-session--extension-registration)
16. [Catalog Integration](#16-catalog-integration)
17. [Options & Configuration](#17-options--configuration)
18. [Common Anti-Patterns to Avoid](#18-common-anti-patterns-to-avoid)

---

## 1. Architecture Overview

```
Spark Connect ExecutePlan / SQL
     │
     ▼
sail-catalog/src/command.rs        ← CatalogCommand dispatch
     │
     ▼
TableFormat::create_writer()       ← LogicalPlan built (IcebergWriteNode / RowLevelWriteNode)
TableFormat::create_deleter()      ← LogicalPlan built (RowLevelWriteNode)
TableFormat::create_updater()      ← LogicalPlan built (RowLevelWriteNode)
TableFormat::create_merger()       ← LogicalPlan built (RowLevelWriteNode)
     │
     ▼
DataFusion LogicalPlan optimization passes
     │
     ▼
ExtensionPlanner::plan_extension() ← Logical → Physical conversion
     │                                 (IcebergPhysicalPlanner / DeltaPhysicalPlanner)
     ▼
Physical ExecutionPlan tree        ← DataFusion parallel execution
```

**Key crates and their roles:**

| Crate | Role |
|---|---|
| `sail-common-datafusion` | `TableFormat` trait, `RowLevelWriteNode`, catalog context types, extension infrastructure |
| `sail-logical-plan` | `RowLevelWriteNode` definition, `expand_merge()` MERGE expansion engine |
| `sail-common` | `WriteOperation`, `AlterTableOperation` spec enums (protocol-agnostic) |
| `sail-catalog` | `CatalogCommand`, `AlterTableOptions`, catalog-to-format bridge (`table_format_alter_operation()`) |
| `sail-plan` | SQL plan resolution (`resolve_catalog_alter_table()`) |
| `sail-sql-analyzer` | SQL AST → spec enum conversion |
| `sail-session` | Registers `ExtensionPlanner`s in `ExtensionQueryPlanner` |
| `sail-delta-lake` | Delta Lake `TableFormat` impl, physical planners, executors |
| `sail-iceberg` | Iceberg `TableFormat` impl, physical planners, executors |

---

## 2. The `TableFormat` Trait

**File:** `crates/sail-common-datafusion/src/datasource.rs:508-567`

This is the **central abstraction**. Every table format (Delta, Iceberg) implements it.

```rust
#[async_trait]
pub trait TableFormat: Send + Sync {
    fn name(&self) -> &str;

    // ── Reads ──
    async fn create_source(&self, ctx: &dyn Session, info: SourceInfo) -> Result<Arc<dyn TableSource>>;
    async fn infer_schema(&self, ctx: &dyn Session, info: SourceInfo) -> Result<SchemaRef>;

    // ── Plain writes (INSERT, CTAS) ──
    async fn create_writer(&self, ctx: &dyn Session, info: SinkInfo) -> Result<LogicalPlan>;

    // ── Row-level DML ──
    async fn create_deleter(&self, ctx: &dyn Session, info: DeleteInfo) -> Result<LogicalPlan>;
    async fn create_merger(&self, ctx: &dyn Session, info: MergeInfo) -> Result<LogicalPlan>;
    async fn create_updater(&self, ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan>;

    // ── Schema DDL ──
    async fn alter_table(&self, runtime_env, path, operation, lakehouse_table) -> Result<()>;

    // ── CREATE TABLE metadata ──
    async fn create_table_metadata(&self, runtime_env, info) -> Result<TableFormatCreateTableResult>;
}
```

### 2.1 `create_deleter` / `create_updater` / `create_merger` Patterns

All three return a **`LogicalPlan::Extension`** wrapping a **`RowLevelWriteNode`**. The `RowLevelWriteNode` is the single unified logical plan node for all row-level operations.

**DELETE pattern** (Delta `table_format.rs:435-462`, Iceberg `table_format.rs:327-353`):
```rust
fn create_deleter(&self, ctx, info: DeleteInfo) -> Result<LogicalPlan> {
    let write_node = RowLevelWriteNode::new_delete(
        Arc::new(EmptyRelation { ... }),  // raw target (empty for DELETE)
        Arc::new(DFSchema::empty()),       // empty schema for DELETE
        info.condition,
        self.name().to_string(),           // "iceberg" or "delta"
        info.path,
        info.table_name,
        info.options,
        info.lakehouse_table,
    );
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(write_node) }))
}
```

**UPDATE pattern** (Iceberg `table_format.rs:355-383`):
```rust
fn create_updater(&self, ctx, info: UpdateInfo) -> Result<LogicalPlan> {
    let write_node = RowLevelWriteNode::new_update(
        Arc::new(EmptyRelation { ... }),
        Arc::new(DFSchema::empty()),
        info.assignments,  // SET expressions
        info.condition,
        self.name().to_string(),
        ...
    );
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(write_node) }))
}
```

**MERGE pattern** (Delta `table_format.rs:464-466`, Iceberg `table_format.rs:385-403`):
```rust
fn create_merger(&self, ctx, info: MergeInfo) -> Result<LogicalPlan> {
    let expansion = expand_merge(info, MERGE_FILE_COLUMN, None)?;
    let write_node = RowLevelWriteNode::new_merge(
        raw_target, raw_source, raw_input_schema,
        Arc::new(expansion.write_plan),
        Arc::new(expansion.touched_files_plan),
        expansion.deletion_vector_plan.map(Arc::new),
        expansion.options,
        expansion.output_schema,
    );
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(write_node) }))
}
```

**Key point:** MERGE uses `expand_merge()` from `sail-logical-plan` at the `create_merger` stage (logical planning, NOT physical planning). The physical planner receives pre-expanded plans.

---

## 3. Logical Plan Nodes

### 3.1 `RowLevelWriteNode`

**File:** `crates/sail-logical-plan/src/merge.rs:143-470`

The **unified logical plan node** for all row-level operations (DELETE, UPDATE, MERGE). Also used by Delta as the single row-level operation node.

**Key fields:**

| Field | Type | Used by |
|---|---|---|
| `command: RowLevelCommand` | `Delete \| Update \| Merge` | All |
| `condition: Option<ExprWithSource>` | WHERE clause | DELETE, UPDATE |
| `assignments: Option<Vec<...>>` | SET expressions | UPDATE |
| `write_plan: Option<Arc<LogicalPlan>>` | Pre-expanded MERGE output | MERGE |
| `touched_files_plan: Option<Arc<LogicalPlan>>` | Files touched by MERGE rewrite | MERGE |
| `deletion_vector_plan: Option<Arc<LogicalPlan>>` | (path, row_index) for DV-based deletes | MERGE (MoR) |
| `target_format: String` | "iceberg" or "delta" | All |
| `target_location: String` | Table URL | All |
| `lakehouse_table: Option<LakehouseExecutionContext>` | Catalog context | All |

### 3.2 Format-specific Write Nodes

**Delta:** `DeltaWriteNode` (Delta `table_format.rs:560-571`)
**Iceberg:** `IcebergWriteNode` (Iceberg `table_format.rs:464-516`)

Both implement `UserDefinedLogicalNodeCore` and wrap their respective format options. They are produced by `create_writer()` for INSERT/CTAS operations only.

---

## 4. Physical Planning (ExtensionPlanner)

### 4.1 Registration

**File:** `crates/sail-session/src/planner.rs:71-81`

All `ExtensionPlanner`s are registered in the session's `ExtensionQueryPlanner`:

```rust
let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
    Arc::new(DeltaPhysicalPlanner),      // Delta Lake
    Arc::new(IcebergPhysicalPlanner),    // Iceberg
    ...
];
let planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
```

### 4.2 The `plan_extension()` Dispatch Pattern

Each format's `ExtensionPlanner` handles **two** logical node types:

**File:** `crates/sail-iceberg/src/physical/table_scan_planner.rs:20-61`

```rust
impl ExtensionPlanner for IcebergPhysicalPlanner {
    async fn plan_extension(&self, planner, node, logical_inputs, physical_inputs, session_state) {
        // 1. Format-specific write node → plan_iceberg_write()
        if let Some(write_node) = node.as_any().downcast_ref::<IcebergWriteNode>() {
            return plan_iceberg_write(session_state, logical_input, physical_input, write_node).await;
        }

        // 2. RowLevelWriteNode (format matched by target_format field)
        if let Some(delete_node) = node.as_any().downcast_ref::<RowLevelWriteNode>()
            && delete_node.target_format().eq_ignore_ascii_case("iceberg")
        {
            return plan_iceberg_row_level_write(session_state, delete_node).await;
        }

        Ok(None)  // Not ours — let the next ExtensionPlanner handle it
    }
}
```

**File:** `crates/sail-delta-lake/src/physical/table_scan_planner.rs:31-54`

Delta uses the same pattern but additionally dispatches to `create_row_level_write_physical_plan()` which handles DELETE/MERGE strategy selection (Eager vs MergeOnRead).

### 4.3 `plan_table_scan()` Pattern

Both formats also implement `plan_table_scan()` to handle table scans for their format-specific `TableSource`:

```rust
async fn plan_table_scan(&self, planner, scan, session_state) {
    let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
        return Ok(None);
    };
    let plan = source.provider().scan(session_state, &scan.projection, &filters, scan.fetch).await?;
    Ok(Some(plan))
}
```

---

## 5. DELETE — Full Reference Pattern

### 5.1 Delta Lake CoW DELETE (the ideal reference)

**File:** `crates/sail-delta-lake/src/physical_plan/planner/op_delete.rs:34-171`

**Flow:**

```
1. open_table() → snapshot, version, table_schema, partition_columns
2. Build physical condition from ExprWithSource
3. Build TWO independent log replay pipelines (writer + remover):
   a. build_log_replay_pipeline_with_options()  → DeltaLogReplayExec
        → reads checkpoint + commit JSONs, replays to get Add actions
   b. build_metadata_filter() → filter Add actions by partition predicates
   c. DeltaDiscoveryExec → identifies matching files
4. Round-robin RepartitionExec → spread Add actions across partitions
5. DeltaScanByAddsExec → scan actual Parquet data files (parallel)
6. FilterExec(NOT condition) → keep surviving rows
7. assemble_commit_plan():
   - DeltaWriterExec(survivors) → writes new Parquet, emits Add actions
   - ∪ DeltaRemoveActionsExec(remover_pipeline) → converts old Add→Remove actions
   - → CoalescePartitionsExec → gather to single partition
   - → DeltaCommitExec → atomic commit to Delta log
```

**Why two independent pipelines?** The writer branch needs to be walked by execution (it produces rows), while the remover branch only needs Add-action metadata. Sharing an `Arc` would starve one branch under distributed execution.

### 5.2 Iceberg DELETE (feat/iceberg-ops — PROBLEMATIC)

**File:** `crates/sail-iceberg/src/physical_plan/delete_exec.rs`

**Current approach (anti-pattern):**
- A single `ExecutionPlan` leaf node with **no children** that does all I/O inline
- Loads metadata/manifests directly from object store during `execute()`
- Iterates manifest entries, tests `data_file_might_match()` for pruning
- For each matched file: reads entire Parquet → evaluates NOT(condition) → writes new Parquet → builds DataFile
- **Serial** per-file processing (not parallelized through DataFusion)
- Commits via `SnapshotProducer` + `commit_iceberg_changes()` with retry loop

**The correct pattern** should follow Delta's approach:
- Use the DataFusion pipeline for scanning, filtering, writing
- Use Iceberg's existing `IcebergDiscoveryExec`, `IcebergScanByDataFilesExec`, `IcebergWriterExec`
- Use the existing `IcebergCommitExec` for commit
- Build a proper `ExecutionPlan` tree where children are TableProvider-based scans

---

## 6. UPDATE — Full Reference Pattern

Same structure as DELETE, but after scanning matching files:

1. Read file → evaluate WHERE condition → BooleanArray mask
2. For each SET assignment: evaluate expression → `arrow::compute::kernels::zip::zip(mask, new_values, original)` → blend old+new values by mask
3. Rebuild RecordBatch with modified columns
4. Write new file, emit Add action
5. Files with NO matching rows stay in `kept_data_files` (unchanged)
6. Files with ANY matching rows go to `files_to_rewrite`
7. Commit: `all_data_files = kept ∪ rewritten`

**Key detail:** UPDATE without WHERE rewrites ALL files (all go to `files_to_rewrite`, none to `kept_data_files`).

---

## 7. MERGE — Full Reference Pattern

### 7.1 Logical Expansion (`expand_merge`)

**File:** `crates/sail-logical-plan/src/merge.rs:481-838`

Called at logical planning time by `TableFormat::create_merger()`. Returns a `MergeExpansion`:

```rust
pub struct MergeExpansion {
    pub write_plan: LogicalPlan,            // The unified output plan with RowLevelOperationType tags
    pub touched_files_plan: LogicalPlan,    // DISTINCT file paths of rewritten rows
    pub deletion_vector_plan: Option<LogicalPlan>,  // (path, row_index) for DV deletes
    pub output_schema: DFSchemaRef,
    pub options: MergeIntoOptions,
}
```

**Expansion steps:**
1. Rename source columns with prefix `__sail_src_*` to avoid name conflicts
2. Detect insert-only fast path (LeftAnti join)
3. Default: Full Outer Join on join keys + residual predicates
4. Add `TARGET_PRESENT` and `SOURCE_PRESENT` boolean columns
5. Build CASE-based projection with first-match semantics per clause
6. Tag rows with `RowLevelOperationType`: Copy, Insert, Update, Delete

### 7.2 Delta Lake MERGE (Copy-on-Write)

**File:** `crates/sail-delta-lake/src/physical_plan/planner/op_merge.rs:105-192`

**Flow:**
1. Get snapshot, version, schema
2. Retrieve pre-expanded `write_plan` from MergeInfo
3. **Targeted rewrite** — only rewrite files that are touched:
   - `build_targeted_writer_input()`:
     - Insert rows: `FilterExec(IS NULL path)` (new rows, not in any existing file)
     - Touched rows: `HashJoinExec(CollectLeft, path = path)` with `touched_files_plan`
     - `UnionExec(inserts, touched)`
4. Strip internal columns (MERGE_FILE_COLUMN)
5. Build Remove source: `build_remove_from_touched_files()` — joins touched paths with log replay → Add actions
6. `assemble_commit_plan()`: DeltaWriterExec ∩ DeltaRemoveActionsExec → Coalesce → Commit

### 7.3 What Iceberg MERGE MUST Implement

**Current Iceberg MERGE on feat/iceberg-ops is INCOMPLETE:**
- Only NOT MATCHED BY TARGET (INSERT) works
- Matched UPDATE and DELETE clauses are stubs
- All target data loaded into memory (not scalable)

**The correct approach** must follow the Delta pattern:
1. Receive pre-expanded `write_plan`, `touched_files_plan` from `expand_merge()`
2. In the physical planner, build a scan pipeline to scan only touched files
3. Apply row-level mutations per the `RowLevelOperationType` tags
4. Write new files, produce Remove actions for old files
5. Commit via the standard commit pipeline

---

## 8. TRUNCATE Pattern

**TRUNCATE = DELETE without a WHERE condition.**

Implementation:
- If no WHERE condition → treat as truncate
- Do NOT read any Parquet files
- Set `files_to_rewrite = vec![]`, `kept_data_files = vec![]`
- Produce an **empty snapshot** (no data files, no manifest entries)
- Commit with operation name `"delete"` or `"truncate"`

For Iceberg: this should produce a `SnapshotProducer` with empty data files and empty parent manifest entries.

---

## 9. INSERT / Overwrite / Predicate Overwrite

### 9.1 Plain INSERT (Append)

**File:** Delta `table_format.rs:400-433`, Iceberg `table_format.rs:162-198`

Creates format-specific write node (`DeltaWriteNode` / `IcebergWriteNode`) wrapping the input plan with `SinkMode::Append`.

Physical pipeline:
```
Projection → Repartition(RoundRobin) → Sort → WriterExec → Coalesce → CommitExec
```

### 9.2 Full Table Overwrite

`SinkMode::Overwrite` → `PhysicalSinkMode::Overwrite`

Same pipeline as Append, but the commit exec creates a new snapshot that replaces all existing data.

### 9.3 Predicate Overwrite (`REPLACE WHERE`)

**File:** Delta `physical_plan/planner/op_write.rs:153-288`

**Flow:**
1. Build old data pipeline: log replay → metadata filter → Discovery → Scan → `Filter(NOT condition)` → survivors
2. Build new data input (the INSERT ... REPLACE WHERE rows)
3. Align old+new schemas for union compatibility
4. `UnionExec(new_data, old_survivors)` → WriterExec
5. `∪ DeltaRemoveActionsExec(touched_files)` → Coalesce → Commit

**Iceberg: `IcebergCommitExec` supports predicate overwrite** (`commit/commit_exec.rs:1101-1109`):
- `filter_parent_manifest_entries()` loads the parent manifest list, filters by partition FieldSummary bounds
- For each partition spec field where a predicate value exists, checks if `[lower_bound, upper_bound]` overlaps
- Overlapping entries are **excluded** (replaced by new data); non-overlapping entries are **kept**

### 9.4 Partition Overwrite

`SinkMode::OverwritePartitions` → `PhysicalSinkMode::OverwritePartitions`

In `IcebergCommitExec` (`commit_exec.rs:1112-1123`): `filter_parent_manifest_entries_by_values()` filters by comparing against written partition value combinations.

---

## 10. The Commit Pipeline

### 10.1 The Standard Commit Tail

**File:** Delta `physical_plan/planner/commit.rs:56-104`

The canonical assembly for all operations:

```
WriterExec                        ← writes data files, emits Add/action batches
  ∪ RemoveActionsExec             ← converts old file metadata to Remove actions
    → CoalescePartitionsExec      ← gather to single partition
      → CommitExec                ← atomic commit
```

### 10.2 Delta CommitExec

**File:** Delta `physical_plan/commit_exec.rs:537-1034`

1. Collect all RecordBatches from children
2. Decode `COL_ACTION` → `CommitAction` (Add, Remove, Protocol, Metadata, etc.)
3. Separate bootstrap actions (Protocol, Metadata) from data actions
4. Accumulate `OperationMetrics`
5. Load reference snapshot (if removals or whole-table reads)
6. `CommitBuilder::build()` → retry loop with `ConflictChecker`
7. Write `{version}.json` to Delta log

### 10.3 Iceberg CommitExec (feat/iceberg-ops)

**File:** `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs`

1. Read child's action batches → decode adds, deletes, commit_meta
2. Resolve catalog commit mode
3. Load current metadata, validate requirements (UUID match, ref snapshot, etc.)
4. Apply schema/partition spec updates
5. Handle bootstrap for empty tables
6. Build transaction action based on operation (Append, Overwrite)
7. Attempt catalog commit → on Conflict: semantic conflict check
8. Write metadata JSON + version-hint.text
9. Update catalog metadata-location if needed

### 10.4 Iceberg ConflictChecker

**File:** `crates/sail-iceberg/src/physical_plan/commit/conflict_checker.rs`

Three-level check:
1. Schema compatibility (concurrent type changes → CONFLICT)
2. Partition conflicts (Append+Overwrite same partition → CONFLICT)
3. File conflicts (our removes ∩ winning removes → CONFLICT)

### 10.5 Action Schema

**File:** `crates/sail-iceberg/src/physical_plan/action_schema.rs`

Actions communicated between Writer and Commit via Arrow RecordBatches:
- `Add(AddFileAction)` — new data file
- `Delete(DeleteFileAction)` — delete file by path
- `CommitMeta(CommitMetaAction)` — commit metadata (schema, partition spec, predicates, requirements)

---

## 11. ALTER TABLE — Full Pipeline

### 11.1 Pipeline Flow

```
SQL ALTER TABLE ...
  │
  ▼
sail-sql-analyzer/src/statement.rs:2241
  from_ast_alter_table_operation()  → spec::AlterTableOperation
  │
  ▼
sail-plan/src/resolver/command/delta.rs:44
  resolve_delta_alter_table_or_catalog()
  │
  ├── AddCheckConstraint → Delta-specific resolver
  └── All others → resolve_catalog_alter_table()  (catalog/table.rs:479)
  │
  ▼
sail-plan/src/resolver/command/catalog/table.rs:479
  resolve_catalog_alter_table()
  converts spec::AlterTableOperation → AlterTableOptions (catalog-layer type)
  creates CatalogCommand::AlterTable
  │
  ▼
sail-catalog/src/command.rs:438
  CatalogCommand::AlterTable.execute()
  │
  ├── Non-lakehouse format → manager.alter_table() directly
  └── Lakehouse format:
      ├── table_format_alter_operation() → TableFormatAlterTableOperation
      ├── manager.resolve_lakehouse_table_status()
      ├── table_format.alter_table(runtime, path, storage_op, lakehouse_ctx)
      ├── (Iceberg only) bump metadata-location in catalog
      └── manager.alter_table(&table, catalog_options)
  │
  ▼
sail-iceberg/src/table_format.rs:405
  IcebergTableFormat::alter_table()
  Matches on TableFormatAlterTableOperation variant
  Each variant reads metadata, applies mutation, writes metadata+version-hint
```

### 11.2 Delta ALTER TABLE Reference

**File:** Delta `table_format.rs:468-516`

```rust
match operation {
    SetTableProperties { changes, if_exists } → alter_table_properties()
    AlterColumnType { column_path, data_type } → alter_table_column_type()
    AlterColumnDefault { column_path, default } → alter_table_column_default()
    AddCheckConstraint { name, expression } → add_check_constraint()
    RenameTable → Ok(())  // catalog-only operation
    AddColumns → not_impl_err!()  // TODO
    DropColumns → not_impl_err!()
    AlterColumnComment → not_impl_err!()
    AlterColumnNullability → not_impl_err!()
    AlterColumnPosition → not_impl_err!()
}
```

### 11.3 Iceberg ALTER TABLE (feat/iceberg-ops)

**File:** Iceberg `table_format.rs:405-451`

```rust
match operation {
    SetTableProperties { changes, if_exists } → alter_table_properties()
    AddColumns { columns } → alter_table_add_columns()
    DropColumns { names, if_exists } → alter_table_drop_columns()
    AlterColumnComment { column_path, comment } → alter_table_column_comment()
    AlterColumnNullability { column_path, nullable } → alter_table_column_nullability()
    AlterColumnPosition { column_path, position } → alter_table_column_position()
    RenameTable → Ok(())  // catalog-only
    _ → not_impl_err!()
}
```

All Iceberg ALTER TABLE operations use `retry_metadata_commit()` (lines 701-833):
1. Load current metadata JSON from object store
2. Apply mutation closure (add/drop columns, change comment, etc.)
3. Serialize and write new metadata JSON with `PutMode::Create` (CAS)
4. Detect post-write conflicts
5. Update `version-hint.text`
6. Max 3 retries

### 11.4 ALTER TABLE on REST Catalog

**File:** `sail-catalog-iceberg/src/provider.rs:1549-2022`

The Iceberg REST catalog has its OWN `alter_table()` in the provider (catalog level, not storage level):

- `RenameTable` → REST `renameTable` API
- `AddColumns` → `commitTable` with `AddSchemaUpdate` + `SetCurrentSchemaUpdate`
- `DropColumns` → `commitTable` with `AddSchemaUpdate` + `SetCurrentSchemaUpdate`
- `SetTableProperties` → REST `updateTable` with `SetPropertiesUpdate`
- `UnsetTableProperties` → REST `updateTable` with `RemovePropertiesUpdate`
- All others → `NotSupported`

Note: For REST-catalog tables, the REST catalog is the source of truth. Schema changes go through the REST API, NOT through direct storage writes. The `retry_metadata_commit` path is for non-REST-catalog tables.

---

## 12. Branch & Tag Creation Pattern

### 12.1 Current State

Branch/tag CREATION is **not yet implemented** anywhere. Only READING existing branches/tags is supported (time travel).

### 12.2 Implementation Plan

Data structures already exist:
- `SnapshotReference` (`sail-iceberg/src/spec/snapshots/snapshot.rs:105`): `snapshot_id`, `type`, TTL settings
- `TableMetadata.refs: HashMap<String, SnapshotReference>` (`spec/metadata/table_metadata.rs:91`)
- `TableUpdate::SetSnapshotRef` / `RemoveSnapshotRef` (`spec/catalog/mod.rs:199-212`)
- REST API models: `SetSnapshotRefUpdate`, `RemoveSnapshotRefUpdate` (`sail-catalog-iceberg/src/models/`)

**Files to modify (in order):**

1. **`sail-common/src/spec/plan.rs:1365`** — Add `CreateBranch(String)` / `CreateTag(String)` / `ReplaceBranch(String)` / `DropBranch(String)` to `AlterTableOperation`
2. **`sail-sql-analyzer/src/statement.rs:2241`** — Add SQL parsing (`ALTER TABLE ... CREATE BRANCH ...`, `CREATE TAG ...`)
3. **`sail-catalog/src/provider/options.rs:140`** — Add to `AlterTableOptions`
4. **`sail-common-datafusion/src/datasource.rs:456`** — Add to `TableFormatAlterTableOperation`
5. **`sail-catalog/src/command.rs:1020`** — Add mapping in `table_format_alter_operation()`
6. **`sail-plan/src/resolver/command/catalog/table.rs`** — Add resolution
7. **`sail-iceberg/src/table_format.rs:405`** — Add `CreateBranch(ref_name, snapshot_id, options)` implementation (read metadata → add SnapshotReference → write metadata)
8. **`sail-catalog-iceberg/src/provider.rs:1549`** — Add REST catalog implementation (call `commitTable` with `SetSnapshotRefUpdate`)

Implementation pattern: read metadata JSON → add/update the `refs` HashMap entry → write metadata with `PutMode::Create` → update `version-hint.text`. Same retry pattern as other ALTER TABLE operations.

---

## 13. Physical Executor Implementation

### 13.1 What a Physical Executor MUST Do

Every physical executor implementing `ExecutionPlan` must:

1. **Implement `execute()`** returning `SendableRecordBatchStream`
2. **Declare `children()`** — the child plans. This is critical for:
   - DataFusion's parallel execution scheduler (children are executed first)
   - Memory tracking (children's memory is accounted for)
   - Plan optimization
3. **Declare `output_partitioning()`** — how output is partitioned
4. **Declare `schema()`** — the output RecordBatch schema
5. **Declare `properties()`** — `Boundedness`, `EmissionType`

### 13.2 How to Structure Row-Level Executors CORRECTLY

**ANTI-PATTERN (Iceberg feat/iceberg-ops):** Leaf executors that do everything inline in `execute()` — load manifests, read Parquet, filter rows, write new Parquet, build DataFiles. This:
- Bypasses DataFusion's parallel scan infrastructure
- Is serial per-file
- Cannot be optimized or cost-evaluated by the planner

**CORRECT PATTERN:** Build a DataFusion `ExecutionPlan` **tree** where:
- The **scan** is a child `ExecutionPlan` (Iceberg uses `IcebergScanByDataFilesExec` or equivalent)
- The **filter/transform** is a middle `FilterExec` or custom transform node
- The **writer** is a parent `ExecutionPlan` that takes data rows and writes Parquet
- The **commit** is the root `ExecutionPlan` that collects action batches and commits

The Delta Lake approach (section 5.1) is the canonical reference.

### 13.3 Output Schema Convention

- **Action-producing executors** (Writer, RemoveActions) should output the format's action schema (e.g., `iceberg_action_schema()`)
- **Metadata executors** (Discovery, ManifestScan) output their own metadata schemas
- **Data-reading executors** (ScanByDataFiles) output the table's data schema

---

## 14. Schema Evolution Integration

### 14.1 `SchemaEvolver` (Iceberg)

**File:** `crates/sail-iceberg/src/schema_evolution.rs`

Key modes:
- `SchemaMode::Merge` — additive changes (add columns, promote types, relax nullability)
- `SchemaMode::Overwrite` — replace the entire schema (requires `overwriteSchema=true`)

### 14.2 How It Integrates With Writes

The `SchemaEvolver` is used in `IcebergWriterExec`:
1. Compare incoming DataFrame schema with the current table schema
2. If `mergeSchema=true`: call `SchemaEvolver::evolve(SchemaMode::Merge)` to add missing columns / promote types
3. If `overwriteSchema=true`: call `SchemaEvolver::evolve(SchemaMode::Overwrite)` to replace schema
4. Apply the evolved schema to the table metadata
5. The writer pads batches to match the new schema (fills missing columns with NULL/default)

### 14.3 How It Integrates With ALTER TABLE

ALTER TABLE ADD/DROP COLUMNS uses `retry_metadata_commit()`:
- ADD: load current schema → add new fields via `SchemaBuilder` → assign field IDs → write metadata
- DROP: load current schema → remove fields by name → write metadata

---

## 15. Session & Extension Registration

### 15.1 `TableFormatRegistry`

**File:** `crates/sail-common-datafusion/src/datasource.rs`

Formats register themselves at session startup:
```rust
// In session initialization:
IcebergTableFormat::register(&registry)?;
DeltaTableFormat::register(&registry)?;
```

### 15.2 `SessionExtension` Pattern

**File:** `crates/sail-common-datafusion/src/extension.rs:9-66`

Format-specific caches and resources use the `SessionExtension` trait:
```rust
pub trait SessionExtension: Send + Sync + 'static {
    fn name() -> &'static str;
}
```

Example: `DeltaTableCache` stores snapshot/log-store pairs keyed by `(table_url, version, lakehouse_table)`.

Access pattern:
```rust
let cache: Arc<DeltaTableCache> = ctx.extension()?;
```

### 15.3 `ExtensionPlanner` Registration

All format-specific `ExtensionPlanner`s are registered in `sail-session/src/planner.rs:71-81`. The order matters: If one planner returns `Ok(Some(_))`, the rest are skipped. If none matches, DataFusion falls back to the default planner.

---

## 16. Catalog Integration

### 16.1 Lakehouse Resolved Table

**File:** `crates/sail-catalog/src/lakehouse.rs:181-235`

The catalog layer resolves a table's lakehouse context:
- `LakehouseAuthority` — who manages the table metadata
- `MetadataPointerAuthority` — where metadata is stored
- `CommitAuthority` — how commits work (Filesystem, IcebergRestCommit, DeltaRatifiedCommit)
- `ScanAuthority` — who plans scans (ClientTableFormat, IcebergRestServerSide)

### 16.2 Catalog Commit Coordination

**File:** `crates/sail-iceberg/src/catalog_support/commit.rs:63-117`

`IcebergCatalogCommitMode`:
- `Filesystem` — path tables, no catalog
- `MetadataLocationCas` — catalog-managed tables using CAS on metadata location
- `CatalogCommit` — REST catalog `updateTable` commit
- `CompatibilityCatalogCommit` — try REST, fall back to filesystem+CAS

### 16.3 Catalog Managed Tables vs Path Tables

When a table is **catalog-managed**:
- The catalog stores the `metadata_location` pointer
- On write: commit metadata JSON, then update the catalog's `metadata_location` property via CAS
- On read: extract `metadata_location` from catalog properties

When a table is **path-based**:
- Metadata is discovered from `version-hint.text` + directory listing
- On write: write metadata JSON + update `version-hint.text`
- On read: follow `version-hint.text`

---

## 17. Options & Configuration

### 17.1 Option Layers

Options flow as `Vec<OptionLayer>` with priority resolution. The lowest-priority layer comes first.

```rust
Vec<OptionLayer>:
  [table_properties]   ← lowest priority
  [session_defaults]
  [query_hints]         ← highest priority
```

Resolution: `IcebergWriteOptions::resolve(ctx, options)` folds all layers.

### 17.2 Write Options

**Iceberg:** `IcebergWriteOptions` (generated from `data/options/iceberg.yaml`)
- `overwrite_schema`, `merge_schema`
- `shred_variants`, `variant_inference_buffer_size`
- `write.data.path`, `write.folder-storage.path`

**Delta:** `DeltaWriteOptions` (generated similarly)

### 17.3 Read Options

- `use_ref`, `snapshot_id`, `timestamp_as_of` (time travel)
- `metadata_as_data_read` (metadata-as-data read path)

---

## 18. Common Anti-Patterns to Avoid

### 18.1 Leaf Executors That Do Everything Inline

**Problem (feat/iceberg-ops):** `IcebergDeleteExec`, `IcebergUpdateExec`, `IcebergMergeExec`, `IcebergCompactExec` are all leaf `ExecutionPlan` nodes with `children() → vec![]` that load manifests, scan files, filter rows, and write new files entirely within their own `execute()` method.

**Fix:** Decompose into DataFusion `ExecutionPlan` trees with proper children (scan → filter → write → commit). Use existing operators like `IcebergScanByDataFilesExec` for scanning, `FilterExec` for filtering, `IcebergWriterExec` for writing, `IcebergCommitExec` for committing.

### 18.2 Serial Per-File Processing

**Problem:** Iterating files one-by-one in a Rust loop inside `execute()`.

**Fix:** Use DataFusion's partition-based parallel execution. Each partition gets a subset of files, and the framework handles parallelism.

### 18.3 Bypassing Catalog Coordination

**Problem:** Row-level executors doing direct object store commits without going through the catalog commit layer.

**Fix:** Always produce action batches and let `IcebergCommitExec` handle the commit, which automatically routes through the correct `IcebergCatalogCommitMode`.

### 18.4 Missing Statistics in Output DataFiles

**Problem:** `DataFile` structs emitted with `lower_bounds: Default::default()`, `upper_bounds: Default::default()`, `nan_value_counts: Default::default()`.

**Fix:** Extract statistics from the Parquet writer metadata and populate `column_sizes`, `value_counts`, `null_value_counts`, `lower_bounds`, `upper_bounds`.

### 18.5 Duplicated Manifest-Loading Code

**Problem:** `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`, `compact_exec.rs` each have identical `load_manifest_list` / `load_manifest_entries` functions.

**Fix:** Extract shared utilities into `crate::io` or `crate::table` modules. Follow Delta's approach where `ctx.open_table()` returns a cached table with pre-loaded manifests.

### 18.6 Loading All Data Into Memory

**Problem:** `merge_exec.rs` reads ALL target files and concatenates them with `concat_batches()`. This will OOM on any non-trivial table.

**Fix:** Use streaming processing. Process files partition-by-partition, joining source rows with only the relevant target file's data.

### 18.7 Bypassing DataFusion Scan for Row-Level Ops

**Problem:** Row-level executors open Parquet files directly with `ParquetObjectReader` instead of using the table provider's scan.

**Fix:** Use the same scan path as reads. For Iceberg, this means building a child plan that uses the `IcebergTableProvider`'s scan infrastructure, which handles partition pruning, metrics-based pruning, projection, and delete file application.

### 18.8 Not Following Delta's Planner Pattern

**Problem:** The Iceberg `plan_iceberg_row_level_write()` in `row_level_planner.rs:79` creates monolithic executors directly instead of using a planner context with session, snapshot, and config.

**Fix:** Follow Delta's pattern:
```rust
pub fn plan_delete(ctx: &PlannerContext, condition: ExprWithSource) -> Result<Arc<dyn ExecutionPlan>>
pub fn plan_merge(ctx: &PlannerContext, merge_info: RowLevelWriteInfo) -> Result<Arc<dyn ExecutionPlan>>
```
Where `PlannerContext` provides: `session()`, `open_table()`, `table_url()`, `options()`, `lakehouse_table()`.

---

## Appendix A: Key File References

| Component | File |
|---|---|
| `TableFormat` trait | `crates/sail-common-datafusion/src/datasource.rs:508` |
| `RowLevelWriteNode` | `crates/sail-logical-plan/src/merge.rs:143` |
| `expand_merge()` | `crates/sail-logical-plan/src/merge.rs:481` |
| `AlterTableOptions` | `crates/sail-catalog/src/provider/options.rs:139` |
| `CatalogCommand` | `crates/sail-catalog/src/command.rs:27` |
| Delta `SessionExtension` | `crates/sail-delta-lake/src/session_extension.rs:44` |
| Extension query planner | `crates/sail-session/src/planner.rs:55` |
| Delta DELETE planner | `crates/sail-delta-lake/src/physical_plan/planner/op_delete.rs:34` |
| Delta MERGE planner | `crates/sail-delta-lake/src/physical_plan/planner/op_merge.rs:105` |
| Delta commit assembly | `crates/sail-delta-lake/src/physical_plan/planner/commit.rs:56` |
| Iceberg commit exec | `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` |
| Iceberg conflict checker | `crates/sail-iceberg/src/physical_plan/commit/conflict_checker.rs` |
| Iceberg ALTER TABLE | `crates/sail-iceberg/src/table_format.rs:405` |
| Iceberg REST ALTER TABLE | `crates/sail-catalog-iceberg/src/provider.rs:1549` |
| Delta ALTER TABLE | `crates/sail-delta-lake/src/table_format.rs:468` |
| Iceberg options YAML | `data/options/iceberg.yaml` |

## Appendix B: Operation Name Conventions

| SQL Operation | Iceberg `Operation` enum |
|---|---|
| INSERT | `Operation::Append` |
| INSERT OVERWRITE | `Operation::Overwrite` |
| DELETE | `Operation::Delete` |
| UPDATE | `Operation::Overwrite` (rewrites files) |
| MERGE | `Operation::Overwrite` (rewrites files) |
| COMPACT | `Operation::Replace` |
| TRUNCATE | `Operation::Delete` (empty snapshot) |
| REPLACE WHERE | `Operation::Overwrite` |
| CREATE TABLE | `Operation::Append` (bootstrap) |

## Appendix C: Idiom Audit & Correction Plan for Iceberg Row-Level Ops

> **Scope:** Audit of `feat/iceberg-ops` changes against Sail 0.6.6 idioms.
> **Reference:** Delta Lake implementation in `crates/sail-delta-lake/src/physical_plan/planner/`.

---

### C1. ExecutionPlan Idioms — The Universal Pattern

Every ExecutionPlan node in Sail follows this exact template. The `feat/iceberg-ops` anti-pattern nodes (`IcebergDeleteExec`, `IcebergUpdateExec`, `IcebergMergeExec`, `IcebergCompactExec`) **violate every line** of this.

**Skeleton (from DeltaWriterExec:282-602, IcebergWriterExec:59-631):**

```rust
pub struct IcebergDeleteScanExec {
    input: Arc<dyn ExecutionPlan>,    // ← MUST have children, NOT a leaf
    table_url: Url,                   // ← Url, not String
    // ... format-specific fields ...
    cache: Arc<PlanProperties>,       // ← stored! Always named `cache` for Delta, sometimes `cache`
}

impl IcebergDeleteScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, ...) -> Self {
        let schema = /* computed or given */;
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,          // ← Final for write nodes, delegates for pure-transform nodes
            Boundedness::Bounded,         // ← Always Bounded
        ));
        Self { input, cache, ... }
    }
}

impl ExecutionPlan for IcebergDeleteScanExec {
    fn name(&self) -> &'static str { "IcebergDeleteScanExec" }

    fn properties(&self) -> &PlanProperties { &self.cache }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }  // NOT vec![]

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn Self>> {
        // Exactly one child for most nodes:
        Ok(Arc::new(Self::new(children.one()?, ...)))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // Pattern A (single-future): for write/commit nodes
        // futures::stream::once(async { ... }).into()

        // Pattern B (unfold): for streaming state machines
        // stream::try_unfold(initial_state, |state| async move { ... }).into()

        // Pattern C (map): for per-batch transforms
        // input.execute(..)?.map(|batch| transform(batch))

        // Pattern D (try_filter_map): for row-filtering transforms
        // input.execute(..)?.try_filter_map(|batch| maybe_filter(batch))

        // NEVER: load manifests inline. NEVER: do blocking I/O in execute() body.
        // The scan pipeline goes through DataFusion children, not direct object_store access.
    }
}
```

**Key rules:**

| Rule | Delta Reference | Iceberg Reference |
|---|---|---|
| Properties stored in `cache: Arc<PlanProperties>` | `writer_exec.rs:295` | `writer_exec.rs:68` |
| Children via `vec![&self.input]` | `writer_exec.rs:558` | `writer_exec.rs:304` |
| Execute returns `SendableRecordBatchStream` | `writer_exec.rs:584` | `writer_exec.rs:326` |
| Single-future uses `stream::once(future)` | `writer_exec.rs:853-857` | `writer_exec.rs:627-631` |
| Unfold uses `stream::try_unfold(state, |mut s| async { ... })` | `scan_by_adds_exec.rs:777` | `scan_by_data_files_exec.rs:315` |
| `try_filter_map` for row-filtering transforms | `discovery_exec.rs:188` | `discovery_exec.rs:163` |
| `Url` not `String` for table paths | All Delta nodes | `writer_exec.rs:60` (uses Url) |
| **No direct object_store access in execute() beyond what child nodes do** | All Delta nodes | All 0.6.6 Iceberg nodes |

---

### C2. Row-Level Operations MUST Follow the PlannerContext Pattern

**Delta reference:** `planner/context.rs:140-338` (PlannerContext), `planner/row_level.rs:103-197` (create_delta_row_level_writer)

**The pattern:**

```rust
// ── File: crates/sail-iceberg/src/physical/row_level_planner.rs ──

// CORRECT PATTERN (replaces feat/iceberg-ops approach entirely):

struct IcebergPlannerContext<'a> {
    session: &'a dyn Session,
    options: IcebergWriteOptions,
    table_url: Url,
    lakehouse_table: Option<LakehouseExecutionContext>,
    // table cache goes here (similar to Delta's session_extension.rs cache)
    table_cache: Arc<IcebergTableCache>,
}

impl<'a> IcebergPlannerContext<'a> {
    async fn open_table(&self) -> Result<Table> {
        // Use store from session's runtime_env, load metadata, cache result
        // Pattern from Delta's PlannerContext::open_table() (context.rs:298)
        let store = self.session.runtime_env()
            .object_store_registry.get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Table::load(self.session, self.table_url.clone()).await
    }

    fn prepare_write_context(&self, ...) -> Result<IcebergWriteContext> {
        // Build write context with schema evolution, partition spec, etc.
        // Pattern from Delta's PlannerContext::prepare_write_context() (context.rs:234)
    }
}

// Entry point called from IcebergPhysicalPlanner::plan_extension():
pub async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    // 1. Resolve options from OptionLayers
    let options = IcebergWriteOptions::resolve(session_state, node.options().clone())?;

    // 2. Parse table URL
    let table_url = IcebergTableFormat::parse_table_url(vec![node.target_location().clone()]).await?;

    // 3. Create planner context
    let ctx = IcebergPlannerContext::new(session_state, options, table_url, node.lakehouse_table().clone());

    // 4. Dispatch by command type
    match node.command() {
        RowLevelCommand::Delete => plan_delete(&ctx, node).await,
        RowLevelCommand::Update => plan_update(&ctx, node).await,
        RowLevelCommand::Merge => plan_merge(&ctx, node).await,
    }
}

// NOTE: The planner function signature matches Delta's pattern:
//   pub fn plan_delete(ctx: &PlannerContext, ...) -> Result<Arc<dyn ExecutionPlan>>
// NOT the anti-pattern:
//   pub fn plan_delete(session_state, node) -> Result<Arc<dyn ExecutionPlan>>
//   where the function creates a monolithic leaf executor
```

---

### C3. DELETE — Correction Plan

**Current anti-pattern** (`feat/iceberg-ops`, `delete_exec.rs`):
- Leaf node with `children() → vec![]`
- Loads manifests in `execute()` via `find_latest_metadata_file_with_catalog_fallback()`
- Iterates files serially: read Parquet → filter → write Parquet → build DataFile
- Commits directly via `SnapshotProducer` + `commit_iceberg_changes()`

**Correct pattern** (following Delta `op_delete.rs:34-171`):

```rust
// ── File: crates/sail-iceberg/src/physical_plan/planner/op_delete.rs ──

pub async fn plan_delete(
    ctx: &IcebergPlannerContext<'_>,
    condition: ExprWithSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    // 1. Open table, get snapshot + schema
    let table = ctx.open_table().await?;
    let snapshot = table.current_snapshot().ok_or_else(||
        DataFusionError::Plan("Cannot delete from empty table".to_string()))?;
    let iceberg_schema = table.current_schema();
    let arrow_schema = iceberg_schema_to_arrow(iceberg_schema)?;

    // 2. Build physical condition from ExprWithSource
    let physical_condition = ctx.session()
        .create_physical_expr(condition.expr.clone(), &arrow_schema.to_dfschema()?)?;

    // 3. Build the SCAN pipeline using existing Iceberg operators:
    //    IcebergManifestScanExec → IcebergDiscoveryExec (+partition_scan) → IcebergScanByDataFilesExec
    //
    //    Step 3a: Scan manifests to get file metadata
    let manifest_scan: Arc<dyn ExecutionPlan> = Arc::new(
        IcebergManifestScanExec::new(table.table_url.clone(), snapshot.clone())
    );
    //    Step 3b: Append partition_scan boolean column
    let discovery: Arc<dyn ExecutionPlan> = Arc::new(
        IcebergDiscoveryExec::new(manifest_scan, table.table_url.to_string(), snapshot.snapshot_id)
    );
    //    Step 3c: Scan actual Parquet data files (in parallel, batched by 1024)
    let data_scan: Arc<dyn ExecutionPlan> = Arc::new(
        IcebergScanByDataFilesExec::new(discovery, table.table_url.to_string(), arrow_schema.clone())
    );

    // 4. Apply NOT(condition) filter to keep surviving rows
    let negated = Arc::new(NotExpr::new(physical_condition));
    let survivor_filter: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExec::try_new(negated, data_scan)?
    );

    // 5. Write survivors using existing IcebergWriterExec
    let writer: Arc<dyn ExecutionPlan> = Arc::new(
        IcebergWriterExec::new(survivor_filter, table_url, partition_columns, ...)
    );

    // 6. Build Remove source from the ORIGINAL files that matched the condition
    //    (we need to know which DataFile paths to remove)
    //    This uses a second independent manifest scan pipeline:
    let remove_manifest_scan = IcebergManifestScanExec::new(table_url, snapshot);
    //    ... filter manifests by partition predicates from condition ...
    //    → IcebergDeleteFileActionsExec (converts manifest entries to Delete action batches)

    // 7. UNION writer output + remove output, coalesce, commit
    let union = UnionExec::try_new(vec![writer, remove_source])?;
    let coalesced = Arc::new(CoalescePartitionsExec::new(union));
    let commit = Arc::new(IcebergCommitExec::new(coalesced, table_url, lakehouse_table));

    Ok(commit)
}
```

**What this replaces:**

| feat/iceberg-ops file | What happens to it | Replaced by |
|---|---|---|
| `delete_exec.rs` (824 lines) | **DELETE entirely** | `planner/op_delete.rs` (~200 lines) + existing operators |
| `update_exec.rs` (833 lines) | **DELETE entirely** | `planner/op_update.rs` (~200 lines) + existing operators |
| `merge_exec.rs` (771 lines) | **DELETE entirely** | `planner/op_merge.rs` (~300 lines) + existing operators |
| `compact_exec.rs` (649 lines) | **DELETE entirely** | `planner/op_compact.rs` (~150 lines) + existing operators |

---

### C4. UPDATE — Correction Plan

Same structure as DELETE, differs only in the transform step after scanning:

**File:** `crates/sail-iceberg/src/physical_plan/planner/op_update.rs`

```rust
// After scanning data files with IcebergScanByDataFilesExec:
// Instead of FilterExec(NOT condition), use a custom transform exec that:
//   1. Evaluates WHERE condition → BooleanArray mask
//   2. For each SET assignment: evaluates expression
//   3. Blends old + new: arrow::compute::zip(mask, &new, &original)
//   4. Rebuilds RecordBatch with modified columns
//   5. Passes ALL rows through (survivors to writer)

// Use the pattern from RelaxedTzCastExec (per-batch transform, .map() on stream)
// Or a custom StreamingExec that implements a per-batch transform with map()
```

**Truncate optimization:** When no WHERE condition is present:
- Skip the entire scan pipeline
- Set survivor_scan to `EmptyExec::new(arrow_schema)`
- Writer produces zero files
- Commit creates an empty snapshot

---

### C5. MERGE — Correction Plan

**Critical issue:** `merge_exec.rs` loads ALL target data into memory with `concat_batches()`. This is fundamentally unscalable.

**Correct pattern** (following Delta `op_merge.rs:105-192`):

1. **Use the pre-expanded plans from `RowLevelWriteNode`:**
   - `write_plan` → the physical plan of the merged output (from `expand_merge()`)
   - `touched_files_plan` → DISTINCT file paths of files that need rewriting

2. **Targeted rewrite** (only rewrite files that are touched):
   ```rust
   // Insert rows: path IS NULL → new rows
   let insert_rows = FilterExec::try_new(
       Arc::new(IsNullExpr::new(Column::new(MERGE_FILE_COLUMN, file_path_idx))),
       // the physical plan from datafusion converting write_plan
   )?;

   // Touched rows: INNER JOIN with touched_files_plan on file path
   let touched_rows = HashJoinExec::try_new(
       FilterExec::try_new(
           Arc::new(IsNotNullExpr::new(...)), // path IS NOT NULL
           // the physical plan
       )?,
       // the physical plan from touched_files_plan
       vec![(Column::new(MERGE_FILE_COLUMN, left_idx), Column::new(PATH_COLUMN, right_idx))],
       None,
       &JoinType::Inner,
       None,
       PartitionMode::CollectLeft,
       NullEquality::NullEqualsNothing,
       false,
   )?;

   let writer_input = UnionExec::try_new(vec![insert_rows, touched_rows])?;
   ```

3. **Strip internal columns** (MERGE_FILE_COLUMN before writing)

4. **Build Remove source** from `touched_files_plan`:
   - Inner join touched paths with manifest scan → Add actions
   - Hydrate Add actions into `IcebergDeleteFileActionsExec`

5. **Commit:** `IcebergWriterExec ∪ IcebergDeleteFileActionsExec → Coalesce → IcebergCommitExec`

---

### C6. COMPACT — Correction Plan

Currently `compact_exec.rs` does bin-packing and all I/O inline.

**Correct pattern:** Build an ExecutionPlan tree:

```rust
// 1. Scan manifests → collect FileInfo (path, size, partition)
let manifest_scan = IcebergManifestScanExec::new(table_url, snapshot);

// 2. New IcebergCompactGroupExec: takes FileInfo batches, groups small files
//    by partition, packs into batches of ~target_file_size
//    Outputs the grouped file info (which files go together)

// 3. IcebergScanByDataFilesExec: scans the actual data files
//    (reuses existing scan infrastructure)

// 4. Group-level streaming merge: concat within group → write single file
//    This can be a custom exec or handled by repartitioning by group key

// 5. Write → Remove old files → Commit
```

---

### C7. ALTER TABLE — Correction Plan

**Current state on 0.6.6:** Only `SetTableProperties` is implemented.

**feat/iceberg-ops adds:** `AddColumns`, `DropColumns`, `AlterColumnComment`, `AlterColumnNullability`, `AlterColumnPosition`.

**These ARE correctly implemented** on feat/iceberg-ops (`table_format.rs:405-451`). The pattern follows the existing `retry_metadata_commit()` idiom. What's missing from the correction plan for ALTER TABLE:

1. Add `AlterColumnType` support — needs `SchemaEvolver` integration for type promotion validation
2. Add `AlterColumnDefault` support — data model exists, just needs wiring
3. Add branch/tag creation — see Appendix C.8 below

**Files to port from feat/iceberg-ops (these are correct):**
- ALTER TABLE match arms in `table_format.rs:405-451` → move to 0.6.6
- `retry_metadata_commit()` in `table_format.rs:701-833` → move to 0.6.6
- Individual alter methods (`alter_table_add_columns`, `alter_table_drop_columns`, etc.) → move to 0.6.6

---

### C8. Branch & Tag Creation — Implementation Plan

Not in feat/iceberg-ops. Follow the pattern in Section 12 of the main document.

**Files to touch (in order):**

| # | File | What to add |
|---|---|---|
| 1 | `sail-common/src/spec/plan.rs` | `CreateBranch(String, snapshot_id)`, `CreateTag(String, snapshot_id)`, `ReplaceBranch(String)`, `DropBranch(String, bool)` variants on `AlterTableOperation` |
| 2 | `sail-sql-analyzer/src/statement.rs` | SQL parsing: `ALTER TABLE t CREATE BRANCH main` → `CreateBranch` |
| 3 | `sail-catalog/src/provider/options.rs` | Add to `AlterTableOptions` |
| 4 | `sail-common-datafusion/src/datasource.rs` | Add to `TableFormatAlterTableOperation` |
| 5 | `sail-catalog/src/command.rs` | Map in `table_format_alter_operation()` |
| 6 | `sail-plan/src/resolver/command/catalog/table.rs` | Map in `resolve_catalog_alter_table()` |
| 7 | `sail-iceberg/src/table_format.rs` | Implementation: read metadata → modify `refs` HashMap → write via `retry_metadata_commit()` |
| 8 | `sail-catalog-iceberg/src/provider.rs` | REST catalog: use `commitTable` with `SetSnapshotRefUpdate` |

---

### C9. Files to Create (New Planner Module)

```
crates/sail-iceberg/src/physical_plan/planner/
├── mod.rs              ← Re-exports: plan_delete, plan_update, plan_merge, plan_compact
├── context.rs          ← IcebergPlannerContext (mirrors Delta's PlannerContext)
├── op_delete.rs        ← plan_delete() function
├── op_update.rs        ← plan_update() function
├── op_merge.rs         ← plan_merge() function
├── op_compact.rs       ← plan_compact() function
└── commit.rs           ← assemble_commit_plan() (mirrors Delta's commit assembly)
```

**Files to DELETE from feat/iceberg-ops (anti-pattern leaf executors):**

| File | Reason |
|---|---|
| `delete_exec.rs` | Replaced by `planner/op_delete.rs` + existing pipeline operators |
| `update_exec.rs` | Replaced by `planner/op_update.rs` + existing pipeline operators |
| `merge_exec.rs` | Replaced by `planner/op_merge.rs` + expand_merge() + existing pipeline operators |
| `compact_exec.rs` | Replaced by `planner/op_compact.rs` + existing pipeline operators |
| `commit_helper.rs` | Merged into `IcebergCommitExec` or `planner/commit.rs` |

**Files to KEEP from feat/iceberg-ops (correct):**

| File | Reason |
|---|---|
| ALTER TABLE methods in `table_format.rs` | Correctly follows `retry_metadata_commit()` pattern |
| `conflict_checker.rs` | New, needed for concurrent write safety |
| `action_schema.rs` additions (Delete action) | Needed for Remove pipeline |
| `utils/metadata.rs` additions (`metadata_files_for_version`, `is_stale_metadata_file`) | Needed for commit conflict detection |
| `operations/snapshot.rs` additions | Bootstrap and manifest entry enhancements |
| Catalog `commit_helper.rs` (rename to `commit_coordination.rs` or merge into `commit.rs`) | Commit mode resolution |

---

### C10. New Physical Operator Needed

**`IcebergDeleteFileActionsExec`** (mirrors `DeltaRemoveActionsExec`):

```rust
// Takes a manifest scan stream as input (IcebergManifestScanExec output)
// Converts DataFile entries to Delete action batches
// Used in DELETE, UPDATE, MERGE to produce Remove actions for old files

pub struct IcebergDeleteFileActionsExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    cache: Arc<PlanProperties>,
}

impl ExecutionPlan for IcebergDeleteFileActionsExec {
    fn name(&self) -> &'static str { "IcebergDeleteFileActionsExec" }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // Pattern: input.try_filter_map(|batch| {
        //     // Decode file_path from manifest metadata batch
        //     // Encode as Delete action batch
        // })
        // → RecordBatchStreamAdapter
    }
    // Schema: iceberg_action_schema()
}
```

---

### C11. Summary: Effort Estimate

| Task | New Files | Lines (est.) | Risk |
|---|---|---|---|
| PlannerContext (`context.rs`) | 1 | ~150 | Low |
| DELETE planner (`op_delete.rs`) | 1 | ~180 | Low (existing operators reused) |
| UPDATE planner (`op_update.rs`) | 1 | ~220 | Medium (need per-batch transform exec) |
| MERGE planner (`op_merge.rs`) | 1 | ~300 | Medium (targeted rewrite per Delta) |
| COMPACT planner (`op_compact.rs`) | 1 | ~150 | Low |
| Commit assembly (`commit.rs`) | 1 | ~100 | Low |
| `IcebergDeleteFileActionsExec` | 1 | ~100 | Low |
| Planner `mod.rs` re-exports | 1 | ~30 | Low |
| Port ALTER TABLE from feat/iceberg-ops | ~2 files modified | ~600 (existing) | Low |
| Port conflict_checker from feat/iceberg-ops | 1 | ~924 (existing) | Low |
| Delete 4 anti-pattern executors | 4 files removed | ~3077 removed | None |
| Update `table_scan_planner.rs` | 1 mod | ~20 new, ~30 removed | Low |
| Update `table_format.rs` | 1 mod | ~40 new | Low |
| **Total** | **12 new, 6 modified, 4 deleted** | **~1200 new, ~3077 removed** | |

**The key principle: build on existing infrastructure.** The `IcebergManifestScanExec`, `IcebergDiscoveryExec`, `IcebergScanByDataFilesExec`, `IcebergWriterExec`, and `IcebergCommitExec` already implement the entire scan→write→commit pipeline. Row-level operations just need to compose them with a filter/transform step in the middle.

---

### C12. Complete Idiom Checklist

When implementing any new Iceberg feature, verify against this checklist:

- [ ] Uses `Url` not `String` for table paths
- [ ] ExecutionPlan stores properties in `cache: Arc<PlanProperties>` or `properties: Arc<PlanProperties>`
- [ ] ExecutionPlan has `children()` returning `vec![&self.input]` (not `vec![]`)
- [ ] `execute()` returns `SendableRecordBatchStream` via DataFusion adapter patterns
- [ ] No direct `object_store` access in `execute()` — use child ExecutionPlan pipelines
- [ ] Planner uses a `PlannerContext` struct with `session()`, `open_table()`
- [ ] Table loading uses `Table::load()` from `sail-iceberg/src/table/mod.rs`
- [ ] Commit goes through `IcebergCommitExec`, never inline `commit_iceberg_changes()`
- [ ] Action communication uses `iceberg_action_schema()` + `encode_actions()`/`decode_actions_and_meta_from_batch()`
- [ ] Configuration uses `IcebergWriteOptions::resolve()` from `OptionLayer` chain
- [ ] Error propagation uses `DataFusionError::External(Box::new(e))` for non-DF errors
- [ ] ALTER TABLE uses `retry_metadata_commit()` with `PutMode::Create` CAS
- [ ] Schema evolution uses `SchemaEvolver::evolve()` from `schema_evolution.rs`
- [ ] Output statistics populated from Parquet metadata (not `Default::default()`)
- [ ] Emitted DataFile actions carry `lower_bounds`, `upper_bounds`, `value_counts`, `null_value_counts`
- [ ] MERGE uses `expand_merge()` at logical planning time, not physical planning time
- [ ] No `concat_batches()` on entire tables — use streaming partition-by-partition
