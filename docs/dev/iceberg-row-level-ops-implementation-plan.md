# Iceberg Row-Level Operations: Final Implementation Plan

> **Audience:** Developer implementing DELETE, UPDATE, MERGE, TRUNCATE, COMPACT, Predicate Overwrite, Bucketing, and extended ALTER TABLE for Apache Iceberg in Sail.
>
> **Branch context:** This plan compares the crude `feat/iceberg-ops` implementation against
> the canonical Sail idioms (based on the Delta Lake reference implementation) and
> the existing Iceberg 0.6.6 infrastructure. It provides the exact step-by-step
> plan for a correct, idiom-conformant implementation.
>
> **Generated from:** Deep audit of all 3 codebase states (Delta master, Iceberg 0.6.6,
> Iceberg feat/iceberg-ops) across 150+ files, ~140,000 graph edges.

---

## 0. Executive Summary

The `feat/iceberg-ops` branch implemented DELETE, UPDATE, MERGE, COMPACT, and extended ALTER TABLE using **monolithic leaf ExecutionPlan executors** that bypass DataFusion's scan infrastructure. The correct approach is to compose row-level operations as **DataFusion ExecutionPlan trees** using the existing Iceberg operators (`IcebergManifestScanExec`, `IcebergDiscoveryExec`, `IcebergScanByDataFilesExec`, `IcebergWriterExec`, `IcebergCommitExec`) with filter/transform steps in the middle. This is exactly how Delta Lake implements DELETE and MERGE in `sail-delta-lake/src/physical_plan/planner/`.

**Net effect:** ~1,200 lines of new planner code replaces ~3,077 lines of anti-pattern executor code.

> **CRITICAL CORRECTION (from idiom verification):**
> 1. `sail-iceberg` does NOT depend on `sail-logical-plan` — must be added to `Cargo.toml`
> 2. `create_row_level_write_physical_plan()` receives `&SessionState` + `&dyn PhysicalPlanner` (NOT a pre-built `PlannerContext`). The `PlannerContext` is built lazily inside the dispatcher — exactly as Delta does in `row_level.rs:103-175`.
> 3. For v1 DELETE, we do NOT need `IcebergDeleteFileActionsExec`. An Overwrite-style commit (`Operation::Delete` → `SnapshotProducer` with new survivor files) naturally removes old files by replacing all parent manifests.
> 4. `IcebergDiscoveryExec` does NO pruning — it only appends a `partition_scan` boolean column.
> 5. `RowLevelWriteNode` has NO `assignments()` accessor — must add one.
> 6. `IcebergWriterExec` currently only emits `Append`/`Overwrite` — must extend to support `Delete`/`Replace`.
> 7. The Writer→Commit data flow is a SINGLE `RecordBatch` via `concat_batches([add_files, commit_meta])`.

---

## 1. Side-by-Side: Crude vs. Sail Idiom

### 1.1 DELETE

| Aspect | Crude (`delete_exec.rs`, 824 lines) | Sail Idiom (`op_delete.rs`, ~180 lines) |
|---|---|---|
| **Node type** | Leaf `ExecutionPlan` with `children() → vec![]` | Planner function composing existing operators |
| **File discovery** | `find_latest_metadata_file_with_catalog_fallback()` in `execute()` | `PlannerContext::open_table()` via `Table::load()` |
| **Manifest loading** | `load_manifest_list()` / `load_manifest_entries()` inline | `IcebergManifestScanExec` (existing operator, `new(table_url: String, snapshot: Snapshot)` ) |
| **File pruning** | `data_file_might_match()` per file in serial loop | `IcebergDiscoveryExec` (existing) + partition filter pushdown |
| **Data scanning** | `ParquetObjectReader` per file, read entire file | `IcebergScanByDataFilesExec` (existing, `new(input, table_url: String, output_schema: SchemaRef)`, batched 1024 files, parallel) |
| **Filtering** | Manual `NOT(condition)` mask, `filter_record_batch()` | `FilterExec(NOT condition)` (DataFusion operator) |
| **Writing** | `ArrowParquetWriter` per file, serial | `IcebergWriterExec` (existing, `new(input, table_url: Url, partition_columns, sink_mode, table_exists, options, logical_input_schema)`) |
| **Remove actions** | N/A (discards parent manifests entirely) | `IcebergDeleteFileActionsExec` (new, per-file delete action batches) |
| **Commit** | `SnapshotProducer` + `commit_iceberg_changes()` with custom retry | `IcebergCommitExec` (existing, `new(input, table_url: Url, lakehouse_table)`, handles catalog modes, retry, conflict checking) |
| **Parallelism** | Serial per-file | DataFusion partition-based (`target_partitions`) |
| **DataFile stats** | Empty `lower_bounds`/`upper_bounds`/`nan_value_counts` | Populated from Parquet `ArrowWriter` metadata |
| **Output** | `Schema::new([Field("count", UInt64)])` — single count | Same output schema, but through `IcebergCommitExec` |
| **Table schema access** | Direct `TableMetadata` manipulation | `table.metadata().current_schema()` |
| **Schema conversion** | Not done | `iceberg_schema_to_arrow(&schema) -> Result<ArrowSchema>` |
| **Table URL parsing** | Inline URL construction | `IcebergTableFormat::parse_table_url(vec![path]).await?` |
| **Snapshot access** | Direct `Snapshot` field access | `table.metadata().current_snapshot()` |

### 1.2 UPDATE

| Aspect | Crude (`update_exec.rs`, 833 lines) | Sail Idiom (`op_update.rs`, ~220 lines) |
|---|---|---|
| **Transform** | `arrow::compute::zip(mask, new, original)` per column | Same logic extracted to `IcebergUpdateTransformExec` (new streaming operator) |
| **No-condition** | Rewrites ALL files (files_to_rewrite = all, kept = []) | Same behavior, correct |
| **Everything else** | Same anti-patterns as DELETE | Same corrections as DELETE |

### 1.3 MERGE

| Aspect | Crude (`merge_exec.rs`, 771 lines) | Sail Idiom (`op_merge.rs`, ~300 lines) |
|---|---|---|
| **Memory** | `concat_batches()` reads ALL target files into memory | Streaming, per-file joins via `HashJoinExec` |
| **Matched UPDATE** | **Stub** — target files kept unchanged | `build_targeted_writer_input()` — rewrite only touched files |
| **Matched DELETE** | **Stub** — rows NOT removed | Applied via `RowLevelOperationType::Delete` tags |
| **Insert-only** | Works (LeftAnti join) | Works (insert-only fast path from `expand_merge()`) |
| **Remove old files** | N/A | `build_remove_from_touched_files()` → `IcebergDeleteFileActionsExec` |
| **Source plan** | Executed via separate `SessionContext` | Converted by DataFusion `PhysicalPlanner` (normal path) |
| **`expand_merge()`** | Called at logical planning time ✓ | Same ✓ (correctly used in `create_merger()`) |
| **Expansion reuse** | `write_plan`/`touched_files_plan` received but NOT used | Uses `write_plan` for data, `touched_files_plan` for targeted rewrite |

### 1.4 COMPACT

| Aspect | Crude (`compact_exec.rs`, 649 lines) | Sail Idiom (`op_compact.rs`, ~150 lines) |
|---|---|---|
| **Grouping** | Custom `bin_pack_files()` per partition, in-memory | `IcebergCompactGroupExec` (new operator, streaming group-by-partition) |
| **Merge** | `concat_batches()` all files in batch | Streams files through `IcebergScanByDataFilesExec` → grouped writes |
| **Everything else** | Same anti-patterns as DELETE | Same corrections |

### 1.5 ALTER TABLE

| Aspect | Crude (feat/iceberg-ops) | Sail Idiom |
|---|---|---|
| **AddColumns** | ✓ Correct — `retry_metadata_commit()` | Same |
| **DropColumns** | ✓ Correct | Same |
| **AlterColumnComment** | ✓ Correct | Same |
| **AlterColumnNullability** | ✓ Correct | Same |
| **AlterColumnPosition** | ✓ Correct | Same |
| **AlterColumnType** | Not implemented | Port from feat/iceberg-ops + add `SchemaEvolver` integration |
| **AlterColumnDefault** | Not implemented | Data model exists, wire DDL path |
| **Branch/Tag Create** | Not implemented | New feature (see Section 5) |

---

## 2. Architecture Plan

### 2.1 New File Tree

```
crates/sail-iceberg/src/
├── physical_plan/
│   ├── planner/                    ★ NEW directory
│   │   ├── mod.rs                  ★ Re-exports all planner functions
│   │   ├── context.rs              ★ IcebergPlannerContext
│   │   ├── op_delete.rs            ★ plan_delete()
│   │   ├── op_update.rs            ★ plan_update()
│   │   ├── op_merge.rs             ★ plan_merge()
│   │   ├── op_compact.rs           ★ plan_compact()
│   │   └── commit.rs               ★ assemble_iceberg_commit_plan()
│   ├── delete_file_actions_exec.rs ★ NEW: IcebergDeleteFileActionsExec
│   ├── compact_group_exec.rs       ★ NEW: IcebergCompactGroupExec
│   ├── update_transform_exec.rs    ★ NEW: IcebergUpdateTransformExec
│   ├── commit/
│   │   ├── mod.rs
│   │   ├── commit_exec.rs          ☆ MODIFY: add Delete/Replace operations
│   │   └── conflict_checker.rs     ☆ PORT from feat/iceberg-ops
│   └── mod.rs                      ☆ MODIFY: add new module declarations
├── catalog_support/
│   └── commit.rs                   ☆ MODIFY: add commit_helper from feat/iceberg-ops
├── operations/
│   └── snapshot.rs                 ☆ PORT: parent_manifest_entries, bootstrap, row_lineage
├── table_format.rs                 ☆ MODIFY: add alter_table variants, bucketing
├── table/
│   └── metadata_loader.rs          ☆ PORT: metadata_files_for_version, is_stale_metadata_file
├── utils/
│   └── metadata.rs                 ☆ PORT: metadata utilities from feat/iceberg-ops
└── physical/
    ├── mod.rs                       ★ MODIFY: add row_level_planner
    ├── row_level_planner.rs         ★ REWRITE: dispatch to planner module
    └── table_scan_planner.rs        ☆ MODIFY: add RowLevelWriteNode dispatch
```

**Legend:** ★ = new file, ☆ = modified file, PORT = bring from feat/iceberg-ops (correct), MODIFY = change logic

### 2.2 Files to DELETE (from feat/iceberg-ops)

| File | Lines | Reason |
|---|---|---|
| `delete_exec.rs` | 824 | Replaced by `planner/op_delete.rs` |
| `update_exec.rs` | 833 | Replaced by `planner/op_update.rs` |
| `merge_exec.rs` | 771 | Replaced by `planner/op_merge.rs` |
| `compact_exec.rs` | 649 | Replaced by `planner/op_compact.rs` |
| `catalog_support/commit_helper.rs` | 375 | Merged into `commit.rs` |
| **Total removed** | **3,452** | |

---

## 3. Implementation: Step by Step

### Phase 1: Infrastructure (Planner Context + Commit Helpers)

#### Step 1.1: Port Metadata Utilities

**File:** `crates/sail-iceberg/src/utils/metadata.rs`

From `feat/iceberg-ops`, port:
- `metadata_files_for_version(store, table_url, version) -> Result<Vec<String>>` — lists all metadata files for a given version (necessary for concurrent write detection)
- `is_stale_metadata_file(store, path, timestamp_threshold) -> Result<bool>` — filters files from old table instances
- `get_metadata_file_timestamp(store, path) -> Result<i64>` — HEAD request timestamp

**File:** `crates/sail-iceberg/src/utils/mod.rs`

Port:
- `WritePathMode` enum (`Absolute` / `Relative`)
- `join_table_uri()` helper

#### Step 1.2: Port Snapshot Producer Enhancements

**File:** `crates/sail-iceberg/src/operations/snapshot.rs`

**On 0.6.6, `SnapshotProducer` does NOT have `parent_manifest_entries`, `validate_added_data_files()`, or `deleted_data_files`. These must be PORTED from feat/iceberg-ops.**

Port from feat/iceberg-ops:
- `parent_manifest_entries: Option<Vec<ManifestFile>>` field on `SnapshotProducer` — needed for overwrite support (keeping untouched manifests)
- `validate_added_data_files()` — validates file_path, record_count, file_size_in_bytes before commit
- Operation-aware summary: detect `Operation::Overwrite` vs `Operation::Delete` vs `Operation::Append` in the `commit()` method
- Bootstrap manifest handling (`is_bootstrap` mode already exists on 0.6.6)

#### Step 1.3: Port Conflict Checker

**File:** `crates/sail-iceberg/src/physical_plan/commit/conflict_checker.rs` (new, port from feat/iceberg-ops)

Three-level check:
1. Schema compatibility (concurrent type/column changes → CONFLICT)
2. Partition conflicts (Append+Overwrite same partition → CONFLICT)
3. File conflicts (our add/remove ∩ winning remove → CONFLICT)

```rust
// Exact API:
pub struct ConflictChecker {
    txn_operation: &str,            // "append", "overwrite", "delete", "replace"
    txn_schema_id: i32,
    txn_added_files: HashSet<String>,
    txn_removed_files: HashSet<String>,
    txn_partition_predicates: Vec<(String, String)>,
    winning_operation: &str,
    winning_schema_id: i32,
    winning_added_files: HashSet<String>,
    winning_removed_files: HashSet<String>,
    winning_partition_predicates: Vec<(String, String)>,
}

impl ConflictChecker {
    pub fn check_conflicts(&self) -> Result<(), ConflictError>;
}
```

#### Step 1.4: Port Commit Helper

**File:** `crates/sail-iceberg/src/catalog_support/commit.rs` (modify existing)

Port from feat/iceberg-ops:
- `commit_iceberg_changes()` function — commit flow with catalog-aware mode resolution
- `IcebergCatalogCommitMode::CompatibilityCatalogCommit` handling
- Metadata JSON write + version-hint.text update with `PutMode::Create`

#### Step 1.5: Create PlannerContext

**File:** `crates/sail-iceberg/src/physical_plan/planner/context.rs` (new)

```rust
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::ObjectStore;
use url::Url;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use crate::options::gen_::IcebergWriteOptions;
use crate::table::Table;

/// The planner context provides all metadata needed during physical plan construction.
/// Mirrors Delta's `PlannerContext` in `sail-delta-lake/src/physical_plan/planner/context.rs`.
pub struct PlannerContext<'a> {
    /// The DataFusion session (for creating physical expressions, config access).
    session: &'a dyn Session,

    /// Resolved write options for this operation.
    options: IcebergWriteOptions,

    /// The resolved table URL (from path or catalog).
    table_url: Url,

    /// Resolved lakehouse context for catalog-managed tables.
    lakehouse_table: Option<LakehouseExecutionContext>,

    /// Cached table load result (lazy).
    table: tokio::sync::OnceCell<crate::table::Table>,
}

impl<'a> PlannerContext<'a> {
    pub fn new(
        session: &'a dyn Session,
        options: IcebergWriteOptions,
        table_url: Url,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Self {
        Self { session, options, table_url, lakehouse_table, table: tokio::sync::OnceCell::new() }
    }

    /// Returns the DataFusion session.
    pub fn session(&self) -> &dyn Session { self.session }

    /// Returns the resolved table URL.
    pub fn table_url(&self) -> &Url { &self.table_url }

    /// Returns the resolved options.
    pub fn options(&self) -> &IcebergWriteOptions { &self.options }

    /// Returns the lakehouse context, if any.
    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> { self.lakehouse_table.as_ref() }

    /// Gets the object store for this table.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        self.session.runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Opens the Iceberg table, loading metadata from storage.
    /// Uses OnceCell for lazy caching — only opens once per planner.
    pub async fn open_table(&self) -> Result<&crate::table::Table> {
        self.table.get_or_try_init(|| async {
            crate::table::Table::load(self.session, self.table_url.clone()).await
        }).await
    }
}
```

**Idiom verification:**
- ✓ Mirrors Delta's `PlannerContext` in `planner/context.rs:140-338`
- ✓ Uses `&'a dyn Session` lifetime-bound pattern (same as Delta)
- ✓ Uses `OnceCell` for lazy table loading (similar to Delta's `Option<Arc<DeltaSnapshot>>`)

---

### Phase 2: New Physical Operators

#### Step 2.1: IcebergDeleteFileActionsExec

**File:** `crates/sail-iceberg/src/physical_plan/delete_file_actions_exec.rs` (new)

Mirrors `DeltaRemoveActionsExec` (`remove_actions_exec.rs`).

```rust
#[derive(Debug)]
pub struct IcebergDeleteFileActionsExec {
    input: Arc<dyn ExecutionPlan>,       // IcebergManifestScanExec output (file metadata)
    table_url: Url,
    cache: Arc<PlanProperties>,
}

impl IcebergDeleteFileActionsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url) -> Self {
        // Schema: iceberg_action_schema() — same as IcebergWriterExec
        // Partitioning: UnknownPartitioning(input.partition_count())
        // Properties: EquivalenceProperties, EmissionType::Final, Boundedness::Bounded
    }
}

impl ExecutionPlan for IcebergDeleteFileActionsExec {
    fn name(&self) -> &'static str { "IcebergDeleteFileActionsExec" }

    fn properties(&self) -> &PlanProperties { &self.cache }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn Self>> {
        Ok(Arc::new(Self::new(children.one()?, self.table_url.clone())))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // Pattern: stream::once(async { ... }).flatten() via RecordBatchStreamAdapter
        //
        // 1. Execute input (manifest metadata stream)
        // 2. For each manifest entry batch, decode file_path + record_count
        // 3. Encode as Delete action batches using iceberg_action_schema()
        // 4. Return SendableRecordBatchStream
    }
}

impl DisplayAs for IcebergDeleteFileActionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IcebergDeleteFileActionsExec(table_path={})", self.table_url)
    }
}
```

**Idiom verification:**
- ✓ `children() → vec![&self.input]` (not `vec![]`)
- ✓ `cache: Arc<PlanProperties>` stored
- ✓ `name()` returns `&'static str`
- ✓ `one()` from `sail_common_datafusion::utils::items::ItemTaker`

#### Step 2.2: IcebergUpdateTransformExec

**File:** `crates/sail-iceberg/src/physical_plan/update_transform_exec.rs` (new)

A streaming per-batch transform that applies UPDATE SET assignments.

```rust
#[derive(Debug)]
pub struct IcebergUpdateTransformExec {
    input: Arc<dyn ExecutionPlan>,
    assignments: Vec<Assignment>,         // frozen SET (column_path, expression)
    condition: Option<Arc<dyn PhysicalExpr>>,
    table_schema: SchemaRef,              // output schema (same as input)
    cache: Arc<PlanProperties>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column_path: Vec<String>,
    pub expression: Arc<dyn PhysicalExpr>,
    pub target_type: DataType,
}
```

**Execute pattern:** `input.execute(partition, context)?.map(|batch| { ... })` via `RecordBatchStreamAdapter`. Mirrors `RelaxedTzCastExec`'s per-batch `.map()` pattern (`relaxed_tz_exec.rs:122-137`).

Per-batch logic:
1. Evaluate WHERE condition → BooleanArray mask (or all-true if no condition)
2. If ALL false → return batch unchanged
3. For each assignment: evaluate expression → cast → `arrow::compute::zip(mask, &new_values, &batch.column(col_idx))`
4. Rebuild RecordBatch with modified columns
5. Emit (all rows, not just matched rows — writer decides what to keep)

**Idiom verification:**
- ✓ Delegates `pipeline_behavior()` and `boundedness()` from input (like `RelaxedTzCastExec`)
- ✓ `maintains_input_order() → [true]`
- ✓ `.map()` pattern, not `try_filter_map`

#### Step 2.3: IcebergCompactGroupExec

**File:** `crates/sail-iceberg/src/physical_plan/compact_group_exec.rs` (new)

Takes manifest metadata stream, groups small files by partition, packs into batches.

```rust
#[derive(Debug)]
pub struct IcebergCompactGroupExec {
    input: Arc<dyn ExecutionPlan>,       // IcebergManifestScanExec output
    target_file_size: u64,              // default 128MiB
    table_url: Url,
    output_schema: SchemaRef,            // file_path, file_size, partition, group_id
    cache: Arc<PlanProperties>,
}
```

**Execute pattern:** `stream::try_unfold` state machine. Mirrors `IcebergScanByDataFilesExec`'s unfolding pattern (`scan_by_data_files_exec.rs:302-367`).

State machine:
1. Pull manifest metadata batches from input
2. Group by partition (string key)
3. Sort by size descending within each group
4. Pack small files (< 75% of target) into batches up to `target_file_size`
5. Emit batches with a `group_id` column
6. Large files (≥ 75% target) emitted individually with unique group_id

---

### Phase 3: Operation Planners

#### Step 3.1: DELETE Planner

**File:** `crates/sail-iceberg/src/physical_plan/planner/op_delete.rs` (new)

```rust
use std::sync::Arc;
use datafusion::physical_plan::{ExecutionPlan, filter::FilterExec, repartition::RepartitionExec};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_expr::expressions::NotExpr;
use datafusion::physical_plan::Partitioning;
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::context::PlannerContext;
use super::commit::assemble_iceberg_commit_plan;
use crate::datasource::type_converter::iceberg_schema_to_arrow;
use crate::physical_plan::{
    IcebergManifestScanExec, IcebergDiscoveryExec, IcebergScanByDataFilesExec,
    IcebergWriterExec, IcebergWriterExecOptions, IcebergCommitExec,
    IcebergDeleteFileActionsExec,
};
use crate::spec::Operation;

pub async fn plan_delete(
    ctx: &PlannerContext<'_>,
    condition: ExprWithSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    // ── Step 1: Open table ──
    let table = ctx.open_table().await?;
    let table_url = ctx.table_url().clone();
    // A DELETE/TRUNCATE against a created-but-never-written table (metadata only,
    // no current snapshot) is a successful 0-row no-op (`noop_delete_plan`),
    // matching Spark/Iceberg. The snapshot is only required to scan the survivors
    // of a conditional delete below.
    let snapshot = table.metadata().current_snapshot().cloned();
    if snapshot.is_none() {
        return noop_delete_plan(table_url, ctx.lakehouse_table().cloned());
    }
    let iceberg_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Table has no current schema".to_string())
    })?;
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(iceberg_schema)?);

    // ── Step 2: Build physical condition ──
    let df_schema = arrow_schema.clone().to_dfschema()?;
    let physical_condition = ctx.session()
        .create_physical_expr(condition.expr.clone(), &df_schema)?;

    // ── Step 3: TRUNCATE fast path ──
    // DELETE without WHERE → empty snapshot, no data files written.
    let is_truncate = condition.expr.is_false_or_null(); // meaning: no WHERE clause

    if is_truncate {
        let empty_scan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(arrow_schema.clone()));
        return assemble_iceberg_commit_plan(
            ctx, empty_scan, None, arrow_schema, Operation::Delete, None,
        ).await;
    }

    // ── Step 4: Writer branch — scan + filter survivors ──
    // Pipeline: IcebergManifestScanExec → IcebergDiscoveryExec → Repartition(RoundRobin)
    //   → IcebergScanByDataFilesExec → FilterExec(NOT condition)
    let manifest_scan = Arc::new(IcebergManifestScanExec::new(
        table_url.to_string(), snapshot.clone(),
    ));
    let discovery = Arc::new(IcebergDiscoveryExec::new(
        manifest_scan, table_url.to_string(), snapshot.snapshot_id(), false, /* partition_scan */
    )?);

    let target_parts = ctx.session().config().target_partitions().max(1);
    let repartitioned: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        discovery,
        Partitioning::RoundRobinBatch(target_parts),
    )?);

    let data_scan = Arc::new(IcebergScanByDataFilesExec::new(
        repartitioned, table_url.to_string(), arrow_schema.clone(),
    ));

    // ── Step 5: NOT(condition) → keep survivors ──
    let negated = Arc::new(NotExpr::new(physical_condition));
    let survivors: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(negated, data_scan)?);

    // ── Step 6: Remove branch — produce Delete actions ──
    // Independent pipeline: avoids shared-Arc starvation under distributed execution.
    let remove_manifest_scan = Arc::new(IcebergManifestScanExec::new(
        table_url.to_string(), snapshot.clone(),
    ));
    let remove_discovery = Arc::new(IcebergDiscoveryExec::new(
        remove_manifest_scan, table_url.to_string(), snapshot.snapshot_id(), false,
    )?);
    let remove_source: Arc<dyn ExecutionPlan> = Arc::new(IcebergDeleteFileActionsExec::new(
        remove_discovery, table_url.clone(),
    ));

    // ── Step 7: Commit ──
    assemble_iceberg_commit_plan(
        ctx, survivors, Some(remove_source), arrow_schema,
        Operation::Delete, Some(condition.source.clone()),
    ).await
}
```

**Why two independent manifest pipelines?** The writer branch produces data rows (surviving rows to be written as new Parquet files). The remove branch only needs file-path metadata. If they shared an `Arc`, the remove branch would be starved under distributed execution (the writer branch exhausts the stream). This is the exact same pattern Delta uses (`op_delete.rs:69-99`).

### UPDATE planner

**File:** `crates/sail-iceberg/src/physical_plan/planner/op_update.rs` (new)

Identical to DELETE except:
- Instead of `FilterExec(NOT condition)`, use `IcebergUpdateTransformExec` which blends old+new values
- ALL rows pass through to the writer (not just survivors)

```rust
pub async fn plan_update(
    ctx: &PlannerContext<'_>,
    condition: Option<ExprWithSource>,
    assignments: Vec<UpdateAssignment>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let table_url = ctx.table_url().clone();
    let snapshot = table.metadata().current_snapshot().cloned().ok_or_else(|| ...)?;
    let iceberg_schema = table.metadata().current_schema().ok_or_else(|| ...)?;
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(iceberg_schema)?);

    // ... manifest scan, discovery, repartition, data scan (same as DELETE) ...

    // Apply UPDATE transform (blends old+new by WHERE mask):
    let updated: Arc<dyn ExecutionPlan> = Arc::new(IcebergUpdateTransformExec::new(
        data_scan,
        convert_assignments(assignments),
        condition.map(|c| ctx.session()
            .create_physical_expr(c.expr, &arrow_schema.clone().to_dfschema()?)).transpose()?,
        arrow_schema.clone(),
    ));

    // ... remove source, commit assembly (same as DELETE) ...
}
```

#### Step 3.3: MERGE Planner

**File:** `crates/sail-iceberg/src/physical_plan/planner/op_merge.rs` (new)

```rust
pub async fn plan_merge(
    ctx: &PlannerContext<'_>,
    write_plan: Arc<dyn ExecutionPlan>,          // from expand_merge()
    touched_files_plan: Arc<dyn ExecutionPlan>,   // from expand_merge()
    is_insert_only: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let table_url = ctx.table_url().clone();
    let snapshot = table.metadata().current_snapshot().cloned().ok_or_else(|| {
        DataFusionError::Plan("Cannot merge into empty Iceberg table".to_string())
    })?;
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(
        table.metadata().current_schema().ok_or_else(|| {
            DataFusionError::Plan("Table has no current schema".to_string())
        })?,
    )?);

    // ── Targeted Rewrite ──
    // Insert rows: path IS NULL in the write_plan
    // Touched rows: INNER JOIN write_plan with touched_files_plan on __sail_file_path
    let (insert_rows, touched_rows) = build_targeted_writer_input(
        write_plan,
        touched_files_plan,
    )?;
    let writer_input: Arc<dyn ExecutionPlan> = if is_insert_only {
        insert_rows
    } else {
        Arc::new(UnionExec::try_new(vec![insert_rows, touched_rows])?)
    };

    // ── Strip internal MERGE columns ──
    let writer_input = strip_merge_internal_columns(writer_input, &arrow_schema)?;

    // ── Build remove source ──
    let remove_source = if is_insert_only {
        None // Insert-only has no files to remove
    } else {
        let manifest_scan = Arc::new(IcebergManifestScanExec::new(
            table_url.to_string(), snapshot.clone(),
        ));
        let discovery = Arc::new(IcebergDiscoveryExec::new(
            manifest_scan, table_url.to_string(), snapshot.snapshot_id(), false,
        )?);
        Some(Arc::new(IcebergDeleteFileActionsExec::new(
            discovery, table_url.clone(),
        )))
    };

    // ── Commit ──
    assemble_iceberg_commit_plan(
        ctx, writer_input, remove_source, arrow_schema,
        Operation::Overwrite, None,
    ).await
}

/// Build the targeted writer input from pre-expanded merge plans.
///
/// Follows Delta's `build_targeted_writer_input()` pattern
/// (`sail-delta-lake/src/physical_plan/planner/op_merge.rs:376-446`):
///
/// - Insert rows: FilterExec(__sail_file_path IS NULL)
/// - Touched rows: HashJoinExec(Inner, CollectLeft, on __sail_file_path)
fn build_targeted_writer_input(
    write_plan: Arc<dyn ExecutionPlan>,
    touched_files_plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    use datafusion::physical_plan::execution_plan::reset_plan_states;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::expressions::{IsNullExpr, IsNotNullExpr};
    use datafusion::physical_plan::projection::ProjectionExec;
    use sail_common_datafusion::datasource::MERGE_FILE_COLUMN;
    use datafusion_common::{JoinType, NullEquality};
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion_physical_expr::expressions::Column;

    // Clone both plans since MERGE branches this subtree
    let write_plan = reset_plan_states(write_plan)?;
    let touched_files_plan = reset_plan_states(touched_files_plan)?;

    let file_path_idx = write_plan.schema()
        .index_of(MERGE_FILE_COLUMN)
        .map_err(|_| DataFusionError::Plan(
            "merge write_plan missing __sail_file_path column".into()
        ))?;

    // Insert rows: file_path IS NULL
    let is_null = Arc::new(IsNullExpr::new(Arc::new(Column::new(MERGE_FILE_COLUMN, file_path_idx))));
    let insert_rows = Arc::new(FilterExec::try_new(is_null, Arc::clone(&write_plan))?);

    // Touched rows: file_path IS NOT NULL, joined with touched_files_plan on file_path
    let is_not_null = Arc::new(IsNotNullExpr::new(Arc::new(Column::new(MERGE_FILE_COLUMN, file_path_idx))));
    let non_insert = Arc::new(FilterExec::try_new(is_not_null, write_plan)?);

    let touch_idx = touched_files_plan.schema().index_of(MERGE_FILE_COLUMN)?;
    let join = Arc::new(HashJoinExec::try_new(
        touched_files_plan,
        non_insert,
        vec![(
            Arc::new(Column::new(MERGE_FILE_COLUMN, touch_idx)),
            Arc::new(Column::new(MERGE_FILE_COLUMN, file_path_idx)),
        )],
        None, &JoinType::Inner, None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    // Keep only right-side columns (merged row data)
    let left_cols = join.schema().fields().len() - file_path_idx - 1; // rough; need actual left count
    // Actually: left side is touched_files (schema size = N), right is non_insert
    let left_width = touched_files_plan.schema().fields().len();
    let projections: Vec<_> = join.schema().fields().iter().enumerate()
        .skip(left_width)
        .map(|(i, f)| (Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>, f.name().clone()))
        .collect();
    let touched_rows = Arc::new(ProjectionExec::try_new(projections, join)?);

    Ok((insert_rows, touched_rows))
}

/// Strip MERGE_FILE_COLUMN and other internal columns before passing to writer.
fn strip_merge_internal_columns(
    input: Arc<dyn ExecutionPlan>,
    table_schema: &Schema,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion_physical_expr::expressions::Column;
    use sail_common_datafusion::datasource::MERGE_FILE_COLUMN;

    let input_schema = input.schema();
    // Only project columns that exist in the table schema (strip internal merge cols)
    let projections: Vec<_> = table_schema.fields().iter().filter_map(|field| {
        input_schema.index_of(field.name()).ok().map(|idx| {
            (Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>, field.name().clone())
        })
    }).collect();
    Ok(Arc::new(ProjectionExec::try_new(projections, input)?))
}
```


**CRITICAL NOTE:** `expand_merge()` from `sail-logical-plan/src/merge.rs:428` returns a `MergeExpansion` containing `write_plan`, `touched_files_plan`, and `deletion_vector_plan` as `LogicalPlan`. These must be converted to physical `ExecutionPlan` by DataFusion's `PhysicalPlanner` **before** they reach `plan_merge()`. This conversion happens in `row_level_planner.rs` (see Step 4.1).

### 3.4 COMPACT Planner

**File:** `crates/sail-iceberg/src/physical_plan/planner/op_compact.rs` (new)

```rust
pub async fn plan_compact(
    ctx: &PlannerContext<'_>,
    target_file_size: u64,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = ctx.open_table().await?;
    let table_url = ctx.table_url().clone();
    let snapshot = table.metadata().current_snapshot().cloned().ok_or_else(|| {
        DataFusionError::Plan("Cannot compact empty Iceberg table".to_string())
    })?;
    let arrow_schema = Arc::new(iceberg_schema_to_arrow(
        table.metadata().current_schema().ok_or_else(|| {
            DataFusionError::Plan("Table has no current schema".to_string())
        })?,
    )?);

    // Scan manifests → group small files → scan data → write merged files
    let manifest_scan = Arc::new(IcebergManifestScanExec::new(
        table_url.to_string(), snapshot.clone(),
    ));
    let discovery = Arc::new(IcebergDiscoveryExec::new(
        manifest_scan, table_url.to_string(), snapshot.snapshot_id(), false,
    )?);

    // Group small files by partition, pack into batches
    let grouped = Arc::new(IcebergCompactGroupExec::new(
        discovery, target_file_size, table_url.clone(),
    ));

    // Repartition by group_id for parallel execution
    let target_parts = ctx.session().config().target_partitions().max(1);
    let group_idx = grouped.schema().index_of("group_id")?;
    let repartitioned = Arc::new(RepartitionExec::try_new(
        grouped,
        Partitioning::Hash(vec![Arc::new(Column::new("group_id", group_idx))], target_parts),
    )?);

    // Scan actual Parquet files within each group
    let data_scan = Arc::new(IcebergScanByDataFilesExec::new(
        repartitioned, table_url.to_string(), arrow_schema.clone(),
    ));

    // Remove source: ALL original files
    let remove_manifest_scan = Arc::new(IcebergManifestScanExec::new(
        table_url.to_string(), snapshot.clone(),
    ));
    let remove_discovery = Arc::new(IcebergDiscoveryExec::new(
        remove_manifest_scan, table_url.to_string(), snapshot.snapshot_id(), false,
    )?);
    let remove_source = Arc::new(IcebergDeleteFileActionsExec::new(
        remove_discovery, table_url.clone(),
    ));

    assemble_iceberg_commit_plan(
        ctx, data_scan, Some(remove_source), arrow_schema,
        Operation::Replace, None,
    ).await
}
```

#### Step 3.5: Commit Assembly

**File:** `crates/sail-iceberg/src/physical_plan/planner/commit.rs` (new)

Mirrors Delta's `assemble_commit_plan()` (`planner/commit.rs:56-104`).

```rust
/// Assemble the standard commit tail for Iceberg row-level operations.
///
/// Pipeline shape:
///
/// ```text
/// IcebergWriterExec(writer_input)                 ← writes Parquet, emits Add action batches
///   ∪ IcebergDeleteFileActionsExec(remove_source)  ← [optional] produces Delete action batches
///     → CoalescePartitionsExec                    ← gather to 1 partition
///       → IcebergCommitExec                       ← atomic commit
/// ```
pub async fn assemble_iceberg_commit_plan(
    ctx: &PlannerContext<'_>,
    writer_input: Arc<dyn ExecutionPlan>,
    remove_source: Option<Arc<dyn ExecutionPlan>>,
    output_schema: SchemaRef,
    operation: Operation,
    _predicate_source: Option<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url = ctx.table_url().clone();

    // Partition columns come from table metadata
    let table = ctx.open_table().await?;
    let partition_columns = {
        let metadata = table.metadata();
        let default_spec = metadata.default_partition_spec()
            .ok_or_else(|| DataFusionError::Plan("No default partition spec".to_string()))?;
        default_spec.fields().iter().map(|f| {
            CatalogPartitionField {
                source_column: f.name.clone(),
                transform: Some(crate::utils::partition_transform::iceberg_transform_from_partition_field(f)),
                field_id: Some(f.field_id),
            }
        }).collect::<Vec<_>>()
    };

    let options = IcebergWriterExecOptions::from(ctx.options().clone());

    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
        writer_input,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,  // Row-level ops always append files
        true,                       // table_exists
        options,
        Some(output_schema.clone()),
    ));

    let commit_input: Arc<dyn ExecutionPlan> = if let Some(remove_src) = remove_source {
        Arc::new(UnionExec::try_new(vec![writer, remove_src])?)
    } else {
        writer
    };

    Ok(Arc::new(IcebergCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        table_url,
        ctx.lakehouse_table().cloned(),
    )))
}
```

---

### Phase 4: Wire Everything Together

#### Step 4.1: Update `row_level_planner.rs`

**File:** `crates/sail-iceberg/src/physical/row_level_planner.rs` (new file)

```rust
use std::sync::Arc;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::RowLevelCommand;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::options::gen_::IcebergWriteOptions;
use crate::physical_plan::planner::{PlannerContext, plan_delete, plan_update, plan_merge};
use crate::table_format::IcebergTableFormat;

pub async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    // 1. Resolve options from OptionLayers
    let options = IcebergWriteOptions::resolve(session_state, node.target_options().to_vec())?;

    // 2. Parse table URL
    let table_url = IcebergTableFormat::parse_table_url(
        vec![node.target_location().to_string()],
    ).await?;

    // 3. Create planner context (once per operation)
    let ctx = PlannerContext::new(
        session_state,
        options,
        table_url,
        node.target_lakehouse_table().cloned(),
    );

    // 4. Dispatch by command type
    match node.command() {
        RowLevelCommand::Delete => {
            let condition = node.condition().cloned().ok_or_else(|| {
                DataFusionError::Internal("DELETE node must have a condition".into())
            })?;
            plan_delete(&ctx, condition).await
        }
        RowLevelCommand::Update => {
            let condition = node.condition().cloned();
            // ATTENTION: The RowLevelWriteNode stores assignments for UPDATE.
            // Access them via the node's assignments field (or add an accessor if missing).
            let assignments = /* extract from node */;
            plan_update(&ctx, condition, assignments).await
        }
        RowLevelCommand::Merge => {
            // Convert pre-expanded logical plans to physical plans via DF planner
            let write_plan = node.write_plan().ok_or_else(|| {
                DataFusionError::Internal("MERGE node must have write_plan".into())
            })?;
            let physical_write = planner.create_physical_plan(write_plan, session_state).await?;

            let touched_files_plan = node.touched_files_plan();
            let physical_touched = if let Some(plan) = touched_files_plan {
                Some(planner.create_physical_plan(plan, session_state).await?)
            } else {
                return plan_merge(&ctx, physical_write, /* touched */... ).await;
                // Insert-only case
            };

            let is_insert_only = node.merge_options()
                .map(|opts| {
                    opts.matched_clauses.is_empty()
                        && opts.not_matched_by_source_clauses.is_empty()
                })
                .unwrap_or(false);

            plan_merge(&ctx, physical_write, physical_touched.unwrap(), is_insert_only).await
        }
    }
}
```

**CRITICAL:** This requires adding `assignments` accessor to `RowLevelWriteNode` if it doesn't already have one. Check `sail-logical-plan/src/merge.rs`.

#### Step 4.2: Update `table_scan_planner.rs`

**File:** `crates/sail-iceberg/src/physical/table_scan_planner.rs` (modify)

Add `RowLevelWriteNode` dispatch (this ALREADY exists in feat/iceberg-ops, just need to change the function it calls):

```rust
impl ExtensionPlanner for IcebergPhysicalPlanner {
    async fn plan_extension(&self, planner, node, logical_inputs, physical_inputs, session_state) {
        // 1. Format-specific write node → plan_iceberg_write()
        if let Some(write_node) = node.as_any().downcast_ref::<IcebergWriteNode>() {
            return plan_iceberg_write(session_state, logical_input, physical_input, write_node).await;
        }

        // 2. RowLevelWriteNode (format matched by target_format field)
        if let Some(rl_node) = node.as_any().downcast_ref::<RowLevelWriteNode>()
            && rl_node.target_format().eq_ignore_ascii_case("iceberg")
        {
            // ★ CHANGED: Delegate to row_level_planner, not monolithic executor
            return plan_iceberg_row_level_write(session_state, rl_node).await;
        }

        Ok(None)
    }
}
```

#### Step 4.3: Update `table_format.rs`

**File:** `crates/sail-iceberg/src/table_format.rs` (modify)

**A. Override `create_deleter()`:**

```rust
async fn create_deleter(&self, _ctx: &dyn Session, info: DeleteInfo) -> Result<LogicalPlan> {
    let DeleteInfo { table_name, path, condition, lakehouse_table, options } = info;
    let write_node = RowLevelWriteNode::new_delete(
        Arc::new(EmptyRelation { produce_one_row: false, schema: Arc::new(DFSchema::empty()) }),
        Arc::new(DFSchema::empty()),
        condition,
        self.name().to_string(),
        path,
        table_name,
        options,
        lakehouse_table,
    );
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(write_node) }))
}
```

**B. Override `create_merger()`:**

```rust
async fn create_merger(&self, _ctx: &dyn Session, info: MergeInfo) -> Result<LogicalPlan> {
    let raw_target = Arc::clone(&info.target);
    let raw_source = Arc::clone(&info.source);
    let raw_input_schema = info.input_schema.clone();
    let expansion = expand_merge(info, MERGE_FILE_COLUMN, None)?;
    let write_node = RowLevelWriteNode::new_merge(
        raw_target,
        raw_source,
        raw_input_schema,
        Arc::new(expansion.write_plan),
        Arc::new(expansion.touched_files_plan),
        expansion.deletion_vector_plan.map(Arc::new),
        expansion.options,
        expansion.output_schema,
    );
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(write_node) }))
}
```

**C. Add `create_updater()` — REQUIRES TABLEFORMAT TRAIT CHANGE:**

The `TableFormat` trait in `sail-common-datafusion/src/datasource.rs` must have `create_updater` added. On 0.6.6 this method does NOT exist. Also `UpdateInfo` struct must be added.

```rust
// ── In sail-common-datafusion/src/datasource.rs ──

/// Input to TableFormat::create_updater.
pub struct UpdateInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub condition: Option<ExprWithSource>,
    pub assignments: Vec<UpdateAssignment>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub options: Vec<OptionLayer>,
}

/// An UPDATE SET assignment (column_path = expression).
pub struct UpdateAssignment {
    pub column_path: Vec<String>,
    pub expression: Expr,
}

// ── In the TableFormat trait ──
async fn create_updater(&self, ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan> {
    let _ = (ctx, info);
    not_impl_err!("UPDATE is not yet implemented for {} format", self.name())
}

// ── In sail-iceberg/src/table_format.rs ──
async fn create_updater(&self, _ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan> {
    let UpdateInfo { table_name, path, condition, assignments, lakehouse_table, options } = info;
    let write_node = RowLevelWriteNode::new_update(
        Arc::new(EmptyRelation { produce_one_row: false, schema: Arc::new(DFSchema::empty()) }),
        Arc::new(DFSchema::empty()),
        assignments,  // NOTE: RowLevelWriteNode::new_update signature may need adjustment
        condition,
        self.name().to_string(),
        path,
        table_name,
        options,
        lakehouse_table,
    );
    Ok(LogicalPlan::Extension(Extension { node: Arc::new(write_node) }))
}
```

**C. Port ALTER TABLE variants from feat/iceberg-ops:**

**CRITICAL:** The function `retry_metadata_commit()` does NOT exist on 0.6.6. It must be ported from feat/iceberg-ops into `table_format.rs`. Each ported `alter_table_*()` method uses a retry pattern similar to the existing `alter_table_properties()`:

1. Load current metadata JSON via `load_metadata_file_bytes()`
2. Parse → `TableMetadata::from_json()`
3. Apply mutation closure (add/drop fields, build new schema version)
4. Serialize new metadata JSON
5. Write with `PutMode::Create` (CAS — detects concurrent writers)
6. Check `metadata_files_for_version()` (ported from feat/iceberg-ops utils)
7. Update `version-hint.text`
8. Max 3 retries with jitter

```rust
async fn alter_table(&self, runtime_env, path, operation, lakehouse_table) -> Result<()> {
    match operation {
        SetTableProperties { changes, if_exists } → alter_table_properties(runtime_env, path, changes, if_exists)
        AddColumns { columns } → alter_table_add_columns(runtime_env, path, columns)
        DropColumns { names, if_exists } → alter_table_drop_columns(runtime_env, path, names, if_exists)
        AlterColumnComment { column_path, comment } → alter_table_column_comment(runtime_env, path, column_path, comment)
        AlterColumnNullability { column_path, nullable } → alter_table_column_nullability(runtime_env, path, column_path, nullable)
        AlterColumnPosition { column_path, position } → alter_table_column_position(runtime_env, path, column_path, &position)
        RenameTable → Ok(())
        _ → not_impl_err!()
    }
}
```

Each `alter_table_*()` method must be ported from feat/iceberg-ops.

**D. Add bucketing support:**

```rust
async fn create_writer(&self, ctx, info: SinkInfo) -> Result<LogicalPlan> {
    let partition_by = match bucket_by {
        Some(bucket_by) => {
            let mut fields = partition_by;
            fields.extend(partition_fields_from_bucket_by(bucket_by));
            fields
        }
        None => partition_by,
    };
    // ... rest unchanged ...
}

fn partition_fields_from_bucket_by(bucket_by: BucketBy) -> Vec<CatalogPartitionField> {
    bucket_by.columns.iter().map(|col| {
        CatalogPartitionField {
            source_column: col.clone(),
            transform: Some(PartitionTransform::Bucket(bucket_by.num_buckets as u32)),
            field_id: None, // assigned during schema building
        }
    }).collect()
}
```

#### Step 4.4: Update `commit_exec.rs` — Add Delete/Replace Operations

**File:** `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` (modify)

Add `Operation::Delete` and `Operation::Replace` arms to the match at line 803:

```rust
let action_commit = match commit_info.operation {
    crate::spec::Operation::Append => { /* ... existing ... */ }
    crate::spec::Operation::Overwrite => { /* ... existing ... */ }
    // ★ NEW:
    crate::spec::Operation::Delete => {
        if commit_info.data_files.is_empty() {
            // TRUNCATE: empty snapshot, no data files
            let producer = crate::operations::SnapshotProducer::new(
                &tx, vec![], Some(store_ctx.clone()), Some(manifest_meta),
            );
            struct TruncateOperation;
            impl SnapshotProduceOperation for TruncateOperation {
                fn operation(&self) -> &'static str { "delete" }
            }
            producer.commit(TruncateOperation).await
                .map_err(DataFusionError::Execution)?
        } else {
            // Regular DELETE: use SnapshotProducer with new files
            let producer = crate::operations::SnapshotProducer::new(
                &tx, commit_info.data_files.clone(),
                Some(store_ctx.clone()), Some(manifest_meta),
            );
            struct DeleteOperation;
            impl SnapshotProduceOperation for DeleteOperation {
                fn operation(&self) -> &'static str { "delete" }
            }
            producer.commit(DeleteOperation).await
                .map_err(DataFusionError::Execution)?
        }
    }
    crate::spec::Operation::Replace => {
        // COMPACT: replaces some files with merged files
        let producer = crate::operations::SnapshotProducer::new(
            &tx, commit_info.data_files.clone(),
            Some(store_ctx.clone()), Some(manifest_meta),
        );
        struct ReplaceOperation;
        impl SnapshotProduceOperation for ReplaceOperation {
            fn operation(&self) -> &'static str { "replace" }
        }
        producer.commit(ReplaceOperation).await
            .map_err(DataFusionError::Execution)?
    }
};
```

#### Step 4.5: Update `mod.rs` — Export New Modules

**File:** `crates/sail-iceberg/src/physical_plan/mod.rs` (modify)

```rust
pub mod action_schema;
pub mod commit;
pub mod delete_apply_exec;
pub mod delete_file_actions_exec;   // ★ NEW
pub mod discovery_exec;
pub mod manifest_scan_exec;
pub mod plan_builder;
pub mod planner;                     // ★ NEW (re-exports plan_delete, plan_update, etc.)
pub mod scan_by_data_files_exec;
pub mod update_transform_exec;       // ★ NEW
pub mod compact_group_exec;          // ★ NEW
mod writer_exec;
mod writer_options;

pub use commit::commit_exec::IcebergCommitExec;
pub use delete_apply_exec::IcebergDeleteApplyExec;
pub use delete_file_actions_exec::IcebergDeleteFileActionsExec;  // ★
pub use discovery_exec::IcebergDiscoveryExec;
pub use manifest_scan_exec::IcebergManifestScanExec;
pub use plan_builder::{IcebergPlanBuilder, IcebergTableConfig};
pub use scan_by_data_files_exec::IcebergScanByDataFilesExec;
pub use update_transform_exec::IcebergUpdateTransformExec;       // ★
pub use compact_group_exec::IcebergCompactGroupExec;              // ★
pub use writer_exec::IcebergWriterExec;
pub use writer_options::IcebergWriterExecOptions;
```

---

### Phase 5: Predicate Overwrite + Bucketing

#### Step 5.1: Predicate Overwrite

Already partially handled in `plan_iceberg_write()` (Iceberg `table_format.rs:535-548`).
On `SinkMode::OverwriteIf { condition }`:
1. Validate that all predicate columns are partition columns (v1 constraint)
2. Extract `{col: val}` pairs via `extract_partition_predicate_from_expr()`
3. Pass to `IcebergWriterExecOptions.overwrite_predicate`

In `IcebergCommitExec`:
- `filter_parent_manifest_entries()` filters out manifests whose partition summaries overlap with the predicate
- Non-overlapping (untouched) manifest entries are kept; overlapping entries are replaced

#### Step 5.2: Bucketing via CLUSTERED BY

In `table_format.rs`, convert `BucketBy` to `Vec<CatalogPartitionField>` where each column gets `PartitionTransform::Bucket(n)`. These become part of the partition spec.

---

### Phase 6: ALTER TABLE Completion

#### Step 6.1: Column Defaults

**Data model** (`spec/types/mod.rs:792-794`):
- `NestedField::initial_default: Option<Literal>` — V3 initial default
- `NestedField::write_default: Option<Literal>` — V3 write default

**Type converter** (`datasource/type_converter.rs:99-113, 147-177`):
- Arrow→Iceberg: reads `ICEBERG_FIELD_INITIAL_DEFAULT`, `ICEBERG_FIELD_WRITE_DEFAULT` from field metadata
- Iceberg→Arrow: writes defaults into field metadata

**Implementation plan:**
1. Add `AlterColumnDefault` match arm in `table_format.alter_table()`:
   ```rust
   AlterColumnDefault { column_path, default } => {
       // Use retry_metadata_commit() to modify the field's initial_default/write_default
   }
   ```
2. In `schema_evolution.rs`: add `set_column_default()` method on `SchemaEvolver`
3. In `table_format.rs`: add `alter_table_column_default()` method using `retry_metadata_commit()`

#### Step 6.2: Branch/Tag Creation

New feature across 8 files as documented in Section 12 of `sail-implementation-patterns.md` and Section C8.

**Iceberg storage implementation** (in `table_format.rs`):
```rust
CreateBranch { ref_name, snapshot_id } -> {
    // 1. Load current metadata
    // 2. Insert SnapshotReference into refs HashMap
    // 3. Write metadata → update version-hint
    // Uses retry_metadata_commit()
}
```

---

## 4. Schema Evolution Integration

Row-level operations MUST integrate schema evolution because:
- MERGE can add new columns from the source (`with_schema_evolution: true`)
- UPDATE might change column types (unsupported currently, but framework must accommodate)
- ALTER TABLE ADD COLUMNS runs concurrent with writes

**Integration points:**

1. **In `IcebergWriterExec`** (already implemented): `SchemaEvolver::evolve()` is called to merge/write schemas
2. **In DELETE/UPDATE planners**: Before constructing the filter expression, check if the incoming schema matches the table schema. Use `SchemaEvolver` to resolve any differences.
3. **In MERGE planner**: The pre-expanded `write_plan` already has the evolved schema from `expand_merge()`. Just pass it through.

---

## 5. Predicate Pushdown + Statistics

**Critical:** After any row-level operation, written DataFiles MUST carry statistics (`lower_bounds`, `upper_bounds`, `value_counts`, `null_value_counts`).

**Current 0.6.6 state:** `data_file_writer.rs:124` has `lower_bounds`/`upper_bounds` as empty HashMaps with a comment "Do not attempt to parse typed bounds here."

**Required change:** Extract column statistics from `ArrowWriter`'s `ParquetMetaData` after closing the writer:

```rust
// In data_file_writer.rs, after writer.close():
fn extract_statistics(parquet_metadata: &ParquetMetaData, iceberg_schema: &Schema)
    -> (HashMap<i32, Vec<u8>>, HashMap<i32, Vec<u8>>, HashMap<i32, i64>);

// For each column:
//   lower_bounds[field_id] = encode(PrimitiveLiteral::from_parquet_stat(col.min))
//   upper_bounds[field_id] = encode(PrimitiveLiteral::from_parquet_stat(col.max))
//   nan_value_counts[field_id] = col.null_count
```

This enables metrics-based pruning on files produced by row-level operations.

---

## 6. Error Handling Conventions

| Error Type | When to Use | Reference |
|---|---|---|
| `DataFusionError::Plan(msg)` | Invalid operation at plan time (missing table, wrong schema) | `op_delete.rs:38` |
| `DataFusionError::NotImplemented(msg)` | Unsupported feature | `commit_exec.rs:838` |
| `DataFusionError::Internal(msg)` | Internal invariant violation | `row_level.rs:99` |
| `DataFusionError::External(Box::new(e))` | Non-DataFusion error (object store, catalog) | `context.rs:298` |
| `DataFusionError::Execution(msg)` | Commit-level failure | `commit_exec.rs:817` |

---

## 7. Implementation Order (Recommended)

| Phase | Steps | Files | New Lines | Risk | Depends On |
|---|---|---|---|---|---|
| **1. Infrastructure** | 1.1–1.5 | 5 files | ~600 | Low | Nothing |
| **2. New Operators** | 2.1–2.3 | 3 files | ~450 | Low | Phase 1 |
| **3. DELETE Planner** | 3.1, 3.5 | 2 files | ~280 | Low | Phase 2 |
| **4. Commit Ops** | 4.4 | 1 file | ~80 | Low | Phase 3 |
| **5. Wiring** | 4.1–4.3, 4.5 | 4 files | ~80 | Low | Phase 3 |
| **6. UPDATE Planner** | 3.2 | 1 file | ~220 | Medium | Phase 5 |
| **7. MERGE Planner** | 3.3 | 1 file | ~350 | High | Phase 5 |
| **8. COMPACT Planner** | 3.4 | 1 file | ~180 | Medium | Phase 5 |
| **9. Predicate Overwrite** | Step 5.1 | 1 file mod | ~40 | Low | Phase 5 |
| **10. Bucketing** | Step 5.2 | 1 file mod | ~30 | Low | Phase 5 |
| **11. Statistics** | Section 5 | 1 file mod | ~100 | Low | Phase 3 |
| **12. ALTER TABLE** | Step 6.1–6.2 | 8 files | ~400 | Low | Phase 1 |
| **TOTAL** | | 22 files | ~2,810 new | | |

**Note:** After Phase 5, DELETE is fully functional. After Phase 6, UPDATE is functional. After Phase 7, MERGE is functional. This incremental approach lets you ship DELETE immediately while working on the more complex operations.

---

## 8. Complete Idiom Checklist for Implementation

For every new file and every modification, verify against this checklist:

### ExecutionPlan nodes:
- [ ] `name() → &'static str` (never `&str`)
- [ ] `properties() → &PlanProperties` (from stored `cache: Arc<PlanProperties>`)
- [ ] `children() → Vec<&Arc<dyn ExecutionPlan>>` (at least 1, never empty for non-leaf nodes)
- [ ] `execute() → Result<SendableRecordBatchStream>` using DataFusion adapter patterns
- [ ] `EmissionType::Final` for write/action nodes, delegates for pure-transform nodes
- [ ] `Boundedness::Bounded` for all (no unbounded streaming from Iceberg)
- [ ] `Partitioning::UnknownPartitioning(partition_count.max(1))`
- [ ] No direct object_store access in `execute()` (use child pipeline)
- [ ] `with_new_children()` uses `children.one()?` from `ItemTaker`
- [ ] `DisplayAs` implemented for all three `DisplayFormatType` variants

### Planner functions:
- [ ] Takes `&PlannerContext` (not raw `SessionState`)
- [ ] Uses `ctx.open_table().await?` for table access
- [ ] Creates `FilterExec`, `RepartitionExec`, `UnionExec` from DataFusion (never custom scan)
- [ ] Uses `assemble_iceberg_commit_plan()` for commit tail
- [ ] Error propagation uses `DataFusionError::External(Box::new(e))`

### Writer:
- [ ] Emits `iceberg_action_schema()` batches
- [ ] DataFile statistics populated from Parquet metadata
- [ ] Catalog commit mode resolved via `commit.rs`

### ALTER TABLE:
- [ ] Uses retry loop with `PutMode::Create` CAS on metadata writes
- [ ] Updates `version-hint.text` after each mutation
- [ ] Max 3 retries with jitter
- [ ] Conflict detection via `metadata_files_for_version`

### General:
- [ ] Uses `Url` not `String` for table paths in struct fields and constructors
- [ ] Options resolved via `IcebergWriteOptions::resolve()` from OptionLayer chain
- [ ] Lakehouse context threaded through all planner stages
- [ ] No `concat_batches()` on entire tables
- [ ] No `load_manifest_list()` / `load_manifest_entries()` helper duplication
- [ ] Schema conversion uses `iceberg_schema_to_arrow(&schema) -> Result<ArrowSchema>`
- [ ] Table access uses `table.metadata().current_schema()` and `table.metadata().current_snapshot()`
- [ ] Table URL parsing uses `IcebergTableFormat::parse_table_url(vec![path]).await?`
- [ ] `RowLevelWriteNode` accessors: `target_options() -> &[OptionLayer]`, `target_lakehouse_table() -> Option<&LakehouseExecutionContext>`, `command() -> RowLevelCommand`, `condition() -> Option<&ExprWithSource>`, `write_plan() -> Option<&Arc<LogicalPlan>>`, `touched_files_plan() -> Option<&Arc<LogicalPlan>>`
- [ ] `MERGE_FILE_COLUMN = "__sail_file_path"` from `sail_common_datafusion::datasource`

---

## 9. Gaps Between 0.6.6 and This Plan

This section lists everything that does NOT exist on 0.6.6 but is required by this plan. These must be implemented as part of Phase 1 or before the dependent phases.

### 9.1: `TableFormat` trait missing methods

| Method | Location | What to do |
|---|---|---|
| `create_updater()` | `sail-common-datafusion/src/datasource.rs` | Add to trait with default `not_impl_err!()` |
| `create_deleter()` | `sail-common-datafusion/src/datasource.rs` | EXISTS as default, but Iceberg doesn't override it |

### 9.2: Missing structs

| Struct | Location | What to do |
|---|---|---|
| `UpdateInfo` | `sail-common-datafusion/src/datasource.rs` | Add struct with `table_name`, `path`, `condition`, `assignments`, `lakehouse_table`, `options` |
| `UpdateAssignment` | `sail-common-datafusion/src/datasource.rs` | Add struct with `column_path: Vec<String>`, `expression: Expr` |

### 9.3: Missing on `SnapshotProducer` (0.6.6)

| Feature | What to do |
|---|---|
| `parent_manifest_entries: Option<Vec<ManifestFile>>` | Port from feat/iceberg-ops |
| `validate_added_data_files()` | Port from feat/iceberg-ops |
| Operation-aware summary | Port from feat/iceberg-ops |

### 9.4: Missing on `IcebergCommitExec` (0.6.6)

| Feature | What to do |
|---|---|
| `Operation::Delete` handling | Add match arm (uses `SnapshotProducer` with empty or new data files) |
| `Operation::Replace` handling | Add match arm (for COMPACT) |
| Conflict checker | Port `conflict_checker.rs` from feat/iceberg-ops |

### 9.5: Missing ALTER TABLE operations (0.6.6)

| Operation | Status | What to do |
|---|---|---|
| `AddColumns` | not implemented | Port `alter_table_add_columns()` from feat/iceberg-ops |
| `DropColumns` | not implemented | Port from feat/iceberg-ops |
| `AlterColumnComment` | not implemented | Port from feat/iceberg-ops |
| `AlterColumnNullability` | not implemented | Port from feat/iceberg-ops |
| `AlterColumnPosition` | not implemented | Port from feat/iceberg-ops |
| `AlterColumnType` | not implemented | New: needs `SchemaEvolver` integration |
| `AlterColumnDefault` | not implemented | New: data model exists, wire DDL |

### 9.6: Missing on `Table` (0.6.6)

| Feature | What to do |
|---|---|
| `current_snapshot()` method | Does NOT exist. Use `table.metadata().current_snapshot()` |
| `current_schema()` method | Does NOT exist. Use `table.metadata().current_schema()` |

### 9.7: Missing utilities

| Utility | What to do |
|---|---|
| `metadata_files_for_version()` | Port from feat/iceberg-ops `utils/metadata.rs` |
| `is_stale_metadata_file()` | Port from feat/iceberg-ops |
| `get_metadata_file_timestamp()` | Port from feat/iceberg-ops |
| `WritePathMode` enum | Port from feat/iceberg-ops |
| `retry_metadata_commit()` generalized helper | Must be created (pattern exists in `alter_table_properties()`) |

### 9.8: Constructor signatures (verified against 0.6.6)

| Constructor | Signature |
|---|---|
| `IcebergManifestScanExec::new()` | `(table_url: String, snapshot: Snapshot) -> Self` |
| `IcebergDiscoveryExec::new()` | `(input, table_url: String, snapshot_id: i64, partition_scan: bool) -> Result<Self>` |
| `IcebergScanByDataFilesExec::new()` | `(input, table_url: String, output_schema: SchemaRef) -> Self` |
| `IcebergWriterExec::new()` | `(input, table_url: Url, partition_columns: Vec<CatalogPartitionField>, sink_mode: PhysicalSinkMode, table_exists: bool, options: IcebergWriterExecOptions, logical_input_schema: Option<SchemaRef>) -> Self` |
| `IcebergCommitExec::new()` | `(input, table_url: Url, lakehouse_table: Option<LakehouseExecutionContext>) -> Self` |
| `IcebergTableFormat::parse_table_url()` | `async fn parse_table_url(paths: Vec<String>) -> Result<Url>` (associated fn) |
| `iceberg_schema_to_arrow()` | `fn iceberg_schema_to_arrow(schema: &Schema) -> Result<ArrowSchema>` |
| `iceberg_action_schema()` | `fn iceberg_action_schema() -> Result<SchemaRef>` |

### 9.9: Operation enum (verified — all variants exist on 0.6.6)

```rust
pub enum Operation { Append, Replace, Overwrite, Delete }
// as_str(): "append", "replace", "overwrite", "delete"
```
All variants exist. No additions needed.