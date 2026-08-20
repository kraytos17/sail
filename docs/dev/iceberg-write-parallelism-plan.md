# Plan: Parallel Iceberg writes (multi-partition `IcebergWriterExec`)

**Status:** Implemented — all four parts (writer, LOAD DATA, general write path, commit
wiring) landed; `cargo check --workspace` clean, `cargo test -p sail-iceberg` 151/151 pass,
my changed files clippy-clean under workspace lints.
**Branch:** `feat/v0.6.6`
**Scope:** Remove the hard single-partition cap on the Iceberg write path so LOAD DATA and
general INSERT/CTAS writes parallelize across `target_partitions` (the cluster's configured
parallelism), matching Delta/Spark behavior. Goal: a 549 MB `LOAD DATA` drops from ~90 s
(1 writer task) toward ~10–15 s (16 writer tasks).
**Spec reference:** Spark `spark.sql.shuffle.partitions` semantics for write parallelism;
Delta's writer contract in this repo (`DeltaWriterExec`) as the in-repo precedent.

---

## 1. Problem statement (evidence from production logs)

Heimdall runs `LOAD DATA INPATH 's3a://data/gl_balances/2025-09-30/*.csv' INTO TABLE
landing.gl_balances` against the deployed `sail-flight-server`. The source set is two CSVs:
`20250930.csv` (small) and `gl.csv` (**549,919,676 bytes ≈ 549 MB**).

The driver's job graph shows the shape:

```
=== stage 0 === partitions=16  placement=Worker
DataSourceExec: file_groups={16 groups: [[gl.csv:0..34369980], [gl.csv:34369980..68739960], ...]}

=== stage 1 === partitions=2  placement=Worker
UnionExec
  IcebergWriterExec(table_path=s3://work/testcat/landing/gl_balances/)
    DataSourceExec: file_groups={1 group: [[gl_balances/2025-09-30/20250930.csv]]}
  IcebergWriterExec(table_path=s3://work/testcat/landing/gl_balances/)
    StageInputExec: input=0            ← gl.csv's 16 read partitions COALESCED to 1

=== stage 2 === partitions=1  placement=Driver
IcebergCommitExec(table_path=s3://work/testcat/landing/gl_balances/)

=== stage 3 === partitions=1  placement=Worker
StageInputExec
```

**Key observation:** stage 0 reads the 549 MB file with **16 parallel byte-range groups**
(`:0..34369980`, `:34369980..68739960`, … 16 splits), but stage 1 runs the writer with
**only 2 partitions** (one writer per source file, and the big file's writer receives a
single coalesced input). Everything funnels through one `IcebergTableWriter` instance which
writes 128 MB parquet files **sequentially**. Wall time for one load: ~85–90 s
(06:55:37 → 06:57:06 in the observed logs).

Spark with default parallelism would split this into ~16 writer tasks in parallel; the
Delta writer in this repo already does (`DeltaWriterExec` requires no single partition for
unpartitioned tables).

---

## 2. Root cause: the Iceberg writer is hard-capped at one partition

Three files enforce the single-partition constraint.

### 2.1 `IcebergWriterExec` — `crates/sail-iceberg/src/physical_plan/writer_exec.rs`

- `required_input_distribution()` returns `vec![Distribution::SinglePartition]`
  (`writer_exec.rs:306-308`).
- `execute(partition)` errors unless `partition == 0`
  (`writer_exec.rs:337-339`) and errors unless the input has exactly one partition
  (`writer_exec.rs:341-346`).
- `execute()` then drives **one** `IcebergTableWriter` over the entire input stream,
  calling `writer.write(&batch).await` for every batch and `writer.close()` once
  (`writer_exec.rs:582-596`), emitting a single action batch per task.

Consequences:
- No matter how many partitions the read produces, the write collapses to one task.
- The `IcebergTableWriter` targets `target_file_size: 134_217_728` (128 MB,
  `writer_exec.rs:562`), so a 549 MB input becomes ~5 parquet files — but all written by
  one task, serially.

### 2.2 LOAD DATA fallback planner — `crates/sail-iceberg/src/physical/load_data_planner.rs`

- For CSV/JSON (non-parquet) sources, each fallback chunk is wrapped in an explicit
  `CoalescePartitionsExec` before the writer (`load_data_planner.rs:195`).
- Files are grouped by format (`group_by_format`, `:230-249`) and chunked by bytes toward
  `TARGET_FILE_SIZE_BYTES = 134_217_728` capped at `target_partitions`
  (`chunk_size_for_bytes`, `:345-357`; used at `:188-191`). This bounds the **number of
  writers** but cannot split a **single large file** across writers, and each writer is
  forced to one partition by the explicit coalesce.
- The `UnionExec` of writer branches is coalesced before `IcebergCommitExec`
  (`:214-219`) — this is fine because only the small *action batches* flow through it.

### 2.3 General write path — `crates/sail-iceberg/src/physical_plan/plan_builder.rs`

- `IcebergPlanBuilder::add_repartition_node` hardcodes
  `Partitioning::RoundRobinBatch(4)` for unpartitioned tables (`plan_builder.rs:96`) and
  `Partitioning::Hash(exprs, 4)` for partitioned tables (`:124`).
- This path serves `INSERT INTO`, CTAS, and every write that goes through
  `resolve_write_with_builder` → `IcebergTableFormat::create_writer` →
  `IcebergWriteNode` → `plan_iceberg_write_plan` (`table_format.rs:622`).
- So general writes are capped at **4** write partitions even though reads may be 16.

### 2.4 Why Delta is not affected (the in-repo precedent)

`DeltaWriterExec::required_input_distribution` (`crates/sail-delta-lake/src/physical_plan/writer_exec.rs:502-525`):

```rust
fn required_input_distribution(&self) -> Vec<Distribution> {
    if self.partition_columns.is_empty() {
        // Upstream repartitioning controls file counts and small-file behavior.
        return vec![Distribution::UnspecifiedDistribution];
    }
    // ... HashPartitioned(partition cols)
}
```

Unpartitioned Delta tables therefore write in parallel across all input partitions. The
Iceberg writer should mirror this exactly.

---

## 3. Supporting facts that make the change safe

- **Commit aggregation already multi-writer aware.** `IcebergCommitExec` requires
  `SinglePartition` input (`commit_exec.rs:666-668`) but its `accumulate_action_batches`
  (`commit_exec.rs:636-654`) iterates **all** incoming action batches, summing `row_count`
  across every commit-meta batch and using the last `commit_meta` for the remaining fields.
  The doc comment even says *"LOAD DATA unions a fast-register branch + one or more rewrite
  writers"*. So N writer partitions → union → coalesce → commit already works.
- **File names are collision-safe.** `DefaultLocationGenerator::with_partition_dir` names
  files `part-<Uuid::new_v4()>-<counter>.parquet`
  (`location_generator.rs:52-74`), so concurrent writer tasks never collide on object keys.
- **`target_partitions` is available at both plan sites.**
  - LOAD DATA already reads it: `ctx.session().config().target_partitions()`
    (`load_data_planner.rs:188`).
  - `IcebergPlanBuilder` already holds `session: &'a dyn Session`
    (`plan_builder.rs:45`), currently marked `#[expect(unused)]`.
- **`IcebergTableWriter` is per-task state.** Each `execute()` call constructs its own
  writer (`writer_exec.rs:573-580`); there is no shared mutable writer state, so running
  one writer per partition is a natural extension.

---

## 4. Implementation plan

### 4.1 Make `IcebergWriterExec` multi-partition — `writer_exec.rs`

**4.1.1 `required_input_distribution` (lines 306-308)**

```rust
fn required_input_distribution(&self) -> Vec<Distribution> {
    if self.partition_columns.is_empty() {
        // Upstream repartitioning controls file counts and small-file behavior.
        return vec![Distribution::UnspecifiedDistribution];
    }
    // For partitioned tables, require grouping by partition key so each task writes only
    // its own partition values without opening many writers concurrently.
    let mut exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(self.partition_columns.len());
    for field in &self.partition_columns {
        let idx = self.input.schema().index_of(&field.column)?; // fall back to Unspecified on error
        exprs.push(Arc::new(Column::new(&field.column, idx)));
    }
    vec![Distribution::HashPartitioned(exprs)]
}
```

**4.1.2 `execute(partition)` (lines 332-348)**

- Remove the `partition != 0` guard.
- Remove the `input_partitions != 1` guard; require only that `partition <
  input.output_partitioning().partition_count()`.
- Execute `self.input.execute(partition, context)` and feed that partition's stream into a
  writer instance created per `execute()` call (already the case).
- The rest of `execute()` (metadata load, schema evolve, `IcebergTableWriter` loop,
  action-batch emission at `:582-659`) stays unchanged — each partition emits its own
  `add_data_files` + `commit_meta` action batch.

**4.1.3 `properties()` / `compute_properties` (lines 125-132, 302-304)**

- Update `PlanProperties` so the output partitioning reflects the new contract. The action
  batch schema is unchanged; `EmissionType::Final`/`Boundedness::Bounded` stay. Keep
  `Partitioning::UnknownPartitioning(1)` or switch to match the new distribution — decide
  by inspecting how the physical optimizer propagates the writer's output into the
  subsequent coalesce/commit. (Low-risk: the union/coalesce path in 4.2 normalizes it.)

**4.1.4 Per-partition execution metrics (refinement from verification)**

- `DeltaWriterExec` tracks per-partition metrics via `MetricBuilder::output_rows(partition)`
  / `output_bytes(partition)` / `elapsed_compute(partition)` (delta
  `writer_exec.rs:610-612`). The Iceberg writer currently has **no** per-partition metric
  builder; when running N writer partitions, `output_rows`/`output_bytes` must be indexed
  by `partition` (or the metric plumbing added) so `EXPLAIN ANALYZE` stays correct.
  Mirror the Delta metric-builder pattern rather than omitting metrics.

### 4.2 LOAD DATA fallback planner — `load_data_planner.rs`

**4.2.1 Remove the per-writer `CoalescePartitionsExec` (line 195).**

The fallback `DataSourceExec` (via `build_fallback_scan`, `:252-303`) already splits each
file into byte-range groups (per-file `PartitionedFile` + size-aware scan planning), so a
single 549 MB file yields 16 parallel read partitions. Feed those directly into a
multi-partition `IcebergWriterExec` instead of coalescing to 1.

**4.2.2 Revisit chunking.**

`chunk_size_for_bytes` (`:345-357`) groups files to bound writer count and avoid
small-file sprawl. With multi-partition writers this logic remains useful (bound total
writer tasks), but its per-file grouping must not force a single big file onto one writer.
Plan:

- Keep byte-aware chunking of the **file list** for small-file accumulation (do not
  fragment tiny files across tasks).
- For a single file whose bytes exceed `TARGET_FILE_SIZE_BYTES`, the 
  `DataSourceExec` byte-range split already provides the parallelism; the writer
  partition count is then governed by `target_partitions`, not by the chunk size.
- Confirm the effective writer partition count for the 549 MB case is ~16 (bounded by
  `target_partitions`), producing ~16 tasks × ~34 MB or a smaller task count if the
  writer applies its own 128 MB target internally. Verify final parquet file sizes stay
  near the 128 MB target without exploding file count.

**4.2.3 Keep the union → coalesce → commit wiring (lines 214-219).**

`UnionExec` of N writer branches, then `CoalescePartitionsExec`, then `IcebergCommitExec`
stays correct: only the small action batches traverse the coalesce. No change expected
beyond confirming branch count semantics.

### 4.3 General write path — `plan_builder.rs`

**4.3.1 `add_repartition_node` (lines 91-128)**

Replace the hardcoded partition counts with the session's configured parallelism:

```rust
let target = self.session.config().target_partitions().max(1);
let repartitioning = if partition_columns.is_empty() {
    Partitioning::RoundRobinBatch(target)
} else {
    Partitioning::Hash(exprs, target)   // exprs = partition source columns (existing logic)
};
```

- Remove `#[expect(unused)]` on the `session` field (`plan_builder.rs:44-45`) since it is
  now read.
- This lifts INSERT/CTAS writes from 4 → 16 (the cluster's `executionDefaultParallelism`).

**4.3.2 Reuse Delta's `create_repartition` semantics (refinement from verification)**

- Delta's `create_repartition` (physical_plan/mod.rs:174-211) uses
  `RoundRobinBatch(n)` for unpartitioned and `Hash(partition_exprs, n)` for partitioned,
  and documents a **partition-columns-moved-to-end** contract (the projection moves
  partition columns to the end so hash expr positions are stable) plus the
  "multiple writers may create files within the same partition directory" note
  (`:184-201`).
- `IcebergPlanBuilder.add_repartition_node` already derives hash exprs from partition
  source columns; only the partition *count* changes. Preserve the existing expr
  derivation and column ordering so partitioned-table writes do not regress. The plan
  should NOT reorder columns or change the hash key — only the `4` → `target_partitions`.

### 4.4 `IcebergCommitExec` — verify, expect no change

- `commit_exec.rs:666-668` keeps `SinglePartition`; `:636-654` already aggregates N
  action batches. Re-run the writer/commit exec tests after 4.1 to confirm.

---

## 5. Edge cases & risks

| # | Risk | Mitigation |
|---|------|------------|
| 1 | **Small loads over-fragment.** A 100 KB CSV with 16 writer partitions would produce 16 tiny parquet files. | The LOAD DATA planner keeps `chunk_size_for_bytes` (small byte totals → 1 chunk → 1 writer; see test `chunk_size_for_bytes_collapses_small_sets_to_one_writer`, `:364-371`). Preserve this behavior: only scale to N partitions when bytes warrant it. For the general write path, `RoundRobinBatch(target)` on tiny inputs may need a byte-aware guard similar to LOAD DATA. |
| 2 | **Partitioned-table writes**: each writer task must see rows grouped by partition value so it only opens writers for its own partitions. | `HashPartitioned(partition_cols)` in `required_input_distribution` (4.1.1) enforces this; the hash must match how the input is partitioned (or rely on `EnforceDistribution` to insert a shuffle). Verify with a partitioned-table write test. |
| 3 | **Commit meta collisions.** N writers each emit a `commit_meta`; `accumulate_action_batches` takes the *last* for non-row-count fields. | All branches target the same table with identical options, so the last is representative; row counts are summed. Add a test asserting the summed count and single commit. |
| 4 | **Output-partitioning bookkeeping.** DataFusion's `EnforceDistribution`/`EnforceSorting` may insert repartitions or reject inconsistent `PlanProperties`. | After changing `required_input_distribution`/`properties`, run the physical-planner test suites and inspect `EXPLAIN` plans in tests to confirm no spurious extra shuffle stages. |
| 5 | **Sequential-vs-parallel commit ordering.** Concurrent writer tasks must all finish before commit (Iceberg requires all data files exist before the metadata commit). | The existing stage graph already inserts a shuffle/barrier between stage 1 (writers) and stage 2 (commit); confirm the `CoalescePartitionsExec` before commit still imposes the required ordering. |
| 6 | **Backwards compatibility with the fast path.** `IcebergLoadDataFastExec` (parquet register) is unchanged and still unions with rewrite writers. | Fast path untouched; only the rewrite-writer branch wiring changes. |

---

## 6. Test plan

1. **Unit tests (in-repo).**
   - `writer_exec.rs`: new test asserting `required_input_distribution` is
     `UnspecifiedDistribution` for unpartitioned and `HashPartitioned` for partitioned;
     test that `execute(p)` succeeds for `p > 0` with a multi-partition input and emits one
     action batch per partition.
   - `commit_exec.rs`: test commit over N synthetic action batches (multiple writers)
     sums row counts and produces one commit.
   - `load_data_planner.rs`: extend chunking tests to cover a single large file (bytes >
     target) with `target_partitions=16` → verify writer plan has >1 partition and no
     per-writer coalesce.
   - `plan_builder.rs`: test `add_repartition_node` uses `target_partitions` (not 4).
2. **Integration (pysail spark tests).** Existing write tests under
   `python/pysail/tests/spark/dml/` and `spark/delta` should still pass; add an
   assertion that a large CSV `LOAD DATA` plan shows multiple writer partitions.
3. **E2E on cluster (heimdall).** Re-run `run_data_load.sh -b 2025-09-30 gl_balances`;
   measure wall time and confirm: multiple writer tasks in the job graph (not
   `partitions=2`), output ~5 × 128 MB parquet files, no small-file sprawl, and the load
   completes in single-digit-to-low-teen seconds.
4. **Regression:** `cargo build`, `cargo clippy`, `cargo test -p sail-iceberg -p
   sail-physical-plan` and the physical-optimizer tests; verify 0 new warnings (the
   `session` field's `#[expect(unused)]` is removed cleanly).

---

## 7. Files touched

- `crates/sail-iceberg/src/physical_plan/writer_exec.rs` — multi-partition writer (core).
- `crates/sail-iceberg/src/physical/load_data_planner.rs` — remove per-writer coalesce;
  chunking review for single-large-file case.
- `crates/sail-iceberg/src/physical_plan/plan_builder.rs` — `target_partitions` in
  `add_repartition_node`; un-`expect(unused)` `session`.
- `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` — verification only.
- `crates/sail-iceberg/src/operations/write/file_writer/location_generator.rs` —
  read-only reference (UUID names already collision-safe).

---

## 8. Rollout & verification checklist

- [ ] 4.1 writer multi-partition implemented + unit tests green.
- [ ] 4.2 LOAD DATA wiring updated; `gl_balances` plan shows >1 writer partition.
- [ ] 4.3 general write path uses `target_partitions`; INSERT/CTAS plans show 16 partitions.
- [ ] Full `cargo test -p sail-iceberg` + clippy clean (0 warnings).
- [ ] Cluster E2E: 549 MB load ~90 s → ~10–15 s; parquet file sizes near 128 MB target.
- [ ] No regression on small loads (1 writer for < 128 MB input).
