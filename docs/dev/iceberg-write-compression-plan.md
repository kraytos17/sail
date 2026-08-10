# Plan: Iceberg write follow-up — compression + file-count policy (v2)

**Status:** Implemented — Snappy default, `compression-codec` option wired, dead repartition
removed. `cargo check --workspace` clean, `cargo test -p sail-iceberg` 147/147 pass.
**Branch:** `feat/v0.6.6`
**Depends on:** `docs/dev/iceberg-write-parallelism-plan.md` (v1) — already landed and verified
(~3.3x faster LOAD DATA: 90s → ~27s via 16-way parallel `IcebergWriterExec`).
**Scope:** (1) Iceberg parquet files are written **uncompressed** (`WriterProperties::default()`
→ `Compression::UNCOMPRESSED`) — enable Snappy by default and wire the `compression-codec`
option; (2) decide/verify the writer partition-count policy now that the optimizer removes the
explicit repartition; (3) confirm the general write path (§4.3 of v1) gets the same benefits.

---

## 1. Findings from the cluster logs (post-v1)

The re-run shows the v1 fix working: `IcebergWriterExec` runs **16 parallel partitions**
(job stage 0, spread across workers 1+2), LOAD DATA completes in **~27-29s** vs ~90s before.

Two side-effects discovered:

### 1.1 The explicit `RepartitionExec` from v1 §4.2 is removed by the optimizer

- **Input plan** (what `load_data_planner.rs` builds now):
  ```
  IcebergCommitExec
    CoalescePartitionsExec
      IcebergWriterExec
        RepartitionExec: partitioning=RoundRobinBatch(5), input_partitions=2
          DataSourceExec: 2 groups
  ```
- **Final optimized plan** (after DataFusion physical optimizer):
  ```
  IcebergCommitExec
    CoalescePartitionsExec
      IcebergWriterExec
        DataSourceExec: 16 groups   ← RepartitionExec gone; scan re-split to 16 byte-ranges
  ```

So the parallelism now comes from **DataFusion's `FileScanConfig` scan splitting** the 549 MB
file into `target_partitions` (16) byte-range groups (`EnforceDistribution` → scan
`repartitioned()`), not from `create_fallback_repartition`. **`create_fallback_repartition` is
effectively dead code** on this path — the optimizer rewrites the `RepartitionExec` away
because the writer now accepts `UnspecifiedDistribution` (v1 §4.1).

### 1.2 Consequence: file count is 16 × ~34 MB, not ~5 × 128 MB

Because the writer runs with 16 partitions and each partition writes what it receives
(up to `target_file_size` 128 MB), a 549 MB load produces **16 parquet files of ~34 MB**
instead of the ~5 files the byte-aware sizing intended.

### 1.3 New primary bottleneck: parquet is written UNCOMPRESSED

- `writer_exec.rs:596` sets `writer_properties: WriterProperties::default()`.
- The `parquet` crate's `WriterProperties::default()` uses
  `Compression::UNCOMPRESSED` (`parquet-58.3.0/src/file/properties.rs:35`).
- **Delta writes SNAPPY** (`sail-delta-lake/src/writer/mod.rs:120`); Iceberg does not.
- Impact: larger output files, more MinIO write bytes, more scan I/O later — no compression
  CPU cost either, but for string-heavy CSV data, Snappy/ZSTD typically gives 5-10x size
  reduction that more than pays back.

---

## 2. Option schema: compression + target size exist but are not wired

`crates/sail-iceberg/data/options/iceberg.yaml` (codegen source for `IcebergWriteOptions`):

| Key | YAML state | Current effect |
|-----|-----------|----------------|
| `compression-codec` | `supported: false` (`:568-576`) | not parsed, no effect |
| `compression-level` | `supported: false` (`:578-586`) | not parsed |
| `target-file-size-bytes` (write opt) | `supported: false` (`:418-426`) | not parsed |
| `write.target-file-size-bytes` (table prop) | `supported: false` (`:1126-1135`) | not parsed |

`IcebergWriteOptions` → `IcebergWriterExecOptions` (`writer_options.rs:86-104`) currently
copies only merge/overwrite-schema, data paths, variant fields. `WriterConfig` already carries
`writer_properties: WriterProperties` (`config.rs:27`) and `target_file_size: u64`
(`:28`), so both knobs have a natural landing spot.

---

## 3. Implementation plan

### 3.1 Enable Snappy by default in the Iceberg writer (primary win)

**File:** `crates/sail-iceberg/src/physical_plan/writer_exec.rs`

Replace the default writer properties (`:596`) with a Snappy builder, mirroring Delta:

```rust
let writer_properties = WriterProperties::builder()
    .set_compression(parquet::basic::Compression::SNAPPY)
    .build();
let writer_config = WriterConfig {
    table_schema: table_schema.clone(),
    partition_columns: partition_columns.clone(),
    writer_properties,
    target_file_size: 134_217_728,
    write_batch_size: 32 * 1024,
    num_indexed_cols: 32,
    stats_columns: None,
    iceberg_schema: Arc::new(iceberg_schema.clone()),
    partition_spec: unbound_spec,
    variant_shredding,
};
```

Covers **all** Iceberg write paths at once (LOAD DATA fallback, INSERT/CTAS via
`IcebergPlanBuilder`, row-level merge/update/delete via `assemble_iceberg_commit_plan`), since
they all construct `IcebergWriterExec`.

### 3.2 Wire the `compression-codec` option (configurable, optional)

**Goal:** allow `OPTIONS (compression-codec 'zstd')` / table property
`write.parquet.compression-codec` to override the default, matching Spark/Iceberg.

**3.2.1 `data/options/iceberg.yaml`** — flip `compression-codec` to `supported: true`:

```yaml
- key: compression_codec
  description: |
    Override the Parquet compression codec for data file writes
    (snappy | zstd | gzip | lz4 | uncompressed | none).
  default:
    value: "snappy"
    parser: crate::options::parsers::parse_string
  supported: true
  rust_type: String
  scopes: [write]
  origins:
    - type: option
      keys: [compression-codec]
      parser: crate::options::parsers::parse_string
    - type: table_property
      keys: [write.parquet.compression-codec]
      case_sensitive: true
      parser: crate::options::parsers::parse_string
```

**3.2.2 `writer_options.rs`** — add `compression_codec: String` to
`IcebergWriterExecOptions` (default `"snappy"`), copy it in `From<IcebergWriteOptions>`.

**3.2.3 `writer_exec.rs`** — map the codec string to `parquet::basic::Compression` via a small
helper (`snappy/zstd/gzip/lz4/uncompressed/none`), then build `WriterProperties` with
`.set_compression(...)`.

**Note on codegen:** `gen::IcebergWriteOptions` is generated from the YAML at build time
(`options.rs` `include!(concat!(env!("OUT_DIR"), "/options/iceberg.rs"))`). Confirm the codegen
pipeline (build script) picks up the YAML change before building.

### 3.3 File-count policy: accept 16 files, remove dead repartition code

**Decision (recommended):** keep the 16-way parallelism (file count is a secondary concern for
load speed; Iceberg handles many small files fine, and `OPTIMIZE`/`rewrite_data_files` can
compact later).

- **Remove `create_fallback_repartition` and its call** in `load_data_planner.rs` — it is dead
  code now (optimizer removes the `RepartitionExec`). This also removes the now-misleading
  "byte-aware sizing" logic and its tests.
- **Keep the union → coalesce → commit wiring** unchanged.
- **Do NOT chase forcing ~5 files** via `ExplicitRepartitionExec`: it would reintroduce a
  shuffle and reduce writer parallelism, trading load speed for file count. Document the
  tradeoff in the plan doc instead.

> Alternative (only if file count matters for scan-heavy workloads): force a smaller writer
> partition count by passing a scan-config `target_partitions` hint (e.g.
> `ceil(bytes/128MB)`) instead of relying on the global `target_partitions`. This is a larger
> change touching the LOAD DATA `FileScanConfigBuilder` and is deferred unless measurements
> show scan regression.

### 3.4 Confirm the general write path (§4.3 of v1) inherits the same benefits

- `IcebergPlanBuilder.add_repartition_node` already uses `target_partitions` (v1 §4.3) and the
  writer is multi-partition (v1 §4.1) → INSERT/CTAS should also run 16-way.
- Compression (3.1) applies automatically since the general path uses the same
  `IcebergWriterExec`.
- **Verify with a cluster test**: `CREATE TABLE ... USING iceberg AS SELECT ...` on a sizable
  input; check job graph shows >1 writer partition and output files are Snappy-compressed.

---

## 4. Edge cases & risks

| # | Risk | Mitigation |
|---|------|------------|
| 1 | **Compression codec parsing**: unknown codec string. | Helper maps known names; unknown → fall back to Snappy + warn (or error, matching Spark). Pick error-on-unknown to fail fast. |
| 2 | **ZSTD/GZIP CPU cost on write** could offset some parallelism gains for CPU-bound loads. | Snappy default is low-CPU/high-speed; user can choose. Verify wall time on the 549 MB load. |
| 3 | **Changing output compression changes file sizes** → existing `TARGET_FILE_SIZE_BYTES`/file-count heuristics in LOAD DATA become approximate. | Since 3.3 removes the byte-aware repartition anyway, this is moot; document that file sizing now follows `target_partitions`. |
| 4 | **Codegen rebuild**: YAML change must regenerate `gen::IcebergWriteOptions`. | Build script reads the YAML; a clean `cargo build -p sail-iceberg` regenerates. Verify the generated struct has `compression_codec`. |
| 5 | **Back-compat**: existing tables written uncompressed keep working (read path independent of write compression). | No read-side change. |
| 6 | **`write.parquet.compression-codec` vs option `compression-codec` precedence.** | Follow existing option-precedence pattern (option layer wins, then table property, then default) — mirror how `shred_variants` resolves in `writer_options.rs:156-180`. |

---

## 5. Test plan

1. **Unit (in-repo).**
   - `writer_options.rs`: `IcebergWriterExecOptions::from(IcebergWriteOptions)` carries
     `compression_codec`; default `"snappy"`.
   - `writer_exec.rs` (or config helper): codec-string→`Compression` mapping incl. unknown
     error path; `WriterConfig.writer_properties.compression()` is Snappy when default.
   - `load_data_planner.rs`: remove `create_fallback_repartition` tests; keep the
     `fallback_writer_partitions_*` sizing tests only if the logic is retained elsewhere (or
     delete them with the dead function).
2. **Integration (pysail spark).** Existing write tests under `python/pysail/tests/spark/dml/`
   and `spark/iceberg/` still pass; add a check that written parquet files report Snappy
   compression.
3. **E2E (heimdall, cluster).** Re-run `run_data_load.sh -b 2025-09-30 gl_balances`;
   confirm: (a) 16 writer partitions (unchanged), (b) output parquet files are
   Snappy-compressed and materially smaller, (c) wall time ≤ current ~27s, ideally ~10-20s,
   (d) job graph no longer shows a `RepartitionExec(RoundRobinBatch(5))` node.
4. **Regression.** `cargo build`, `cargo clippy -p sail-iceberg --all-targets`, `cargo test -p
   sail-iceberg` (151 tests), `cargo test -p sail-physical-plan -p sail-physical-optimizer`.

---

## 6. Files touched

- `crates/sail-iceberg/src/physical_plan/writer_exec.rs` — Snappy default + codec→Compression
  mapping (3.1, 3.2.3).
- `crates/sail-iceberg/src/physical_plan/writer_options.rs` — `compression_codec` field +
  conversion (3.2.2).
- `crates/sail-iceberg/data/options/iceberg.yaml` — `compression_codec` `supported: true`
  (3.2.1).
- `crates/sail-iceberg/src/physical/load_data_planner.rs` — remove dead
  `create_fallback_repartition` + its tests (3.3).
- `crates/sail-iceberg/src/options.rs` — verify no change needed beyond codegen pickup.

---

## 7. Rollout & verification checklist

- [ ] 3.1 Snappy default: `cargo test -p sail-iceberg` green; cluster output files compressed.
- [ ] 3.2 `compression-codec` option wired end-to-end (YAML → codegen → options → writer).
- [ ] 3.3 dead `create_fallback_repartition` removed; LOAD DATA still 16-way parallel.
- [ ] 3.4 general write path (INSERT/CTAS) confirmed 16-way + Snappy on cluster.
- [ ] E2E: 549 MB load ≤ ~27s, parquet files Snappy + smaller, no `RepartitionExec(5)` in plan.
- [ ] No small-load regression (tiny CSV → 1 writer, no tiny-file sprawl).
