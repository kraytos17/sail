# Porting feat/0.7.0 → feat/0.7.1 — Doc 08: Iceberg LOAD DATA & the Data-Write Path

> Part of the `docs/dev/port-v0.7.0/` inventory. Documents the **Iceberg LOAD DATA**
> implementation and the shared **data-file writer hardening** on `feat/0.7.0` vs base
> `f0b137d6` (commits `ee78c38d`, `efcfca36` "some changes for LOAD path", and follow-ups).
> Ground truth: `feat/0.7.0` tip `c07ad0c8`.

---

## 1. Scope

| File | Delta |
|---|---|
| `sail-iceberg/data/options/iceberg.yaml` | `compression_codec` + `target_file_size_bytes` now supported (option + table-property origins) |
| `sail-iceberg/src/operations/write/async_buffer.rs` | NEW — `AsyncShareableBuffer` (shared in-memory AsyncWrite sink w/ atomic byte counter) |
| `sail-iceberg/src/operations/write/arrow_parquet.rs` | writer buffers into the shareable buffer; `buffered_size()`; `close()` via `.close()` + `into_inner()`; NEW `build_writer_properties()`; tests |
| `sail-iceberg/src/operations/write/base_writer/data_file_writer.rs` | `aggregate_from_parquet_metadata_with_field_map` |
| `sail-iceberg/src/operations/write/table_writer.rs` | size-based data-file rolling; shared `record_finished_writer`; partition-values threading; tests |
| `sail-iceberg/src/physical/load_classifier.rs` | NEW — fast-register vs fallback classification (564 LOC) |
| `sail-iceberg/src/physical/load_data_planner.rs` | NEW — `plan_load_data` (541 LOC) |
| `sail-iceberg/src/physical_plan/load_data_exec.rs` | NEW — `IcebergLoadDataFastExec` (206 LOC) |
| `sail-iceberg/src/physical_plan/writer_exec.rs` | hash-distribution requirement for partitioned tables; metrics; zstd/table-property writer props; rolling target; overwrite partition values; `commit_operation` |
| `sail-iceberg/src/physical_plan/writer_options.rs` | new `IcebergWriterExecOptions` fields + `From<IcebergWriteOptions>` mapping; tests |
| `sail-logical-plan/src/load_data.rs` | NEW — `LoadDataNode` extension leaf node |
| `sail-plan/src/resolver/command/load.rs` | NEW — `resolve_command_load_data` |
| `sail-plan/src/resolver/command/mod.rs` | dispatches `CommandNode::LoadData` |
| `sail-iceberg/src/physical/table_scan_planner.rs` | `plan_extension` routes `LoadDataNode` → `plan_load_data` (doc 07 §7.7) |
| `sail-iceberg/src/physical/mod.rs` | `pub mod load_classifier; pub mod load_data_planner;` |
| `sail-iceberg/src/physical_plan/mod.rs`, codec/proto (doc 03) | `pub use load_data_exec::IcebergLoadDataFastExec;`; remote codec support + `IcebergLoadDataFastExecNode` (#56) |

---

## 2. Write-option surface (`iceberg.yaml`)

- `target_file_size_bytes`: previously `supported: false`; now `supported: true`, default
  `""` via `parse_optional_u64`. Description: when unset the Iceberg
  `write.target-file-size-bytes` default of **128 MB** applies; row groups / data files roll
  once the encoded size is reached so a single partition no longer accumulates one unbounded
  parquet file. Origins: option key `target-file-size-bytes` **and** table property
  `write.target-file-size-bytes` (case-sensitive).
- `compression_codec`: previously unsupported optional string; now `supported: true`, default
  `"zstd"` (`parse_string`), rust_type `String`. Origins: option `compression-codec` and
  table property `write.parquet.compression-codec` (case-sensitive). Accepted values
  zstd|snappy|gzip|lz4|brotli|uncompressed|none (see `resolve_compression_codec`, §5).

---

## 3. Writer plumbing

### 3.1 `AsyncShareableBuffer` (`operations/write/async_buffer.rs`, NEW)

Private copy of delta-rs/sail-delta-lake's `AsyncShareableBuffer`, kept local so the Iceberg
writer can measure total flushed bytes without coupling crates. `Arc<TokioRwLock<Vec<u8>>>` +
`Arc<AtomicU64>` `bytes_written`; implements `tokio::io::AsyncWrite` (appends + bumps the
counter); `into_inner(self) -> Option<Vec<u8>>` uses `Arc::try_unwrap` (fails if other clones
exist — `close()` drops the writer clone first).

### 3.2 `ArrowParquetWriter` (`arrow_parquet.rs`)

- Writer + buffer now both hold `AsyncShareableBuffer` clones; `close()` calls
  `writer.close()` (parquet async close) then `buffer.into_inner()`.
- **`buffered_size() -> u64`** = `bytes_written()` (flushed, atomic) + `in_progress_size()`
  (current row group). Synchronous on purpose — the parquet writer is not `Sync`, so writers
  check this without holding it across an await. Mirrors DataFusion's `ArrowWriter` and the
  DeltaLake writer (`buffer.len() + in_progress_size()`).
- Tests: `buffered_size_reflects_written_bytes`, `close_returns_full_buffer_and_row_count`.

### 3.3 `build_writer_properties(table_properties, compression)` (new, in arrow_parquet.rs)

Single place data-file parquet `WriterProperties` are derived (consistent across LOAD and
INSERT). Consumes only recognized keys (unrecognized ignored, forward compatible); missing
keys fall back to parquet-crate defaults:

- `write.parquet.row-group-size-bytes` → `max_row_group_bytes`
- `write.parquet.page-size-bytes` → `data_page_size_limit`
- `write.parquet.dict-size-bytes` → `dictionary_page_size_limit`
- `write.metadata.metrics.default` = `none`|`counts`|`full` → `EnabledStatistics::None|Chunk|Page`
- per-column, prefixed `write.parquet.`: `stats-enabled.column.<name>` (bool),
  `bloom-filter-fpp.column.<name>`, `bloom-filter-ndv.column.<name>`,
  `bloom-filter-enabled.column.<name>`.
- Invalid values error out.

Tests: `build_writer_properties_sets_sizes`, `..._sets_table_wide_statistics`,
`..._sets_per_column_stats_and_bloom`, `..._ignores_unknown_keys`, `..._rejects_invalid_values`.

### 3.4 `aggregate_from_parquet_metadata_with_field_map` (`base_writer/data_file_writer.rs`)

Like `aggregate_from_parquet_metadata` but re-keys column statistics from parquet-embedded
field IDs to **Iceberg schema field ids** via a `column_name → field_id` map (columns not in
the map are dropped; lower/upper bounds left empty). Needed because external parquet files
carry no Iceberg field IDs (LOAD DATA fast path).

### 3.5 `IcebergTableWriter` rolling (`table_writer.rs`)

- `write_aligned_batch` now takes `partition_values: Vec<Option<Literal>>`; unpartitioned
  writes pass `Vec::new()`; partitioned splits carry each part's values (used when rolling).
- After writing into a partition state, if the state is `Open { writer, .. }` and
  `config.target_file_size > 0` and `writer.buffered_size() >= target_file_size`, the writer
  is finished immediately (`finish_partition_state` → `record_finished_writer`) and the
  partition state is **not** re-inserted (a fresh state opens on the next batch). `0` disables
  rolling (single file per partition until close).
- `record_finished_writer(partition_dir, partition_values, writer)` extracted (used by both
  close-time flush and size rolling): close → put to store → build `DataFile` via
  `DataFileWriter::new(spec_id, file_path, partition_values).finish(meta)` → push to `written`.
- Tests: `rolls_multiple_data_files_at_target_size` (20×5000 rows, target 1024 → >1 file,
  total rows preserved, empty partition tuples), `zero_target_file_size_keeps_single_file`.

---

## 4. `IcebergWriterExec` / `IcebergWriterExecOptions`

### 4.1 Options (`writer_options.rs`)

`IcebergWriterExecOptions` (Default) gains:

```rust
compression_codec: String,      // default "zstd"
target_file_size: u64,          // default 134_217_728 (128 MB)
commit_operation: Option<Operation>,   // DELETE → Delete, MERGE → Overwrite, COMPACT → Replace
touched_file_paths: Vec<String>,
overwrite_predicate: Option<String>,   // JSON Vec<(String,String)> REPLACE WHERE pairs
```

`From<IcebergWriteOptions>` maps `compression_codec`, `target_file_size_bytes.unwrap_or(128MB)`;
keeps commit/overwrite fields empty. Tests guard the regression that the LOAD fallback writer
previously used `..Default::default()` and silently dropped a user `compression-codec`.

### 4.2 Writer exec (`writer_exec.rs`)

- `required_input_distribution()`: **unpartitioned** tables → `UnspecifiedDistribution` as
  before; **partitioned** tables → `HashPartitioned` over the partition-key columns
  (each task writes its partitions without opening many writers concurrently; falls back to
  unspecified if any partition column is missing from the input schema).
- Metrics (`ExecutionPlanMetricsSet`): `output_rows`, `output_bytes`, `elapsed_compute` per
  partition; counts are added per batch in the write loop.
- Writer properties: `resolve_compression_codec(&options.compression_codec)` →
  `build_writer_properties(&options.table_properties, compression)`; replaces the old
  hard-coded `WriterProperties::default()`. `target_file_size` comes from options (was
  hard-coded 134_217_728).
- **Overwrite partition values**: when `sink_mode == OverwritePartitions`, computes the set of
  unique partition tuples written (`Vec<String>` per tuple; `None` literal rendered as
  `"__NULL__"`, values via `format!("{lit:?}")`), serialized to JSON and attached as
  `CommitMeta.overwrite_partition_values` (used by commit-time parent-manifest filtering, doc
  07 §9.2).
- `CommitMeta.operation`: `options.commit_operation.unwrap_or(if sink_mode is Overwrite |
  OverwriteIf | OverwritePartitions then Operation::Overwrite else Append)`.
- `CommitMeta` now carries `touched_file_paths`, `overwrite_predicate` (from options) and the
  computed `overwrite_partition_values`.
- Tests: `resolve_compression_codec_maps_codecs`,
  `default_writer_properties_use_zstd`.

---

## 5. Compression resolution

`resolve_compression_codec(codec: &str) -> Result<Compression>` (writer_exec.rs): zstd→
`ZSTD(default)`, snappy→`SNAPPY`, gzip→`GZIP(default)`, lz4→`LZ4_RAW`, brotli→
`BROTLI(default)`, none|uncompressed→`UNCOMPRESSED`; anything else errors listing the valid
set. Case-insensitive.

---

## 6. LOAD DATA v1 design

### 6.1 Logical node (`sail-logical-plan/src/load_data.rs`, NEW)

`LoadDataNode` — a leaf `UserDefinedLogicalNodeCore` (no children/exprs; empty DFSchema)
carrying `location`, `local`, `overwrite`, `target_format`, `target_location`,
`target_table_name`, `target_options: Vec<OptionLayer>`, `target_lakehouse_table`.
`fmt_for_explain`: `LoadData: table=…, format=…, path=…, overwrite=…`.

### 6.2 Resolver (`sail-plan/src/resolver/command/load.rs`, NEW)

`resolve_command_load_data(local, location, table, overwrite, partition, state)`:
- rejects `LOCAL` (`LOAD DATA LOCAL is not supported`) and non-empty `PARTITION`
  (`LOAD DATA ... PARTITION is not supported`);
- requires a table (not a view) and `format == iceberg` (else explicit unsupported errors);
- requires the table to have a location;
- builds a `LakehouseOperation::Write` context and options = `[TablePropertyList { properties }]`;
- returns the `LoadDataNode` extension. V1 = remote object-store paths, unpartitioned LOAD.

### 6.3 Source classification (`physical/load_classifier.rs`, NEW)

`classify_source_files(object_store, source_url, location, table_schema, table_arrow_schema,
partition_spec_id, allow_fast) -> ClassifiedFiles { fast_files: Vec<DataFile>,
fallback_files: Vec<(String /*full URL*/, u64 /*size*/)> }`:

1. Build `column_name → Iceberg field_id` map from the table schema.
2. `resolve_source_files`: single file (no trailing `/`, no `*`) validated by `head`
   (error if missing); directory or glob expanded via object-store `list`, filtered by
   `split_glob` suffix, key joined to the URL origin; results sorted by key.
3. Split: only `*.parquet` files when `allow_fast` are fast candidates; everything else
   (`.csv`/`.json`/mismatch) → fallback. `allow_fast=false` (partitioned tables) forces all
   files through the rewrite fallback because the fast path registers empty partition tuples
   (invalid for a non-empty spec).
4. Footer reads parallelized with `buffer_unordered(available_parallelism*4)`; each footer:
   - `schema_matches` (every table column present by name **and** type; extra file columns
     allowed) → `build_data_file(...)` (full-URL path, empty partition, `record_count`,
     `file_size`, field-id-remapped stats + split offsets) → fast; on any footer parse/build
     error or mismatch → fallback.

Tests: `classifies_matching_parquet_as_fast`, `mismatched_schema_goes_to_fallback`,
`csv_file_goes_to_fallback`, `directory_lists_parquet_files_with_full_urls`,
`glob_filters_parquet_files`, `glob_splitting`, `partitioned_tables_disable_fast_path`.

### 6.4 Fast-register exec (`physical_plan/load_data_exec.rs`, NEW)

`IcebergLoadDataFastExec` — childless, single-partition, `Bounded`, emits **one** action-schema
batch built from the pre-classified `DataFile`s (`encode_add_data_files` + `encode_commit_meta`
with `row_count` = sum of records, operation/requirements/table_properties/lakehouse carried;
concat via `concat_batches`). `execute()` requires partition 0. Display shows
`table_path/files`. Accessors mirror the codec needs (data_files, table_url, operation,
requirements, table_properties, lakehouse_table). Its proto round-trip is doc 03 §2.4.

### 6.5 `plan_load_data` (`physical/load_data_planner.rs`, NEW)

1. `metadata_location_from_options` / `catalog_managed_iceberg_from_options`;
   `split_iceberg_write_options_and_table_properties(target_options)` keeps `option.*` keys
   out of the committed Iceberg table properties while still feeding option resolution
   (tested by `load_data_table_properties_exclude_catalog_options`).
2. `IcebergWriteOptions::resolve` → `PlannerContext` (loads the table via catalog
   metadata-location for managed tables, doc 07 §7.1).
3. `operation = overwrite ? Overwrite : Append`. Requirements:
   `LastAssignedFieldIdMatch { last_column_id }` + `CurrentSchemaIdMatch { current_schema_id }`.
4. Classify sources (source store resolved from the **source** URL, glob cut at first `*`).
   `allow_fast = !partitioned`; partitioned tables always rewrite.
5. **All-fast**: `IcebergCommitExec(IcebergLoadDataFastExec(...))`.
6. **Mixed/fallback**: fast branch (if any) + one writer branch **per source format group**
   (`group_by_format` buckets csv/json/parquet; unknown ext defaults csv). Each writer is
   `IcebergWriterExec` built **from the resolved `IcebergWriteOptions`**
   (preserves user `compression-codec`/`write.data.path`; only `commit_operation`,
   `lakehouse_table`, `table_properties` overridden). Branches are unioned, coalesced, then
   committed by one `IcebergCommitExec`. No extra repartition — `build_fallback_scan` byte-range
   repartitions large files to `target_partitions`, small files stay one task each.
7. `build_fallback_scan`: one `FileGroup` per file with real sizes; per-format `FileSource`
   (CSV `with_has_header(true)`, JSON, Parquet) over the table schema; compression inferred by
   `infer_source_compression` from the extension (`gz/bz2/xz/zst`, case-insensitive); then
   `DataSourceExec::repartitioned(target_partitions, config)` (FileGroupPartitioner splits
   large files first; declines compressed/small/unsplittable → one task/file).

Tests: `splits_large_csv_files_across_target_partitions`,
`repartitioned_groups_tile_file_bytes_exactly` (chunks tile `[0,size)` exactly, in order),
`keeps_single_partition_for_small_files`, `does_not_split_compressed_csv`,
`infers_compression_from_extension`, `load_data_table_properties_exclude_catalog_options`.

---

## 7. Remote execution & codec

`IcebergLoadDataFastExecNode` (proto #56) serializes `data_files_json`, `table_url`,
`operation`, `requirements_json`, `table_properties_json`, `lakehouse_table_json` (all JSON
strings) so the fast LOAD commit can execute on a worker (doc 03 §2.4 + round-trip test).
Note: `session_config` work-stealing disable (doc 01 §3.1) explicitly calls out the LOAD DATA
fallback byte-range split scan as a motivating case (each partition must not drain the shared
file queue).

---

## 8. Interactions / port notes

1. **Writer changes are shared by INSERT/CTAS/row-level ops**: `writer_exec`/`writer_options`
   are used by `assemble_iceberg_commit_plan` (doc 07 §7.3) and plain iceberg writes
   (`plan_iceberg_write`), so rolling + zstd default + distribution + metrics changes apply to
   every write. Verify against 0.7.1's existing `IcebergWriterExec` (the base already had the
   `IcebergWriterExecOptions` struct with `commit_operation`, per the codec import on base).
2. `build_writer_properties`/`AsyncShareableBuffer` need the parquet-crate feature set already
   used on 0.7.1 (`async_writer`, bloom filters, `EnabledStatistics`, compression variants).
3. `iceberg.yaml` keys are code-generated into `options::r#gen` — the config YAML change plus
   the parser functions must land before the option structs gain fields.
4. `LoadDataNode` + resolver depend on `CatalogManager.get_table_or_view` +
   `resolve_lakehouse_table_context(.., Write, ..)`; both exist on 0.7.1.
5. LOAD v1 limits (LOCAL/PARTITION rejected, iceberg-only, fast path disabled for partitioned
   tables, per-partition empty tuple fast registration) are intentional and documented in
   error strings/doctests.
