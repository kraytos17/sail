# 04 — `LOAD DATA INPATH` into Iceberg (natively via Sail)

> Full implementation of `LOAD DATA INPATH '<path>' [OVERWRITE] INTO TABLE ns.tbl`
> executed natively by Sail (no Spark dependency), with a **fast-register** path for
> schema-matching parquet and a **rewrite fallback** for CSV/JSON/mismatched parquet.

Files:
- `crates/sail-logical-plan/src/load_data.rs` (new, 141)
- `crates/sail-plan/src/resolver/command/load.rs` (new, 96)
- `crates/sail-iceberg/src/physical/load_classifier.rs` (new, 576 incl. tests)
- `crates/sail-iceberg/src/physical/load_data_planner.rs` (new, 355 incl. tests)
- `crates/sail-iceberg/src/physical_plan/load_data_exec.rs` (new, 202)
- `crates/sail-iceberg/src/operations/parquet_utils.rs` (new)
- `crates/sail-iceberg/src/operations/write/base_writer/data_file_writer.rs` (+54)
- `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` (+604, count plumbing)
- `crates/sail-execution/src/proto/codec.rs` + `physical.proto` (`IcebergLoadDataFastExecNode`)
- `crates/sail-iceberg/src/physical/table_scan_planner.rs` (`LoadDataNode` dispatch)

---

## 1. Logical node — `sail-logical-plan/src/load_data.rs`

`LoadDataNode` — leaf `UserDefinedLogicalNodeCore` (`name() = "LoadData"`, no inputs,
empty schema):

```rust
pub struct LoadDataNode {
    location: String,             // s3a://… file, glob, or directory
    local: bool,                  // LOAD DATA LOCAL — unsupported in v1 (always false)
    overwrite: bool,              // OVERWRITE → full-table replace
    target_format: String,        // "iceberg"
    target_location: String,
    target_table_name: Vec<String>,
    target_options: Vec<OptionLayer>,
    target_lakehouse_table: Option<LakehouseExecutionContext>,
    schema: DFSchemaRef,          // empty
}
```

Accessors: `location()`, `is_local()`, `overwrite()`, `target_format()`,
`target_location()`, `target_table_name()`, `target_options()`,
`target_lakehouse_table()`.
`fmt_for_explain` → `LoadData: table=..., format=..., path=..., overwrite=...`.

---

## 2. Resolver — `sail-plan/src/resolver/command/load.rs`

`resolve_command_load_data(local, location, table, overwrite, partition, state)`:
1. `local` → `NotSupported("LOAD DATA LOCAL is not supported")`.
2. `!partition.is_empty()` → `NotSupported("LOAD DATA ... PARTITION is not supported")`.
3. `CatalogManager::get_table_or_view(table.parts())`; must be
   `TableKind::Table` with a `location` and format `iceberg`.
4. `resolve_lakehouse_table_context(&table_name, LakehouseOperation::Write, Some(format), vec![])`.
5. `options = vec![OptionLayer::TablePropertyList { items: properties }]`.
6. `LoadDataNode::new(location, local, overwrite, format, table_location, table_name,
   options, Some(lakehouse_table))` → Extension plan.

---

## 3. Classification — `physical/load_classifier.rs`

### 3.1 `ClassifiedFiles`

```rust
pub(crate) struct ClassifiedFiles {
    pub fast_files: Vec<DataFile>,          // registered without rewrite
    pub fallback_files: Vec<(String, u64)>, // (full URL, size in bytes)
    pub total_rows: u64,                    // sum of row counts across all sources
}
```

### 3.2 `classify_source_files(...)`

```rust
pub(crate) async fn classify_source_files(
    object_store: Arc<dyn ObjectStore>,
    source_url: &Url,
    location: &str,
    table_schema: &Schema,           // Iceberg schema (for field ids)
    table_arrow_schema: &ArrowSchema,
    partition_spec_id: i32,
    allow_fast: bool,
) -> DFResult<ClassifiedFiles>
```

1. Builds `field_id_map: HashMap<String,i32>` (column name → Iceberg field id; external
   parquet has no field ids).
2. `resolve_source_files(store, source_url, location)` → `Vec<(key, full_url, size)>`:
   - single file (no trailing `/`, no `*`): `head`-validate; error
     `"source path does not exist: {location}"` on failure.
   - directory or glob: `split_glob` → listing prefix + suffix filter; object-store
     `list`; join the store-root-relative key with the URL's origin
     (`prefix_url.join(&format!("/{key}"))`); sort by key.
3. Split by extension: `allow_fast && url.ends_with(".parquet")` → fast candidates;
   everything else (CSV/JSON) → fallback. When `allow_fast` is false (partitioned
   tables), **all** files go to fallback so the writer computes partition values.
4. Parallel footer reads with bounded concurrency
   (`available_parallelism().unwrap_or(16) * 4`, `buffer_unordered`).
5. Per file:
   - footer OK → `total_rows += row_count`;
   - `schema_matches(footer.arrow_schema, table_arrow_schema)` (every table column
     present with identical Arrow type; extra file columns allowed) →
     `build_data_file(&url, &footer, &field_id_map, partition_spec_id)`; build failure →
     warn + fallback;
   - schema mismatch → debug + fallback;
   - footer read error → warn + fallback.

### 3.3 `build_data_file`

Calls `aggregate_from_parquet_metadata_with_field_map(&footer.parquet_metadata,
field_id_map)` to build column stats keyed by Iceberg field id, then returns a `DataFile`
with `file_path = FULL URL`, `record_count`, `file_size`, `partition: vec![]`,
`partition_spec_id`, no sort order, no equality ids.

### 3.4 Helpers

- `split_glob(location) -> (prefix, Option<suffix>)`:
  `s3a://bucket/dir/*.parquet` → `(s3a://bucket/dir/, Some(".parquet"))`;
  `s3a://bucket/dir/` → `(…dir/, None)`.
- `schema_matches(file_schema, table_schema) -> bool`.

### 3.5 Tests

`classifies_matching_parquet_as_fast`, `mismatched_schema_goes_to_fallback`,
`csv_file_goes_to_fallback`, `directory_lists_parquet_files_with_full_urls`,
`glob_filters_parquet_files`, `glob_splitting`, `partitioned_tables_disable_fast_path`.

---

## 4. `operations/parquet_utils.rs`

```rust
pub(crate) struct ParquetFooterInfo {
    pub parquet_metadata: ParquetMetaData,
    pub arrow_schema: SchemaRef,
    pub row_count: u64,
    pub file_size: u64,
}

pub(crate) async fn read_parquet_footer(store: &Arc<dyn ObjectStore>, path: &str)
    -> Result<ParquetFooterInfo, String>
```

`head` for size, then `ParquetObjectReader` + `ParquetRecordBatchStreamBuilder` to read
the footer metadata + schema without decoding rows; `num_rows` from the footer.

---

## 5. `data_file_writer.rs` — `aggregate_from_parquet_metadata_with_field_map`

```rust
pub(crate) fn aggregate_from_parquet_metadata_with_field_map(
    metadata: &ParquetMetaData,
    field_id_map: &HashMap<String, i32>,
) -> Result<(column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets), String>
```

Aggregates per-column stats, re-keying the parquet columns by their Iceberg field ids via
`field_id_map`.

---

## 6. Physical planner — `physical/load_data_planner.rs`

`plan_load_data(session_state, node)`:

1. `IcebergWriteOptions::resolve`; `IcebergTableFormat::parse_table_url([target_location])`;
   `PlannerContext::new(...)`.
2. `operation = if node.overwrite() { Operation::Overwrite } else { Operation::Append }`.
3. Requirements for the commit: `[LastAssignedFieldIdMatch { metadata.last_column_id },
   CurrentSchemaIdMatch { metadata.current_schema_id }]`.
4. `table_properties` flattened from the `TablePropertyList` option layers.
5. `table_arrow_schema = iceberg_schema_to_arrow(current_schema)`; `spec_id` from the
   default partition spec; `partitioned = default_spec.fields() is non-empty`.
6. Resolve the **source** object store from the source URL (any bucket) — globs truncated
   at the first `*` so the prefix parses as a valid URL.
7. `classify_source_files(source_store, &source_url, node.location(), table_schema,
   &table_arrow_schema, spec_id, allow_fast = !partitioned)`.
8. `partition_columns` from the default spec (`catalog_partition_field_from_iceberg`).

**Fast path only** (`fallback_files.is_empty()`):
- `IcebergLoadDataFastExec::new(fast_files, table_url, operation, requirements,
  table_properties, lakehouse_table, total_rows)` → `IcebergCommitExec` (no coalesce —
  the fast exec is single-partition).

**Mixed path**:
- one `IcebergLoadDataFastExec` branch (if any fast files, with `fast_rows` sum);
- one `IcebergWriterExec` branch **per fallback format group**;
- `UnionExec(branches)` → `CoalescePartitionsExec` → `IcebergCommitExec`.

`group_by_format(files)`: groups `(url, size)` by extension
(`csv` | `json`/`jsonl` → json | `parquet` → parquet | default csv).

`build_fallback_scan(session_state, files, format, table_schema)`:
- object-store URL derived from the first file (`..BeforePath`);
- one `PartitionedFile` **per file** (per-file parallelism), paths relative to the store,
  real sizes; `FileGroup::new` per file;
- `CsvSource` (with `has_header`), `JsonSource`, or `ParquetSource`, all with the table's
  Arrow schema;
- `infer_source_compression` (`.gz|.bz2|.xz|.zst`, case-insensitive) →
  `FileCompressionType`.

Test: `infers_compression_from_extension`.

---

## 7. Executor — `physical_plan/load_data_exec.rs`

`IcebergLoadDataFastExec` — a no-child source plan:
- fields: `data_files: Vec<DataFile>`, `table_url`, `operation`, `requirements`,
  `table_properties`, `lakehouse_table`, `reported_row_count`, `cache`.
- `execute(partition 0)`: builds `CommitMeta { table_uri, row_count: sum(record_count),
  operation, requirements, table_properties, lakehouse_table, schema: None,
  partition_spec: None, touched_file_paths: vec![], overwrite_predicate: None,
  overwrite_partition_values: None }`; emits `encode_add_data_files(data_files)` +
  `encode_commit_meta(meta)` concatenated into one action-schema batch.
- `DisplayAs` → `IcebergLoadDataFastExec(table_path=..., files=...)`.

---

## 8. Commit-count plumbing (`commit/commit_exec.rs`)

- `accumulate_action_batches` **sums** the `row_count` across every commit-meta batch so
  a LOAD DATA union (fast branch + one or more rewrite writers) reports the correct
  total; the last commit-meta wins for the other fields.
- `reported_row_count` (from the exec, here `None`) overrides the sum when set.
- Tests: `count_sums_multiple_commit_meta` (fast 3 + writer 2 → 5), `count_single_commit_meta`.

---

## 9. Codec (`proto/codec.rs`, `physical.proto`)

```proto
message IcebergLoadDataFastExecNode {
  string data_files_json = 1;      // Vec<DataFile>, JSON
  string table_url = 2;
  string operation = 3;            // Operation, JSON
  string requirements_json = 4;    // Vec<TableRequirement>, JSON
  string table_properties_json = 5;// Vec<(String,String)>, JSON
  string lakehouse_table_json = 6; // empty = None
  uint64 reported_row_count = 7;
}
// NodeKind: IcebergLoadDataFastExecNode iceberg_load_data_fast = 55;
```

Encode/decode round-trip test included.

---

## 10. Wiring

- `sail-logical-plan/src/lib.rs`: `pub mod load_data;`
- `sail-plan/src/resolver/command/mod.rs`: `mod load;` +
  `CommandNode::LoadData { local, location, table, overwrite, partition } =>
  resolve_command_load_data(...)`.
- `sail-iceberg/src/operations/mod.rs`: `pub mod parquet_utils;`
- `sail-iceberg/src/physical/mod.rs`: `pub mod load_classifier; pub mod load_data_planner;`
- `sail-iceberg/src/physical_plan/mod.rs`: `pub mod load_data_exec;` +
  `pub use load_data_exec::IcebergLoadDataFastExec;`
- `table_scan_planner.rs`: `LoadDataNode` → `plan_load_data`.

---

## 11. Contracts / limitations to preserve

- v1 supports **remote object-store paths only** (LOCAL rejected) and **no PARTITION
  clause**.
- Only **Iceberg** targets.
- Fast path requires: `.parquet` extension, unpartitioned table, and a name+type
  schema match (extra file columns dropped at registration). Partitioned tables always
  rewrite (partition values computed by the writer).
- The source store is resolved from the **source** URL, not the table's store.
- The commit requirements guard schema/field-id drift concurrently with the load.
