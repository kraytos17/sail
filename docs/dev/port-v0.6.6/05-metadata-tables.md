# 05 — Iceberg Metadata Tables (`db.table.snapshots` / `db.table.refs`)

> Read-only Iceberg *metadata tables*: `SELECT` against `<db>.<table>.snapshots` and
> `<db>.<table>.refs` materializes the base table's metadata as rows. Mirrors Apache
> Iceberg's `RefsTable` / `SnapshotsTable`.

Files:
- `crates/sail-iceberg/src/datasource/metadata_table.rs` (new, 595 lines incl. tests)
- `crates/sail-common-datafusion/src/catalog/iceberg.rs` — `IcebergMetadataTableType`
- `crates/sail-common-datafusion/src/datasource.rs` — `SourceInfo.metadata_table`
- `crates/sail-common-datafusion/src/catalog/mod.rs` — re-export
- `crates/sail-plan/src/resolver/query/read.rs` (+106) — `try_resolve_iceberg_metadata_table`
- `crates/sail-iceberg/src/table_format.rs` — `build_iceberg_metadata_source`
- `crates/sail-iceberg/src/datasource/mod.rs` — `pub mod metadata_table;`

---

## 1. `IcebergMetadataTableType` — `catalog/iceberg.rs`

```rust
pub enum IcebergMetadataTableType {
    Snapshots,   // db.table.snapshots
    Refs,        // db.table.refs
}

impl IcebergMetadataTableType {
    pub fn from_name(name: &str) -> Option<Self>  // case-insensitive: "snapshots"|"refs"
}
impl fmt::Display for IcebergMetadataTableType { ... }
```

Re-exported as `pub use iceberg::IcebergMetadataTableType;` from `catalog/mod.rs`.

---

## 2. `SourceInfo.metadata_table`

```rust
pub struct SourceInfo {
    ...
    /// When set, this `SourceInfo` describes a read of an Iceberg *metadata table*
    /// (e.g. `db.table.refs` / `db.table.snapshots`) instead of the table's data.
    /// The base table is described by `paths` / `lakehouse_table` / `options`.
    pub metadata_table: Option<IcebergMetadataTableType>,
    ...
}
```

All existing `SourceInfo` constructions set `metadata_table: None` (the resolver builds
`Some` only for metadata-table reads).

---

## 3. Resolver — `sail-plan/src/resolver/query/read.rs`

`resolve_query_plan` → `ReadNamedTable` path:
1. Before normal table resolution, calls
   `try_resolve_iceberg_metadata_table(&name, state)`.
2. If it returns a plan, that plan is used; otherwise the normal read proceeds.

`try_resolve_iceberg_metadata_table`:
- `parts = name.parts()`; `parts.last()` matched against
  `IcebergMetadataTableType::from_name` — no match → `Ok(None)`.
- Requires `base_parts.len() >= 2` (`db.table.<meta>` minimum).
- Resolves the base table via `CatalogManager::get_table_or_view`; any error → `Ok(None)`.
- Must be `TableKind::Table { format: "iceberg", location: Some, properties, .. }`; else
  `Ok(None)`.
- `resolve_lakehouse_table_context(&base_reference, LakehouseOperation::Read,
  Some(format), vec![])`.
- Builds a read plan carrying `SourceInfo { paths: [location], ..., metadata_table:
  Some(metadata_type) }` and returns `Ok(Some(plan))`.

---

## 4. Format layer — `table_format.rs`

`IcebergTableFormat::create_source(ctx, info)`:
```rust
if info.metadata_table.is_some() {
    return build_iceberg_metadata_source(ctx, info).await;
}
// else the normal data provider
```

`build_iceberg_metadata_source(ctx, info)`:
1. `validate_iceberg_read_lakehouse_context(lakehouse_table)`:
   - `validate_iceberg_lakehouse_storage_access` (warns on remote signing / vended
     credentials — not implemented);
   - rejects `ScanAuthority::IcebergRestServerSide`:
     `"Iceberg REST catalog table {..} requires server-side scan planning, which is not implemented yet"`.
2. `table_url = parse_table_url(paths)`.
3. `metadata_location = metadata_location_from_options(&options)`; only honored for
   catalog-managed tables (`catalog_managed_iceberg_from_options`).
4. `Table::load_with_metadata_location(ctx, table_url, metadata_location)`.
5. `IcebergMetadataTableProvider::new(table_url, metadata.clone(), metadata_type)`
   → `provider_as_source(provider)`.

---

## 5. Provider — `datasource/metadata_table.rs`

`IcebergMetadataTableProvider` — a read-only `TableProvider`:

- `new(table_uri, metadata: TableMetadata, metadata_type)`; fixed Arrow schema per type.
- `schema()`; `table_type() = TableType::Base`.
- `supports_filters_pushdown` → all `TableProviderFilterPushDown::Unsupported` (tiny
  tables; predicates applied above the scan).
- `scan(...)` → `build_batch()` (a single `RecordBatch`) →
  `MemorySourceConfig::try_new(&[vec![batch]], self.schema(), projection.cloned())` →
  `DataSourceExec`.

### 5.1 Snapshots table

Schema:
| column | type |
|---|---|
| `committed_at` | `Timestamp(Microsecond, None)` (not null) — `timestamp_ms() * 1000` |
| `snapshot_id` | `Int64` (not null) |
| `parent_id` | `Int64` (null) |
| `operation` | `Utf8` (null) — `summary().operation` |
| `manifest_list` | `Utf8` (null when empty) |
| `summary` | `Map<string,string>` (null) — operation always present as a key, plus all `additional_properties` |

Built by `snapshots_batch` + `build_summary_map` (MapArray from parallel keys/values +
offsets).

### 5.2 Refs table

Schema:
| column | type |
|---|---|
| `name` | `Utf8` (not null) |
| `type` | `Utf8` (not null) — "branch"/"tag" |
| `snapshot_id` | `Int64` (not null) |
| `max_reference_age_in_ms` | `Int64` (null) — `max_ref_age_ms()` |
| `min_snapshots_to_keep` | `Int32` (null) |
| `max_snapshot_age_ms` | `Int64` (null) — `max_snapshot_age_ms()` |

Populated from `metadata.refs` → `SnapshotReference.retention`
(`SnapshotRetention::Branch` / `Tag`).

### 5.3 Tests

Unit tests materialize batches for both metadata types and assert columns/rows
(snapshots row fields, refs branch/tag rows with retention columns).

---

## 6. Spec accessors required by this feature

`sail-iceberg/src/spec/snapshots/snapshot.rs` (+30) adds:

```rust
impl SnapshotReference {
    pub fn min_snapshots_to_keep(&self) -> Option<i32>;  // Branch retention
    pub fn max_snapshot_age_ms(&self) -> Option<i64>;     // Branch retention
    pub fn max_ref_age_ms(&self) -> Option<i64>;          // Branch + Tag retention
}
```

`sail-iceberg/src/spec/metadata/table_metadata.rs` (+7) adds:

```rust
impl TableMetadata { pub fn snapshot(&self, snapshot_id: i64) -> Option<&Snapshot>; }
```

---

## 7. Wiring

- `sail-iceberg/src/datasource/mod.rs`: `pub mod metadata_table;`
- `sail-iceberg/src/table_format.rs`: `create_source` metadata branch + the
  `build_iceberg_metadata_source`/`validate_*` helpers.
- `sail-common-datafusion/src/catalog/iceberg.rs` + `mod.rs`: the type + re-export.
- `sail-plan/src/resolver/query/read.rs`: the resolver hook.

---

## 8. Behavior contracts to preserve

- Only `snapshots` and `refs` metadata tables are supported in v1 (`from_name` returns
  `None` for anything else — no error).
- A metadata-table read requires the base table to be an **Iceberg table with a
  location**; anything else silently falls through to normal resolution (then a normal
  "table not found"-style error).
- No filter/limit pushdown; the whole table is one Arrow batch.
- The `summary` column is a real Arrow `Map<string,string>`.
