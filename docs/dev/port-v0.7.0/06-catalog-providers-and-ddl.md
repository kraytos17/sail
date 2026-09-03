# Porting feat/0.7.0 → feat/0.7.1 — Doc 06: Catalog DDL & Providers — ALTER TABLE, DESCRIBE, SHOW TBLPROPERTIES, CALL, CREATE OR REPLACE

> Part of the `docs/dev/port-v0.7.0/` inventory. Documents the **catalog command layer**
> (`sail-catalog`) and **catalog provider** deltas on `feat/0.7.0` vs base `f0b137d6`:
> the expanded ALTER TABLE operation set, catalog-authority Iceberg REST DDL, DESCRIBE
> column/extended, SHOW TBLPROPERTIES, CALL procedure dispatch, CREATE OR REPLACE on REST
> catalogs, and the shared `TableFormat`/datasource contracts these ride on. Also covers
> the shared-datafusion `IcebergMetadataTableType` and the `SourceInfo.metadata_table`
> mechanical field.
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`. Sibling docs: 05 (frontend spec), 07
> (row-level ops incl. the empty-table DELETE on catalog-managed Iceberg), 09
> (procedure/metadata-table/GC execution).

---

## 1. Files

| File | Change |
|---|---|
| `sail-catalog/src/provider/options.rs` | `AlterTableOptions` +6 variants; new `AddColumn`, `CallProcedureOptions` |
| `sail-catalog/src/command.rs` | `CatalogCommand::CallProcedure`, `ShowTblProperties`, `DescribeTable.column`; execute() logic incl. catalog-authority ALTER delegation + CALL dispatch; row schemas; mapping fns; tests |
| `sail-catalog/src/error.rs` | `CatalogObject::Column` |
| `sail-catalog-iceberg/src/provider.rs` | REST provider `alter_table`, `create_table` OR-REPLACE semantics, managed-location create, access-delegation; +~380 LOC of impl helpers + mocks/tests |
| `sail-catalog-iceberg/src/lib.rs` | re-exports `IcebergRestAccessDelegation` |
| `sail-catalog-iceberg/tests/rest_integration_test.rs` | options gain `access_delegation` |
| `sail-catalog-memory/src/provider.rs` (+Cargo dep on `sail-common`) | memory `RenameTable` implemented; other new ops → NotSupported |
| `sail-catalog-hms/src/provider.rs` | new ops → NotSupported errors |
| `sail-catalog-glue/src/managed_table.rs` | new ops → NotSupported errors |
| `sail-catalog-onelake/src/provider.rs` | REST provider construction passes `access_delegation: default` |
| `sail-common-datafusion/src/datasource.rs` | `SourceInfo.metadata_table`; `UpdateInfo`/`UpdateAssignment`; `TableFormatAlterTableOperation` new variants; `TableFormatProcedureOperation`; `TableFormat::create_updater` + `call_procedure` (default impls) |
| `sail-common-datafusion/src/catalog/iceberg.rs` (+mod.rs) | `IcebergMetadataTableType` (`Snapshots`/`Refs`) |
| `sail-data-source/src/formats/rate/mod.rs`, `socket/mod.rs`, `listing/source.rs`, `sail-delta-lake/src/table_format.rs` | `metadata_table: _` destructure updates; Delta `alter_table` arm additions for new variants |

---

## 2. Catalog option types (`sail-catalog/src/provider/options.rs`)

`AlterTableOptions` (already `Hash/Ord`/serde) gains:

```rust
RenameTable { new_name: Vec<String> },
AddColumns   { columns: Vec<AddColumn> },
DropColumns  { names: Vec<String>, if_exists: bool },
AlterColumnComment      { name: Vec<String>, comment: Option<String> },
AlterColumnNullability  { name: Vec<String>, nullable: bool },
AlterColumnPosition     { name: Vec<String>, position: sail_common::spec::ColumnPosition },
// pre-existing: SetTableProperties, UnsetTableProperties, AlterColumnDefault, AddCheckConstraint
```

New structs:

```rust
pub struct AddColumn {
    pub name: Vec<String>,          // dotted path parts
    pub data_type: DataType,        // Arrow DataType
    pub nullable: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
}

pub enum CallProcedureOptions {     // procedure-name-specific args only; table is separate
    RollbackToSnapshot { snapshot_id: i64 },
    SetCurrentSnapshot { snapshot_id: Option<i64>, r#ref: Option<String> },
    ExpireSnapshots   { older_than_ms: Option<i64>, retain_last: Option<i32> },
}
```

---

## 3. `CatalogCommand` additions & execute() behavior (`sail-catalog/src/command.rs`)

### 3.1 New/changed variants

```rust
AlterTable { table: Vec<String>, if_exists: bool, options: AlterTableOptions },  // pre-existing
CallProcedure { table: Vec<String>, procedure: CallProcedureOptions },           // NEW
DescribeTable { table: Vec<String>, extended: bool, column: Option<String> },    // + column
ShowTblProperties { table: Vec<String>, property_key: Option<String> },          // NEW
```

All get `operation_name()` labels, output-schema arms, and an `execute()` branch. New row
structs (`Serialize/Deserialize` via `ArrowSerializer`): `ShowTblPropertiesRow { key, value }`.

### 3.2 ALTER TABLE execution — the storage-first vs catalog-authority split

`execute()` for `AlterTable`:
- Computes `table_format_alter_operation(&options)` → `TableFormatAlterTableOperation`
  (see §4), resolves the lakehouse execution context, and then branches:
  - **Catalog-authority Iceberg**: `is_catalog_authority_iceberg_alter(&format, lakehouse_table.commit)`
    i.e. `format == "iceberg" (case-insensitive) && commit == CommitAuthority::IcebergRestCommit`.
    Then `manager.alter_table(&table, options)` (the REST provider) and return the single
    `true`-bool batch. Comment: the storage-first + catalog-sync flow would otherwise
    re-apply the op (e.g. add the column twice) and fail its `Assert*` requirements — the
    REST provider issues one atomic `update_table` commit (rename / set+unset properties /
    add / drop columns).
  - **Otherwise** (filesystem-authority Iceberg, Delta, listing formats): storage-first
    `table_format.alter_table(runtime, location, storage_operation, Some(lakehouse_table))`,
    then the existing catalog-sync path (`catalog_sync_alter_options`) updates the catalog
    registration.

### 3.3 CALL procedure execution

`CallProcedure { table, procedure }`:
1. `manager.get_table_or_view(&table)` → require `TableKind::Table { location: Some(loc), format }`
   (else `NotSupported`: "CALL procedures are only supported for tables with a location" /
   "…on tables").
2. Require `format.eq_ignore_ascii_case("iceberg")` → else
   `NotSupported("CALL procedures are only supported for Iceberg tables, got '{format}'")`.
3. `ctx.extension::<TableFormatRegistry>()` → `registry.get(&format)`.
4. `manager.resolve_lakehouse_table_status(&table, &table_status, LakehouseOperation::Maintenance).execution`.
5. `table_format.call_procedure(runtime, &location, table_format_procedure_operation(&procedure), Some(lakehouse_table))`
   → returns the result `RecordBatch` (doc 09 owns the implementation and `CallProcedureOutput`).

### 3.4 DESCRIBE TABLE (`column` support + extended)

- New `column: Option<String>`: when present, look up the single column by name
  (`CatalogError::NotFound(CatalogObject::Column, name)` when missing) and emit one row.
- When absent, the pre-existing behavior runs, now restructured: base column rows; if
  `extended`, the `# Partition Information`/`# col_name` header rows (when partition columns
  exist) and then the `# Detailed Table Information` key/value rows from
  `table_status.describe_extended_metadata()`.
- New unit tests: `test_describe_table_with_column`, `test_describe_table_with_missing_column`.

### 3.5 SHOW TBLPROPERTIES

`ShowTblProperties { table, property_key }`:
- `manager.get_table_or_view`; views rejected (`NotSupported`).
- With a key: single matching row (or empty when absent). Without: all rows.
- Rows are **sorted by key** (`rows.sort_by(|a,b| a.key.cmp(&b.key))`).

### 3.6 Mapping helpers

- `table_format_alter_operation(options) -> TableFormatAlterTableOperation` — new arms:
  - `RenameTable` → `..::RenameTable`
  - `AddColumns { columns }` → `..::AddColumns { columns: Vec<TableFormatCreateTableColumn> }`
    (maps `AddColumn` → `TableFormatCreateTableColumn { name: name.join("."), data_type,
    nullable, comment, default, generated_always_as: None, identity: None }`)
  - `DropColumns { names, if_exists }` → `..::DropColumns`
  - `AlterColumnComment/Nullability/Position` → respective format op (position carries
    `sail_common::spec::ColumnPosition`)
- `table_format_procedure_operation(options: &CallProcedureOptions) -> TableFormatProcedureOperation`
  (mirror of the catalog→storage bridge).
- `call_procedure_schema(procedure) -> SchemaRef` — the fixed output schema per procedure:
  - `RollbackToSnapshot`/`SetCurrentSnapshot`: `previous_snapshot_id int64`,
    `current_snapshot_id int64`.
  - `ExpireSnapshots`: six int64 count columns (`deleted_data_files_count`,
    `deleted_position_delete_files_count`, `deleted_equality_delete_files_count`,
    `deleted_manifest_files_count`, `deleted_manifest_lists_count`,
    `deleted_statistics_files_count`) — matches `CallProcedureOutput` (doc 09).
- `is_catalog_authority_iceberg_alter(format, commit) -> bool` + unit test
  `detects_catalog_authority_iceberg_alter`.

---

## 4. Shared `TableFormat` contract (`sail-common-datafusion/src/datasource.rs`)

### 4.1 `SourceInfo.metadata_table`

New field:

```rust
pub metadata_table: Option<IcebergMetadataTableType>,
```

"When set, this `SourceInfo` describes a read of an Iceberg *metadata table* (e.g.
`db.table.refs`/`db.table.snapshots`) instead of the table's data." This is a **mechanical
compile-time ripple**: every exhaustive `SourceInfo` destructure must add `metadata_table: _`.
In this delta: `RateTableFormat`, `SocketTableFormat`, `ListingTableFormat<T>`,
`DeltaTableFormat` (three arms), plus `sail-iceberg` provider/table_format destructures
(doc 09). Do not port these as separate commits — fold into the metadata-tables work
(doc 09).

### 4.2 UPDATE support types

```rust
pub struct UpdateInfo {
    pub table_name: Vec<String>,
    pub path: String,
    pub target: Arc<LogicalPlan>,        // resolved logical target scan
    pub condition: Option<ExprWithSource>,
    pub assignments: Vec<UpdateAssignment>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    pub options: Vec<OptionLayer>,
}
pub struct UpdateAssignment { pub column_path: Vec<String>, pub expression: Expr }
```

And on the trait (default returns `not_impl_err!("UPDATE is not yet implemented for …")`):

```rust
async fn create_updater(&self, ctx: &dyn Session, info: UpdateInfo) -> Result<LogicalPlan>;
```

### 4.3 `TableFormatAlterTableOperation` new variants

`RenameTable`, `AddColumns { columns: Vec<TableFormatCreateTableColumn> }`,
`DropColumns { names: Vec<String>, if_exists: bool }`,
`AlterColumnComment { column_path, comment }`, `AlterColumnNullability { column_path, nullable }`,
`AlterColumnPosition { column_path, position: spec::ColumnPosition }` added alongside the
existing property/column-default ops. The trait's default `alter_table` dispatches them to
`not_impl_err!` (except `RenameTable` → `Ok(())` — "For Iceberg, this is a no-op at the
storage level (metadata path stays the same); the catalog handles the name change").

### 4.4 Procedures

```rust
pub enum TableFormatProcedureOperation {
    RollbackToSnapshot { snapshot_id: i64 },
    SetCurrentSnapshot { snapshot_id: Option<i64>, r#ref: Option<String> },
    ExpireSnapshots   { older_than_ms: Option<i64>, retain_last: Option<i32> },
}
// trait default:
async fn call_procedure(&self, runtime_env, path, operation, lakehouse_table) -> Result<RecordBatch>;
```

### 4.5 `IcebergMetadataTableType` (`catalog/iceberg.rs`, re-exported from `catalog/mod.rs`)

```rust
pub enum IcebergMetadataTableType { Snapshots, Refs }   // Copy/Hash/Eq
impl { pub fn from_name(name: &str) -> Option<Self> }   // case-insensitive; None for unknown
impl Display  // "snapshots" / "refs"
```

Used at name-resolution time as a cheap case-insensitive detector (`db.table.snapshots`,
`db.table.refs`). Implemented/produced in `sail-iceberg` (doc 09).

---

## 5. Iceberg REST catalog provider DDL (`sail-catalog-iceberg/src/provider.rs`)

### 5.1 `IcebergRestCatalogOptions` + access delegation

`IcebergRestCatalogOptions` gains `access_delegation: IcebergRestAccessDelegation`
(default `VendedCredentials`). In `load_table_result(...)` for table loads, the
`X-Iceberg-Access-Delegation: vended-credentials` header is sent **only** when delegation is
`VendedCredentials` (`REST_ACCESS_DELEGATION_VENDED_CREDENTIALS`); `None` omits it. (The old
code always sent vended-credentials.)

### 5.2 `create_table` — CREATE / CREATE OR REPLACE / REPLACE + managed location

- Unchanged early branch for `ignore_if_exists`/already-existing handling (`CreateIfNotExists`
  still short-circuits to the existing table).
- For `mode.is_replace()` (previously `NotSupported`), it now probes the current registration
  with `self.get_table(...)` (treating `NotFound` as None) and applies Spark + in-repo
  `MemoryCatalogProvider` semantics:
  - `Create` on existing → `AlreadyExists`.
  - `CreateIfNotExists` on existing → handled earlier.
  - `CreateOrReplace`/`Replace` on existing → `drop_table(db, table, DropTableOptions { if_exists: true, purge: true })`
    then recreate.
  - `Replace` on missing → `NotFound`.
  - Missing + `Create`/`CreateOrReplace` → proceed.
- **Managed location**: the REST `CreateTableRequest.location` is sent only when
  `is_external` is true; for managed tables the request location is `None` so the catalog
  autogenerates a location under its storage (a planner-computed default would be sent to a
  remote authority that cannot resolve it). External tables (user-specified location) keep
  forwarding the location.
- New tests (wiremock): `create_table_create_or_replace_on_missing_table_creates`,
  `create_table_create_or_replace_on_existing_table_drops_then_creates`,
  `create_table_replace_on_missing_table_returns_not_found`,
  `create_table_create_on_existing_table_returns_already_exists`.

### 5.3 `alter_table` — full REST DDL

Implemented against the REST `update_table` endpoint (replacing the blanket `NotSupported`).
Dispatches:

- **RenameTable { new_name }** → `POST /tables/rename` with source = `{namespace: db, name:
  table}` and destination resolved from the new name parts: single-part → same namespace;
  multi-part → namespace = all-but-last parts, name = last. Retried with auth
  (`with_auth_retry`); non-2xx → `CatalogError::External("Failed to rename table: …")`.
- **SetTableProperties / UnsetTableProperties** → `alter_table_properties`:
  1. load current metadata; `properties.unwrap_or_default()`;
  2. skip keys rejected by `is_reserved_iceberg_table_property`;
  3. for removals when `!if_exists && !current.contains(key)` →
     `CatalogError::InvalidArgument("cannot remove property '{key}' because it is not set on
     the table")`;
  4. build `TableUpdate::SetProperties`/`RemoveProperties` (skip if empty);
  5. commit via `commit_alter_table_updates` with
     `TableRequirement::AssertTableUuid`.
- **AddColumns { columns }** → `alter_table_add_columns`:
  loads current schema (via `find_by_id_or_last`), starts from `last_column_id+1`, converts
  each Arrow `DataType` via `arrow_type_to_iceberg`, appends `NestedField::optional(id,
  name.join("."), ty)` (with doc when comment present); new schema id = max+1; commits
  `AddSchema { schema, last_column_id: Some(next_id-1) }` + `SetCurrentSchema` with
  `AssertTableUuid` + `AssertCurrentSchemaId` + `AssertLastAssignedFieldId { last_assigned_field_id: last_column_id }`.
- **DropColumns { names, if_exists }** → `alter_table_drop_columns`:
  removes matching top-level fields by name (position-based); missing column + `!if_exists` →
  `InvalidArgument("Column '{name}' not found in Iceberg table schema")`; when nothing was
  removed returns Ok **without committing** (schema unchanged); removes dropped ids from
  `identifier_field_ids`; commits the new schema with the same three assert requirements
  (`AddSchema` keeps `last_column_id` at the old value).
- Anything else → `NotSupported("alter table in Iceberg catalog")`.

`commit_alter_table_updates` maps REST errors to `CatalogError`: 404 → `NotFound(Table,
db.table)`; 409 → `Conflict`; 401 → `Unauthorized`; 403 → `Forbidden`; 429 → `RateLimited`;
other with status → `External("Failed to alter Iceberg table …")`; no status →
`External("Failed to commit table")`. (Names quoted with `quote_namespace_if_needed` /
`quote_name_if_needed`.)

### 5.4 New helper tests/utilities

`gen_struct_field_to_nested_field` converts a REST `StructField` to a
`sail_iceberg::spec::NestedField`. Test mocks: `mock_load_table`, `mock_load_table_with_schema`,
`create_table_response`, `mock_get_table_404`. New tests: `test_alter_table_rename` (same +
cross-namespace; asserts the exact rename JSON body incl. prefix behavior),
`test_alter_table_set_properties`, `test_alter_table_unset_properties`,
`test_alter_table_unset_properties_missing_key` (InvalidArgument),
`test_alter_table_add_columns` (asserts full add-schema JSON incl. new field + last-column-id),
`test_alter_table_drop_columns`, `test_alter_table_drop_columns_missing_key`,
`test_alter_table_drop_columns_if_exists_missing_key` (no commit POST → proves no-op).

---

## 6. Other providers

- **Memory** (`sail-catalog-memory`): `RenameTable` implemented in place (`db.tables.remove`
  then reinsert under `new_name.last()`; `NotFound` when the old table is missing;
  `InvalidArgument` when new name is empty). `AddColumns`/`DropColumns`/`AlterColumnComment`/
  `AlterColumnNullability`/`AlterColumnPosition` → `NotSupported` messages. New dependency:
  `sail-common` (for `ColumnPosition`).
- **HMS** and **Glue**: the new `AlterTableOptions` variants each return explicit
  `NotSupported` strings (no silent ignore) in `apply_alter_table_options`.
- **OneLake**: REST-backed path constructs `IcebergRestCatalogOptions` with
  `access_delegation: IcebergRestAccessDelegation::default()`.
- **Catalog error**: `CatalogObject::Column` added (used by DESCRIBE-column 404 and the
  column lookups).

---

## 7. Delta `alter_table` arm updates (`sail-delta-lake/src/table_format.rs`)

`DeltaTableFormat::alter_table` matches the new variants: `RenameTable => Ok(())`,
`AddColumns/DropColumns/AlterColumnComment/AlterColumnNullability/AlterColumnPosition` →
`not_impl_err!`. `delta_alter_operation_name` gained names for all six (used by
`reject_catalog_managed_delta_alter`). (`metadata_table: _` destructure additions at the three
SourceInfo sites.)

---

## 8. Port notes / risks

1. **Ripple risk**: `SourceInfo.metadata_table` touches every format's `create_reader` /
   destructure sites; 0.7.1 may have extra/newer destructure sites (e.g. newer formats or
   refactors) — grep `SourceInfo {` exhaustively rather than diff-applying.
2. `TableFormatAlterTableOperation`/`TableFormatProcedureOperation`/`UpdateInfo` are shared
   contract types — 0.7.1 may already carry some of these (upstream merged ALTER/DELETE
   work); check before adding to avoid duplicate variants.
3. The `sail-common` dependency added to `sail-catalog-memory` and the re-export of
   `IcebergRestAccessDelegation` from `sail-catalog-iceberg` are small but required for the
   config option to compile end-to-end.
4. Catalog-authority ALTER delegation (`is_catalog_authority_iceberg_alter`) must match
   0.7.1's `CommitAuthority` naming (`IcebergRestCommit`) — verify the enum on 0.7.1.
5. New wiremock tests rely on `mount` helpers already in the file; port the helper additions
   (`mock_load_table*`, `create_table_response`, `mock_get_table_404`) with the tests.
