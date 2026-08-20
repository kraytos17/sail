# 06 — Catalog DDL Commands & Catalog Providers

> DESCRIBE TABLE/VIEW, SHOW TBLPROPERTIES, ALTER TABLE (storage-level **and**
> catalog-level), plus the Iceberg REST / Memory / Glue / HMS / OneLake provider
> implementations behind them.

Files:
- `crates/sail-catalog/src/command.rs` (+235) — command dispatch + execution
- `crates/sail-catalog/src/error.rs` (+2)
- `crates/sail-catalog/src/provider/options.rs` (+32) — `access_delegation`
- `crates/sail-catalog-iceberg/src/provider.rs` (+1096) — the big one
- `crates/sail-catalog-iceberg/src/lib.rs` — `IcebergRestAccessDelegation` re-export
- `crates/sail-catalog-iceberg/tests/rest_integration_test.rs` — integration tests
- `crates/sail-catalog-memory/src/provider.rs` (+111) — rename support
- `crates/sail-catalog-glue/src/managed_table.rs`, `crates/sail-catalog-hms/src/provider.rs`,
  `crates/sail-catalog-onelake/src/provider.rs` — alter stubs
- `crates/sail-iceberg/src/table_format.rs` (+725) — storage-level alter + trait impls
- `crates/sail-iceberg/src/datasource/type_converter.rs` (+32) — `arrow_type_to_iceberg`
- `crates/sail-common/src/spec/plan.rs` — spec alter types (see `01`)

---

## 1. `crates/sail-catalog/src/command.rs`

### 1.1 New command variant

```rust
CatalogCommand::ShowTblProperties {
    table: Vec<String>,
    property_key: Option<String>,
}
```

- `name()` → `"ShowTblProperties"`.
- Schema → `ArrowSerializer::default().schema::<ShowTblPropertiesRow>()`.
- Execution:
  - `manager.get_table_or_view(&table)`; must be a **table** (not a view) →
    `NotSupported("SHOW TBLPROPERTIES is not supported for views")`.
  - rows = all `(key, value)` properties, filtered by `property_key` (exact match) when
    provided; **sorted by key**; `ShowTblPropertiesRow { key, value }` (new struct).

### 1.2 `DescribeTable` gains a column

```rust
CatalogCommand::DescribeTable { table, extended, column: Option<String> }
```

- With `column`: find `col.name == column_name` → `NotFound(CatalogObject::Column, name)`
  when missing; emit a single `DescribeTableRow`.
- Without: existing column list; when `extended`, after columns the output appends:
  - `# Partition Information` + `# col_name` / `data_type` / `comment` header + partition
    columns;
  - a blank row;
  - `# Detailed Table Information` + `table_status.describe_extended_metadata()` rows.
  (Structure matches Spark's `DESCRIBE EXTENDED`.)

### 1.3 ALTER TABLE routing (catalog-owned vs storage)

In `CatalogCommand::AlterTable` execution, after resolving the lakehouse table:

```rust
if lakehouse_table.commit == CommitAuthority::IcebergRestCommit {
    manager.alter_table(&table, options).await?;      // catalog-owned metadata
    return Ok(display.bools().to_record_batch(vec![true])?);
}
table_format.alter_table(runtime, &location, storage_operation, Some(lakehouse_table)).await?;
```

So Iceberg-REST-managed tables (e.g. Polaris) mutate **through the catalog provider**;
filesystem tables mutate storage metadata through the format layer.

### 1.4 `table_format_alter_operation(options)` mapping

New mappings from `AlterTableOptions` → `TableFormatAlterTableOperation`:
- `RenameTable` → `RenameTable`.
- `AlterColumnComment` / `AlterColumnNullability` / `AlterColumnPosition` →
  the matching `AlterColumn*` variants.
- `AddColumns { columns }` → `AddColumns { columns: Vec<TableFormatCreateTableColumn> }`
  (converts each catalog `AddColumn`).
- `DropColumns { names, if_exists }` → `DropColumns { names, if_exists }`.
- Existing mappings (`SetTableProperties`, `UnsetTableProperties`, `SetTableLocation`,
  `AddCheckConstraint`, ...) unchanged.

### 1.5 Resolver-side mapping — `sail-plan/src/resolver/command/catalog/table.rs`

`resolve_catalog_alter_table` converts `spec::AlterTableOperation` → the catalog's
`AlterTableOptions` (used before the `CatalogCommand::AlterTable`/`manager.alter_table`
dispatch):
- `RenameTable { new_name }` → `AlterTableOptions::RenameTable { new_name: new_name.into() }`.
- `AddColumns { items }` → `AlterTableOptions::AddColumns`; each `ColumnDefinition` is
  resolved into a catalog `AddColumn` (`name`, `data_type` via
  `self.resolve_data_type`, `nullable`, `default`, `comment`).
- `DropColumns { names, if_exists }` → `AlterTableOptions::DropColumns` (multi-part names
  joined with `.`).
- `AlterColumnComment` / `AlterColumnNullability` / `AlterColumnPosition` → the
  matching `AlterTableOptions::AlterColumn*`.

### 1.6 `crates/sail-catalog/src/error.rs` (+2)

`CatalogError::NotFound(CatalogObject::Column, name)` path + any additional helpers used
by describe/alter.

---

## 2. Catalog provider options

`crates/sail-catalog/src/provider/options.rs` (+32):

- `AlterTableOptions` gains: `RenameTable { new_name: Vec<String> }`, `AddColumns {
  columns: Vec<AddColumn> }`, `DropColumns { names: Vec<String>, if_exists: bool }`,
  `AlterColumnComment { name, comment }`, `AlterColumnNullability { name, nullable }`,
  `AlterColumnPosition { name, position: sail_common::spec::ColumnPosition }`.
- New type:
  ```rust
  pub struct AddColumn {
      pub name: Vec<String>,
      pub data_type: DataType,
      pub nullable: bool,
      pub default: Option<String>,
      pub comment: Option<String>,
  }
  ```
- The Iceberg REST provider's `options` gains `access_delegation: IcebergRestAccessDelegation`.

### 2.1 Access-delegation config (`IcebergRestAccessDelegation`)

`crates/sail-common/src/config/application.rs`:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergRestAccessDelegation {
    #[default]
    VendedCredentials,   // default
    None,
}
```

`crates/sail-catalog-iceberg/src/lib.rs`: `pub use sail_common::config::IcebergRestAccessDelegation;`

`crates/sail-catalog-iceberg/src/provider.rs`:
- `IcebergRestCatalogOptions { credentials, properties, access_delegation: IcebergRestAccessDelegation }`
  (new field).

Wiring — `crates/sail-session/src/catalog.rs` (`create_catalog_manager`): the Iceberg
REST catalog config's `access_delegation` is destructured and passed into
`IcebergRestCatalogOptions { ..., access_delegation: access_delegation.clone().unwrap_or_default() }`.

`crates/sail-catalog-memory/Cargo.toml` adds `sail-common = { path = "../sail-common" }`
(needed for the same option type); `crates/sail-iceberg/Cargo.toml` adds
`sail-logical-plan` and `datafusion-datasource` (workspace) dependencies plus a `tokio`
dev-dependency.

---

## 3. Iceberg REST catalog provider — `crates/sail-catalog-iceberg/src/provider.rs`

### 3.1 `CREATE OR REPLACE` / `REPLACE` (was: `"Replace table is not supported yet"`)

In `create_table`:

```rust
let existing = match self.get_table(database, table).await {
    Ok(status) => Some(status),
    Err(CatalogError::NotFound(CatalogObject::Table, _)) => None,
    Err(e) => return Err(e),
};
match (existing, mode) {
    (Some(_), Create)                       => Err(AlreadyExists(Table, table)),
    (Some(_), CreateIfNotExists)            => return Ok(get_table(...)),   // ignore-if-exists
    (Some(_), CreateOrReplace | Replace)    => {
        self.drop_table(database, table, DropTableOptions { if_exists: true, purge: true }).await?;
    }
    (None, Replace)                          => Err(NotFound(Table, table)),
    (None, _)                                => {}
}
```

So `CREATE OR REPLACE` / `REPLACE` **drops the existing registration (metadata + data
with purge)** then recreates — matching Spark semantics and `MemoryCatalogProvider`.
`Replace` requires an existing table; `CreateOrReplace` on a missing table creates it.

**Managed-vs-external location:**

```rust
location: if is_external { location } else { None },
```

- For managed tables the catalog autogenerates the location under its own storage; the
  planner-computed default (e.g. `spark-warehouse/<table>-<uuid>`) is **never** sent to a
  remote authority that cannot resolve it.
- Only a user-specified location (external table) is forwarded.

### 3.2 `alter_table` — full implementation

```rust
match options {
    AlterTableOptions::RenameTable { new_name } => { ... REST rename_table ... }
    AlterTableOptions::SetTableProperties { properties } =>
        self.alter_table_properties(database, table,
            properties.into_iter().map(|(k,v)| (k, Some(v))).collect(), false),
    AlterTableOptions::UnsetTableProperties { keys, if_exists } =>
        self.alter_table_properties(database, table,
            keys.into_iter().map(|k| (k, None)).collect(), if_exists),
    AlterTableOptions::AddColumns { columns } => self.alter_table_add_columns(...),
    AlterTableOptions::DropColumns { names, if_exists } => self.alter_table_drop_columns(...),
    _ => Err(NotSupported("alter table in Iceberg catalog")),   // comment/nullability/position
}
```

- **Rename**: source `TableIdentifier` (database, table) → destination namespace/name
  (`[name]` stays in the same namespace; multi-part moves); REST `rename_table`.
- Error mapping via `map_update_table_alter_error`.

### 3.3 `alter_table_properties`

- `is_reserved_iceberg_alter_property(key)`: `__sail.*` | `metadata.*` |
  metadata-location keys | `previous-metadata-location` | `format-version` | `uuid` |
  `snapshot-count` | `current-snapshot-summary` | `current-snapshot-id` |
  `current-snapshot-timestamp-ms` | `current-schema` | `default-partition-spec` |
  `default-sort-order` — reserved keys are **skipped**, never mutated through ALTER.
- `UNSET` of a missing key without `IF EXISTS` → `InvalidArgument("cannot remove property
  '{key}' because it is not set on the table")`.
- Commits `SetPropertiesUpdate` / `RemovePropertiesUpdate` with
  `TableRequirement::AssertTableUuid { uuid }` via `update_table`.

### 3.4 `alter_table_add_columns`

- Loads current metadata; current schema via
  `find_by_id_or_last(metadata.schemas, current_schema_id, |s| s.schema_id)`.
- For each column: `arrow_type_to_iceberg(&col.data_type)`; `NestedField::optional(next_id,
  name.join("."), field_type)` (+ doc comment); ids from `last_column_id + 1`.
- New schema id = max(existing schema ids) + 1; preserves `identifier_field_ids`.
- Requirements: `AssertTableUuid`, `AssertCurrentSchemaId`, `AssertLastAssignedFieldId`.
- Updates: `AddSchemaUpdate { schema, last_column_id: Some(next_id - 1) }` +
  `SetCurrentSchemaUpdate { schema_id: new_schema_id }`.

### 3.5 `alter_table_drop_columns`

- Removes matching fields; missing column without `IF EXISTS` →
  `InvalidArgument("Column '{name}' not found in Iceberg table schema")`.
- **No-op when nothing changed** (`new_fields == current_schema.fields` → return early;
  never commit an identical schema).
- Drops the removed field ids from `identifier_field_ids`.
- New schema id = max+1; `last_column_id` unchanged.
- Same requirements + `AddSchemaUpdate` + `SetCurrentSchemaUpdate`.

### 3.6 Error mapping — `map_update_table_alter_error` (+ `load_table`)

`map_update_table_alter_error(database, table, e)` maps an Iceberg REST
`update_table` commit error into a `CatalogError`, preserving the server response body:
- HTTP `404` → `CatalogError::NotFound(CatalogObject::Table, "<db>.<table>")`
- HTTP `409` → `CatalogError::Conflict("Iceberg REST catalog commit conflict for <db>.<table>: <body>")`
- HTTP `401` → `CatalogError::Unauthorized("... unauthorized ...")`
- HTTP `403` → `CatalogError::Forbidden("... forbidden ...")`
- HTTP `429` → `CatalogError::RateLimited("... rate limited ...")`
- any other HTTP status → `CatalogError::External("Failed to alter Iceberg table <db>.<table>: status <code>: <body>")`
- non-HTTP errors → `CatalogError::External("Failed to alter table: <e>")`

Names are quoted via `quote_namespace_if_needed` / `quote_name_if_needed`. Separately,
`load_table_result` (the table-load path) gains a catch-all `ResponseError` mapping into
`CatalogError::External("Failed to load table <db>.<table>: server responded with <status>: <body>")`.

### 3.7 Integration tests

`tests/rest_integration_test.rs`:
- `create_table_create_or_replace_on_missing_table_creates`,
  `create_table_create_or_replace_on_existing_table_drops_then_creates`,
  `create_table_replace_on_missing_table_returns_not_found`,
  `create_table_create_on_existing_table_returns_already_exists`,
- `test_alter_table_rename[_impl]`, `test_alter_table_set_properties`,
  `test_alter_table_unset_properties`, `test_alter_table_unset_properties_missing_key`,
- `test_alter_table_add_columns`, `test_alter_table_drop_columns`,
  `test_alter_table_drop_columns_missing_key`, `test_alter_table_drop_columns_if_exists_missing_key`.

---

## 4. Iceberg storage-level alter — `sail-iceberg/src/table_format.rs`

### 4.1 Trait impls

```rust
async fn alter_table(&self, runtime_env, path, operation, lakehouse_table) -> Result<()> {
    reject_catalog_managed_iceberg_alter(lakehouse_table.as_ref(), &operation)?;
    match operation {
        SetTableProperties { changes, if_exists }   => alter_table_properties(...),
        AddColumns { columns }                       => alter_table_add_columns(...),
        DropColumns { names, if_exists }             => alter_table_drop_columns(...),
        AlterColumnComment { column_path, comment }  => alter_table_column_comment(...),
        AlterColumnNullability { column_path, nullable } => alter_table_column_nullability(...),
        AlterColumnPosition { column_path, position }=> alter_table_column_position(...),
        RenameTable                                  => Ok(()),  // catalog-only
        op => not_impl_err!("unsupported Iceberg ALTER TABLE operation: {op:?}"),
    }
}
```

`reject_catalog_managed_iceberg_alter(lakehouse_table, operation)` now **allows
`RenameTable`** (Iceberg has no rename metadata update — the catalog renames) and rejects
the rest for catalog-managed tables.

### 4.2 `retry_metadata_commit` (extracted + generalized)

`alter_table_properties` previously inlined the retry loop. Now:

```rust
pub(crate) async fn retry_metadata_commit<F>(
    object_store: Arc<dyn ObjectStore>,
    store_ctx: &StoreContext,
    table_url: &Url,
    initial_latest_meta: String,
    check_post_write: bool,
    mutate: F,                     // F: Fn(&mut TableMetadata) -> Result<()>
) -> Result<()>
```

- Loop: reload latest meta (reuse `initial_latest_meta` on attempt 1), `mutate(&mut
  table_meta)`, next version = current + 1.
- Pre-write: if `metadata_files_for_version(next)` is non-empty, only **real** conflicts
  (`!is_stale_metadata_file(ts, current_ts)`) retry; stale leftover files are ignored.
- Post-write (when `check_post_write`): same stale-aware conflict check; on conflict
  delete the just-written file and retry.
- Writes the new metadata file + `version-hint.text`.
- Retry cap `MAX_ALTER_TABLE_PROPERTIES_COMMIT_RETRIES`.

`alter_table_properties` now delegates with `mutate = apply_table_property_changes(...)`.

### 4.3 Column alter implementations

`alter_table_add_columns` / `alter_table_drop_columns` / `alter_table_column_comment` /
`alter_table_column_nullability` / `alter_table_column_position`: load latest metadata,
mutate the schema's `NestedField`s (new fields / removed fields / doc / `required` /
reordering via `ColumnPosition`), build a new schema (new id), and commit through
`retry_metadata_commit` with `CurrentSchemaIdMatch` + `LastAssignedFieldIdMatch` guards.

### 4.4 `arrow_type_to_iceberg` — `datasource/type_converter.rs`

Converts an Arrow `DataType` to an Iceberg `Type` (used by the REST ADD COLUMNS path) plus
an `is_utc_timezone` helper.

---

## 5. Other catalog providers

### 5.1 Memory — `crates/sail-catalog-memory/src/provider.rs` (+111)

`alter_table` implements `RenameTable`: validates the new name (`"RENAME TO requires a
valid table name"`), rewrites the in-memory table key to the new name (matching Spark
semantics). Other alter ops rejected.

### 5.2 Glue — `crates/sail-catalog-glue/src/managed_table.rs`

`alter_table` rejects `RenameTable`
(`"AWS Glue catalog does not support ALTER TABLE RENAME TO"`); stubs the column ops
(Add/Drop/comment/nullability/position) as unsupported.

### 5.3 HMS — `crates/sail-catalog-hms/src/provider.rs`

Same shape: `"Hive Metastore catalog does not support ALTER TABLE RENAME TO"` + column-op
stubs.

### 5.4 OneLake — `crates/sail-catalog-onelake/src/provider.rs`

Column-op stubs (add/drop/comment/nullability/position/rename rejected).

### 5.5 Delta Lake — `crates/sail-delta-lake/src/table_format.rs` (+31)

- Adds `metadata_table: _` to the three `SourceInfo` destructures (compiles with the new
  field).
- `alter_table` handles the new `TableFormatAlterTableOperation` variants:
  - `RenameTable` → `Ok(())` (catalog-only rename, like Iceberg);
  - `AddColumns` / `DropColumns` / `AlterColumnComment` / `AlterColumnNullability` /
    `AlterColumnPosition` → `not_impl_err!("... is not yet supported for Delta Lake")`.
- The `Display` impl for `TableFormatAlterTableOperation` gains the six new variant
  labels (`"ALTER TABLE RENAME TO"`, `"ALTER TABLE ADD COLUMNS"`, `"ALTER TABLE DROP
  COLUMNS"`, `"ALTER TABLE ALTER COLUMN COMMENT"`, `"... NULLABILITY"`, `"... POSITION"`).

### 5.6 `sail-catalog-iceberg` config

`IcebergRestAccessDelegation` config value flows into `IcebergRestCatalogProvider`'s
`options.access_delegation` (used for credential/delegation behavior).

---

## 6. Behavior contracts to preserve

- Iceberg REST catalogs own their metadata: SET/UNSET/ADD/DROP go through the REST API
  with `AssertTableUuid` (+ schema/field-id guards); Rename is catalog-only everywhere.
- Storage-level ALTER works only for filesystem Iceberg tables; catalog-managed tables
  short-circuit to the catalog provider.
- `CREATE OR REPLACE` purges data+metadata of the replaced table on REST catalogs.
- `DESCRIBE TABLE <t> <col>` returns `CatalogError::NotFound` for unknown columns.
- `SHOW TBLPROPERTIES` is sorted and rejects views.
