# Plan: Support `CREATE OR REPLACE` / `INSERT OVERWRITE` for catalog-managed Iceberg (REST/Polaris)

**Status:** Implemented — provider fix + 4 unit tests + pysail integration test landed;
`sail-catalog-iceberg` 39/39 pass, workspace builds clean.
**Branch:** `feat/v0.6.6`
**Scope:** Fix `CREATE OR REPLACE TABLE ... AS SELECT` (and the related `INSERT OVERWRITE`
flows) for **catalog-managed Iceberg tables** (Polaris via `IcebergRestCatalogProvider`). The
dbt-sail adapter emits `create or replace table <t> using iceberg as <select>` for table
materialization and `store_failures`; on a Polaris-backed catalog this currently fails with
`not supported: Replace table is not supported yet`.
**Spec reference:** Spark `CREATE OR REPLACE TABLE` semantics (drop + recreate, purge data);
Apache Iceberg REST catalog `CreateTable`/`CommitTable` with `REPLACE`-style requirements.

---

## 1. Root cause (verified in code)

dbt-sail's `sail__create_table_as` (for `file_format` in `['delta','iceberg',none]`) emits:

```sql
create or replace table stg.gl_balances using iceberg as select * from landing.gl_balances
```

Sail's write resolver maps this through:

1. `resolve_catalog_create_table_as_select` (sail-plan/.../catalog/table.rs:97-209)
   → `WriteMode::Replace { error_if_absent: false }`
2. `resolve_write_with_builder` (sail-plan/.../command/write.rs) `WriteTarget::Table` +
   `WriteMode::Replace` → builds a `CatalogCommand::CreateTable` precondition with
   `CreateTableMode::CreateOrReplace` (write.rs:411-421, 480-509)
3. `CatalogCommand::CreateTable` execution (sail-catalog/command.rs:338-365)
   → `prepare_create_table_storage_metadata` → `manager.create_table` → provider `create_table`

The **blocker** is `IcebergRestCatalogProvider::create_table`:

```rust
// crates/sail-catalog-iceberg/src/provider.rs:1326-1330
if mode.is_replace() {
    return Err(CatalogError::NotSupported(
        "Replace table is not supported yet".to_string(),
    ));
}
```

`CreateTableMode::is_replace()` is true for **both** `CreateOrReplace` and `Replace`
(sail-common/.../spec/plan.rs:930-932). So even a first-run `CREATE OR REPLACE` on a table
that does **not exist** is rejected — it should behave as a plain create.

### 1.1 What already works (verified)

- **`INSERT OVERWRITE`** → `InsertMode{overwrite:true}` → `WriteMode::Truncate`
  (sail-plan/.../command/insert.rs:129-137) → uses the **existing-table lakehouse writer path**
  (`WriteMode::Truncate` is in the `use_existing` set, write.rs:322-328) → `SinkMode::Overwrite`
  → `IcebergWriterExec` `PhysicalSinkMode::Overwrite` → `IcebergCommitExec` with
  `Operation::Overwrite`. This does **not** hit provider `create_table` and already works.
- **`CREATE OR REPLACE` on filesystem/path-managed Iceberg** (LOCATION given): the storage
  layer `IcebergTableFormat::create_table_metadata` handles `replace` (table_format.rs:156-230:
  reuses field IDs, bumps schema/spec IDs). Works in pysail `test_iceberg_io.py`.
- **Plain `CREATE TABLE` on an existing table** already returns `AlreadyExists` at the command
  layer (`prepare_create_table_storage_metadata`, command.rs:817-822: `Ok(_)` + `!is_replace()`
  → `AlreadyExists`). So the "normalize create-on-existing" item is **already correct** at the
  command layer; only the REST provider's unconditional `is_replace()` reject is wrong.
- **`drop_table`** on the REST provider already supports `purge` (provider.rs:1458-1485).

### 1.2 What the memory provider does (the in-repo precedent)

`MemoryCatalogProvider::create_table` (sail-catalog-memory/provider.rs:169-210):

```rust
if let Some(status) = db.tables.get(table) {
    match mode {
        CreateTableMode::CreateIfNotExists => return Ok(status.clone()),
        CreateTableMode::CreateOrReplace | CreateTableMode::Replace => {
            db.tables.remove(table);
        }
        CreateTableMode::Create => return Err(CatalogError::AlreadyExists(...)),
    }
} else if mode.replace_requires_existing() {
    return Err(CatalogError::NotFound(...));   // plain `Replace` requires existing
}
// ... create
```

This is the exact semantics to mirror in the REST provider.

---

## 2. Design (aligned with Sail idioms)

### 2.1 Fix `IcebergRestCatalogProvider::create_table` — mirror the memory provider

**File:** `crates/sail-catalog-iceberg/src/provider.rs` (currently rejects at `:1326`)

Replace the unconditional `is_replace()` reject with existence-aware handling:

```rust
// (inside `create_table`, after `mode` is destructured, replacing the current
//  `if mode.is_replace() { ... }` block)

if mode.ignore_if_exists() {
    if let Ok(existing) = self.get_table(database, table).await {
        return Ok(existing);
    }
}

// Existence check: `CreateOrReplace`/`Replace` must drop the existing catalog
// registration first; plain `Replace` requires an existing table.
let existing = match self.get_table(database, table).await {
    Ok(status) => Some(status),
    Err(CatalogError::NotFound(CatalogObject::Table, _)) => None,
    Err(e) => return Err(e),
};
match (existing, mode) {
    (Some(_), CreateTableMode::Create) => {
        return Err(CatalogError::AlreadyExists(
            CatalogObject::Table,
            table.to_string(),
        ));
    }
    (Some(_), CreateTableMode::CreateIfNotExists) => {
        // unreachable (handled above); kept for exhaustiveness
        return Ok(self.get_table(database, table).await?);
    }
    (Some(_), CreateTableMode::CreateOrReplace | CreateTableMode::Replace) => {
        // Drop the old catalog registration (metadata + data with purge), then create fresh.
        // Matches Spark `CREATE OR REPLACE TABLE` (drop + recreate).
        self.drop_table(
            database,
            table,
            DropTableOptions {
                if_exists: true,
                purge: true,
            },
        )
        .await?;
    }
    (None, CreateTableMode::Replace) => {
        return Err(CatalogError::NotFound(CatalogObject::Table, table.to_string()));
    }
    (None, _) => {} // Create / CreateOrReplace on missing table → create
}
// ... existing create logic (unchanged)
```

Notes / decisions:

- **Purge = true** per user decision: dbt `table` materialization and `store_failures`
  re-create from scratch, so deleting the old table's data files is correct (Spark
  `CREATE OR REPLACE` semantics). Confirm no workflow needs to preserve files across replace.
- **Only Iceberg (REST) provider** is changed. HMS/Unity still reject `is_replace()` (they are
  separate providers; dbt targets Polaris → REST). Document as a known limitation.
- The **command-layer `AlreadyExists`/`NotFound` normalization is already correct**
  (command.rs:817-822); do not duplicate it in the provider — but keeping the `Create` →
  `AlreadyExists` branch in the provider is harmless and makes the provider self-contained
  (matches the memory provider).

### 2.2 No change needed for `INSERT OVERWRITE` (verify only)

`INSERT OVERWRITE` already flows through the lakehouse writer (`Operation::Overwrite` +
`IcebergCommitExec`). Add a regression test to confirm, but no code change expected.

### 2.3 Confirm the write-precondition ordering for replace

`resolve_write_with_builder` builds the `CreateTable` precondition (write.rs:480-509) plus the
lakehouse writer. For `CreateOrReplace` on a fresh table this is a single precondition; for an
existing table the provider drop + the writer's `SinkMode::Overwrite` must be consistent. Verify
with a test that:
- second `CREATE OR REPLACE TABLE ... AS SELECT` on the same table replaces rows and metadata
  (fresh snapshot, old files purged),
- the table's storage location is reused (catalog-managed tables get their location from the
  catalog; dropping + recreating must not orphan the warehouse path).

---

## 3. Implementation plan

1. **`IcebergRestCatalogProvider::create_table`** (provider.rs ~:1326): replace the
   unconditional `is_replace()` reject with the existence-aware match above.
   - Ensure `DropTableOptions` is in scope (already used by `drop_table`).
   - Confirm `CatalogError::NotFound(CatalogObject::Table, _)` is the right variant for
     `get_table` misses (check how `get_table` returns misses today).

2. **Unit test (in-repo).** `crates/sail-catalog-iceberg/src/provider.rs` has a `#[cfg(test)]`
   module (see `test_drop_table_impl`, etc.). Add:
   - `create_table` with `CreateOrReplace` on a **missing** table → creates (no error).
   - `create_table` with `CreateOrReplace` on an **existing** table → drops then recreates
     (old table gone, new one queryable).
   - `create_table` with `Replace` on a **missing** table → `NotFound`.
   - `create_table` with `Create` on an **existing** table → `AlreadyExists` (regression).

3. **Integration test (pysail).** In `python/pysail/tests/spark/catalog/iceberg_rest/`
   (the conftest wires a REST catalog + object store):
   - `CREATE OR REPLACE TABLE ... USING iceberg AS SELECT ...` (no LOCATION → catalog-managed),
     run twice; second run replaces rows + metadata.
   - `INSERT OVERWRITE` on a catalog-managed table (regression).

4. **E2E (heimdall).** Re-run:
   `./run_dbt_project.sh stg gl_balances -s landing.gl_balances -b 2025-09-30 -m 1`
   - Expect: model `stg.gl_balances` created (no "Replace table is not supported yet"),
     row-count capture succeeds, 2 data tests pass (their `store_failures` tables also use
     `create or replace` → covered by the same fix).

5. **Regression.** `cargo build`, `cargo clippy -p sail-catalog-iceberg -p sail-iceberg`,
   `cargo test -p sail-catalog-iceberg -p sail-iceberg`, plus the pysail iceberg_rest suite.

---

## 4. Edge cases & risks

| # | Risk | Mitigation |
|---|------|------------|
| 1 | **Purge deletes data on replace.** If a downstream workflow overwrites a table while other refs/tags still point at old snapshots, purge is destructive. | User confirmed drop+recreate is correct for dbt table materialization. Document that `CREATE OR REPLACE` is destructive (matches Spark). |
| 2 | **Reusing the warehouse path after drop.** The catalog-managed table location comes from the catalog; dropping + recreating must reuse the same `location` so the write path writes to the right prefix. | Verify via integration test: after replace, `SELECT` returns only new rows and the metadata-log shows a fresh version at the same path. |
| 3 | **REST drop vs create race.** Drop-then-create in one provider call is not atomic against concurrent writers. | Accept for now (single-writer dbt); note as a future enhancement (REST `CreateTable` with `REPLACE`/`AssertTableDoesNotExist` semantics, or an `ALTER`-style update). |
| 4 | **HMS/Unity still reject replace.** Only REST is fixed. | Document as known limitation; out of scope unless dbt targets those catalogs. |
| 5 | **`Create` on existing already handled at command layer** — don't double-error. | Provider's `AlreadyExists` branch is defensive parity with memory provider; integration test asserts no double-error. |
| 6 | **`get_table` error-variant mismatch.** If `get_table` returns a different NotFound variant than `CatalogError::NotFound(CatalogObject::Table, _)`, the match misses. | Check `get_table`'s actual error type during implementation; use the same variant as `prepare_create_table_storage_metadata` (command.rs:824). |

---

## 5. Test plan

1. **Provider unit tests** (`sail-catalog-iceberg/src/provider.rs` `#[cfg(test)]`): the 4 cases
   in §3.2.
2. **pysail iceberg_rest integration**: `CREATE OR REPLACE` ×2 + `INSERT OVERWRITE` on
   catalog-managed tables.
3. **heimdall E2E**: full `run_dbt_project.sh` — model + 2 data tests green.
4. **Regression**: `cargo test -p sail-catalog-iceberg -p sail-iceberg`, `cargo clippy`,
   `cargo check --workspace`.

---

## 6. Files touched

- `crates/sail-catalog-iceberg/src/provider.rs` — `create_table` replace handling (core).
- `crates/sail-catalog-iceberg/src/provider.rs` tests — new unit tests.
- `python/pysail/tests/spark/catalog/iceberg_rest/` — integration tests (new or extend).
- No change to `sail-plan`/`sail-catalog` command layer (already correct).

---

## 7. Rollout & verification checklist

- [ ] Provider `create_table` handles `CreateOrReplace` on missing + existing; `Replace` on
      missing → `NotFound`; `Create` on existing → `AlreadyExists`.
- [ ] `CREATE OR REPLACE TABLE ... USING iceberg AS SELECT` works ×2 on Polaris-backed catalog.
- [ ] `INSERT OVERWRITE` regression passes on catalog-managed table.
- [ ] heimdall `run_dbt_project.sh` green: model + 2 data tests.
- [ ] No orphaned warehouse paths after replace (same location reused).
- [ ] HMS/Unity replace rejection documented as known limitation.
