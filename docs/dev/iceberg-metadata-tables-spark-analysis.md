# Spark/Iceberg metadata tables — deep analysis for Sail emulation

**Status:** Research findings
**Target:** Implement Iceberg metadata tables (`.refs`, `.snapshots`) in Sail to match Spark/Iceberg semantics
**Sources:** `$HOME/spark` (Apache Spark core, via codebase-memory MCP graph) + `iceberg-core` / `iceberg-spark` 1.10.0 sources (Maven)

---

## 1. Why this analysis

heimdall needs `SELECT ... FROM <schema>.<table>.refs` and `... .snapshots` to work over
Sail (see `docs/dev/heimdall-parity-plan.md`, Phase A). Sail has no metadata-table support
today. To build it idiomatically, we must first understand exactly how Spark + Iceberg
resolve and materialize these tables, then map that contract onto Sail's existing patterns.

Key verified facts up front:

- **Spark core knows nothing about metadata tables.** It only splits a multi-part name
  into `(catalog, Identifier(namespace, name))` and delegates the whole thing to the
  catalog's `loadTable`/`loadRelation`.
- **The Iceberg catalog does the split**: namespace = all parts except the last;
  last part = metadata table name (`refs`, `snapshots`, …).
- **Resolution order is "try base table first; on miss try metadata table".**
- Each metadata table is a read-only `Table` with a **fixed schema** and an in-memory
  row materialization; Spark pushes `WHERE`/`ORDER BY`/`LIMIT` above it.

---

## 2. Spark core: pure delegation

### 2.1 The analyzer path

Traced in `$HOME/spark/sql/catalyst`:

```
UnresolvedRelation(db.table.snapshots)
  └─ Analyzer.ResolveRelations.resolveRelation
       └─ RelationResolution.resolveRelation
            └─ expandIdentifier
                 └─ LookupCatalog.CatalogAndIdentifier.unapply(nameParts)
                      → (CatalogPlugin, Identifier)
  └─ CatalogV2Util.loadTable(catalog, ident) / RelationCatalog.loadRelation(ident)
```

### 2.2 `CatalogAndIdentifier.unapply` (`sql/catalyst/.../connector/catalog/LookupCatalog.scala:111`)

```scala
def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Identifier)] = {
  assert(nameParts.nonEmpty)
  if (nameParts.length == 1) {
    Some((currentCatalog, Identifier.of(catalogManager.currentNamespace, nameParts.head)))
  } else if (nameParts.head.equalsIgnoreCase(globalTempDB)) {
    Some((catalogManager.v2SessionCatalog, nameParts.asIdentifier))
  } else {
    try {
      val catalog = catalogManager.catalog(nameParts.head)
      val ident = nameParts.tail.asIdentifier
      if (CatalogV2Util.isSessionCatalog(catalog)) {
        // Reject only when namespace is empty (e.g. spark_catalog.t with no database).
        // Allow multi-part namespace for metadata tables (e.g. default.table.snapshots).
        if (ident.namespace().isEmpty) {
          throw QueryCompilationErrors.requiresSinglePartNamespaceError(...)
        }
      }
      Some((catalog, ident))
    } catch {
      case _: CatalogNotFoundException =>
        Some((currentCatalog, nameParts.asIdentifier))
    }
  }
}
```

**The critical line**: Spark core explicitly *allows multi-part namespaces* so that
`default.table.snapshots` can reach the catalog with `namespace=[default, table]`,
`name=snapshots`. Without this, 3-part names would be rejected for the session catalog.

### 2.3 `asIdentifier` (`sql/catalyst/.../catalog/CatalogV2Implicits.scala:219`)

```scala
def asIdentifier: Identifier = Identifier.of(parts.init.toArray, parts.last)
```

**Rule**: the **last part is always the table name**, every preceding part is the
namespace. This is the universal split contract.

### 2.4 Hand-off to the catalog

- `CatalogV2Util.loadTable(catalog, ident)` → `catalog.asTableCatalog.loadTable(ident)`
  (or `RelationCatalog.loadRelation(ident)` in the single-RPC path).
- Spark core performs **no** metadata-table detection. The catalog decides.

---

## 3. Iceberg catalog: the actual split

From `iceberg-core-1.10.0` (Maven sources).

### 3.1 `BaseMetastoreCatalog.loadTable(TableIdentifier)` (`org/apache/iceberg/BaseMetastoreCatalog.java:45`)

```java
public Table loadTable(TableIdentifier identifier) {
  Table result;
  if (isValidIdentifier(identifier)) {
    TableOperations ops = newTableOps(identifier);
    if (ops.current() == null) {
      // the identifier may be valid for both tables and metadata tables
      if (isValidMetadataIdentifier(identifier)) {
        result = loadMetadataTable(identifier);
      } else {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }
    } else {
      result = new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
    }
  } else if (isValidMetadataIdentifier(identifier)) {
    result = loadMetadataTable(identifier);
  } else {
    throw new NoSuchTableException("Invalid table identifier: %s", identifier);
  }
  return result;
}
```

**Resolution order**:
1. If the full identifier resolves as a real base table (`ops.current() != null`) →
   **base table** wins.
2. Else if it is a valid metadata identifier → **metadata table**.
3. Else → `NoSuchTableException`.

### 3.2 `isValidMetadataIdentifier` (`BaseMetastoreCatalog.java:116`)

```java
protected boolean isValidMetadataIdentifier(TableIdentifier identifier) {
  return MetadataTableType.from(identifier.name()) != null
      && isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
}
```

i.e. **last part is a known metadata-table type** AND **the namespace (all prior parts)
is a valid base-table identifier**.

### 3.3 `loadMetadataTable` (`BaseMetastoreCatalog.java:99`)

```java
private Table loadMetadataTable(TableIdentifier identifier) {
  String tableName = identifier.name();
  MetadataTableType type = MetadataTableType.from(tableName);
  if (type != null) {
    TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
    TableOperations ops = newTableOps(baseTableIdentifier);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: %s", baseTableIdentifier);
    }
    return MetadataTableUtils.createMetadataTableInstance(
        ops, name(), baseTableIdentifier, identifier, type);
  } else {
    throw new NoSuchTableException("Table does not exist: %s", identifier);
  }
}
```

**Split**: base table = `namespace.levels()`; metadata type = `name()`.

### 3.4 `TableIdentifier` (`iceberg-api`, `catalog/TableIdentifier.java:36`)

```java
public static TableIdentifier of(String... names) {
  return new TableIdentifier(
      Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
}
```

So `db.table.snapshots` → `namespace=[db, table]`, `name="snapshots"` — confirming the
same "last = name" contract as Spark.

### 3.5 `MetadataTableType` (`org/apache/iceberg/MetadataTableType.java`)

```java
public enum MetadataTableType {
  ENTRIES, FILES, DATA_FILES, DELETE_FILES, HISTORY, METADATA_LOG_ENTRIES,
  SNAPSHOTS, REFS, MANIFESTS, PARTITIONS,
  ALL_DATA_FILES, ALL_DELETE_FILES, ALL_FILES, ALL_MANIFESTS, ALL_ENTRIES,
  POSITION_DELETES;

  public static MetadataTableType from(String name) {
    try { return MetadataTableType.valueOf(name.toUpperCase(Locale.ROOT)); }
    catch (IllegalArgumentException ignored) { return null; }
  }
}
```

`from` is **case-insensitive** and returns `null` for unknown names — which is exactly
what makes "is this a metadata table name?" a cheap check.

### 3.6 `MetadataTableUtils.createMetadataTableInstance` (`MetadataTableUtils.java:97`)

```java
public static Table createMetadataTableInstance(
    TableOperations ops, String catalogName,
    TableIdentifier baseTableIdentifier, TableIdentifier metadataTableIdentifier,
    MetadataTableType type) {
  String baseTableName = BaseMetastoreCatalog.fullTableName(catalogName, baseTableIdentifier);
  String metadataTableName = BaseMetastoreCatalog.fullTableName(catalogName, metadataTableIdentifier);
  return createMetadataTableInstance(ops, baseTableName, metadataTableName, type);
}
```

Dispatch table: `ENTRIES→ManifestEntriesTable`, `FILES→FilesTable`,
`SNAPSHOTS→SnapshotsTable`, `REFS→RefsTable`, `HISTORY→HistoryTable`, `MANIFESTS→ManifestsTable`,
`PARTITIONS→PartitionsTable`, etc.

---

## 4. The metadata table implementations heimdall needs

Each is a `BaseMetadataTable extends BaseReadOnlyTable` with a **fixed schema** and a
`StaticDataTask` that materializes rows from the base table's `TableMetadata`.

### 4.1 `SnapshotsTable` (`org/apache/iceberg/SnapshotsTable.java`)

Schema (`SNAPSHOT_SCHEMA`, field ids 1–8):

| col | Iceberg type | required | row source |
|---|---|---|---|
| `committed_at` | timestamptz | yes | `snapshot.timestampMillis() * 1000` |
| `snapshot_id` | long | yes | `snapshot.snapshotId()` |
| `parent_id` | long | no | `snapshot.parentId()` |
| `operation` | string | no | `snapshot.operation()` |
| `manifest_list` | string | no | `snapshot.manifestListLocation()` |
| `summary` | map<string,string> | no | `snapshot.summary()` |

Row converter:

```java
private static StaticDataTask.Row snapshotToRow(Snapshot snap) {
  return StaticDataTask.Row.of(
      snap.timestampMillis() * 1000,   // micros → committed_at
      snap.snapshotId(),
      snap.parentId(),
      snap.operation(),
      snap.manifestListLocation(),
      snap.summary());
}
```

Scan: `StaticTableScan` (a `BaseMetadataTableScan`) → `StaticDataTask.of(...)` with rows
built from `table().snapshots()`. `planFiles()` is overridden to **not** require a current
snapshot (snapshots table lists all snapshots).

### 4.2 `RefsTable` (`org/apache/iceberg/RefsTable.java`)

Schema (`SNAPSHOT_REF_SCHEMA`, field ids 1–6):

| col | Iceberg type | required | row source |
|---|---|---|---|
| `name` | string | yes | ref map key |
| `type` | string | yes | `SnapshotRefType.name()` (branch/tag) |
| `snapshot_id` | long | yes | `ref.snapshotId()` |
| `max_reference_age_in_ms` | long | no | `ref.maxRefAgeMs()` |
| `min_snapshots_to_keep` | int | no | `ref.minSnapshotsToKeep()` |
| `max_snapshot_age_in_ms` | long | no | `ref.maxSnapshotAgeMs()` |

Row converter:

```java
private static Function<String, StaticDataTask.Row> referencesToRows(Map<String, SnapshotRef> refs) {
  return refName -> StaticDataTask.Row.of(
      refName,
      refs.get(refName).type().name(),
      refs.get(refName).snapshotId(),
      refs.get(refName).maxRefAgeMs(),
      refs.get(refName).minSnapshotsToKeep(),
      refs.get(refName).maxSnapshotAgeMs());
}
```

Scan: same `StaticTableScan`/`StaticDataTask` shape over `table().refs()`.

### 4.3 `BaseMetadataTable` / `BaseReadOnlyTable`

- `BaseMetadataTable` (abstract) implements `table()` → the wrapped base `BaseTable`,
  `schema()` → the fixed metadata schema, `refs()/snapshots()/history()/currentSnapshot()`
  → delegate to the base table. It is **read-only** (no writers).
- `BaseMetadataTableScan` extends `BaseTableScan`; `StaticTableScan` returns a `DataTask`
  that produces the in-memory rows.

### 4.4 Filter/sort/limit handling

`StaticDataTask` materializes all rows in memory; **Spark applies `WHERE` / `ORDER BY` /
`LIMIT` above the metadata table scan**. Because metadata tables are tiny, this is the
idiomatic and correct approach. This is exactly what heimdall's queries rely on:
- `WHERE name='main'` on `.refs`
- `WHERE snapshot_id = N`, `ORDER BY committed_at DESC LIMIT 1`, `WHERE parent_id = N` on `.snapshots`

---

## 5. Time travel interplay (for completeness)

- `SparkCatalog.loadTable(ident, version)` / `loadTable(ident, timestamp)` handle
  `VERSION AS OF` / `TIMESTAMP AS OF` (Spark's `TimeTravelSpec`).
- Not required for metadata tables, but note that in `SparkCatalog.load(ident)`, when the
  base load fails, Iceberg tries `namespace` as the base table and then checks the name
  against snapshot selectors (`.at_ts`, `snap_id`, `branch`, `tag`, `rewrite`, changelog)
  **in addition to** metadata-table names. Sail's metadata-table feature is independent of
  this; `VERSION AS OF` already works in Sail via the `snapshotId`/`ref`/`timestampAsOf`
  read options.

---

## 6. Mapping to heimdall's exact queries

| heimdall SQL | Spark/Iceberg resolution | base table |
|---|---|---|
| `SELECT CAST(snapshot_id AS STRING) FROM <schema>.<table>.refs WHERE name='main'` | `Identifier(namespace=[<schema>, <table>], name=refs)` → `REFS` | `<schema>.<table>` |
| `SELECT CAST(snapshot_id AS STRING) FROM <schema>.<table>.snapshots ORDER BY committed_at DESC LIMIT 1` | `name=snapshots` → `SNAPSHOTS` | `<schema>.<table>` |
| `SELECT 1 FROM <schema>.<table>.snapshots WHERE snapshot_id = N` | `SNAPSHOTS` | `<schema>.<table>` |
| `SELECT CAST(parent_id AS STRING) FROM <schema>.<table>.snapshots WHERE snapshot_id = N` | `SNAPSHOTS` | `<schema>.<table>` |

In all cases the trailing identifier is the metadata-table name and everything before it
is the base table reference.

---

## 7. Transferring the contract to Sail (verified against Sail idioms)

### 7.1 Interception point

`resolve_query_read_named_table` in `crates/sail-plan/src/resolver/query/read.rs:31`:

- Today it calls `resolve_table_reference(&name)` which **only accepts 1–3 parts**
  (`schema.rs:12`). A 3-part `[landing, gl_balances, refs]` would be misread as
  `catalog=landing, schema=gl_balances, table=refs` — wrong.
- **Mirror Spark**: before `resolve_table_reference`/`get_table_or_view`, check whether
  the last part of `name.parts()` is a known metadata-table name (`refs`, `snapshots`, …)
  **and** the prefix `parts[..len-1]` resolves to an Iceberg table. If so, build a
  metadata-table read for the base table.

### 7.2 Seam for the format-aware hook

- `sail-plan` cannot depend on `sail-iceberg` (no dep in `Cargo.toml`). The read path's
  only format-aware seam is `TableFormat::create_source(ctx, SourceInfo)`
  (`IcebergTableFormat::create_source` → `build_iceberg_provider` →
  `create_iceberg_provider_concrete` → `Table::load_with_metadata_location`).
- Idiomatic approach: add a `metadata_table: Option<String>` field to `SourceInfo`
  (`crates/sail-common-datafusion/src/datasource.rs`). The resolver sets it (plus the base
  table's `paths`/`lakehouse_table`) when it detects the trailing metadata name;
  `IcebergTableFormat::create_source` branches to a metadata-table provider when present.

### 7.3 Provider shape

New `IcebergMetadataTableProvider` (in `sail-iceberg/src/datasource/`), modeled on
`IcebergTableProvider` (`datasource/provider.rs`) + the materialization idiom in
`sail-catalog-system` (`SystemTableExec`/`SystemTableService`,
`catalog-system/src/physical_plan.rs`, `service.rs`):

- Loads `TableMetadata` via the existing `Table::load` path.
- Exposes the fixed Arrow schema per metadata-table type (mirror `SnapshotsTable` /
  `RefsTable` schemas exactly).
- `scan()` returns one in-memory `RecordBatch`; DataFusion applies `WHERE`/`ORDER BY`/
  `LIMIT` above (metadata tables are tiny — no pushdown needed, matching Spark).

### 7.4 Schema parity (from Sail's existing `TableMetadata` fields)

Sail already models `TableMetadata.snapshots: Vec<Snapshot>` and
`TableMetadata.refs: HashMap<String, SnapshotReference>` (`spec/metadata/table_metadata.rs`),
and `Snapshot { snapshot_id, parent_snapshot_id, timestamp_ms, manifest_list, summary, … }`
(`spec/snapshots/snapshot.rs`), `SnapshotReference { snapshot_id, retention }`. So the rows
can be built directly:

- `.snapshots` → `Snapshot` fields
- `.refs` → `SnapshotReference` fields (+ `is_branch()` for the type)

---

## 8. Reference files

Spark (`$HOME/spark`):
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala` — `ResolveRelations`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RelationResolution.scala` — `expandIdentifier`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/LookupCatalog.scala` — `CatalogAndIdentifier.unapply`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Implicits.scala` — `asIdentifier`
- `sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Util.scala` — `loadTable`/`getTable`
- `sql/api/src/main/java/org/apache/spark/sql/connector/catalog/Identifier.java` — `of(namespace, name)`

Iceberg 1.10.0 (Maven sources):
- `org/apache/iceberg/BaseMetastoreCatalog.java` — `loadTable`, `isValidMetadataIdentifier`, `loadMetadataTable`
- `org/apache/iceberg/MetadataTableType.java`
- `org/apache/iceberg/MetadataTableUtils.java`
- `org/apache/iceberg/BaseMetadataTable.java`, `BaseMetadataTableScan.java`, `StaticTableScan.java`, `StaticDataTask.java`
- `org/apache/iceberg/SnapshotsTable.java`, `org/apache/iceberg/RefsTable.java`
- `org/apache/iceberg/catalog/TableIdentifier.java` (iceberg-api)
- `org/apache/iceberg/spark/SparkCatalog.java` (`load`, `buildIdentifier`) — Spark integration wrapper

Sail:
- `crates/sail-plan/src/resolver/query/read.rs` — interception point (`resolve_query_read_named_table`)
- `crates/sail-plan/src/resolver/schema.rs` — `resolve_table_reference`
- `crates/sail-common-datafusion/src/datasource.rs` — `SourceInfo`, `TableFormat`
- `crates/sail-iceberg/src/table_format.rs` — `create_source`, `build_iceberg_provider`, `create_iceberg_provider_concrete`, `metadata_location_from_options`
- `crates/sail-iceberg/src/datasource/provider.rs` — `IcebergTableProvider` (model)
- `crates/sail-catalog-system/src/physical_plan.rs`, `service.rs` — in-memory batch materialization idiom
- `crates/sail-iceberg/src/spec/metadata/table_metadata.rs`, `spec/snapshots/snapshot.rs` — the source data fields

---

## 9. Key design takeaways for Sail

1. **"Last part = metadata table name; prefix = base table"** is the universal contract —
   mirror it in the read resolver.
2. **Try base table first, then metadata table** — matches Iceberg's `loadTable` order and
   avoids shadowing real tables.
3. **Cheap detection**: a fixed case-insensitive set of metadata-table names
   (`refs`, `snapshots`, …) is all that's needed, exactly like `MetadataTableType.from`.
4. **Fixed schema + in-memory rows + DataFusion filters above** — no pushdown required,
   matching Spark's `StaticDataTask`.
5. **Reuse `SourceInfo` + `Table::load`** to get `TableMetadata`; the provider is a thin
   materializer over the fields Sail already models.
