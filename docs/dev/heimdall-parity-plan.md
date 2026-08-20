# Plan: Full heimdall parity in the Sail engine + Arrow Flight SQL transport

**Status:** Draft / to be implemented
**Branch:** `feat/v0.6.6` (HEAD — where `LOAD DATA` lives)
**Scope:** Replace the `sail-rest-service` REST surface consumed by heimdall with
engine-native SQL functionality exposed over Arrow Flight SQL. No REST interface
is added or kept.

---

## 1. Context

heimdall (Go, in `smartreg-dbt-base`) is the orchestration engine that drives the
SmartReg ETL: it loads CSV landing data, runs dbt models, captures before/after row
counts, and performs snapshot rollback/rollforward/cleanup against Iceberg tables
via Polaris.

Today heimdall consumes a REST API implemented by `sail-rest-service` (only present
on the `feat/iceberg-ops` branch):

| REST endpoint | Purpose | heimdall caller |
|---|---|---|
| `POST /engine/dbt/query` | run one SQL statement, return columns/rows/rowCount | `postIcebergQuery`, `ExecuteSQL*` |
| `POST /engine/dbt/load` | load one CSV file into a table | `LoadFile` (per-file loop) |
| `POST /engine/dbt/batch` | run many statements, return results | `ExecuteBatchSQL` |
| `GET/DELETE /engine/dbt/session` | session create/delete | (unused — default session) |
| `POST /engine/dbt/read` | read a file into a view + select | (unused) |
| `GET /engine/dbt/health` | health check | (unused) |

The REST path is slow (per-file JSON round trips, no batching, no pushdown).
The decision: **bake all heimdall functionality directly into the Sail engine** and
expose it over **Arrow Flight SQL** (`sail-flight`, `sail flight server`, default
port 32010), which is already present in the engine. heimdall talks to Sail via a
Go Flight SQL client (`apache/arrow-go/v18` `flightsql`), not REST.

`LOAD DATA` (the `/engine/dbt/load` replacement) is already implemented and E2E
validated. This document plans the remaining work to reach **full heimdall SQL
parity** and the transport migration.

---

## 2. heimdall SQL surface vs engine support (gap analysis)

heimdall sends the following SQL through the engine. `✅` = supported on
`feat/v0.6.6` today; `❌` = gap that this plan fills.

| # | heimdall SQL | Engine status |
|---|---|---|
| 1 | `LOAD DATA INPATH '<s3a://…>' [OVERWRITE] INTO TABLE ns.tbl` | ✅ implemented (this repo's feature) |
| 2 | `CREATE SCHEMA IF NOT EXISTS <schema>` | ✅ |
| 3 | `CREATE TABLE IF NOT EXISTS landing.<tbl> (…)` (from `*_ddl.sql`) | ✅ |
| 4 | `DROP TABLE IF EXISTS landing.<tbl>` | ✅ |
| 5 | `ALTER TABLE … ADD COLUMNS (version string)` | ✅ (`AlterTableOperation::AddColumns`) |
| 6 | `INSERT INTO … VALUES …` | ✅ |
| 7 | `DELETE FROM … WHERE version = '…'` | ✅ |
| 8 | `SELECT COUNT(*) FROM ns.tbl` | ✅ |
| 9 | `SHOW TABLES IN <schema>` | ✅ |
| 10 | `SELECT … FROM ns.tbl VERSION AS OF <snap_id>` | ✅ (time travel via `snapshotId` option) |
| 11 | `SELECT CAST(snapshot_id AS STRING) FROM ns.tbl.refs WHERE name='main'` | ❌ no Iceberg metadata tables |
| 12 | `SELECT CAST(snapshot_id AS STRING) FROM ns.tbl.snapshots ORDER BY committed_at DESC LIMIT 1` | ❌ |
| 13 | `SELECT 1 FROM ns.tbl.snapshots WHERE snapshot_id = N` | ❌ |
| 14 | `SELECT CAST(parent_id AS STRING) FROM ns.tbl.snapshots WHERE snapshot_id = N` | ❌ |
| 15 | `SELECT snap.sid, cnt.cnt FROM (… ns.tbl.refs …) snap CROSS JOIN (SELECT COUNT(*) FROM ns.tbl) cnt` | ❌ |
| 16 | `CALL <catalog>.system.rollback_to_snapshot('ns.tbl', N)` | ❌ no CALL statement |
| 17 | `CALL <catalog>.system.set_current_snapshot('ns.tbl', N)` | ❌ |
| 18 | `CALL <catalog>.system.expire_snapshots('ns.tbl', TIMESTAMP '…')` | ❌ |
| 19 | `TRUNCATE TABLE ns.tbl` | ❌ no TRUNCATE statement (but DELETE-without-WHERE machinery exists) |

Everything else heimdall sends is already supported. The work is therefore three
engine features (metadata tables, CALL procedures, TRUNCATE) plus the transport
migration.

---

## 3. Verified codebase idioms (MCP graph + source reading)

The plan below is grounded in the actual Sail idioms. Key facts:

- **Command flow.** `LoadDataNode` (`crates/sail-logical-plan/src/load_data.rs`,
  `UserDefinedLogicalNodeCore`, empty `DFSchema`) → resolver
  `resolve_command_load_data` (`crates/sail-plan/src/resolver/command/load.rs`)
  wired from `CommandNode::LoadData` in
  `crates/sail-plan/src/resolver/command/mod.rs:313` → physical planner
  `plan_load_data` dispatched from `IcebergPhysicalPlanner::plan_extension`
  (`crates/sail-iceberg/src/physical/table_scan_planner.rs:31`). This is the exact
  template for new command nodes (TRUNCATE, CALL).
- **Spec serialization.** `spec::CommandNode` (`crates/sail-common/src/spec/plan.rs:288`)
  is serde-JSON (camelCase). `from_ast_statement`
  (`crates/sail-sql-analyzer/src/statement.rs`) maps `Statement::* → CommandNode`.
  New **command** variants need no proto changes (only *physical exec* nodes do,
  e.g. `IcebergLoadDataFastExecNode` proto field 55).
- **Metadata-only commits (filesystem).** `IcebergTableFormat::retry_metadata_commit`
  (`crates/sail-iceberg/src/table_format.rs:659`) with a closure mutating
  `&mut TableMetadata`; used by `alter_table_add_columns` (`:790`). Model for
  CALL/expire storage-layer commits.
- **REST-catalog (Polaris) commits.** `IcebergCatalogCommitMode::resolve`
  (`crates/sail-iceberg/src/catalog_support/commit.rs:60`) maps
  `CommitAuthority::IcebergRestCommit → CatalogCommit`. `commit_lakehouse_table`
  (`crates/sail-catalog-iceberg/src/provider.rs:1552`) accepts generic JSON
  `TableUpdate`. `spec::TableUpdate::SetSnapshotRef` and `RemoveSnapshots`
  **already exist** (`crates/sail-iceberg/src/spec/catalog/mod.rs:167`).
- **Maintenance operation.** `LakehouseOperation::Maintenance`
  (`crates/sail-common-datafusion/src/catalog/lakehouse.rs:40`) fits CALL.
- **Time travel works.** `crates/sail-plan/src/resolver/query/time_travel.rs` →
  `snapshotId` / `ref` / `timestampAsOf` options; `IcebergReadOptions` has
  `use_ref` / `snapshot_id` / `timestamp_as_of`.
- **DELETE-without-WHERE is TRUNCATE.** `plan_delete`
  (`crates/sail-iceberg/src/physical_plan/planner/op_delete.rs:52-65`):
  `condition.is_none()` → `EmptyExec` → `assemble_iceberg_commit_plan(Operation::Delete)`.
  `DELETE` parser has optional `WHERE`. Iceberg `create_deleter`
  (`table_format.rs:317`) already produces `RowLevelWriteNode::new_delete`.
- **Metadata-table interception point.** `resolve_table_reference`
  (`crates/sail-plan/src/resolver/schema.rs:12`) only accepts **1–3 parts** —
  `db.tbl.refs` would resolve as `Full{catalog: db, schema: tbl, table: refs}` and
  look up a table literally named `refs`. Therefore metadata tables must be
  intercepted in `resolve_query_read_named_table`
  (`crates/sail-plan/src/resolver/query/read.rs:31`) **before**
  `resolve_table_reference` / `get_table_or_view`.
- **Virtual-table precedent.** `SystemTableSource` + `TableKind::TemporaryView`
  (`crates/sail-catalog-system/src/table_source.rs`, `provider.rs`) shows the
  read-only `TableSource` shape to model the metadata table on.
- **Flight server runs commands.** `get_flight_info_statement`
  (`crates/sail-flight/src/service.rs:90`) executes Query **and** Command plans
  eagerly and streams results — LOAD DATA / CALL / TRUNCATE / DDL all flow through
  `CommandStatementQuery`. Only `do_put_statement_update` (`ExecuteUpdate`) is
  unimplemented, and heimdall does not need it (LOAD DATA returns the count row).
- **Flight inherits k8s cluster mode.** Flight uses the same `ServerSessionFactory`;
  `crates/sail-session/src/session_factory/server.rs:159-175` selects
  `KubernetesCluster` `JobRunner` from `config.mode`. Running `sail flight server`
  with `SAIL_MODE=kubernetes-cluster` (same env as the spark server) gives worker
  spawning for LOAD DATA.
- **heimdall client.** `apache/arrow-go/v18` v18.7.0 resolves offline; Go 1.26 OK.
  `flightsql.Client.Execute` (CommandStatementQuery) is sufficient for every call;
  no `ExecuteUpdate` needed.

---

## 4. Phase A — Iceberg metadata tables (`.refs`, `.snapshots`)

**Goal:** `SELECT … FROM ns.tbl.refs` and `SELECT … FROM ns.tbl.snapshots` return
rows from `TableMetadata` (fields `snapshots: Vec<Snapshot>`,
`refs: HashMap<String, SnapshotReference>` already exist).

### 4.1 Interception (read path)

No new SQL statement. In `resolve_query_read_named_table`
(`crates/sail-plan/src/resolver/query/read.rs:31`), after the `<format>.<path>`
early-return and CTE check but **before** `resolve_table_reference` /
`get_table_or_view`:

- Inspect `name.parts()`. If the last part is a known metadata-table name
  (`refs`, `snapshots`; extensible to `files`, `manifests`, `history`,
  `partitions`, `properties`, `all_*` later) and the remaining prefix resolves to
  an Iceberg table:
  - Resolve the base table as a normal read (get catalog status → location +
    `resolve_lakehouse_table_context(…, LakehouseOperation::Read)` → load
    `TableMetadata`).
  - Build an `IcebergMetadataTableProvider` and wrap it via the existing
    `resolve_table_source_with_rename` (or a new `IcebergMetadataTableSource`),
    so column renaming / DFSchema registration stays consistent with normal reads.
- Otherwise fall through to the existing behavior unchanged.

### 4.2 Provider

New files in `crates/sail-iceberg/src/` (e.g. `metadata_table.rs`), modeled on
`SystemTableSource`:

- `IcebergMetadataTableProvider: TableProvider` + a lightweight `TableSource`
  adapter so it can be handed to `resolve_table_source_with_rename`.
- `scan()` materializes the whole metadata as Arrow batches (metadata tables are
  small; DataFusion handles `WHERE` / `ORDER BY` / `LIMIT` on the in-memory batch).
  Optional simple filter pushdown later.

**`.snapshots` schema** (columns heimdall needs; keep minimal):
| column | type |
|---|---|
| `committed_at` | `TIMESTAMP` (from `Snapshot::timestamp_ms`) |
| `snapshot_id` | `BIGINT` |
| `parent_id` | `BIGINT` (nullable) |
| `operation` | `STRING` (from `summary`) |
| `manifest_list` | `STRING` |

**`.refs` schema:**
| column | type |
|---|---|
| `name` | `STRING` |
| `snapshot_id` | `BIGINT` |
| `type` | `STRING` (`branch` / `tag` via `is_branch()`) |
| `min_snapshots_to_keep` | `INT` (nullable) |
| `max_snapshot_age_ms` | `BIGINT` (nullable) |
| `max_ref_age_ms` | `BIGINT` (nullable) |

### 4.3 Tests

- Unit tests feeding synthetic `TableMetadata` (build via
  `crates/sail-iceberg/src/operations/bootstrap.rs`), assert exact schemas + rows
  for `refs` and `snapshots`.
- Assert the exact heimdall predicates produce correct results:
  - `WHERE name='main'` on `.refs`
  - `WHERE snapshot_id = N` on `.snapshots`
  - `ORDER BY committed_at DESC LIMIT 1` on `.snapshots`
  - `WHERE parent_id = N` on `.snapshots`
- A resolver-level test that `db.tbl.refs` is intercepted and `db.tbl` still reads
  data normally.

---

## 5. Phase B — `CALL <catalog>.system.*` procedures

**Goal:** parse and execute the Iceberg CALL procedures heimdall uses (per the
[Iceberg Spark procedures spec](https://iceberg.apache.org/docs/latest/spark-procedures/)):

- `CALL <catalog>.system.rollback_to_snapshot('ns.tbl', N)`
- `CALL <catalog>.system.set_current_snapshot('ns.tbl', N)`
- `CALL <catalog>.system.expire_snapshots('ns.tbl', TIMESTAMP '…')`

### 5.1 Parser (`crates/sail-sql-parser`)

- Add `CALL` to `crates/sail-sql-parser/data/keywords.txt` (the build script
  `build.rs` generates the `Keyword` enum / map from this file). Note:
  `TRUNCATE`, `ROLLBACK`, `SYSTEM`, `TIMESTAMP`, `TO` already exist; `CALL` does
  not.
- Add `Statement::Call` variant to `crates/sail-sql-parser/src/ast/statement.rs`:
  ```rust
  Call {
      call: Call,
      name: ObjectName,                       // catalog.system.procedure
      arguments: Vec<CallArgument>,           // positional or named
  }
  ```
  with `CallArgument = Expr | NamedArg { name: Ident, value: Expr }` supporting
  `arg => value` (the `FatArrow` operator `=>` is already defined in
  `ast/operator.rs`).
- The chumsky `TreeParser` derive generates the parser from field names /
  keyword types automatically (see `Statement::LoadData` for the pattern).

### 5.2 Analyzer (`crates/sail-sql-analyzer`)

- `Statement::Call → spec::CommandNode::CallProcedure { name, arguments }`
  (new `CommandNode` variant; serde-only, no proto).
- Convert `arg => value` into `Vec<(Option<Identifier>, Expr)>`; positional args
  have `None` name.

### 5.3 Resolver (`crates/sail-plan/src/resolver/command/call.rs`)

New `resolve_command_call_procedure`, wired into `command/mod.rs`.

- Match `name.parts()` → `[catalog, "system", procedure]`. Reject anything else
  with `PlanError::unsupported`.
- Resolve procedure arguments to constants (table ref string, snapshot id int64,
  timestamp literal). Validate arity / types per procedure.
- Load table status via `catalog_manager.get_table_or_view(table.parts())`,
  require Iceberg, resolve lakehouse context with
  `LakehouseOperation::Maintenance`.
- Build a `CallProcedureNode` (a `UserDefinedLogicalNodeCore` leaf, mirroring
  `LoadDataNode`) carrying: table location, lakehouse context, procedure name,
  resolved args, target table properties.
- Return `LogicalPlan::Extension(Extension { node: Arc::new(node) })`.

### 5.4 Physical planner

New branch in `IcebergPhysicalPlanner::plan_extension`
(`crates/sail-iceberg/src/physical/table_scan_planner.rs`), e.g.
`plan_call_procedure(session_state, node)` in a new
`crates/sail-iceberg/src/physical/call_procedure_planner.rs`:

- Load `TableMetadata` for the table.
- Map each procedure to Iceberg metadata updates:
  - `rollback_to_snapshot('ns.tbl', N)` → verify snapshot `N` exists →
    `TableUpdate::SetSnapshotRef { ref_name: "main", reference: SnapshotReference::branch(N) }`
    (+ optionally `SetSnapshotRef` under the same update batch).
  - `set_current_snapshot('ns.tbl', N)` → same shape (point `main` at `N`).
  - `expire_snapshots('ns.tbl', TIMESTAMP '…')` → compute snapshots with
    `timestamp_ms < older_than` that are not ancestors of retained snapshots →
    `TableUpdate::RemoveSnapshots { snapshot_ids }` (+
    `RemoveSnapshotRef` for expired branch/tag refs).
- Commit through the existing machinery so both storage and REST-catalog tables
  work:
  - **Filesystem tables:** mirror `retry_metadata_commit` (mutate `table_meta.refs`
    / `table_meta.snapshots` / `current_snapshot_id`, write next metadata file).
  - **Catalog-managed tables (Polaris):** produce the JSON `TableUpdate`s and send
    via `IcebergCommitExec` / `commit_lakehouse_table` (the `IcebergCatalogCommitMode`
    resolution picks `CatalogCommit` automatically).
- Result: a `count`-style single-column batch (reuse the `IcebergCommitExec`
  output shape) so Flight clients see a normal result.

> **Note on scope (decision pending):** v1 `expire_snapshots` removes snapshots +
> refs (metadata-only). Physical garbage collection of orphaned data files is a
> follow-up.

### 5.5 Tests

- Resolver unit tests: arg binding, validation errors (missing table, bad snapshot
  id, unknown procedure), non-Iceberg rejection.
- Integration test (in `sail-iceberg`, non-catalog table): append 2 snapshots →
  `rollback_to_snapshot` to snapshot 1 → assert `.refs` `main` and
  `current_snapshot_id` updated; `expire_snapshots` old → assert `.snapshots`
  gone.
- Flight-level test: run `CALL …` over `CommandStatementQuery`, assert success.

---

## 6. Phase C — `TRUNCATE TABLE`

**Goal:** `TRUNCATE TABLE ns.tbl` empties the table (new empty snapshot).

The physical machinery already exists — `plan_delete` with `condition.is_none()`
produces the empty-snapshot `Operation::Delete` commit. So this is mostly wiring:

1. **Parser:** add `Statement::TruncateTable { table: ObjectName }`
   (`TRUNCATE` keyword already in `data/keywords.txt:330`).
2. **Analyzer:** → `spec::CommandNode::TruncateTable { table }`.
3. **Resolver:** new `resolve_command_truncate_table` in
   `crates/sail-plan/src/resolver/command/truncate.rs`, wired into
   `command/mod.rs`. Reuse the DELETE path with no condition:
   - `catalog_manager.get_table_or_view(table.parts())`
   - `get_table_info_for_delete(…)` (same helper as `resolve_command_delete`)
   - `DeleteInfo { table_name, path, condition: None, lakehouse_table, options }`
   - `registry.get(format)?.create_deleter(…)` → Iceberg `create_deleter` already
     produces `RowLevelWriteNode::new_delete` → `plan_delete` TRUNCATE branch.
4. **Tests:** load data → `TRUNCATE TABLE` → `SELECT COUNT(*)` = 0; `.snapshots` /
   `.refs` reflect the new empty snapshot; a subsequent `LOAD DATA` append works.

---

## 7. Phase D — Flight SQL transport (verify + auth decision)

The `sail-flight` server already executes every statement heimdall needs via
`CommandStatementQuery` (`get_flight_info_statement` + `do_get_statement`):
LOAD DATA, CALL, TRUNCATE, `.refs`/`.snapshots` reads, DDL, `VERSION AS OF`,
`SHOW TABLES`, `SELECT COUNT(*)`.

Work in this phase:

1. **Verification / tests.** Add flight-level integration tests exercising
   `LOAD DATA`, `CALL …system…`, `TRUNCATE TABLE`, `SELECT … FROM …refs` /
   `…snapshots`, `SELECT … VERSION AS OF` through the Flight server.
2. **Auth decision (required before Phase E).** heimdall sends
   `Authorization: Bearer <token>`. Today `do_handshake`
   (`crates/sail-flight/src/service.rs:73`) is a no-op accepting anything.
   Options:
   - **Open for internal cluster** (recommended first): leave handshake permissive;
     heimdall does not send a token.
   - **Bearer validation:** validate the token in `do_handshake` against a
     configured `flight.token` (new config), and have heimdall pass the token.
   This decision changes the heimdall wiring in Phase E.
3. **Not required** (defer): `GetSqlInfo`, `GetTables`, `GetCatalogs`,
   `GetDBSchemas`, session actions, `do_put_statement_update`/`ExecuteUpdate`.
   `flightsql.NewClient` + `Execute` + `DoGet` need none of these.

---

## 8. Phase E — heimdall Go client rewrite (`smartreg-dbt-base`)

### 8.1 Dependency

Add `github.com/apache/arrow-go/v18` (Flight SQL client) to `heimdall/go.mod`.
Resolves offline; Go 1.26 compatible.

### 8.2 Rewrite `internal/db/iceberg.go`

Keep the existing response shapes (`icebergQueryResp`, `engineLoadResp`) so all
callers (`cli/*.go`, `repository.go`) remain unchanged. Replace the HTTP client
with a `flightsql.Client`:

| Old REST call | New implementation |
|---|---|
| `postIcebergQuery(sql)` | `client.Execute(ctx, sql)` → `client.DoGet(ticket)` → read Arrow → build `icebergQueryResp` (columns/rows/rowCount). |
| `ExecuteSQLWithRowCount(sql)` / `ExecuteSQL(sql)` | same `Execute` path; rowCount from result rows. |
| `LoadFile(schema, table, s3aPath)` | build `LOAD DATA INPATH '<s3aPath>' INTO TABLE <schema>.<table>` (or `OVERWRITE INTO` for overwrite mode); `Execute`; read the returned count row as `rowsLoaded`. |
| `ExecuteBatchSQL(statements)` | loop `Execute` per statement, preserve ordering and per-statement status. |
| `RollbackToSnapshot` / `SetCurrentSnapshot` | `CALL <cat>.system.rollback_to_snapshot('<ns>.<t>', N)` / `set_current_snapshot` via `Execute`. |
| `ExpireSnapshots` | `CALL <cat>.system.expire_snapshots('<ns>.<t>', TIMESTAMP '<iso>')` via `Execute`. |
| `TruncateTable` | `TRUNCATE TABLE <schema>.<table>` via `Execute`. |
| `GetCurrentIcebergSnapshotID` etc. | `SELECT CAST(snapshot_id AS STRING) FROM <schema>.<table>.refs WHERE name='main'` etc. via `Execute` (needs Phase A). |

### 8.3 `internal/cli/load.go` — one LOAD DATA glob per business date

Replace the per-file `LoadFile` loop in `loadTable` / `loadCSVFiles` with **one**
statement per table + business date:

```sql
LOAD DATA INPATH 's3a://<bucket>/<tbl>/<businessDate>/*.csv' INTO TABLE landing.<tbl>
```

This uses the LOAD DATA fast/glob path (one atomic commit). Keep per-file
`LoadFile` only where a single versioned CSV must be loaded (mapper version
loads).

### 8.4 Config / scripts

- `ICEBERG_URL` → `grpc://<flight-host>:32010` (was `http://…/engine/dbt`).
- `ICEBERG_TOKEN` → only if Phase D auth is enabled; otherwise drop.
- Update `run.sh`, `scripts/_common.sh`, and README env docs.

---

## 9. Phase F — Deployment

1. **k8s** (`k8s/sail.yaml`): currently runs only `sail spark server`
   (Spark Connect, port 50051 — used by dbt-sail). Add the Flight SQL server as a
   second container in the same pod (or a separate Deployment) with:
   - command: `sail flight server --ip 0.0.0.0 --port 32010`
   - same env as the spark server: `SAIL_MODE=kubernetes-cluster`,
     `SAIL_CLUSTER__*`, `SAIL_KUBERNETES__*` (so LOAD DATA spawns worker pods).
   - a Service entry for port 32010 (`sail-flight-service`).
2. **heimdall** points at `grpc://sail-flight-service:32010`.
3. **Remove** `sail-rest-service` from use (it exists only on
   `feat/iceberg-ops`; the whole point is to not use REST).

---

## 10. Suggested sequencing

1. **Phase C (TRUNCATE)** — smallest; machinery already exists (DELETE-no-WHERE);
   unblocks `truncate` immediately.
2. **Phase A (metadata tables)** — self-contained; unblocks all snapshot-id
   queries.
3. **Phase B (CALL procedures)** — largest; reuse `TableUpdate::SetSnapshotRef` /
   `RemoveSnapshots` + the `IcebergCatalogCommitMode` commit machinery.
4. **Phase D** (flight verification + auth decision) → **Phase E** (heimdall
   rewrite) → **Phase F** (deploy + E2E per TEST_PLAN + heimdall flows).

---

## 11. Open decisions

1. **Flight auth:** add bearer-token validation to `do_handshake`, or keep open
   for the internal cluster? (Affects Phase E token wiring.)
2. **`expire_snapshots` scope:** metadata-only (recommended first) vs. also
   deleting orphaned data files (physical GC follow-up).
3. **Metadata-table scope:** `refs` + `snapshots` only (what heimdall needs,
   recommended first) vs. full Iceberg metadata-table set (`files`, `manifests`,
   `history`, `partitions`, `properties`, `all_*`).

---

## 12. Reference files

- `crates/sail-logical-plan/src/load_data.rs` — `LoadDataNode` (node template)
- `crates/sail-plan/src/resolver/command/load.rs` — resolver template
- `crates/sail-plan/src/resolver/command/mod.rs` — command dispatch
- `crates/sail-iceberg/src/physical/table_scan_planner.rs` — `plan_extension`
- `crates/sail-iceberg/src/physical/load_data_planner.rs` — planner template
- `crates/sail-iceberg/src/physical_plan/planner/op_delete.rs` — TRUNCATE machinery
- `crates/sail-iceberg/src/physical_plan/commit/commit_exec.rs` — commit exec
- `crates/sail-iceberg/src/catalog_support/commit.rs` — `IcebergCatalogCommitMode`
- `crates/sail-iceberg/src/spec/catalog/mod.rs` — `TableUpdate::{SetSnapshotRef, RemoveSnapshots}`
- `crates/sail-iceberg/src/table_format.rs` — `retry_metadata_commit`,
  `create_deleter`, `alter_table_add_columns`
- `crates/sail-common/src/spec/plan.rs` — `CommandNode`
- `crates/sail-common/src/spec/expression.rs` — `ObjectName`
- `crates/sail-plan/src/resolver/query/read.rs` — metadata-table interception point
- `crates/sail-plan/src/resolver/schema.rs` — `resolve_table_reference`
- `crates/sail-catalog-system/src/table_source.rs` — virtual-table precedent
- `crates/sail-flight/src/service.rs` — Flight SQL service
- `crates/sail-sql-parser/data/keywords.txt` — keyword registry
- `crates/sail-sql-macro/src/tree/parser.rs` — chumsky derive
- heimdall: `internal/db/iceberg.go`, `internal/cli/load.go`, `internal/cli/mapper.go`,
  `run.sh`, `scripts/_common.sh`
