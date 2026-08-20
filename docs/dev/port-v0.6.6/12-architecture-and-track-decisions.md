# 12 — Architecture & Track Decisions: Catalog Commands vs Logical Plans

> **Purpose:** This is the single source of truth for *where each v0.6.6 feature lands*
> when ported to `feat/0.7.0` (= `tag/v0.7.0`). It resolves the architecture question
> ("is a logical plan REALLY required in the catalog layer?") and records the decisions
> so nobody ever re-litigates them. Every claim below was verified against the v0.7.0
> tree.
>
> **Reading order:** §0 TL;DR → §2 (the two tracks) → §7 (the decisions) → §9 (the
> re-scoped plan). §3–§6 are the "why" and the "how to add new things".

---

## 0. The one-paragraph summary

Sail v0.7.0 has **two execution tracks**:

- **Track A — catalog commands** (`CatalogCommand` → `CatalogCommandNode` → `CatalogCommandExec`):
  for **metadata-only operations** that produce a single result `RecordBatch` and never
  move data through DataFusion executors (CREATE/DROP/ALTER/SHOW/DESCRIBE, …).
  The "logical node" here is a **thin leaf wrapper** — no real plan, no children.
- **Track B — dataflow** (`TableFormat::create_*` → real logical node → format
  `ExtensionPlanner` → physical executor tree): for anything that **reads or writes
  table data** (SELECT/INSERT/UPDATE/DELETE/MERGE/LOAD DATA).

**Decision rule:** if the feature *moves rows through executors* or is *read as a
table*, it is Track B and genuinely needs logical+physical plans. If it only *mutates or
reads metadata* and returns rows, it is Track A and needs only the leaf command wrapper.

**The three port decisions this document locks in:**
1. **CALL procedures → Track A** (re-architected as `CatalogCommand::CallProcedure` +
   `TableFormat::call_procedure`). Removes the v0.6.6 `CallProcedureNode`/`CallProcedureExec`/
   planner-arm/driver-placement/codec work.
2. **LOAD DATA → Track B** with a dedicated `LoadDataNode` (keeps the fast
   parquet-register path; mirrors `IcebergWriteNode`).
3. **Row-level ops, metadata tables → Track B** (unchanged from v0.6.6; these are
   already how v0.7.0 does Delta, and metadata tables are `SELECT`s).

---

## 1. The core question and why it matters

Question: *"Is logical plan and stuff REALLY required in the catalog layer? Why would it
be required?"*

Answer:
- **Not for metadata operations.** The catalog layer already executes them through the
  leaf `CatalogCommandNode`/`CatalogCommandExec` pair; the real logic lives in
  `CatalogCommand::execute`. No dataflow plan is involved.
- **Yes for anything that moves data.** UPDATE/DELETE/MERGE/LOAD DATA and every
  `SELECT` (including the metadata tables) must be real logical→physical plans so that
  DataFusion parallelism, optimizer, metrics, remote/distributed execution, EXPLAIN,
  gold-data diffing, and reattach all work.

This matters because it decides how much of the v0.6.6 machinery we re-implement. The
v0.6.6 CALL implementation (logical node + physical exec + driver placement + codec) is
**over-engineered for v0.7.0**; the rest of the v0.6.6 plan is aligned with v0.7.0's own
patterns.

---

## 2. The verified two-track architecture (v0.7.0)

### 2.1 Track A — the catalog-command path

Verified files: `crates/sail-catalog/src/command.rs`,
`crates/sail-plan/src/catalog.rs`, `crates/sail-physical-plan/src/catalog_command.rs`,
`crates/sail-session/src/planner.rs`.

```
resolver (sail-plan)
  resolve_command → resolve_catalog_command(command)
      └─ LogicalPlan::Extension(CatalogCommandNode::try_new(ctx, command)?)
                                              │  (leaf: inputs=[], expressions=[],
                                              │   schema = command.schema(ctx))
                                              ▼
session ExtensionPhysicalPlanner (sail-session/src/planner.rs)
  plan_extension: downcast CatalogCommandNode → CatalogCommandExec::new(node.command().clone(), schema)
                                              │  (leaf physical exec, 1 partition)
                                              ▼
CatalogCommandExec::execute(partition 0, ctx)
  → manager = ctx.extension::<CatalogManager>()?
  → batch = command.execute(ctx, manager.as_ref()).await?
  → RecordBatchStreamAdapter(schema, once(batch))            // one result batch
```

Properties:
- `CatalogCommand` derives `Debug, Clone, Eq, PartialEq, PartialOrd, Hash, Serialize,
  Deserialize` — it is a **serializable value**, round-trips through the remote-exec
  codec (`NodeKind::CatalogCommand`), so driver placement + reattach come free.
- `CatalogCommandNode.name()` is `"CatalogCommand: <variant>"`; `fmt_for_explain` prints
  it; no children, no expressions.
- `CatalogCommandExec.properties()` = 1 partition, `EmissionType::Final`,
  `Boundedness::Bounded`.
- **Storage-backed operations reach the format from inside `CatalogCommand::execute`** via
  `ctx.extension::<TableFormatRegistry>()?.get(&format)` then a format method. Two
  verified precedents:
  - `CatalogCommand::AlterTable.execute` (command.rs ~441): resolves
    `manager.resolve_lakehouse_table_status(&table, &table_status, LakehouseOperation::Alter)`
    → `table_format.alter_table(runtime, &location, storage_operation, Some(lakehouse_table))`
    → then `manager.alter_table(&table, catalog_options)` (catalog sync).
  - `CatalogCommand::CreateTable.execute` (command.rs ~875): →
    `table_format.create_table_metadata(ctx.runtime_env(), TableFormatCreateTableInfo{...})`
    → then `manager.create_table(...)`.
- The `LakehouseResolvedTable` (with `.execution: LakehouseExecutionContext`) comes from
  `manager.resolve_lakehouse_table_status(table, status, operation)` (sail-catalog/src/manager/table.rs:51).

### 2.2 Track B — the dataflow logical-plan path

Verified files: `crates/sail-common-datafusion/src/datasource.rs` (the `TableFormat`
trait), `crates/sail-iceberg/src/table_format.rs`, `crates/sail-iceberg/src/physical/table_scan_planner.rs`,
`crates/sail-session/src/planner.rs`.

```
resolver (sail-plan)
  resolve_command → format = registry.get(info.format)
  → format.create_writer(SinkInfo) / create_deleter(DeleteInfo) / create_merger(MergeInfo) / create_source(SourceInfo)
      └─ real logical node: IcebergWriteNode / RowLevelWriteNode / IcebergTableSource (TableScan)
                                              │
                                              ▼
DataFusion optimizer passes
                                              ▼
format ExtensionPlanner (IcebergPhysicalPlanner / DeltaPhysicalPlanner)
  plan_extension: downcast IcebergWriteNode → plan_iceberg_write(...)      // physical tree
                  downcast RowLevelWriteNode → plan_iceberg_row_level_write(...)
  plan_table_scan: downcast IcebergTableSource → provider.scan(...)
                                              ▼
Physical executor tree (scan → filter → writer → commit)
```

Properties:
- Real nodes implement `UserDefinedLogicalNodeCore`; real execs implement
  `ExecutionPlan` with children.
- The format's `ExtensionPlanner` returns `Ok(Some(plan))` only for its own nodes;
  `Ok(None)` defers to the next planner.
- Writes funnel through the format's commit exec (Iceberg `IcebergCommitExec`) which
  resolves `IcebergCatalogCommitMode` (§13 of `sail-idioms-and-patterns.md`).

### 2.3 The decision rule (classify any feature)

Ask three questions, in order:

1. **Is it read as a table?** (i.e. does the user write `SELECT ... FROM it`?)
   → **Track B.** No exceptions. (This is why metadata tables are Track B.)
2. **Does it move rows through executors** (scan/filter/join/write)?
   → **Track B.** (Row-level DML, INSERT, LOAD DATA.)
3. **Otherwise it is metadata maintenance returning rows.**
   → **Track A.** (CREATE/DROP/ALTER/SHOW/DESCRIBE/**CALL**.)

If Track A: add a `CatalogCommand` variant (+ optional `TableFormat` method). If Track B:
add the format logical node + planner arm + physical execs.

---

## 3. Why the "logical plan" exists at all (the real reasons)

1. **The protocol requires plans.** Spark Connect and Flight SQL send *plan* protos; the
   server executes `LogicalPlan → ExecutionPlan`. Every operation, including
   `SHOW TABLES`, must become a plan. Track A's leaf `CatalogCommandNode` is the minimal
   plan that satisfies this.
2. **EXPLAIN / gold-data / reattach.** Plans are what gets explained, diffed against
   Spark gold data, and replayed on reattach. Catalog commands serialize through the
   codec for the same reason.
3. **Dataflow needs the engine.** Parallelism, partition-aware scans, memory accounting,
   metrics, and the optimizer all come from being a real plan. There is no shortcut for
   operations that read/write data.
4. **Single execution pipeline.** One path (`ExecutePlan`) drives everything; commands
   and queries differ only in how deep their plan is.

---

## 4. Feature-by-feature master classification

| # | v0.6.6 feature | Track | v0.7.0 base state | Port action |
|---|---|---|---|---|
| 1 | SHOW TBLPROPERTIES | **A** | absent | add `CatalogCommand::ShowTblProperties` + spec/analyzer/parser |
| 2 | DESCRIBE TABLE `<col>` | **A** | absent (`DescribeTable{table,extended}` only) | add `column` field to the command |
| 3 | DESCRIBE VIEW | **A** | absent (`DescribeItem::View` missing) | parser/analyzer/spec + describe path |
| 4 | ALTER TABLE (rename, add/drop columns, column comment/nullability/position) | **A** | limited (4 variants) | add 6 variants × 3 enums + `table_format_alter_operation` + REST provider + `CommitAuthority::IcebergRestCommit` branch |
| 5 | **CALL procedures** (rollback/set-current/expire) | **A** *(re-architected)* | absent | `CatalogCommand::CallProcedure` + `TableFormat::call_procedure` + expire-GC helper (see §7.1) |
| 6 | Metadata tables (`db.table.snapshots`/`refs`) | **B** | absent | `SourceInfo.metadata_table` + `IcebergMetadataTableProvider` + resolver hook (a `SELECT`) |
| 7 | Row-level UPDATE/DELETE/TRUNCATE/MERGE | **B** | absent for Iceberg (Delta has it) | `create_updater`/`create_deleter`/`create_merger` + `RowLevelWriteNode` reuse + planner module + commit machinery |
| 8 | LOAD DATA | **B** | grammar present, resolver `todo!` | dedicated `LoadDataNode` + `plan_load_data` (see §7.7) |
| 9 | Worker-pool accounting / readiness / spawn-retry / RPC hardening / session self-heal / config | — (execution infra) | partial (ActivityTracker, ServerBuilderOptions present) | port the deltas (unchanged plan) |

---

## 5. Track A deep dive — how to add a metadata command (recipe)

Adding any Track-A feature (SHOW TBLPROPERTIES, CALL, DESCRIBE col, new ALTER ops):

1. **Spec layer** (`sail-common/src/spec/plan.rs`): add the `CommandNode` variant (+
   helper types) so it serializes over the wire. *(Optional for pure catalog-internal
   commands — check whether the parser produces it.)*
2. **Parser + analyzer** (`sail-sql-parser`, `sail-sql-analyzer`): grammar + AST variant +
   `from_ast_statement` arm → `spec::CommandNode`.
3. **Catalog options** (`sail-catalog/src/provider/options.rs`): any serializable
   argument struct (e.g. `CallProcedureOptions`, new `AlterTableOptions` variants).
4. **Catalog command** (`sail-catalog/src/command.rs`): `CatalogCommand::<Variant>`
   + `name()` + `schema()` (via `ArrowSerializer::default().schema::<Row>()`) +
   `execute(ctx, manager)`.
5. **Resolver** (`sail-plan/src/resolver/command/*.rs`): `resolve_command` arm →
   build the `CatalogCommand` → `resolve_catalog_command(...)` (wraps in
   `CatalogCommandNode`). *(Nothing else needed — no planner arm, no codec, no driver
   placement.)*
6. **Storage backing** (only if it touches storage): add a `TableFormat` method
   (mirror `alter_table`) + implement in Iceberg/Delta. Call it from inside
   `CatalogCommand::execute`.

---

## 6. Track B deep dive — how to add a dataflow path (recipe)

Adding any Track-B feature (row-level ops, LOAD DATA, metadata tables):

1. **Shared types** (`sail-common-datafusion`): `SourceInfo`/`SinkInfo`/`DeleteInfo`/
   `MergeInfo`/`UpdateInfo` extensions; `OptionLayer`; new `TableFormat` trait methods.
2. **Logical node** (`sail-logical-plan` or format): `UserDefinedLogicalNodeCore`
   impl (educe, `ItemTaker`, `fmt_for_explain`).
3. **Resolver** (`sail-plan`): `resolve_command` arm → format
   `create_*`/`create_source` → returns the extension node (or `CatalogCommand` for
   metadata-only parts).
4. **Format impl** (`sail-iceberg/src/table_format.rs`): `TableFormat` methods
   (`create_updater`/`create_deleter`/`create_merger`/`create_writer`/`create_source`).
5. **Physical planner** (`sail-iceberg/src/physical/table_scan_planner.rs`): dispatch arm
   in `plan_extension`/`plan_table_scan`.
6. **Physical execs** (`sail-iceberg/src/physical_plan/**`): the `ExecutionPlan` tree,
   following the idiom template (§10 of `sail-idioms-and-patterns.md`).
7. **Commit** (writes): action batches (`iceberg_action_schema`) → `IcebergCommitExec`.
8. **Remote execution** (only if the exec crosses driver↔worker): proto `NodeKind` +
   codec arms + round-trip test.

---

## 7. Per-feature port decisions (the authoritative decision record)

### 7.1 CALL procedures → Track A  ⚠️ CHANGED from v0.6.6

**Rationale:** CALL is metadata maintenance. It loads table metadata, computes
`TableUpdate`s, commits them (filesystem or catalog), optionally deletes expired files,
and returns one result row. **No data flows through executors.** Track A is the natural
home, matching `AlterTable`/`CreateTable` exactly.

**Target shape:**
- `CatalogCommand::CallProcedure { table: Vec<String>, procedure: CallProcedureOptions }`
  where `CallProcedureOptions` (in `sail-catalog/src/provider/options.rs`) is a
  serializable enum:
  ```rust
  pub enum CallProcedureOptions {
      RollbackToSnapshot { table: String, snapshot_id: i64 },
      SetCurrentSnapshot { table: String, snapshot_id: Option<i64>, r#ref: Option<String> },
      ExpireSnapshots { table: String, older_than_ms: Option<i64>, retain_last: Option<i32> },
  }
  ```
- `TableFormat` method (mirror `alter_table`):
  `async fn call_procedure(&self, runtime_env, path: &str, operation: TableFormatProcedureOperation, lakehouse_table: Option<LakehouseExecutionContext>) -> Result<RecordBatch>`
  with `TableFormatProcedureOperation` (in `sail-common-datafusion/src/datasource.rs`)
  mirroring `TableFormatAlterTableOperation` and defaulted to `not_impl_err!`.
- Iceberg impl: `Table::load` → `compute_procedure_updates` + `procedure_requirements`
  (the retain-set algorithm from v0.6.6 `call_procedure_exec.rs`) → commit via
  `IcebergCatalogCommitMode`/`IcebergCatalogCommitCoordinator` (filesystem via the
  `retry_metadata_commit`-style loop) → expire GC via a **reusable function**
  (`expire_snapshots_gc.rs` moved to a plain utility, not an exec) → build the result
  `RecordBatch` (`CallProcedureOutput` schema).
- Resolver: `CommandNode::CallProcedure` → `resolve_command_call_procedure` → builds the
  `CatalogCommand` (with resolved scalar args) → `resolve_catalog_command`.
- Output schema: reuse the v0.6.6 `CallProcedureOutput` schemas
  (`previous_snapshot_id`/`current_snapshot_id`; six `deleted_*_count`) as the command's
  `schema()` + row serializer.

**Explicitly REMOVED from the port** (do not re-create):
- `CallProcedureNode` logical node (sail-logical-plan).
- `CallProcedureExec` as an `ExecutionPlan`.
- The `IcebergPhysicalPlanner::plan_extension` arm for CALL.
- `job_graph/planner.rs` driver-stage detection for `CallProcedureExec`.
- `CallProcedureExecNode` proto + codec arms.

**Fallback (only if you insist on keeping all CALL logic out of the catalog):** keep the
v0.6.6 Track-B shape. It works on v0.7.0 but adds the removed surface back. **Decision:
Track A. Do not revisit unless the commit machinery cannot be reached from a
`TableFormat` method — it can (the format owns `Table::load` + the commit coordinator).**

### 7.2 SHOW TBLPROPERTIES → Track A (unchanged from v0.6.6)

`CatalogCommand::ShowTblProperties { table, property_key }` +
`ShowTblPropertiesRow { key, value }`, rows sorted by key, tables only (reject views).
Spec/analyzer/parser as in v0.6.6.

### 7.3 DESCRIBE TABLE `<col>` / DESCRIBE VIEW → Track A (unchanged)

Add `column: Option<String>` to `CatalogCommand::DescribeTable`; single-column describe
with `NotFound(CatalogObject::Column)`; `DescribeItem::View` in parser/analyzer.

### 7.4 ALTER TABLE → Track A (unchanged, + one v0.7.0 fix)

Add the six variants (`RenameTable`, `AddColumns`, `DropColumns`, `AlterColumnComment`,
`AlterColumnNullability`, `AlterColumnPosition`) to: `spec::AlterTableOperation`,
`AlterTableOptions`, `TableFormatAlterTableOperation` + `Display` labels; the converter
`table_format_alter_operation`; resolver `resolve_catalog_alter_table`; the Iceberg
filesystem `TableFormat::alter_table` (+ `retry_metadata_commit` extraction); the REST
provider `alter_table` (+ rename/properties/add/drop columns +
`map_update_table_alter_error`); memory/glue/hms/onelake/delta arms.
**v0.7.0-specific fix:** add the `CommitAuthority::IcebergRestCommit` branch in
`CatalogCommand::AlterTable.execute` (v0.7.0 currently routes every lakehouse format to
`table_format.alter_table`, which rejects catalog-managed tables).

### 7.5 Metadata tables → Track B (forced)

`SELECT` on `db.table.snapshots`/`refs` is a read → `SourceInfo.metadata_table` +
`TableFormat::create_source` branch → `IcebergMetadataTableProvider` (a `TableProvider`).
Keep v0.6.6 shape. The mechanical `metadata_table: None` additions land in this phase.

### 7.6 Row-level UPDATE/DELETE/TRUNCATE/MERGE → Track B (unchanged)

Matches v0.7.0's Delta pattern: `TableFormat::create_updater/create_deleter/create_merger`
→ `RowLevelWriteNode` (reuse existing) → `physical_plan/planner/*` module →
targeted rewrite + commit machinery. Add `create_updater` + `UpdateInfo`/`UpdateAssignment`
to the trait (v0.6.6 additions).

### 7.7 LOAD DATA → Track B, dedicated `LoadDataNode`  ⚠️ DECISION

**Rationale:** LOAD DATA writes data (append/overwrite), so a physical pipeline is
required. A dedicated `LoadDataNode` mirrors `IcebergWriteNode`, is handled by the
format's `ExtensionPlanner` (same as v0.6.6), and **preserves the fast parquet-register
path** (footer registration without rewriting). The alternative — resolving LOAD DATA to
`INSERT INTO t SELECT * FROM <path>` — would lose the fast path and is **rejected**.

Keep: `load_classifier`, `load_data_planner`, `IcebergLoadDataFastExec`,
`operations/parquet_utils`, `aggregate_from_parquet_metadata_with_field_map`, codec
`IcebergLoadDataFastExecNode` (proto =55), commit count-summing.

### 7.8 Distributed-execution features → independent of track

Worker-pool accounting, readiness gate, spawn retry, RPC hardening, session self-heal,
config keys, activity-tracker executor wiring: these are execution infrastructure and
follow the Phase-10 plan in `11-refactor-plan.md` unchanged.

---

## 8. What the re-scoping REMOVES from the v0.6.6 plan

| Removed (do not port) | v0.6.6 location | Why removed |
|---|---|---|
| `CallProcedureNode` logical node | `sail-logical-plan/src/call_procedure.rs` (the node; keep the `CallProcedure` enum as `CallProcedureOptions` in catalog options instead) | CALL is Track A |
| `CallProcedureExec` `ExecutionPlan` | `sail-iceberg/src/physical_plan/call_procedure_exec.rs` (as an exec; keep the pure functions) | Track A — command returns a batch |
| `IcebergPhysicalPlanner` CALL arm | `physical/table_scan_planner.rs` | Track A |
| `job_graph/planner.rs` CALL driver-placement | `sail-execution/src/job_graph/planner.rs` | `CatalogCommandExec` already runs driver-side + serializes |
| `CallProcedureExecNode` proto/codec | `physical.proto` =56, `codec.rs` | `CatalogCommand` already round-trips through the codec |
| `update.rs`/`call.rs`/`load.rs` resolver modules stay **Track B** where data flows | — | unchanged |

**Keep from v0.6.6 CALL work (as functions, not execs):**
- `compute_procedure_updates`, `expire_snapshot_updates`, `retained_snapshot_ids`,
  `set_main_snapshot_ref`, `resolve_target_snapshot_id`, `is_current_ancestor`,
  `procedure_requirements`, `validate_procedure_requirements`, `apply_procedure_updates`,
  `CallProcedureOutput` schemas → live in the Iceberg `TableFormat::call_procedure`
  implementation (or a helper module).
- `expire_files_gc`, `collect_files`, `diff_files`, `delete_files`, `FileKind`,
  `ExpireGcCounts` → plain utility module (not an exec).

---

## 9. The re-scoped phase plan (supersedes the Phase-8 of `11-refactor-plan.md`)

| Phase | Scope | Track |
|---|---|---|
| 0 | deps (`sail-iceberg` += `sail-logical-plan`, `datafusion-datasource`, tokio dev; `sail-catalog-memory` += `sail-common`; `sail-logical-plan` += `serde`) | — |
| 1 | spec + parser + analyzer: CALL, SHOW TBLPROPERTIES, TRUNCATE, DESCRIBE VIEW, ALTER ops | A (grammar) |
| 2 | common surface: `SourceInfo.metadata_table` (+ mechanical None), `UpdateInfo`/`UpdateAssignment`, `TableFormat::create_updater`, six `TableFormatAlterTableOperation` variants + labels, `IcebergMetadataTableType`, `TableFormatProcedureOperation` | A+B |
| 3 | logical plans: `load_data.rs` (`LoadDataNode`); `merge.rs` `expand_update`/`UpdateExpansion`. **No `call_procedure.rs` node.** | B |
| 4 | resolvers: `command/load.rs`, `command/update.rs`, metadata-table hook, ALTER mapping, **`command/call.rs` → builds `CatalogCommand::CallProcedure` (not a node)** | A+B |
| 5 | **catalog commands:** SHOW TBLPROPERTIES, DESCRIBE col, six ALTER ops, **CALL**, REST provider (replace/alter/rename/access-delegation), memory rename, glue/hms/onelake/delta stubs, `IcebergRestCommit` routing fix | A |
| 6 | metadata tables: `IcebergMetadataTableProvider` + snapshot accessors | B |
| 7 | row-level ops + commit machinery (unchanged `11` §Phase 7) | B |
| 8 | **CALL execution backend**: `TableFormat::call_procedure` impl + expire-GC utility module (no exec/no codec) | A |
| 9 | LOAD DATA: classifier/planner/fast-exec/codec | B |
| 10 | distributed execution (unchanged) | — |
| 11 | docker/k8s/build.sh/python tests/docs | — |

Verification per phase: `cargo check -p <crate>`; targeted `cargo test -p <crate>`;
Python `pytest` subset for 11.

---

## 10. Open decisions — resolved

| Decision | Resolution | Who/why |
|---|---|---|
| CALL: Track A vs Track B | **Track A** (CatalogCommand + TableFormat method) | §7.1 — metadata-only op; matches AlterTable/CreateTable; removes ~5 pieces of machinery |
| LOAD DATA: dedicated node vs INSERT-rewrite | **Dedicated `LoadDataNode`** | §7.7 — preserves fast parquet-register; mirrors `IcebergWriteNode` |
| Metadata tables: Track B | forced | §7.5 — they are `SELECT`s |
| Row-level ops: Track B | forced | §7.6 — they move data; identical to v0.7.0 Delta |
| `CallProcedureOptions` location | `sail-catalog/src/provider/options.rs` | Track A convention (like `AlterTableOptions`) |
| expire-GC as exec vs utility | **utility** | §8 — only called from `call_procedure`; no plan needed |

---

## 11. Confusion-prevention glossary & decision log

| Term | Meaning |
|---|---|
| Track A | catalog command path: `CatalogCommand` → `CatalogCommandNode` → `CatalogCommandExec` → `execute` → one `RecordBatch` |
| Track B | dataflow path: `TableFormat::create_*` → logical node → `ExtensionPlanner` → physical exec tree |
| `CatalogCommandNode` | thin leaf logical node wrapping a `CatalogCommand` (Track A) |
| `CatalogCommandExec` | leaf physical exec that runs `CatalogCommand::execute` |
| `TableFormat::call_procedure` | new trait method (v0.6.6 didn't have it) enabling Track-A CALL |
| `CallProcedureOptions` / `TableFormatProcedureOperation` | serializable procedure-argument types (Track A) |
| `LoadDataNode` | Track-B logical node for LOAD DATA (kept) |
| "logical plan required?" | required ⇔ the feature moves data OR is read as a table; otherwise Track A |

**Decision log:**
- 2026-08-17 — CALL re-architected to Track A (`CatalogCommand::CallProcedure` +
  `TableFormat::call_procedure`); removed v0.6.6 CALL logical node / exec / driver
  placement / codec.
- 2026-08-17 — LOAD DATA stays Track B with dedicated `LoadDataNode`.
- 2026-08-17 — SHOW/DESCRIBE/ALTER stay Track A; metadata tables + row-level ops stay
  Track B.
