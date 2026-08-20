# Sail Idioms & Implementation Patterns (v0.7.0)

> **Purpose:** The canonical idioms and structural patterns of the Sail codebase, audited
> against the current tree (**`feat/0.7.0` = `tag/v0.7.0`, commit `f0b137d6`**). Any port
> from `feat/v0.6.6` or any new feature/edit must land in the "logical home" described
> here — matching how the existing code is layered, named, and wired. Where a pattern
> differs from `feat/v0.6.6`, the delta is called out so the port lands correctly.
>
> This document supersedes the planning-oriented
> `feat/v0.6.6` `sail-implementation-patterns.md` as the **current-codebase** reference;
> line references below were verified against the v0.7.0 tree.

---

## Table of Contents

1. [The request pipeline (architecture at a glance)](#1-the-request-pipeline)
2. [Crate roles & dependency layering](#2-crate-roles--dependency-layering)
3. [Rust idioms used throughout](#3-rust-idioms-used-throughout)
4. [The spec → analyzer → resolver → logical → physical pipeline](#4-the-pipeline-in-detail)
5. [Core abstractions](#5-core-abstractions)
6. [SQL front-end pattern (parser + analyzer + spec)](#6-sql-front-end-pattern)
7. [Resolver pattern (`PlanResolver`)](#7-resolver-pattern)
8. [Logical node idiom (`UserDefinedLogicalNodeCore`)](#8-logical-node-idiom)
9. [Physical planning (`ExtensionPlanner`)](#9-physical-planning)
10. [Physical executor idiom (`ExecutionPlan`)](#10-physical-executor-idiom)
11. [The write + commit pipeline (action batches)](#11-the-write--commit-pipeline)
12. [Options & configuration generation](#12-options--configuration)
13. [Catalog integration (path vs catalog-managed)](#13-catalog-integration)
14. [Actor framework & distributed execution](#14-actor-framework--distributed-execution)
15. [Codec / proto for remote execution](#15-codec--proto)
16. [Naming & code conventions](#16-naming--code-conventions)
17. [Test conventions](#17-test-conventions)
18. [Anti-patterns to avoid](#18-anti-patterns-to-avoid)
19. [Positioning guide for the v0.6.6 port](#19-positioning-guide-for-the-v066-port)
20. [Appendix: canonical file map + idiom checklist](#20-appendix)

---

## 1. The request pipeline

Every Spark Connect / Flight-SQL request flows through the same stages. New features
slot into the stage that owns their concern:

```
SQL / Spark Connect plan (client)
   │
   ▼
sail-sql-parser        AST (Statement, ObjectName, Expr, ...)
   ▼
sail-sql-analyzer      from_ast_statement(): AST → spec::Plan      (protocol-agnostic)
   ▼
sail-common::spec      spec::Plan { Query(QueryPlan) | Command(CommandPlan) }
   │                    CommandNode variants (Select, Insert, Delete, ...)
   ▼
sail-plan              PlanResolver: spec → DataFusion LogicalPlan
   │                     resolve_query_plan / resolve_command_*
   │                     + PlanResolverState (field ids, temp views, ...)
   ▼
DataFusion LogicalPlan (TableScan, Filter, Extension{format node}, ...)
   ▼
DataFusion optimizer passes (sail-physical-optimizer, sail-plan rewriters)
   ▼
sail-session/planner   ExtensionQueryPlanner::create_physical_plan
   │                     → DefaultPhysicalPlanner + registered ExtensionPlanners
   │                       (IcebergPhysicalPlanner, DeltaPhysicalPlanner, ...)
   ▼
sail-iceberg /         format ExtensionPlanner::plan_extension() → ExecutionPlan tree
sail-delta-lake          (scan → filter → writer → commit)
   ▼
sail-execution         distributed execution (job graph, driver/worker actors,
                       task assigner) OR local execution
   ▼
object stores / delta log / iceberg catalog
```

**Invariant:** `sail-common::spec` is the protocol-agnostic intermediate. The parser
crate produces parser-specific AST; the analyzer lowers it to `spec`; the resolver
consumes `spec` and produces DataFusion `LogicalPlan`. Each layer only talks to the one
below via these types.

---

## 2. Crate roles & dependency layering

| Crate | Role | Depends on |
|---|---|---|
| `sail-common` | shared config (`AppConfig`), `spec::{plan,expression,data_type,literal}`, `runtime`, common error | — |
| `sail-sql-parser` | SQL grammar → AST (`Statement`, `ObjectName`, `Expr`), keyword tables, `syntax.json` gold data | `sail-common` |
| `sail-sql-analyzer` | AST → `spec::Plan` (`from_ast_statement`), `SqlError` | parser, common |
| `sail-common-datafusion` | **cross-cutting DataFusion glue**: `TableFormat` trait, `TableFormatRegistry`, `OptionLayer`, `SourceInfo/SinkInfo/DeleteInfo/MergeInfo`, `SessionExtension`, `LakehouseExecutionContext`, `MergeCapableSource`, `ActivityTracker`, rename/extension utils | `sail-common` |
| `sail-logical-plan` | shared logical plan nodes: `RowLevelWriteNode`, `expand_merge`, `MergeCardinalityCheckNode`, `MonotonicIdNode`, barriers | common-datafusion, `sail-function` |
| `sail-plan` | `PlanResolver`: spec → DataFusion `LogicalPlan`; `PlanError` | all of the above |
| `sail-catalog` | `CatalogProvider` trait, `CatalogManager`, `CatalogCommand`, `AlterTableOptions`, `TableFormatAlterTableOperation` bridge | common-datafusion |
| `sail-catalog-*` | concrete catalogs (memory, iceberg-rest, hms, glue, onelake, unity) | `sail-catalog` |
| `sail-iceberg` | Iceberg `TableFormat` impl, physical planners/executors, spec (TableMetadata, manifests, snapshots), catalog_support/commit | `sail-logical-plan`, catalog |
| `sail-delta-lake` | Delta `TableFormat` impl + physical layer | same as iceberg |
| `sail-data-source` | generic file sources (parquet/csv/json, listing), `ResolveOptions`/`PartialOptions` traits | common |
| `sail-session` | session manager, `ExtensionQueryPlanner`, planner registration, `SessionFactory` | everything |
| `sail-spark-connect` | Spark Connect gRPC front-end | session |
| `sail-flight` | Flight SQL front-end | session |
| `sail-execution` | distributed execution: actors, job graph, driver/worker, task assigner, codec/proto | sail-server, iceberg (driver-stage detection) |
| `sail-server` | the actor framework (`actor.rs`), gRPC server builder, `RetryStrategy` | common |
| `sail-physical-plan` / `sail-physical-optimizer` | shared physical operators (barrier, merge-cardinality-check) + optimizer rules | common |
| `sail-function` | scalar/aggregate/window UDFs | common |

**Layering rules observed:**
- Direction is always downward (a crate uses the ones beneath it). No cycles between
  the format crates (`sail-iceberg` ↔ `sail-delta-lake` never reference each other).
- Cross-format shared code lives in `sail-common-datafusion` / `sail-logical-plan`, never
  in a format crate.
- `sail-plan` references formats only via `TableFormatRegistry` (strings), not concrete
  format types.

---

## 3. Rust idioms used throughout

### 3.1 Error types — `thiserror` enum + constructor fns + `Result` alias

Every layer has its own error enum deriving `thiserror::Error`, with `#[from]` for
downstream errors and **short constructor helpers** so call sites read like prose:

```rust
// crates/sail-plan/src/error.rs
pub type PlanResult<T> = Result<T, PlanError>;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("error in DataFusion: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    ...
}

impl PlanError {
    pub fn todo(message: impl Into<String>) -> Self { PlanError::NotImplemented(message.into()) }
    pub fn unsupported(message: impl Into<String>) -> Self { PlanError::NotSupported(message.into()) }
    pub fn invalid(message: impl Into<String>) -> Self { PlanError::InvalidArgument(message.into()) }
    pub fn missing(...) / internal(...) / analysis(...)
}
```

The constructor-fn idiom (`todo`, `unsupported`, `invalid`, `missing`) is repeated in
`SqlError`, `CatalogError`, `PlanError`, `DataSourceError`, `ExecutionError`. **Use the
existing constructor instead of building variants inline.**

- `PlanError` also has `From<CommonError>` and friends via match.
- A small `IntoPlanResult<T>` blanket-trait pattern lets `Ok(x).into_plan_result()`.
- `DataFusionError::External(Box::new(e))` wraps non-DF errors at crate boundaries
  (esp. physical layer); `internal_err!` / `not_impl_err!` / `plan_err!` macros from
  `datafusion_common` are used inside `ExecutionPlan` impls.

### 3.2 `Arc`, `Send + Sync`, `'static`

- Logical nodes and execution plans are wrapped in `Arc`; traits require
  `Send + Sync + 'static` (physical plans cross actor/thread boundaries).
- `UserDefinedLogicalNodeCore` nodes store `schema: DFSchemaRef` (an `Arc<DFSchema>`)
  and `input: Arc<LogicalPlan>`.
- `TableFormat: Send + Sync`; format impls are stateless structs (e.g. `IcebergTableFormat;`)
  registered as `Arc<dyn TableFormat>`.

### 3.3 Serialization — `serde` + `educe`

- Protocol-agnostic types derive `Serialize, Deserialize` with
  `#[serde(rename_all = "camelCase")]` (spec plan/expression) or `#[serde(rename =
  "...")]` for action enums (`ExecAction`).
- Logical nodes use **`educe`** to hand-roll `Debug/PartialEq/PartialOrd` while skipping
  non-comparable fields (schema, caches):
  ```rust
  #[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
  #[educe(PartialOrd)]
  pub struct SomeNode {
      input: Arc<LogicalPlan>,
      #[educe(PartialOrd(ignore))]
      schema: DFSchemaRef,
  }
  ```
- Secrets are `SecretString` with `serialize_optional_secret`.

### 3.4 `#[async_trait]`

Every async trait (`TableFormat`, `CatalogProvider`, `WorkerManager`, `ExecutionPlan`'s
async methods) uses `#[async_trait]`. Format planner functions are `pub async fn`.

### 3.5 Concurrency primitives

- `std::sync::OnceLock` / `tokio::sync::OnceCell` for lazy singleton state
  (e.g. k8s pod API client `OnceCell`, test runtime `OnceLock`).
- `Arc<dyn ObjectStore>` from `RuntimeEnv::object_store_registry`.
- Actors use mpsc channels + `ActorHandle` (see §14).

### 3.6 `ItemTaker` and small utils

`sail_common_datafusion::utils::items::ItemTaker` provides `.zero()`, `.one()`,
`.one_opt()`, `.to_tuple()` used to destructure `Vec<LogicalPlan>`/`Vec<Expr>` in
`with_exprs_and_inputs` — e.g. `exprs.zero()?; inputs.one()?;`. **New logical nodes
should use it.**

---

## 4. The pipeline in detail

### 4.1 `spec::Plan` (protocol-agnostic)

```rust
// crates/sail-common/src/spec/plan.rs
pub enum Plan { Query(QueryPlan), Command(CommandPlan) }
pub struct QueryPlan { #[serde(flatten)] pub node: QueryNode, pub plan_id: Option<i64> }
pub struct CommandPlan { #[serde(flatten)] pub node: CommandNode, pub plan_id: Option<i64> }
```

- `QueryNode` covers reads/writes/setops; `CommandNode` covers DDL/DML commands
  (e.g. `CreateTable`, `Insert`, `Delete`, `MergeInto`, `AlterTable`, `LoadData`,
  `ShowTables`, `DescribeDatabase`).
- **Port note (v0.6.6):** `CommandNode::CallProcedure` and `CommandNode::ShowTblProperties`
  do **not exist** in v0.7.0 — they belong here when ported.

### 4.2 Analyzer (`sail-sql-analyzer/src/statement.rs`)

Single entry `from_ast_statement(Statement) -> SqlResult<spec::Plan>`. Huge `match` on
every `Statement` variant, producing `spec::Plan::Command(CommandPlan::new(node))`.
Helper `from_ast_*` fns (`from_ast_expression`, `from_ast_alter_table_operation`, ...).
**Port rule:** new statements → new `Statement` AST variant (parser) + one `match` arm
here → `spec::CommandNode`.

### 4.3 Resolver (`sail-plan/src/resolver/`)

`PlanResolver` methods:
- `resolve_query_plan(spec::QueryPlan, state)` → for reads/writes.
- `resolve_command(&CommandPlan, state)` → dispatches `CommandNode` to
  `resolve_command_*` per variant (`resolve_command/mod.rs`).
- Command submodules mirror the variant groups:
  `resolver/command/{create_table,insert,merge,delete,update,alter,load,call,...}.rs`
  (the exact files vary by version; v0.6.6 added `call.rs`, `load.rs`, `update.rs`).
- `PlanResolverState` holds field-id maps, temp views, and other mutable resolver state.
- Expression resolution: `resolve_expression(expr, schema, state)`.

**The "rename then un-rename" idiom:** resolvers build a resolution schema whose columns
are opaque field ids (`state.register_fields`), then `expression_before_rename(...)` /
`rename_logical_plan(plan, &real_names)` to convert back to user-facing names before the
format layer consumes them. UPDATE/MERGE use this (see `resolver/command/update.rs`).

---

## 5. Core abstractions

### 5.1 `TableFormat` (the central trait)

`crates/sail-common-datafusion/src/datasource.rs`:

```rust
#[async_trait]
pub trait TableFormat: Send + Sync {
    fn name(&self) -> &str;
    async fn create_source(&self, ctx: &dyn Session, info: SourceInfo) -> Result<Arc<dyn TableSource>>;
    async fn infer_schema(&self, ...) -> Result<SchemaRef> { /* default via create_source */ }
    async fn infer_metadata(&self, ...) -> Result<TableFormatMetadata> { /* default */ }
    async fn create_writer(&self, ctx, info: SinkInfo) -> Result<LogicalPlan>;
    async fn create_table_metadata(&self, runtime_env, info) -> Result<TableFormatCreateTableResult> { /* default no-op */ }
    async fn create_deleter(&self, ctx, info: DeleteInfo) -> Result<LogicalPlan> { /* default not_impl_err */ }
    async fn create_merger(&self, ctx, info: MergeInfo) -> Result<LogicalPlan> { /* default not_impl_err */ }
    async fn alter_table(&self, runtime_env, path, operation: TableFormatAlterTableOperation, lakehouse_table) -> Result<()> { /* default match + per-op default fns */ }
    // + private defaults: alter_table_properties, alter_table_column_type, ...
}
```

Key traits on it:
- **The `create_*` fns return `LogicalPlan`** (extension nodes), not physical plans —
  physical planning happens later in the format's `ExtensionPlanner`.
- Default methods use `not_impl_err!` with the format name, so unimplemented ops degrade
  gracefully per format.
- **Port note (v0.6.6):** v0.7.0 has **no `create_updater`**, no `UpdateInfo`, no
  `UpdateAssignment` — they are v0.6.6 additions. They belong as a new defaulted trait
  method here + a struct in this file.

### 5.2 `TableFormatRegistry`

```rust
pub struct TableFormatRegistry { formats: Mutex<HashMap<String, Arc<dyn TableFormat>>> }
impl TableFormatRegistry {
    pub fn register(&self, format: Arc<dyn TableFormat>) -> Result<()>;
    pub fn get(&self, name: &str) -> Result<Arc<dyn TableFormat>>;
}
```
Registered at session startup by each format. Resolvers look formats up **by string**
(`registry.get(&info.format)`), never by concrete type.

### 5.3 `OptionLayer` and option resolution

```rust
pub enum OptionLayer {
    TablePropertyList { items: Vec<(String,String)> },   // lowest priority
    OptionList { items: Vec<(String,String)> },
    TableLocation { value: String },
    AsOfTimestamp { value: DateTime<Utc> },
    AsOfIntegerVersion { value: i64 },
    AsOfStringVersion { value: String },
}
```

Resolution protocol (`crates/sail-data-source/src/options/mod.rs`):

```rust
pub trait ResolveOptions: Sized { fn resolve(ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self>; }
pub trait PartialOptions { type Options; fn initialize() -> Self; fn merge(&mut self, other: Self); fn finalize(self) -> DataSourceResult<Self::Options>; }
pub trait BuildPartialOptions<T> { fn build_partial_options(self) -> DataSourceResult<T>; }
```

A format's options type (generated, see §12) implements `ResolveOptions` by folding the
layers: `partial = initialize(); for layer { partial.merge(layer.build_partial_options()?) } partial.finalize()`.
Later layers override earlier ones. **Port rule:** any new option is added to the
format's `data/options/<format>.yaml`, regenerated, and resolved via
`<Format>WriteOptions::resolve(ctx, node.options().to_vec())`.

### 5.4 The `*Info` structs

`SourceInfo` (read; note the `metadata_table: Option<...>` field is a **v0.6.6 addition**,
absent in v0.7.0), `SinkInfo` (write), `DeleteInfo`, `MergeInfo`. All carry
`Vec<OptionLayer>` for options + `Option<LakehouseExecutionContext>` for catalog context.

### 5.5 `CatalogProvider` + `CatalogManager` + `CatalogCommand`

- `CatalogProvider` (`sail-catalog/src/provider/mod.rs`): `create_database/get_database/
  list_databases/drop_database/create_table/get_table/list_tables/drop_table/alter_table/
  create_or_replace/...` returning `*Status` structs. Sync-ish async API; namespaced.
- `CatalogManager` (`manager.rs`): wraps providers, holds the registry of catalog names,
  exposes `get_table_or_view`, `alter_table`, `create_table`, etc.
- `CatalogCommand` (`command.rs`): a serializable enum of "what the client asked"
  (`CreateTable`, `AlterTable`, `DescribeTable`, `ShowTables`, ...). The resolver builds
  one; `CatalogCommand::execute(ctx)` runs it. Row types derive from `ArrowSerializer`
  (`schema::<ShowTableExtendedRow>()`, `build_record_batch(&rows)`).
- `AlterTableOptions` (`provider/options.rs`) is the **catalog-layer** ALTER enum;
  `TableFormatAlterTableOperation` (`common-datafusion/datasource.rs`) is the
  **storage-layer** ALTER enum; `sail-catalog/src/command.rs::table_format_alter_operation`
  converts between them. **Port note:** the six v0.6.6 ALTER variants
  (`RenameTable`, `AddColumns`, `DropColumns`, `AlterColumnComment/Nullability/Position`)
  must be added to all three enums + the converter + `Display` impls.

### 5.6 `SessionExtension` / `SessionExtensionAccessor`

```rust
pub trait SessionExtension: Send + Sync + 'static { fn name() -> &'static str; }
pub trait SessionExtensionAccessor {
    fn extension<T: SessionExtension>(&self) -> Result<Arc<T>>;
    fn runtime_env(&self) -> Arc<RuntimeEnv>;
}
```
Format caches (`DeltaTableCache`), `ActivityTracker`, `CatalogManager`, etc. are
registered extensions; read via `ctx.extension::<T>()?` or `self.ctx.extension::<T>()?`.

### 5.7 `LakehouseExecutionContext`

`crates/sail-common-datafusion/src/catalog/lakehouse.rs`:
```rust
pub enum LakehouseOperation { Read /*default*/, Write, Create, Maintenance, ... }
pub enum CommitAuthority { Filesystem /*default*/, IcebergMetadataLocationCas, IcebergRestCommit, DeltaRatifiedCommit, ... }
pub enum ScanAuthority { ClientTableFormat /*default*/, IcebergRestServerSide, ProviderNative }
pub struct LakehouseExecutionContext {
    pub catalog_provider_id, pub catalog_table: Vec<String>, pub table_identity,
    pub operation: LakehouseOperation, pub commit: CommitAuthority, pub scan: ScanAuthority,
    pub rest_session: Option<...>,
}
```
Resolved per operation via `resolve_lakehouse_table_context(table, operation, format, options)`.
`commit == IcebergRestCommit` is the switch that routes writes through
`IcebergCatalogCommitCoordinator` (§13).

---

## 6. SQL front-end pattern

1. **`sail-sql-parser/data/keywords.txt`** — add a keyword (e.g. `CALL`) → the macro
   regenerates the keyword table. Non-reserved words used as statement starters need
   this.
2. **`sail-sql-parser/src/ast/statement.rs`** — add a `Statement` variant with
   `#[parser]` field annotations. Structurally rich variants may use helper parser
   combinators (`compose(e, o)` for `name => value` arg lists).
3. **`sail-sql-parser/tests/gold_data/syntax.json`** — golden parse outputs for the new
   statement. **`spark-gold-data`** runs the same SQL against Spark and diffs.
4. **`sail-sql-analyzer/src/statement.rs`** — one `match` arm in `from_ast_statement`,
   converting via `from_ast_*` helpers to `spec`.
5. **`sail-common/src/spec/plan.rs`** — the `CommandNode`/`QueryNode` variant.
6. **`sail-plan` resolver** — `resolve_command` arm → `resolve_command_<x>`.

---

## 7. Resolver pattern

- `PlanResolver` methods take `(&self, ...spec args..., state: &mut PlanResolverState)`
  and return `PlanResult<LogicalPlan>`.
- Look up catalog objects via `self.ctx.extension::<CatalogManager>()?.get_table_or_view(...)`.
- Build lakehouse context via `resolve_lakehouse_table_context(...)`.
- Convert spec → DataFusion `Expr`/`LogicalPlan` with `resolve_expression`,
  `resolve_data_type`, `resolve_constant_literal`, etc.
- For command nodes, produce either a `CatalogCommand` (→ `resolve_catalog_command`) or a
  format extension node via `TableFormatRegistry::get(format)?.create_*`.
- **Port rule:** new commands go in a new `resolver/command/<x>.rs` module +
  `mod` declaration + `CommandNode::<X>` arm in `command/mod.rs`.

---

## 8. Logical node idiom

Every custom logical node implements `UserDefinedLogicalNodeCore`:

```rust
impl UserDefinedLogicalNodeCore for IcebergWriteNode {
    fn name(&self) -> &str { "IcebergWrite" }              // PascalCase, no "Node" suffix
    fn inputs(&self) -> Vec<&LogicalPlan> { ... }
    fn schema(&self) -> &DFSchemaRef { &self.schema }
    fn expressions(&self) -> Vec<Expr> { ... }
    fn fmt_for_explain(&self, f, t) { write!(f, "IcebergWrite: table=..., mode=...") }
    fn with_exprs_and_inputs(&self, exprs, inputs) -> Result<Self> {
        exprs.zero()?; inputs.one()?;  // ItemTaker
        Ok(Self { input: inputs.one()?, .. })
    }
    fn necessary_children_exprs(&self, _o) -> Option<Vec<Vec<usize>>> { None }
}
```

Conventions:
- `name()` returns the display name (`RowLevelWrite`, `MergeCardinalityCheck`).
- `fmt_for_explain` renders the human-readable summary used in `EXPLAIN`.
- `educe` for `PartialOrd` with schema ignored.
- The node struct is `pub`, fields private, with `pub fn new(...)` + per-field accessors.

---

## 9. Physical planning

### 9.1 `ExtensionQueryPlanner` (session level)

`crates/sail-session/src/planner.rs`: builds a `DefaultPhysicalPlanner` whose
`extension_planners` include every format's planner (Delta, Iceberg, catalog-system,
listing, console, python, noop, ...). `create_physical_plan` runs physical planning; the
registered `ExtensionPlanner`s handle extension nodes in **registration order** — the
first to return `Ok(Some(plan))` wins; `Ok(None)` defers to the next / default planner.

### 9.2 The `ExtensionPlanner` dispatch pattern (per format)

`crates/sail-iceberg/src/physical/table_scan_planner.rs` (v0.7.0):

```rust
pub struct IcebergPhysicalPlanner;
#[async_trait]
impl ExtensionPlanner for IcebergPhysicalPlanner {
    async fn plan_extension(&self, planner, node, logical_inputs, physical_inputs, session_state)
        -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<IcebergWriteNode>() else {
            return Ok(None);                      // not ours
        };
        let [logical_input] = logical_inputs else { return internal_err!(...); };
        let [physical_input] = physical_inputs else { return internal_err!(...); };
        plan_iceberg_write(session_state, logical_input, physical_input.clone(), node).await.map(Some)
    }

    async fn plan_table_scan(&self, planner, scan: &TableScan, session_state)
        -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else { return Ok(None); };
        let filters = unnormalize_cols(scan.filters.clone());
        let plan = source.provider().scan(session_state, scan.projection.as_ref(), &filters, scan.fetch).await?;
        Ok(Some(plan))
    }
}
```

Idioms:
- `node.as_any().downcast_ref::<T>()` with early `return Ok(None)` for foreign nodes.
- `let [a, b] = inputs else { internal_err!(...) }` slice destructuring.
- `plan_table_scan` routes the format's `TableSource` to the provider scan; it also owns
  special scan routing (e.g. the v0.6.6 file-path-column scan for row-level ops).
- **Port note:** v0.7.0's Iceberg planner handles only `IcebergWriteNode`. v0.6.6 added
  arms for `RowLevelWriteNode`, `LoadDataNode`, `CallProcedureNode` — they go here.

---

## 10. Physical executor idiom

The universal `ExecutionPlan` template (canonical examples: `IcebergWriterExec`,
`IcebergScanByDataFilesExec`, `IcebergCommitExec`):

```rust
pub struct SomeExec {
    input: Arc<dyn ExecutionPlan>,        // children — NEVER a leaf for data flow
    table_url: Url,                        // Url, not String, for table paths
    ...,
    cache: Arc<PlanProperties>,            // always stored, named `cache`
}

impl SomeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, ...) -> Self {
        let schema = ...;
        let output_partitions = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Final,            // Final for write/commit; delegates for transforms
            Boundedness::Bounded,           // always Bounded
        ));
        Self { input, cache, ... }
    }
}

impl ExecutionPlan for SomeExec {
    fn name(&self) -> &'static str { "SomeExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn with_new_children(&self, children) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.one()?, ...)))     // exactly the child set
    }
    fn required_input_distribution(&self) -> Vec<Distribution> { ... }   // writer controls partitioning
    fn execute(&self, partition, context) -> Result<SendableRecordBatchStream> { ... }
}
```

### `execute()` stream idioms (choose by shape)

| Shape | Pattern |
|---|---|
| One future, one batch | `futures::stream::once(async move { ... })` → `RecordBatchStreamAdapter::new(schema, stream)` |
| Streaming state machine | `stream::try_unfold(state, |mut s| async move { ... })` → `RecordBatchStreamAdapter` |
| Per-batch transform | `input.execute(..)?.map(|batch| transform(batch))` |
| Row filtering | `input.execute(..)?.try_filter_map(|batch| maybe_filter(batch))` |
| Multi-input fan-in | `UnionExec`, `HashJoinExec`, then Coalesce |

Rules:
- **No direct object-store I/O in `execute()`** for data flow — children do the scanning.
- `execute()` must be async-compatible: builds a future/stream, returns the adapter.
- Output schema is fixed in `PlanProperties`; action-producing execs use the format's
  action schema (§11); data execs use the table schema; metadata execs their own.
- **Metrics** (`ExecutionPlanMetricsSet` + `MetricBuilder`, `fn metrics()`): in v0.7.0 this
  is a **Delta** convention (`delta writer_exec`, `remove_actions_exec`, `commit_exec`,
  `dv_writer_exec`). Iceberg's writer gains it in v0.6.6 (part of the port).

---

## 11. The write + commit pipeline

```
WriterExec (data rows in → Parquet files out, emits action RecordBatches)
   ∪ RemoveActionsExec (old file metadata → Remove actions)     [row-level ops]
     → CoalescePartitionsExec (gather to 1 partition)
       → CommitExec (collects batches, decodes actions, commits atomically)
```

**Iceberg action schema** (`crates/sail-iceberg/src/physical_plan/action_schema.rs`):
- `ExecAction::{ Add(AddFileAction) | Delete(DeleteFileAction) | CommitMeta(CommitMetaAction) }`
  with `#[serde(rename = "add"/"delete"/"commit_meta")]`.
- `ActionRow` (single-row batch); `encode_actions(rows)` / `decode_actions_and_meta_from_batch(batch)`;
  `iceberg_action_schema()` returns the Arrow schema.
- Writer→Commit communication is **Arrow RecordBatches over the plan tree**, not Rust
  structs — this is what makes distributed execution possible (codec, §15).

**Commit exec** (`commit/commit_exec.rs`): reads action batches, decodes adds/deletes +
`CommitMeta`, resolves `IcebergCatalogCommitMode`, validates requirements, applies
schema/spec updates, handles bootstrap, produces the snapshot via `SnapshotProducer`,
commits through catalog or filesystem, retries on conflict.
**Port note (v0.6.6):** v0.7.0's commit_exec handles only Append/Overwrite; the
parent-manifest filtering, `Delete`/`Replace` operations, `reported_row_count`, and
`accumulate_action_batches` are v0.6.6 additions.

---

## 12. Options & configuration

Two independent generation systems:

1. **Format write/read options** — `data/options/iceberg.yaml` (also `delta.yaml`,
   etc.) defines every option with `key`, `type`, `default`, `supported`, `scopes`,
   `origins` (option/table-property/session). A build script (`sail-build-scripts`) emits
   `$OUT_DIR/options/iceberg.rs` which is `include!`d from `crates/sail-iceberg/src/options.rs`
   into `pub mod r#gen { ... }`. That module contains `<Format>ReadPartialOptions` /
   `<Format>ReadOptions` / write equivalents. Hand-written `impl ResolveOptions` in
   `options.rs` (or `writer_options.rs` for the exec struct) fold layers. **Adding an
   option = editing the YAML + reusing the generator**, never hand-writing a struct.
2. **App config** — `crates/sail-common/src/config/application.yaml` is
   `include_str!`'d into `application.rs`; structs derive `Deserialize` with
   `#[serde(rename_all = "snake_case")]`; typed env overrides are expressed via
   `ClusterConfigEnv`-style marker structs (each key an associated constant string).
   New cluster settings = new YAML entry + struct field + env constant.

---

## 13. Catalog integration

**Path-based (filesystem) tables:**
- Metadata discovered via `version-hint.text` + directory listing
  (`find_latest_metadata_file`).
- Commit = write metadata JSON (`PutMode::Create` CAS) + update `version-hint.text`.

**Catalog-managed tables (e.g. Iceberg REST / Polaris):**
- Catalog stores the `metadata-location` pointer.
- `IcebergCatalogCommitMode` (`catalog_support/commit.rs`):
  `Filesystem | MetadataLocationCas | CatalogCommit | CompatibilityCatalogCommit`.
  `IcebergCatalogCommitCoordinator::commit(...)` returns
  `CatalogCommitOutcome::{ Committed, NotSupported, Conflict }`.
- `CommitAuthority::IcebergRestCommit` (from `LakehouseExecutionContext.commit`) selects
  the catalog path; ALTER and DDL also branch on it in `sail-catalog/src/command.rs`.

**Port note:** v0.6.6's `CallProcedureExec` and the REST-catalog REPLACE/alter work both
reuse `IcebergCatalogCommitCoordinator` (present in v0.7.0) — do not reimplement.

---

## 14. Actor framework & distributed execution

`crates/sail-server/src/actor.rs`:
- `Actor` trait: `type Message`, `type Options`, `new`, optional `start`/`stop`,
  `receive(&mut self, ctx, msg) -> ActorAction` (**synchronous, non-blocking**).
- `ActorContext`: `handle()`, `send(msg)`, `send_with_delay(msg, d)`, `spawn(fut)`,
  `reap()`. `ActorSystem::spawn` → `ActorHandle` (cloneable, async `send`).
- Single-threaded per actor ⇒ lock-free state mutation.

Execution actors: `DriverActor` (per session `ClusterJobRunner`), `WorkerActor`.
Driver owns `WorkerPool` + `JobScheduler` + `TaskAssigner`; workers register over gRPC,
heartbeat, execute tasks, stream results. Task dispatch is gRPC
(`TaskStreamFlightServer`), job graph = stages/partitions/regions, `TaskRegion` units.

**Port note (v0.6.6):** v0.7.0's driver gateway lives in
`crates/sail-execution/src/driver/gateway.rs` (not `actor/rpc.rs`); the worker server in
`worker/actor/rpc.rs`. Keepalive/io-runtime wiring and worker-pool accounting are v0.6.6
additions that land in those v0.7.0 files.

---

## 15. Codec / proto

Remote (driver↔worker) physical-plan serialization:
- `crates/sail-execution/proto/sail/plan/physical.proto` — `ExtendedPhysicalPlanNode` with
  `NodeKind` oneof; every `ExecutionPlan` gets a `NodeKind` + message.
- `crates/sail-execution/src/proto/codec.rs` — `encode_node`/`decode` match on `NodeKind`,
  with `try_encode_message`/`try_decode_message`/`try_encode_schema`/`try_encode_physical_expr`
  helpers. JSON (`serde_json`) for complex payloads (DataFiles, TableUpdates,
  procedures), Arrow/byte encoding for schemas and plans.
- **Port rule:** new exec nodes = proto message + `NodeKind` field number + encode/decode
  arms + round-trip test.

---

## 16. Naming & code conventions

| Concern | Convention |
|---|---|
| Statement/command variants | PascalCase, no suffix (`Statement::Call`, `CommandNode::AlterTable`) |
| Logical node types | PascalCase + `Node` suffix (`CallProcedureNode`, `LoadDataNode`) |
| Physical exec types | PascalCase + `Exec` suffix (`IcebergLoadDataFastExec`, `CallProcedureExec`) |
| Traits | PascalCase (`TableFormat`, `MergeCapableSource`, `ResolveOptions`) |
| Files | `snake_case.rs` matching module name; one module per concern |
| Internal columns | `__sail_` prefix: `__sail_file_path`, `__sail_merge_target_row_id`, `__sail_src_*` |
| Explain strings | `Node: field=..., field=...` |
| serde names | `camelCase` for spec; explicit `#[serde(rename=...)]` for action enums |
| Options keys | `kebab-case` (`write.parquet.compression-codec`); Rust field `snake_case` |
| Error constructors | `todo()` / `unsupported()` / `invalid()` / `missing()` / `internal()` |
| Re-exports | `pub use` at module top so callers use short paths |
| Doc comments | `///` on pub items; `//!` module docs; Apache license header in new files |

---

## 17. Test conventions

- **In-file `#[cfg(test)] mod tests`** for unit tests (execs, resolvers, helpers), using
  `#[tokio::test]` for async and `#[test]` for pure logic; `OnceLock` shared runtime in
  tests outside a runtime.
- **Gold data**: `crates/sail-sql-parser/tests/gold_data/syntax.json` for parser;
  `crates/sail-spark-connect/tests/gold_data/**` for end-to-end plan/schema diffs against
  Spark (`ddl_load_data.json`, `function/*.json`).
- **Python** (`python/pysail/tests`): `pytest` with Spark comparisons; feature files
  (`*.feature`) + `__snapshots__` (`*.yaml`), `test_*.py`. Flight-SQL tests via a
  `FlightSqlServer` fixture.
- **Catalog integration**: `crates/sail-catalog-iceberg/tests/rest_integration_test.rs`
  with wiremock-style `MockServer`.

---

## 18. Anti-patterns to avoid

1. **Leaf executors doing everything inline in `execute()`** — no children, direct
   object-store reads, serial per-file processing. Always build an `ExecutionPlan` tree
   (scan → filter/transform → writer → commit).
2. **`concat_batches()` on whole tables** — OOM; stream partition-by-partition.
3. **Bypassing `IcebergCommitExec`** — commits go through the commit exec so the catalog
   commit mode / retry / conflict handling applies.
4. **Direct object-store access for data in planners** — use children; only metadata
   reads at plan time (e.g. `Table::load`, footers) are acceptable in async planners.
5. **Reinventing shared machinery** — `RowLevelWriteNode`, `expand_merge`,
   `MergeCapableSource`, `IcebergCatalogCommitCoordinator`, `OptionLayer`,
   `iceberg_action_schema()` already exist; compose them.
6. **Hand-writing option structs** instead of editing the YAML option spec.
7. **Leaking internal columns** into written files — strip `__sail_*` before write.
8. **`String` for table paths in executors** — use `Url`.
9. **Broken commit conflict detection** — always check for stale sibling metadata files
   (v0.6.6 `is_stale_metadata_file`) before declaring a conflict.

---

## 19. Positioning guide for the v0.6.6 port

| v0.6.6 feature | Logical home in v0.7.0 | Idiom to follow |
|---|---|---|
| CALL procedure (spec/parser/analyzer) | `spec::CommandNode::CallProcedure` + parser AST + analyzer match | §6 |
| CALL resolver | `sail-plan/src/resolver/command/call.rs` (+ `mod` + dispatch) | §7 |
| `CallProcedureExec` | `sail-iceberg/src/physical_plan/call_procedure_exec.rs` (+ `pub mod`/`pub use`) | §10 + codec §15 + `job_graph/planner.rs` driver-stage detection |
| expire_snapshots GC | `sail-iceberg/src/physical_plan/expire_snapshots_gc.rs` | §10 (leaf single-future), commit via coordinator §13 |
| `create_updater` / `UpdateInfo` / `UpdateAssignment` | `sail-common-datafusion/src/datasource.rs` | §5.1 (defaulted trait method) |
| Iceberg row-level planners | `sail-iceberg/src/physical_plan/planner/{mod,context,helpers,commit,op_*}.rs` | §9/§10 (`PlannerContext`, compose existing execs) |
| `IcebergTableSource` file-column | `sail-iceberg/src/logical/table_source.rs` + `scan_by_data_files_exec.rs` | §5 `MergeCapableSource` |
| LOAD DATA | `resolver/command/load.rs` + `load_classifier`/`load_data_planner`/`load_data_exec` + `operations/parquet_utils.rs` | §7/§10; reuse existing parser+spec grammar |
| Metadata tables | `IcebergMetadataTableType` + `datasource/metadata_table.rs` + `SourceInfo.metadata_table` + `read.rs` hook | §5.1 `create_source` branch; §4 resolver |
| SHOW TBLPROPERTIES / DESCRIBE col / VIEW | spec + analyzer + `CatalogCommand` + catalog providers | §6/§5.5 (`ArrowSerializer` rows) |
| ALTER column ops / rename / add-drop | spec + `AlterTableOptions` + `TableFormatAlterTableOperation` + iceberg `table_format.rs` + REST provider | §5.5 converter + §12 options + `retry_metadata_commit`-style loop |
| Worker pool accounting / readiness / spawn retry | `sail-execution/src/driver/{worker_pool,task_assigner,actor}/*` | §14 |
| RPC keepalive / io-runtime | `sail-execution/src/rpc.rs`, `driver/gateway.rs`, `worker/actor/rpc.rs`, `sail-server/src/builder.rs` | §14 |
| ActivityTracker streaming wiring | `sail-spark-connect/src/{executor,service/plan_executor}.rs` | §5.6 (extension access) |
| Config keys | `sail-common/src/config/application.yaml` + `application.rs` + env constants | §12 |
| Docker/K8s/build.sh/Python tests | `docker/*`, `k8s/sail.yaml`, `build.sh`, `python/pysail/tests/flight|spark` | repo convention |

---

## 20. Appendix

### 20.1 Canonical file map (v0.7.0)

| Concern | File |
|---|---|
| `TableFormat` trait + registry + info structs + `OptionLayer` | `crates/sail-common-datafusion/src/datasource.rs` |
| `SessionExtension` / accessor | `crates/sail-common-datafusion/src/extension.rs` |
| Lakehouse context / authorities | `crates/sail-common-datafusion/src/catalog/lakehouse.rs` |
| Shared row-level node + `expand_merge` | `crates/sail-logical-plan/src/merge.rs` |
| `spec` plan/expr | `crates/sail-common/src/spec/{plan,expression,data_type}.rs` |
| Resolver | `crates/sail-plan/src/resolver/{command,query}/**` |
| Resolver errors | `crates/sail-plan/src/error.rs` |
| Catalog provider trait | `crates/sail-catalog/src/provider/mod.rs` |
| Catalog commands | `crates/sail-catalog/src/command.rs` |
| Catalog manager | `crates/sail-catalog/src/manager.rs` |
| Iceberg `TableFormat` impl | `crates/sail-iceberg/src/table_format.rs` |
| Iceberg physical planner | `crates/sail-iceberg/src/physical/table_scan_planner.rs` |
| Iceberg execs | `crates/sail-iceberg/src/physical_plan/**` |
| Iceberg commit machinery | `crates/sail-iceberg/src/catalog_support/commit.rs` |
| Iceberg action schema | `crates/sail-iceberg/src/physical_plan/action_schema.rs` |
| Option specs | `crates/sail-iceberg/data/options/iceberg.yaml`, `crates/sail-delta-lake/data/options/delta.yaml` |
| Options entry (generated include) | `crates/sail-iceberg/src/options.rs` |
| App config | `crates/sail-common/src/config/application.{rs,yaml}` |
| Extension query planner | `crates/sail-session/src/planner.rs` |
| Actor framework | `crates/sail-server/src/actor.rs` |
| Driver gateway (server) | `crates/sail-execution/src/driver/gateway.rs` |
| Physical-plan proto + codec | `crates/sail-execution/proto/sail/plan/physical.proto`, `crates/sail-execution/src/proto/codec.rs` |

### 20.2 Idiom checklist (run before merging any ported code)

- [ ] New spec type? → `spec/plan.rs` with `camelCase` serde.
- [ ] New statement? → parser AST + keyword + `syntax.json` + analyzer arm + spec + resolver arm.
- [ ] New logical node? → `UserDefinedLogicalNodeCore` + educe + `ItemTaker` + `fmt_for_explain`.
- [ ] New command? → `resolver/command/<x>.rs` + `CatalogCommand`/`spec` variant.
- [ ] New `ExecutionPlan`? → children (not leaf), `cache: Arc<PlanProperties>`, `Url` paths,
      `RecordBatchStreamAdapter`, no inline object-store data I/O.
- [ ] New option? → format YAML, regenerate, resolve via `ResolveOptions`/`PartialOptions`.
- [ ] New config? → `application.yaml` + struct field + env constant.
- [ ] Writes commit through the format's commit exec (never inline).
- [ ] Errors via existing constructor fns; `External(Box::new(..))` at boundaries.
- [ ] Remote-exec-visible exec? → proto `NodeKind` + codec arms + round-trip test.
- [ ] `__sail_*` internal columns stripped before writing files.
- [ ] Tests: in-file unit tests + gold data / python parity where a new statement exists.
