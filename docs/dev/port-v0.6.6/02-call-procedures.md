# 02 — Iceberg CALL Stored Procedures

> Full implementation of `CALL <catalog>.system.<procedure>(...)` for heimdall's three
> procedures, including the physical file-GC pass for `expire_snapshots`.

**Supported procedures (only the `system` namespace is accepted):**
- `rollback_to_snapshot('<ns>.<table>', <snapshot_id>)`
- `set_current_snapshot('<ns>.<table>', <snapshot_id>)` or `set_current_snapshot('<ns>.<table>', ref => '<branch_or_tag>')`
- `expire_snapshots('<ns>.<table>', [TIMESTAMP '<older_than>'], [<retain_last>])`

Files:
- `crates/sail-logical-plan/src/call_procedure.rs` (new, 152 lines)
- `crates/sail-plan/src/resolver/command/call.rs` (new, 383 lines)
- `crates/sail-iceberg/src/physical/call_procedure_planner.rs` (new, 97 lines)
- `crates/sail-iceberg/src/physical_plan/call_procedure_exec.rs` (new, 1617 lines incl. tests)
- `crates/sail-iceberg/src/physical_plan/expire_snapshots_gc.rs` (new, 766 lines incl. tests)
- `crates/sail-iceberg/src/physical_plan/planner/mod.rs` wiring (`physical/mod.rs`, `physical_plan/mod.rs`, `table_scan_planner.rs`)
- `crates/sail-execution/src/proto/codec.rs` + `proto/sail/plan/physical.proto` (serialization)

---

## 1. Logical plan node — `CallProcedure` and `CallProcedureNode`

`crates/sail-logical-plan/src/call_procedure.rs`:

```rust
pub enum CallProcedure {
    RollbackToSnapshot { table: String, snapshot_id: i64 },
    SetCurrentSnapshot { table: String, snapshot_id: Option<i64>, r#ref: Option<String> },
    ExpireSnapshots { table: String, older_than_ms: Option<i64>, retain_last: Option<i32> },
}
impl CallProcedure {
    pub fn table_name(&self) -> &str { ... }   // "<ns>.<table>"
}
```

`CallProcedureNode` — a leaf `UserDefinedLogicalNodeCore` (`name() = "CallProcedure"`,
no inputs, no expressions, empty schema) carrying the resolved procedure plus the target
table context:

- `procedure: CallProcedure`
- `target_location: String`
- `target_table_name: Vec<String>`
- `target_options: Vec<OptionLayer>`
- `target_lakehouse_table: Option<LakehouseExecutionContext>`
- `schema: DFSchemaRef` (always empty)

`fmt_for_explain` → `CallProcedure: table=<last>, procedure=<Debug>`.
`with_exprs_and_inputs` requires zero exprs/inputs (`ItemTaker::zero`).

---

## 2. Resolver — `crates/sail-plan/src/resolver/command/call.rs`

### 2.1 `resolve_command_call_procedure(name, arguments, state)`

1. `procedure_parts = Vec<String>::from(name)`; requires exactly
   `[catalog, system, procedure]` — anything else → `unsupported`
   (`"CALL requires a fully qualified <catalog>.system.<procedure> name"`).
2. `system` must equal `"system"` (case-insensitive) → else `unsupported`.
3. Dispatches on `procedure_name.to_ascii_lowercase()`:
   - `"rollback_to_snapshot"` → `resolve_table_and_snapshot_id`
   - `"set_current_snapshot"` → `resolve_table_and_snapshot_target`
   - `"expire_snapshots"` → `resolve_table_and_expire_args`
   - otherwise → `unsupported system procedure: <other>`
4. Resolves the target table via `CatalogManager::get_table_or_view(&parts)` (dotted
   string split by `.`). Must be `TableKind::Table` (not a view), format
   `iceberg` (case-insensitive), with a `location`.
5. `resolve_lakehouse_table_context(&table_name, LakehouseOperation::Maintenance,
   Some(format), vec![])`.
6. `options = vec![OptionLayer::TablePropertyList { items: properties }]`.
7. Returns `LogicalPlan::Extension(CallProcedureNode::new(...))`.

### 2.2 Argument resolution helpers

- `resolve_named_arg(arguments, name, position, state)`:
  - finds the first argument whose name `eq_ignore_ascii_case(name)`;
  - else falls back to the `position`-th **unnamed** argument
    (`filter(|(n,_)| n.is_none()).nth(position)`) — a named argument can never shadow a
    positional slot;
  - error `missing required argument '{name}' for CALL` when absent;
  - evaluates the expr via `evaluate_constant_expr`.
- `resolve_optional_named_arg(...)` — same but `Ok(None)` when absent.
- `evaluate_constant_expr(expr, state)` — resolves with an empty schema then
  `LiteralEvaluator::new().evaluate(&resolved)`; error `CALL argument must be a constant`.
- `scalar_to_table_name(&ScalarValue)` — accepts `Utf8 | LargeUtf8 | Utf8View`;
  error otherwise.
- `scalar_to_snapshot_id` — accepts all int widths (`Int8..UInt64`), UInt64 guarded by
  `i64::try_from`.
- `scalar_to_i32` — int widths down-cast guarded.
- `scalar_to_timestamp_ms` — matches the timestamp `ScalarValue` variants directly
  (`TimestampSecond*1000`, `TimestampMillisecond`, `TimestampMicrosecond/1000`,
  `TimestampNanosecond/1_000_000`) — the per time-travel idiom, so `TIMESTAMP '...'`
  literals resolve to an epoch value — and falls back to string parsing via
  `parse_timestamp_ms`.
- `parse_timestamp_ms(value)` — RFC3339 first, then naive formats
  `%Y-%m-%d %H:%M:%S`, `%Y-%m-%d %H:%M:%S%.f`, `%Y-%m-%dT%H:%M:%S`, `%Y-%m-%dT%H:%M:%S%.f`;
  error `invalid timestamp '{value}' for CALL; expected e.g. 'YYYY-MM-DD HH:MM:SS'`.

### 2.3 Procedure-specific validation

- `resolve_table_and_snapshot_id` → `(table, i64)`; both required.
- `resolve_table_and_snapshot_target` → `(table, Option<i64>, Option<String>)`; exactly
  one of `snapshot_id` (positional 1 or named) and `ref` (named only):
  - both → `Either snapshot_id or ref must be provided, not both`
  - neither → `Either snapshot_id or ref must be provided for set_current_snapshot`
- `resolve_table_and_expire_args` → `(table, Option<i64>, Option<i32>)`; both optional
  (`older_than` at position 1, `retain_last` at position 2).

---

## 3. Physical planner — `call_procedure_planner.rs`

`plan_call_procedure(session_state, node)`:
1. `IcebergTableFormat::parse_table_url(vec![node.target_location()])`.
2. `Table::load(session_state, table_url)`; `metadata = table.metadata()`.
3. `updates = compute_procedure_updates(node.procedure(), metadata)` — validates the
   arguments **at plan time** (snapshot existence, rollback ancestry, expire retain-set).
4. `requirements = procedure_requirements(metadata)` — the optimistic-concurrency guard.
5. `output = compute_procedure_output(node.procedure(), metadata)`.
6. For `ExpireSnapshots`, captures `pre_commit_metadata = metadata.clone()` — the
   physical GC needs the pre-commit state because the expired snapshots no longer exist
   after commit.
7. Returns `CallProcedureExec::new_with_pre_commit_metadata(...)`.

`compute_procedure_output`:
- `RollbackToSnapshot` / `SetCurrentSnapshot` → `CallProcedureOutput::SnapshotRef {
  previous_snapshot_id, current_snapshot_id }`, where
  `previous = metadata.refs[main].snapshot_id.or(current_snapshot_id).unwrap_or(0)` and
  `current = resolve_target_snapshot_id(...)?.unwrap_or(0)`.
- `ExpireSnapshots` → `CallProcedureOutput::ExpireSnapshots` with **all zero** counts
  (filled in at execution after the real GC pass).

---

## 4. Executor — `call_procedure_exec.rs`

### 4.1 `CallProcedureExec` shape

A single-partition bounded `ExecutionPlan` (no children, `Partitioning::UnknownPartitioning(1)`,
`EmissionType::Final`, `Boundedness::Bounded`, output schema = procedure output schema).
Fields:

- `procedure`, `table_url: Url`, `lakehouse_table: Option<LakehouseExecutionContext>`,
- `updates: Vec<TableUpdate>`, `requirements: Vec<TableRequirement>`,
- `output: CallProcedureOutput`,
- `pre_commit_metadata: Option<TableMetadata>`,
- `cache: Arc<PlanProperties>`.

Constructors: `new(...)` and `new_with_pre_commit_metadata(...)`.
Accessors for every field (used by the codec).

### 4.2 `execute_call(context)` — the commit

1. Resolve commit authority:
   - `catalog_table = lakehouse_table.map(|c| c.catalog_table().to_vec())`;
   - if Some, `IcebergCatalogCommitCoordinator::load_table_info(...)` →
     `catalog_table_info`, else `CatalogTableInfo::default()`.
   - `commit_mode = IcebergCatalogCommitMode::resolve(lakehouse_table, &catalog_table_info, &[])`.
2. `committed_metadata_location: Option<String> = None`.

**Filesystem mode:**
- `get_object_store_from_context` + `StoreContext::new`.
- `find_latest_metadata_file` for the initial meta.
- `IcebergTableFormat::retry_metadata_commit(object_store, store_ctx, &table_url,
  initial_latest_meta, true, |table_meta| { validate_procedure_requirements(...)?;
  apply_procedure_updates(...)?; Ok(()) })`.

**CatalogCommit / CompatibilityCatalogCommit mode:**
- Requires `lakehouse_table` and `catalog_table`.
- `IcebergCatalogCommitCoordinator::new(...)`.
- `coordinator.commit(lakehouse_table, requirements, updates)`:
  - `Committed(committed)` → `committed_metadata_location =
    committed.metadata_location().map(ToString::to_string)`.
  - `NotSupported` → internal error.
  - `Conflict` → internal error `"Iceberg catalog commit conflict while executing CALL"`.

**MetadataLocationCas mode:** → internal error `"CALL procedures do not support
metadata-location CAS commit yet"`.

### 4.3 Post-commit GC for `ExpireSnapshots`

After a successful commit:
- Requires `pre_commit_metadata`.
- `object_store`/`store_ctx` for the table URL.
- `post_commit = Self::reload_post_commit_metadata(&object_store, &table_url,
  committed_metadata_location.as_deref())`:
  - with a committed location → `metadata_location_to_object_path_string` +
    `load_metadata_file_bytes` + `TableMetadata::from_json`;
  - without → `find_latest_metadata_file`.
  - Reload failures propagate (matching the Iceberg reference which refreshes
    post-commit metadata and throws).
- `counts = expire_files_gc(&store_ctx, pre_commit, &post_commit)`.
- Returns the real-count output batch. Other procedures return the plan-time
  `output.to_record_batch()`.

### 4.4 `compute_procedure_updates(procedure, metadata)`

- `RollbackToSnapshot`: `metadata.snapshot(id)` must exist, else
  `"snapshot {id} does not exist"`; **must be an ancestor of the current state**
  (`is_current_ancestor`), else
  `"Cannot roll back to snapshot, not an ancestor of the current state: {id}"`;
  → `vec![set_main_snapshot_ref(metadata, id)]`.
- `SetCurrentSnapshot`: resolve target via `resolve_target_snapshot_id` (ref→snapshot id
  from the refs map); must exist; → `set_main_snapshot_ref`.
- `ExpireSnapshots` → `expire_snapshot_updates(metadata, older_than_ms, retain_last)`.

`set_main_snapshot_ref` builds `TableUpdate::SetSnapshotRef { ref_name: "main",
reference: SnapshotReference { snapshot_id, retention } }`, preserving the current
`main` retention policy (or a default `SnapshotRetention::Branch { None, None, None }`).

### 4.5 `expire_snapshot_updates` — the spec retain-set algorithm

Constants: `DEFAULT_MAX_SNAPSHOT_AGE_MS = 432_000_000` (5 days),
`DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 1`.

- Defaults from table properties `history.expire.max-snapshot-age-ms` /
  `history.expire.min-snapshots-to-keep` (parse failures → defaults).
- `older_than = older_than_ms.unwrap_or(now - max_age)`; `retain_last =
  retain_last.unwrap_or(min_keep)`.
- `(retained, referenced) = retained_snapshot_ids(metadata, older_than, retain_last)`:
  - `retained_refs`: `main` always; non-main refs kept only if their snapshot still
    exists AND `now - snap.timestamp_ms() <= max_ref_age_ms` (per-ref override, default
    from `history.expire.max-ref-age-ms`, default never).
  - `retained` starts as the retained refs' targets; for each retained **branch**, walk
    head-inclusive ancestry keeping snapshots while `kept < min_snapshots_to_keep` (per
    branch override) OR `timestamp >= cutoff_ms` (per-branch `max_snapshot_age_ms`,
    defaulting to `older_than`); every walked snapshot is `referenced`.
  - tags just add their target to `referenced`.
  - finally, unreferenced-but-recent snapshots (`timestamp >= older_than`) are retained.
- `expired_ids` = all snapshots not in `retained`; if empty → `Ok(vec![])`.
- updates = `[RemoveSnapshots { expired_ids }]` + one `RemoveSnapshotRef` for every
  non-`main` ref whose target snapshot was expired.

### 4.6 `resolve_target_snapshot_id`, `is_current_ancestor`

- `resolve_target_snapshot_id`: `Rollback` → `Some(id)`; `SetCurrentSnapshot` →
  `(id,None) → Some(id)`, `(None,Some(ref))` → `metadata.refs.get(ref)` or error
  `"Cannot find matching snapshot ID for ref {ref}"`, both → error, neither → `None`;
  `Expire` → `None`.
- `is_current_ancestor(metadata, id)`: walk `current_snapshot()` down
  `parent_snapshot_id()`; the current snapshot is its own ancestor.

### 4.7 `procedure_requirements` + filesystem-path guard

- `procedure_requirements(metadata)` = `[RefSnapshotIdMatch { ref: "main",
  snapshot_id: metadata.refs[main].map(snapshot_id) }]`.
- `validate_procedure_requirements(table_meta, requirements)`: for each requirement,
  `main` is matched against `table_meta.current_snapshot_id` (filtered to `>= 0`; `None`
  ⇒ "must not exist"); mismatch → `"Iceberg commit failed: reference '{ref}' expected
  snapshot {:?} but found {:?}"`. Any non-`RefSnapshotIdMatch` requirement → internal
  error. Only ever the one requirement a CALL issues.
- `apply_procedure_updates(table_meta, updates)`:
  - `SetSnapshotRef`: insert into `refs`; if `ref_name == main` also set
    `current_snapshot_id`.
  - `RemoveSnapshots`: retain snapshots not in the id set; **also drop
    `statistics` / `partition_statistics` keyed by a removed snapshot** (matches the
    spec's `removeStatistics` / `removePartitionStatistics`).
  - `RemoveSnapshotRef`: remove from `refs`.
  - anything else → internal error `"unsupported TableUpdate for CALL procedure"`.

### 4.8 `CallProcedureOutput` and schema

```rust
pub enum CallProcedureOutput {
    SnapshotRef { previous_snapshot_id: i64, current_snapshot_id: i64 },
    ExpireSnapshots {
        deleted_data_files_count: i64,
        deleted_position_delete_files_count: i64,
        deleted_equality_delete_files_count: i64,
        deleted_manifest_files_count: i64,
        deleted_manifest_lists_count: i64,
        deleted_statistics_files_count: i64,
    },
}
```

- `schema()`: SnapshotRef → 2 Int64 cols `previous_snapshot_id`, `current_snapshot_id`;
  ExpireSnapshots → 6 Int64 cols `deleted_*_count` (mirrors Apache Iceberg Spark
  procedures output tables).
- `to_record_batch()` → single-row batch.

---

## 5. Physical GC — `expire_snapshots_gc.rs`

### 5.1 `FileKind` + `ExpireGcCounts`

```rust
pub enum FileKind { Data(DataContentType), Manifest, ManifestList, Statistics }
impl FileKind { pub fn tag(&self) -> &'static str { "DATA" | "POSITION_DELETES" |
    "EQUALITY_DELETES" | "Manifest" | "Manifest List" | "Statistics Files" } }

pub struct ExpireGcCounts { data_files, position_delete_files, equality_delete_files,
    manifest_files, manifest_lists, statistics_files: u64 }   // all Default
```

The `(path, kind)` keying mirrors the Iceberg Spark action's `DeleteSummary` type tags.

### 5.2 `collect_files(store_ctx, metadata, snapshots) -> Vec<(String, FileKind)>`

For each snapshot (mirroring the Spark action's `fileDS`):
- `load_manifest_list(snapshot.manifest_list())`; each `ManifestFile` →
  `(manifest_path, Manifest)`; for `Data`/`Deletes` manifests, `load_manifest` and push
  each `Added`/`Existing` entry's data file as `FileKind::Data(content_type)`.
- the snapshot's own manifest list → `ManifestList`.
Then for `metadata.statistics` / `metadata.partition_statistics` whose `snapshot_id` is
in the expired set → `(path, Statistics)`.

### 5.3 `diff_files(candidates, valid)`

Anti-join on `(path, kind.tag())`, deduped by a local `seen` set. A file still
reachable from a retained snapshot is never returned.

### 5.4 `delete_files(store_ctx, files) -> ExpireGcCounts`

Best-effort per-file `store.delete(&object_path)`:
- success → increment the matching count;
- `NotFound` → skip silently (debug log), **not counted**;
- any other error → warn + skip; a single failure never aborts the pass.
Counts are the number of **successful** deletes. (Note: S3/InMemory deletes of missing
keys succeed, so a ghost file is still counted — tested.)

### 5.5 `expire_files_gc(store_ctx, pre_commit, post_commit) -> ExpireGcCounts`

1. `post_ids` = snapshot ids in post-commit metadata.
2. `expired` = pre-commit snapshots whose id is not in `post_ids`; empty → zero counts.
3. `candidates = collect_files(pre_commit, expired)` (expired snapshots' files).
4. `valid = collect_files(post_commit, post_commit.snapshots)` (retained state).
5. `to_delete = diff_files(candidates, valid)`.
6. `delete_files(store_ctx, to_delete)`.

### 5.6 Tests (6 tokio tests)

- `diff_returns_only_files_unique_to_candidates`, `diff_dedupes_and_ignores_same_path_different_kind`.
- `expire_files_gc_deletes_expired_only_files` (data + manifest + manifest list counts).
- `expire_files_gc_never_deletes_shared_manifest_content` (shared manifest survives).
- `expire_files_gc_deletes_statistics_files_of_expired_snapshots`.
- `expire_files_gc_counts_position_and_equality_deletes`.
- `expire_files_gc_counts_idempotent_delete_of_missing_data_file`.

---

## 6. WIRING

- `crates/sail-iceberg/src/physical/mod.rs`: `pub mod call_procedure_planner;`
- `crates/sail-iceberg/src/physical_plan/mod.rs`: `pub mod call_procedure_exec; pub mod
  expire_snapshots_gc;` + `pub use call_procedure_exec::{CallProcedureExec,
  CallProcedureOutput};`
- `crates/sail-iceberg/src/physical/table_scan_planner.rs`:
  `IcebergPhysicalPlanner::plan_extension` now matches `CallProcedureNode` →
  `plan_call_procedure(...)`.
- `crates/sail-execution/src/job_graph/planner.rs` — **driver placement**: `CallProcedureExec`
  is added to both `plan_job_graph_stages`'s driver-stage detection and
  `is_driver_stage_plan` (alongside `IcebergCommitExec`, `DeltaCommitExec`,
  `FileDeleteExec`, ...). The CALL commit must run on the **driver**, never dispatched to
  a worker. Port this together with the exec.
- `crates/sail-plan/src/resolver/command/mod.rs`: `mod call;` + `CommandNode::CallProcedure
  { name, arguments } => self.resolve_command_call_procedure(...)`.
- `crates/sail-logical-plan/src/lib.rs`: `pub mod call_procedure;`

---

## 7. Codec (remote execution)

`crates/sail-execution/src/proto/codec.rs` + `proto/sail/plan/physical.proto`:

```proto
message CallProcedureExecNode {
  string procedure = 1;               // CallProcedure, JSON
  string table_url = 2;
  string lakehouse_table_json = 3;    // empty = None
  string updates_json = 4;            // Vec<TableUpdate>, JSON
  string requirements_json = 5;       // Vec<TableRequirement>, JSON
  string output_json = 6;             // CallProcedureOutput, JSON
  string pre_commit_metadata_json = 7;// TableMetadata, JSON (empty = None)
}
// NodeKind: CallProcedureExecNode call_procedure = 56;
```

Encode path (`NodeKind::CallProcedure`): serializes procedure/updates/requirements/
output/pre-commit metadata with `serde_json`; lakehouse table via
`try_encode_lakehouse_table`. Decode path rebuilds
`CallProcedureExec::new_with_pre_commit_metadata`. Round-trip test included.

---

## 8. Resolver dependency: `LakehouseOperation::Maintenance`

`resolve_lakehouse_table_context` is called with `LakehouseOperation::Maintenance` for
CALL (read+write maintenance access). `Maintenance` already exists on `feat/0.7`.

---

## 9. Port notes / behavior contracts to preserve

- `rollback_to_snapshot` requires **ancestry**; `set_current_snapshot` only requires
  existence (Iceberg contract).
- `expire_snapshots` commit is a metadata commit; the GC runs **after** the commit and
  deletes only files uniquely owned by expired snapshots.
- The `main`-ref `RefSnapshotIdMatch` guard gives optimistic concurrency on both the
  catalog and filesystem commit paths.
- `expire_snapshots` reloads post-commit metadata to compute the retained set; a reload
  failure propagates.
- Reserved `CALL` argument names: `table`, `snapshot_id`, `ref`, `older_than`,
  `retain_last` (all case-insensitive).
