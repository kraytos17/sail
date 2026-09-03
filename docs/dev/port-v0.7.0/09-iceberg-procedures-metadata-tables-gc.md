# Porting feat/0.7.0 → feat/0.7.1 — Doc 09: Iceberg CALL Procedures, Metadata Tables & Expire-Snapshots GC

> Part of the `docs/dev/port-v0.7.0/` inventory. Documents the Iceberg maintenance surface
> implemented on `feat/0.7.0` vs base `f0b137d6`: `CALL <catalog>.system.<procedure>`
> (`rollback_to_snapshot`, `set_current_snapshot`, `expire_snapshots`), the read-only
> `db.table.snapshots` / `db.table.refs` metadata tables, and the physical file GC pass that
> backs snapshot expiry. Ground truth: `feat/0.7.0` tip `c07ad0c8`. Frontend parsing/spec is
> doc 05; catalog dispatch and the `TableFormatProcedureOperation` bridge are doc 06; the
> commit-exec/`call_procedure`/`retry_metadata_commit` integration points are in docs 07/08.

---

## 1. Scope

| File | Delta |
|---|---|
| `sail-iceberg/src/datasource/metadata_table.rs` | NEW — `IcebergMetadataTableProvider` (598 LOC) |
| `sail-iceberg/src/datasource/mod.rs`, `table_format.rs` (`build_iceberg_metadata_source`) | metadata-source routing (doc 07 §11.4) |
| `sail-iceberg/src/operations/expire_snapshots_gc.rs` | NEW — `FileKind`, `ExpireGcCounts`, `collect_files`, `diff_files`, `delete_files`, `expire_files_gc` (766 LOC) |
| `sail-iceberg/src/operations/parquet_utils.rs` | NEW — `read_parquet_footer`/`ParquetFooterInfo` |
| `sail-iceberg/src/operations/procedure.rs` | NEW — update computation + output shapes for the three procedures (465 LOC) |
| `sail-iceberg/src/operations/snapshot.rs` | `SnapshotProducer.parent_manifest_entries` override + operation→`Summary` mapping (Delete/Replace) |
| `sail-iceberg/src/operations/mod.rs` | module wiring |
| `sail-iceberg/src/spec/metadata/table_metadata.rs`, `spec/snapshots/snapshot.rs` | `TableMetadata::snapshot(id)`, `SnapshotReference::min_snapshots_to_keep/max_snapshot_age_ms/max_ref_age_ms` (doc 07 §1) |
| `sail-plan/src/resolver/command/call.rs` | NEW — CALL resolver (331 LOC) |
| `sail-plan/src/resolver/command/mod.rs` | dispatch `CommandNode::CallProcedure` → `resolve_command_call_procedure` (doc 07 §11.1) |
| `sail-catalog`/`sail-catalog-iceberg`/`TableFormat::call_procedure` | doc 06; iceberg `call_procedure` impl doc 07 §4.4 |
| `sail-iceberg/Cargo.toml` | +`sail-logical-plan`, +`datafusion-datasource`, +`tokio`, dev +`tempfile` |

---

## 2. Metadata tables (`datasource/metadata_table.rs`)

`IcebergMetadataTableProvider` — read-only `TableProvider` exposing `db.table.refs` /
`db.table.snapshots` (mirrors Iceberg `RefsTable`/`SnapshotsTable`). Rows are materialized in
memory from the base table's `TableMetadata`; no pushdown (`WHERE`/`ORDER BY`/`LIMIT` run above
the scan, metadata tables are tiny).

```rust
pub struct IcebergMetadataTableProvider {
    table_uri: String,          // base table URI
    metadata: TableMetadata,    // snapshot of base metadata at source-build time
    metadata_type: IcebergMetadataTableType,   // Snapshots | Refs
    schema: SchemaRef,          // fixed per type
}
impl IcebergMetadataTableProvider {
    pub fn new(table_uri, metadata, metadata_type) -> Self;
    pub fn table_uri(&self) -> &str;
    pub fn metadata_type(&self) -> IcebergMetadataTableType;
    pub fn build_batch(&self) -> Result<RecordBatch>;
}
impl TableProvider { schema, table_type = Base, supports_filters_pushdown → Unsupported…,
    scan → MemorySourceConfig over build_batch (with projection) }
```

### 2.1 `snapshots` schema/rows

`committed_at` (Timestamp(us), not null; snapshot `timestamp_ms * 1000`), `snapshot_id`
(Int64), `parent_id` (Int64 null), `operation` (Utf8 from summary), `manifest_list` (Utf8
null when empty), `summary` (Map<Utf8,Utf8> from `Summary::operation` + `additional_properties`,
built with `build_summary_map` — each map entry list includes `operation` first, then the
extra properties).

### 2.2 `refs` schema/rows

`name`, `type` (`branch`/`tag` via `is_branch()`), `snapshot_id`, `max_reference_age_in_ms`,
`min_snapshots_to_keep`, `max_snapshot_age_in_ms` (branch retention fields populated; tag rows
emit nulls for the branch-only fields). Order = `refs` map key iteration.

### 2.3 Read path wiring

Built by `IcebergTableFormat::create_source` when `SourceInfo.metadata_table.is_some()` via
`build_iceberg_metadata_source` (table_format.rs, doc 07 §11.4): validates the lakehouse read
context, loads the base table (`Table::load_with_metadata_location`, catalog-managed uses the
metadata location), constructs the provider, returns `provider_as_source`. Detection at
name-resolution is `try_resolve_iceberg_metadata_table` in the plan resolver (doc 07 §11.4).

### 2.4 Tests

`snapshots_metadata_table_materializes_rows`, `refs_metadata_table_materializes_rows`, plus
heimdall-shaped query tests executed through a real `SessionContext`:
`refs_supports_heimdall_current_snapshot_query` (WHERE name='main'),
`snapshots_supports_heimdall_latest_snapshot_query` (ORDER BY committed_at DESC LIMIT 1),
`snapshots_supports_heimdall_snapshot_exists_and_parent_queries`.

---

## 3. CALL procedures (`operations/procedure.rs`)

### 3.1 Dispatch contract

`TableFormatProcedureOperation` (doc 06 §4.4) drives everything:
`RollbackToSnapshot { snapshot_id }`, `SetCurrentSnapshot { snapshot_id: Option<i64>, ref:
Option<String> }`, `ExpireSnapshots { older_than_ms: Option<i64>, retain_last: Option<i32> }`.

### 3.2 `compute_procedure_updates(procedure, metadata) -> Vec<TableUpdate>`

- **RollbackToSnapshot**: snapshot must exist (`internal_err!` otherwise) and be an ancestor of
  the current state (`is_current_ancestor` walks `parent_snapshot_id` from the current
  snapshot; an empty table has no ancestors). Produces one update pointing `main` at it.
- **SetCurrentSnapshot**: target resolved by `resolve_target_snapshot_id` (id or ref name from
  `metadata.refs`, error "Cannot find matching snapshot ID for ref …" when missing, error if
  both or neither for this path); must exist. Same `set_main_snapshot_ref`.
- **ExpireSnapshots**: `expire_snapshot_updates` — see below.

### 3.3 `set_main_snapshot_ref`

`TableUpdate::SetSnapshotRef { ref_name: MAIN_BRANCH, reference: SnapshotReference { snapshot_id,
retention } }`, preserving the existing `main` retention policy (or a default `Branch { None,
None, None }` when `main` is absent).

### 3.4 `expire_snapshot_updates` — the retain-set algorithm

Defaults: `history.expire.max-snapshot-age-ms` (spec default **432_000_000 ms = 5 days**,
`DEFAULT_MAX_SNAPSHOT_AGE_MS`), `history.expire.min-snapshots-to-keep` (spec default **1**).
`older_than = arg.or(now − max_age)`; `retain_last = arg.or(min_keep)`.

`retained_snapshot_ids(metadata, older_than, retain_last) -> (retained, referenced)`:

1. **Retained refs**: `main` always; other refs only if their snapshot still exists and its age
   ≤ `max-ref-age-ms` (per-ref via `max_ref_age_ms()`, else table property
   `history.expire.max-ref-age-ms`, default never / `i64::MAX`). Dangling refs dropped.
2. Seed `retained` with every retained ref's target.
3. For each retained **branch**: walk `parent_snapshot_id` from the ref target, keeping
   head-inclusive ancestors while `kept < min_keep` (per-ref `min_snapshots_to_keep()`, else
   `retain_last`, floor 1) **or** `snapshot.timestamp_ms() >= cutoff_ms` where cutoff = per-ref
   `max_snapshot_age_ms() → now−age`, else `older_than`. Each walked snapshot also goes into
   `referenced`. Tag targets go into `referenced` only.
4. Unreferenced-but-recent snapshots: any snapshot not in `referenced` with
   `timestamp_ms() >= older_than` is retained.
5. Expired = all snapshots not in `retained`. If none, return `Ok(vec![])`. Otherwise emit
   `TableUpdate::RemoveSnapshots { snapshot_ids }` plus, for each non-`main` ref whose target is
   expired, `TableUpdate::RemoveSnapshotRef { ref_name }`.

### 3.5 `resolve_target_snapshot_id`

`Rollback` → `Some(id)`; `SetCurrentSnapshot`: `(Some,None)`→id, `(None,Some(ref))`→ref target
(error if unknown), `(Some,Some)`→error "Either snapshot_id or ref must be provided, not both",
`(None,None)`→`None`. `Expire` → `None`.

### 3.6 Commit guard + output

- `procedure_requirements(metadata)`: single `TableRequirement::RefSnapshotIdMatch { ref: main,
  snapshot_id: current main target }` (the optimistic-concurrency guard; asserted by
  `validate_procedure_requirements` on the filesystem path and by the catalog path on the REST
  side). For `main`, the actual value is read from `current_snapshot_id` (with `<0` treated as
  none); mismatch → conflict-style `internal_err!`.
- `apply_procedure_updates(metadata, updates)`: applies `SetSnapshotRef` (also updates
  `current_snapshot_id` when ref == main), `RemoveSnapshots` (retains snapshots + drops
  `statistics`/`partition_statistics` entries keyed to removed ids), `RemoveSnapshotRef`;
  anything else errors.

### 3.7 `CallProcedureOutput` + `compute_procedure_output`

Spec-shaped single-row output (mirrors the Apache Iceberg Spark procedures):
- `SnapshotRef { previous_snapshot_id, current_snapshot_id }` — previous = main ref target
  (fallback `current_snapshot_id`, else 0) before commit, current = resolved target (or 0).
- `ExpireSnapshots { six deleted_*_count fields }` — zero-filled here; **real counts** are
  filled by `IcebergTableFormat::call_procedure` from the post-commit GC pass (§4) into
  `to_record_batch()`.

`CallProcedureOutput::schema()` matches the catalog `call_procedure_schema` (doc 06 §3.6):
two i64 fields for snapshot-ref procedures, six i64 fields for expire.

---

## 4. Physical GC: `expire_files_gc` (`operations/expire_snapshots_gc.rs`)

### 4.1 Types

- `FileKind { Data(DataContentType), Manifest, ManifestList, Statistics }` with `tag()` strings
  matching Iceberg Spark's `DeleteSummary` type tags: `DATA`, `POSITION_DELETES`,
  `EQUALITY_DELETES`, `Manifest`, `Manifest List`, `Statistics Files`.
- `ExpireGcCounts { data_files, position_delete_files, equality_delete_files, manifest_files,
  manifest_lists, statistics_files }` — successful deletes only.

### 4.2 `collect_files(store_ctx, metadata, snapshots) -> Vec<(String, FileKind)>`

For each snapshot (mirroring the Spark action's `fileDS`): the manifest list itself
(`ManifestList`); every manifest file in the list (`Manifest`); for data/delete manifests, every
content file (data/position/equality deletes) with an `Added`/`Existing` manifest entry
(`Data(content_type)`). Plus table-level `statistics` and `partition_statistics` files whose
`snapshot_id` is in the given snapshot set.

### 4.3 `diff_files(candidates, valid)`

Anti-join on the `(path, kind.tag())` pair with dedup: a file reachable from a retained
snapshot (`valid`) is never returned; same path + different kind is treated as a different
file.

### 4.4 `delete_files(store_ctx, files) -> ExpireGcCounts`

Best-effort delete: `NotFound` skipped (not counted, debug log); other failures warn and
continue — one failure never aborts the pass. Counts successful deletes per kind. (S3/InMemory
deletes are idempotent, so a missing key counts as success there; the strict NotFound path only
triggers on stores that error.)

### 4.5 `expire_files_gc(store_ctx, pre_commit, post_commit) -> ExpireGcCounts`

Expired set = snapshot ids in `pre_commit` not in `post_commit` (mirrors
`ExpireSnapshotsSparkAction.findExpiredSnapshotIds`). Empty → no-op. Else:
`candidates = collect_files(pre_commit metadata, expired snapshots)`;
`valid = collect_files(post_commit metadata, post_commit.snapshots)`;
`to_delete = diff_files(candidates, valid)`; `delete_files(to_delete)`.

### 4.6 Integration in `IcebergTableFormat::call_procedure` (table_format.rs, doc 07 §4.4)

Only the **filesystem** commit path is supported today (catalog-managed tables return
`not_impl_err!` — `call_procedure` receives only a runtime env, no session/catalog access).
Flow: parse URL → store context → `find_latest_metadata_file` + load `pre_commit` metadata →
`compute_procedure_updates` + `procedure_requirements` + `compute_procedure_output` →
`retry_metadata_commit(object_store, store_ctx, url, latest_meta, check_post_write=true,
{ validate_procedure_requirements; apply_procedure_updates })` → for `ExpireSnapshots` only,
re-load the `post_commit` metadata and run `expire_files_gc(pre, post)`, then build the output
with the real counts; other procedures return the computed output batch.

### 4.7 GC tests (in-module)

`diff_returns_only_files_unique_to_candidates`, `diff_dedupes_and_ignores_same_path_different_kind`,
`expire_files_gc_deletes_expired_only_files` (data+manifest+list of the expired snapshot gone,
retained untouched), `expire_files_gc_never_deletes_shared_manifest_content` (shared manifest
and its content files survive — only the expired snapshot's own manifest list is deleted),
`expire_files_gc_deletes_statistics_files_of_expired_snapshots`,
`expire_files_gc_counts_position_and_equality_deletes`,
`expire_files_gc_counts_idempotent_delete_of_missing_data_file`.

---

## 5. `SnapshotProducer` parent-manifest override (`operations/snapshot.rs`)

New field `parent_manifest_entries: Option<Vec<ManifestFile>>` + builder
`with_parent_manifest_entries(...)`. In `commit()`: when `Some`, used verbatim as the parent
manifest entries; when `None`, the old default applies (Append & !bootstrap loads the parent
manifest list; overwrite-style ops start empty). Also, the `commit` summary operation is now
derived from the operation string via `SnapshotProducerOperation::operation()`: `append`,
`overwrite`, **`delete` → `Operation::Delete`**, **`replace` → `Operation::Replace`**
(unknown → Append fallback), instead of only binary overwrite/append detection. This is what
lets `IcebergCommitExec` arms and `IcebergWriterExec.commit_operation` produce Delete/Replace
snapshots.

---

## 6. `operations/parquet_utils.rs` (NEW)

`read_parquet_footer(store, path) -> Result<ParquetFooterInfo, String>` — heads the file, opens
a `ParquetObjectReader`/`ParquetRecordBatchStreamBuilder`, and returns the `ParquetMetaData`,
Arrow schema, row count and file size **without decoding row data** (used by the LOAD fast-path
classifier, doc 08 §6.3).

---

## 7. CALL resolver (`sail-plan/src/resolver/command/call.rs`, NEW)

`resolve_command_call_procedure(name, arguments, state)`:
- Requires exactly 3 name parts `<catalog>.system.<procedure>` (else unsupported) and
  `system` namespace (case-insensitive).
- Dispatches on lowercase procedure: `rollback_to_snapshot` (table + `snapshot_id` i64),
  `set_current_snapshot` (table + exactly one of `snapshot_id` i64 / `ref` string),
  `expire_snapshots` (table + optional `older_than` timestamp / `retain_last` i32) →
  `CallProcedureOptions` (doc 06 §2) → `CatalogCommand::CallProcedure` via
  `resolve_catalog_command`.
- Argument resolution helpers `resolve_named_arg`/`resolve_optional_named_arg`: case-insensitive
  **name first**, then **position among unnamed-only** args (named args cannot shadow positional
  slots); constants evaluated with `LiteralEvaluator` over an empty DFSchema.
- Scalar coercion helpers `scalar_to_table_name(_parts)`, `scalar_to_snapshot_id` (all int
  widths), `scalar_to_i32`, `scalar_to_timestamp_ms` (TimestampSecond→×1000,
  TimestampMillisecond→value, Microsecond→÷1000, Nanosecond→÷1e6, or string parsed as RFC3339
  or `%Y-%m-%d %H:%M:%S[.f]` / `T`-separated naive → epoch ms).

---

## 8. Port notes / risks

1. **Metadata tables need the shared `IcebergMetadataTableType`** (doc 06 §4.5), the
   `SourceInfo.metadata_table` field and its `metadata_table: None` destructure ripple, and the
   resolver read-path hook — port those as one unit (doc 06/07 note the sites).
2. `procedure.rs` depends on `spec::SnapshotReference` retention accessors and
   `TableMetadata::snapshot(id)`/`current_snapshot()` (added in this branch, doc 07) and on
   spec `TableUpdate`/`TableRequirement` variants (`RemoveSnapshots`, `RemoveSnapshotRef`,
   `SetSnapshotRef`, `RefSnapshotIdMatch`) — confirm they exist on 0.7.1's `sail-iceberg` spec
   surface (the spec crate is vendored, so likely yes; verify names).
3. The GC reads real manifests at runtime (`load_manifest_list`/`load_manifest` from
   `sail_iceberg::io`) — unchanged base machinery; only the orchestration is new.
4. `call_procedure` **filesystem-only** limitation and the catalog `call_procedure_schema`
   must stay in lockstep (doc 06 §3.6). Extending procedures to catalog-managed tables is
   explicitly a follow-up (needs session-level catalog access inside `call_procedure`).
5. `expire_snapshots` zero-count output when GC is skipped must match the six-column schema
   everywhere (catalog schema fn + `CallProcedureOutput::schema`).
