# Plan: `expire_snapshots` physical file GC (D3) — spec-accurate

**Status:** Implemented (filesystem + catalog-managed paths)
**Branch:** `feat/v0.6.6`
**Scope:** Close the last remaining CALL-procedure deviation (D3): `expire_snapshots`
currently removes snapshot/ref **metadata** only; the Iceberg spec also **physically deletes**
data/delete files, manifests, manifest lists, and statistics files that are uniquely owned
by the expired snapshots, and reports real `deleted_*_count` columns.
**Spec reference:** [Apache Iceberg Spark procedures — `expire_snapshots`](https://iceberg.apache.org/docs/latest/spark-procedures/) + [Iceberg spec — Snapshot expiration](https://iceberg.apache.org/spec/#table-metadata-and-snapshots) + Iceberg Java reference (`RemoveSnapshots`, `ExpireSnapshotsSparkAction`, `ReachableFileCleanup`, `BaseSparkAction.DeleteSummary`).

---

## 1. Spec semantics (verified against Iceberg Java reference)

The authoritative behavior comes from two implementations that agree on the invariant:

> `expire_snapshots` removes old snapshots **and their files** which are no longer needed —
> i.e. files that are **uniquely required by the expired snapshots**. It **never removes
> files still required by a non-expired snapshot.** ("never remove files which are still
> required by a non-expired snapshot" — Spark procedures docs.)

### 1.1 The deleted-set algorithm (anti-join)

**Spark path** (`ExpireSnapshotsSparkAction.expireFiles()`):
```java
TableMetadata originalMetadata = ops.current();
expireSnapshots.cleanupLevel(CleanupLevel.NONE).commit();      // metadata-only commit
TableMetadata updatedMetadata = ops.refresh();                  // post-commit metadata
Dataset<FileInfo> validFileDS  = fileDS(updatedMetadata);                       // all files reachable from RETAINED snapshots
Set<Long> deletedSnapshotIds = original.snapshots() − updated.snapshots();      // expired ids
Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds); // all files reachable from EXPIRED snapshots
this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);                 // ANTI-JOIN on (path, type)
```

**Core path** (`ReachableFileCleanup.cleanFiles`) — the same invariant with an in-memory
reference set:
1. `manifestListsToDelete` = every expired snapshot's `manifest_list` location.
2. `deletionCandidates` = manifests reachable from expired snapshots.
3. `pruneReferencedManifests`: remove every manifest still reachable from any **retained**
   snapshot → `manifestsToDelete`; also collect `currentManifests` (every retained manifest).
4. `findFilesToDelete` = union of all file paths in `manifestsToDelete` **minus** union of
   all file paths in `currentManifests` — a file referenced by *any* entry
   (added/existing/deleted) in any retained manifest is protected.
5. Delete: data files, then manifests, then manifest lists, then statistics files.

**Safety property (fail-safe):** in the core path, if listing the retained manifests fails
for *any* reason, `findFilesToDelete` returns an **empty set** — nothing is deleted
(`ReachableFileCleanup.java:223-226`). Deletes are **best-effort**: `NotFoundException`
(missing file) is ignored, other failures are retried then suppressed; only **successful**
deletes are counted.

### 1.2 Ordering: metadata commit happens BEFORE any file deletion

Both engines commit the metadata (removing expired snapshots/refs) first, then delete files.
The deleted files' paths are **not** rewritten out of remaining manifests — retained
manifests are used only as a read-only protection set; manifests owned *only* by expired
snapshots are deleted whole.

### 1.3 Output counts (the 6 `deleted_*_count` columns)

From `BaseSparkAction.DeleteSummary` — **counts reflect successful physical deletes** per
type tag, **not** the candidate-set size:

| column | tag |
|---|---|
| `deleted_data_files_count` | `DATA` (from `DataFile.content`) |
| `deleted_position_delete_files_count` | `POSITION_DELETES` |
| `deleted_equality_delete_files_count` | `EQUALITY_DELETES` |
| `deleted_manifest_files_count` | `Manifest` |
| `deleted_manifest_lists_count` | `Manifest List` |
| `deleted_statistics_files_count` | `Statistics Files` |

Bulk deletes count `submitted − failed`; per-file deletes count only on success.

### 1.4 `CleanupLevel` and `gc.enabled`

- `CleanupLevel.NONE` = metadata only; `METADATA_ONLY` = manifests + manifest lists +
  statistics (retain data files); `ALL` = everything (Spark procedure's effective level).
- `gc.enabled` (default `true`) **gates expiration entirely** — if `false`, snapshot
  expiration is rejected ("GC is disabled"). Sail does not currently read `gc.enabled`.

---

## 2. Current Sail state (what exists / what is missing)

### Exists (reuse as-is)
- Retain-set algorithm producing `TableUpdate::RemoveSnapshots { snapshot_ids }` +
  `RemoveSnapshotRef` — `compute_procedure_updates` (`crates/sail-iceberg/src/physical_plan/call_procedure_exec.rs:268`).
- Spec-shaped output `CallProcedureOutput::ExpireSnapshots` (six `deleted_*_count` columns,
  currently hardcoded `0`) — `call_procedure_exec.rs:539-546`.
- Dual-path commit in `execute_call` (`call_procedure_exec.rs:116-200`): filesystem via
  `IcebergTableFormat::retry_metadata_commit`, catalog via
  `IcebergCatalogCommitCoordinator::commit`.
- Async manifest traversal primitives (`crates/sail-iceberg/src/io/mod.rs`):
  - `load_manifest_list(store_ctx, manifest_list_str) -> Result<ManifestList>` (`:80`)
  - `load_manifest(store_ctx, manifest_path_str) -> Result<Manifest>` (`:95`)
- `StoreContext::resolve(raw) -> (store_ref, ObjectPath)` for absolute/relative path
  resolution (`io/mod.rs:45`) — reuse for both reads and deletes.
- Types: `ManifestList { entries: Vec<ManifestFile> }`, `ManifestFile { manifest_path,
  content: ManifestContentType, sequence_number, ... }`, `Manifest { entries:
  Vec<ManifestEntryRef> }`, `ManifestEntry { status: ManifestStatus, snapshot_id, ...,
  data_file: DataFile }`, `DataFile { content: DataContentType, file_path, ... }`,
  `ManifestStatus::{Added,Existing,Deleted}`, `ManifestContentType::{Data,Deletes}`,
  `DataContentType::{Data,PositionDeletes,EqualityDeletes}`.
- Stats files: `StatisticsFile { snapshot_id, statistics_path, ... }` and
  `PartitionStatisticsFile { snapshot_id, statistics_path }`, stored on
  `TableMetadata.statistics` / `partition_statistics` (`spec/metadata/statistic_file.rs`).
- `TableMetadata::snapshot(id)`, `current_snapshot()`, `properties: HashMap<String,String>`.
- Metadata re-read after commit: `find_latest_metadata_file(object_store, &table_url)`
  (`table/metadata_loader.rs:157`) + `TableMetadata::from_json`.

### Missing (this plan)
- Post-commit **physical file deletion** for `expire_snapshots`.
- Real (non-zero) `deleted_*_count` output.
- `gc.enabled` gating (reject expire when `false`).
- Pre-commit metadata capture needed by the exec to compute the candidate set after commit.

---

## 3. Design

### 3.1 High-level flow (mirrors the Spark action)

```
plan_call_procedure
  ├─ load TableMetadata (plan-time)                 [already done]
  ├─ compute retain-set → updates (RemoveSnapshots) [already done]
  ├─ REQUIREMENT: capture PRE-COMMIT metadata into the exec   ← NEW
  └─ build CallProcedureExec { updates, requirements, output, pre_commit_metadata }

CallProcedureExec::execute_call
  ├─ dual-path commit (metadata)                   [already done]
  ├─ IF procedure == ExpireSnapshots:              ← NEW
  │    ├─ reload post-commit metadata (filesystem) / re-fetch (catalog)
  │    ├─ expired_ids = pre_commit.snapshots − post_commit.snapshots
  │    ├─ candidates = all files reachable from expired snapshots (from pre_commit metadata)
  │    ├─ valid     = all files reachable from retained snapshots (from post_commit metadata)
  │    ├─ to_delete = candidates ∖ valid           (anti-join on (path, type))
  │    └─ best-effort delete; count successes per type
  └─ output batch with real counts                ← NEW
```

### 3.2 Anti-join sets — what "a file" means

A file is `(path, kind)` where kind ∈ {`DATA`, `POSITION_DELETES`, `EQUALITY_DELETES`,
`Manifest`, `Manifest List`, `Statistics Files`}. The candidate and valid sets are each the
union of:

| source | kind tag | how discovered |
|---|---|---|
| `entry.data_file.file_path` in each expired/retained snapshot's data **and** delete manifests, entries with `status ∈ {Added, Existing}` | `DATA` / `POSITION_DELETES` / `EQUALITY_DELETES` from `DataFile.content` | `load_manifest_list(snapshot.manifest_list())` → filter `ManifestFile.content` → `load_manifest` → entries |
| `ManifestFile.manifest_path` in each snapshot's manifest list | `Manifest` | same manifest-list read |
| each snapshot's `manifest_list` location | `Manifest List` | `Snapshot::manifest_list()` |
| `StatisticsFile.statistics_path` / `PartitionStatisticsFile.statistics_path` whose `snapshot_id ∈` the snapshot set | `Statistics Files` | `TableMetadata.statistics` / `partition_statistics` |

Note: this mirrors `BaseSparkAction.fileDS(metadata, snapshotIds)` which unions
`contentFileDS ∪ manifestDS ∪ manifestListDS ∪ statisticsFileDS`.

**Retained manifests as the protection set:** matching the core path, the `valid` set for
data/delete files must include **every** file path reachable from **any retained manifest**
(not only files of retained snapshots' live entries). To stay spec-faithful and safe, build
the valid content-file set from all retained snapshots (all manifests, all Added/Existing
entries). Since the anti-join already protects any path present in `valid`, a shared
manifest referenced by both an expired and a retained snapshot is automatically safe.

### 3.3 What gets deleted (final pipeline)

1. **Data/delete files:** `to_delete_files = candidate_content ∖ valid_content`, grouped by
   `DataContentType`.
2. **Manifests:** `to_delete_manifests = candidate_manifests ∖ valid_manifests`.
3. **Manifest lists:** expired snapshots' `manifest_list` locations, minus any still present
   in the valid manifest-list set (normally none, but keep the anti-join for safety).
4. **Statistics files:** expired snapshots' `statistics_path` entries, minus those still
   present in the post-commit metadata.

**Best-effort deletes:** per file, resolve via `store_ctx.resolve(path)` then
`store_ref.delete(&path)`. `NotFound` → skip (not an error, not counted). Other errors →
`log::warn!` and continue (mirror `FileCleanupStrategy.deleteFiles`). Only successful
deletes increment the matching counter.

### 3.4 Ordering and atomicity

- Metadata commit is already atomic (filesystem CAS / catalog commit). File deletion
  happens strictly after a successful commit, matching the spec.
- A delete failure must **not** fail the CALL — `expire_snapshots` returns counts and the
  metadata is already correct. Failures reduce the reported counts (and are logged).
- Data-correctness invariant: never delete a path present in the valid set. The fail-safe
  "empty set on retained-manifest read error" is **required** for data files.

---

## 4. Implementation steps

### Step 1 — `gc.enabled` gate
In `compute_procedure_updates` (`ExpireSnapshots` arm) and/or the resolver, read
`metadata.properties["gc.enabled"]` (default `true`). If `false`, return
`PlanError::unsupported("Cannot expire snapshots: GC is disabled ...")` (resolver) or the
exec error. Since both `RemoveSnapshots` (Java core) and the Spark action gate expiration
entirely, gate at **plan** time so no commit happens.

### Step 2 — carry pre-commit metadata into the exec
`CallProcedureExec` gains a field, e.g.
```rust
pre_commit_metadata: Option<crate::spec::TableMetadata>,
```
populated by `plan_call_procedure` (planner already has `metadata`). This is serialized in
`CallProcedureExecNode` (see Step 6) as `table_metadata_json` (reuse `TableMetadata`'s
serde derives — it is `Serialize`/`Deserialize`). Keep it `Option` so non-expire procedures
serialize `null`/empty and cluster round-trips stay small. Only set it for
`ExpireSnapshots`.

### Step 3 — new GC module
New file `crates/sail-iceberg/src/physical_plan/expire_snapshots_gc.rs` (or under
`operations/`). Public surface:
```rust
/// Result of a physical expire GC pass; each count = successful deletes.
pub struct ExpireGcCounts {
    pub data_files: u64,
    pub position_delete_files: u64,
    pub equality_delete_files: u64,
    pub manifest_files: u64,
    pub manifest_lists: u64,
    pub statistics_files: u64,
}

/// Collect candidate file keys (path, kind) reachable from `snapshots`.
async fn collect_files(
    store_ctx: &StoreContext,
    metadata: &TableMetadata,
    snapshots: &[Snapshot],
) -> Result<Vec<(String, FileKind)>>;

/// Anti-join: candidate ∖ valid.
fn diff_files(candidates: Vec<(String, FileKind)>, valid: Vec<(String, FileKind)>) -> Vec<(String, FileKind)>;

/// Best-effort delete; returns per-kind success counts.
async fn delete_files(
    store_ctx: &StoreContext,
    files: &[(String, FileKind)],
) -> Result<ExpireGcCounts>;

/// Full pass: after a successful metadata expire, delete uniquely-owned files.
pub async fn expire_files_gc(
    store_ctx: &StoreContext,
    pre_commit: &TableMetadata,
    post_commit: &TableMetadata,
) -> Result<ExpireGcCounts>;
```
- `FileKind` mirrors the tags (`Data(DataContentType)`, `Manifest`, `ManifestList`,
  `Statistics`).
- `collect_files` for a snapshot set: `io::load_manifest_list` → per `ManifestFile`
  `io::load_manifest` → for each `ManifestEntry` with `status ∈ {Added, Existing}`, push
  `(entry.data_file.file_path, Data(content))`; also push `(mf.manifest_path, Manifest)`
  and the snapshot's `manifest_list` as `ManifestList`; add stats from
  `metadata.statistics`/`partition_statistics` filtered by `snapshot_id ∈ set`.
- `diff_files`: exact `(path, kind)` set difference (path alone is not enough because a
  data file and a manifest could theoretically share a path; matching Spark's `FileInfo`
  equality on `(path, type)`).
- `delete_files`: iterate, `let (store, path) = store_ctx.resolve(&p)?; match
  store.delete(&path).await { Ok(_) => count++, Err(NotFound) => {}, Err(e) => warn }`.

### Step 4 — wire into `execute_call`
After the commit `match commit_mode { ... }` block and **only** for
`CallProcedure::ExpireSnapshots`:
- Reload post-commit metadata via `reload_post_commit_metadata` (Step 4 helper):
  - Filesystem: `find_latest_metadata_file` + `TableMetadata::from_json` (same store/url).
  - Catalog commit: use the `metadata_location` returned by the commit
    (`CatalogCommittedTable::metadata_location()`), falling back to
    `find_latest_metadata_file`.
- `pre_commit` = `self.pre_commit_metadata` (Step 2).
- `expired_ids = pre_commit.snapshots − post_commit.snapshots` (set difference, matching
  `findExpiredSnapshotIds`).
- `expire_files_gc(store_ctx, pre_commit, post_commit)` → `ExpireGcCounts`.
- Build the output batch from those counts.

### Step 5 — real counts in the output
Change `CallProcedureOutput::ExpireSnapshots` from a unit variant to carry counts:
```rust
CallProcedureOutput::ExpireSnapshots {
    data_files: i64,
    position_delete_files: i64,
    equality_delete_files: i64,
    manifest_files: i64,
    manifest_lists: i64,
    statistics_files: i64,
}
```
`to_record_batch` emits these instead of zeros. `compute_procedure_output` (planner) still
produces zeros at plan time (it cannot know post-commit deletes); the exec replaces the
output with real counts at execution time. The schema is unchanged (six Int64 nullable
columns), so no proto/type-schema change beyond the enum payload.

### Step 6 — codec / proto
`CallProcedureExecNode` (proto field 56, `physical.proto`) gains
`string pre_commit_metadata_json = 7;` (empty = `None`), with encode/decode arms in
`crates/sail-execution/src/proto/codec.rs`:
- encode: `self.try_encode_json(&exec.pre_commit_metadata, "...")`-style (or the existing
  `try_encode_lakehouse_table`-like helper for `Option<TableMetadata>`).
- decode: empty string → `None`, else `try_decode_json`.
Update `test_round_trip_call_procedure_exec` to include the metadata (and keep the
`SetCurrentSnapshot{ref}` case). `CallProcedureOutput::ExpireSnapshots` fields must also
round-trip (serde JSON — automatic, but assert in the test).

### Step 7 — metadata-side: statistics removal
On the **filesystem** path, `apply_procedure_updates` (`RemoveSnapshots`) currently only
truncates `table_meta.snapshots`. To match the spec (`TableMetadata.Builder` removes
statistics for removed snapshots), also drop `table_meta.statistics` /
`partition_statistics` entries whose `snapshot_id ∈ removed ids`. (The catalog path sends
`RemoveSnapshots` to Polaris, which handles it; this is filesystem-path parity.)

---

## 5. Catalog (Polaris) path nuance

- After a catalog commit, the post-commit metadata is reloaded from the **`metadata_location`
  returned by the commit** (`CatalogCommittedTable::metadata_location()`), falling back to
  `find_latest_metadata_file` on `table_url` when the location is absent.
- Deletes use the same object store resolved from `table_url` (Polaris vends storage via
  the table's location / credentials), so `store_ctx` from `get_object_store_from_context`
  applies.
- Reload/collection failures **propagate** (the CALL errors after a successful metadata
  commit) — matching the Iceberg reference (`ops.refresh()` post-commit throws; manifest
  reads use `throwFailureWhenFinished`). There is no silent zero-count degradation.

---

## 6. Safety invariants (non-negotiable)

1. **Never delete a path in the valid set.** The anti-join is authoritative.
2. **Computation failures propagate (spec-accurate):** failures to reload post-commit
   metadata or to read manifests during the candidate/valid computation propagate — matching
   the Iceberg reference (`ops.refresh()` post-commit throws; manifest reads use
   `throwFailureWhenFinished`). The metadata commit has already succeeded, so the CALL
   errors without deleting anything.
3. **Best-effort deletes:** `NotFound` skipped, errors logged, never fatal to the CALL.
   Counts reflect successful deletes only.
4. **Metadata first:** physical deletes only after a successful commit.
5. **`gc.enabled` is always-on:** no gate (per project decision); expire always runs GC.

---

## 7. Tests

### Unit (`call_procedure_exec.rs` + new `expire_snapshots_gc.rs`)
- **Anti-join / diff:** candidate ∪ valid → only unique-to-candidate returned; shared file
  (present in both expired and retained) is never returned.
- **collect_files:** a synthetic metadata with one expired + one retained snapshot sharing a
  manifest → manifests: only the expired-only manifest is a candidate; content files from
  the shared manifest are candidates but excluded by the valid set; manifest lists and stats
  collected correctly per snapshot id.
- **delete best-effort:** mock store — successful delete counts; `NotFound` skipped;
  non-`NotFound` error logged + not counted.
- **Fail-safe:** retained manifest read error → `ExpireGcCounts` all zero, no delete calls.
- **Output:** `CallProcedureOutput::ExpireSnapshots{...}` builds a 6-column batch with the
  provided counts (schema unchanged).
- **gc.enabled:** `ExpireSnapshots` on a table with `gc.enabled=false` → rejected.

### Integration (`sail-iceberg`, filesystem table)
Build via existing helpers (`bootstrap`, writer) a table with ≥2 snapshots whose manifests
reference distinct data files; `CALL …expire_snapshots` with `older_than`/`retain_last`
expiring the oldest; assert:
- expired snapshot gone from `.snapshots`;
- data files uniquely owned by the expired snapshot are removed from the store;
- files still referenced by the retained snapshot are **present**;
- `deleted_*_count` columns report the actual numbers;
- re-run is idempotent (already-deleted files → `NotFound` skipped → zero counts).

### Codec
`test_round_trip_call_procedure_exec` covers `pre_commit_metadata_json` and the counted
`ExpireSnapshots` output.

### Resolver
`gc.enabled=false` rejection + (optionally) error message parity.

---

## 8. Docs updates

- `docs/dev/call-procedures-spec-deviations.md`: mark **D3 ✅ resolved**; update matrix,
  detail section, and follow-up F → DONE; the deviations doc becomes fully resolved.
- `TEST_PLAN.md` §13.4 / §14: document real counts and physical deletes; keep the
  metadata-only note only for the `gc.enabled=false` case or catalog fallback.

---

## 9. Open decisions

1. **Catalog post-commit re-fetch:** which exact re-load helper for catalog tables
   (confirm during implementation; fallback = skip GC + zero counts).
2. **`gc.enabled=false` behavior:** reject the whole CALL (matches Iceberg) vs. allow
   metadata-only expire. **Recommend: reject** (spec parity).
3. **Concurrency window:** between pre-commit capture and post-commit re-read, a concurrent
   writer could add files. The `RefSnapshotIdMatch` requirement + post-commit re-read of the
   *latest* metadata keep the valid set current; the anti-join protects those files. Note
   this mirrors the Spark action's own TOCTOU and is the accepted spec behavior.

---

## 10. Suggested sequencing

1. Step 1 (gc.enabled gate) + Step 5 (output counts) — small, independent.
2. Step 3 (GC module) + unit tests — the core.
3. Step 2 + Step 6 (exec field + codec) — plumbing.
4. Step 4 (wire into execute_call, filesystem first) + integration tests.
5. Step 7 (statistics metadata removal) + catalog path.
6. Docs + full verification.

## 11. Reference files

- Exec: `crates/sail-iceberg/src/physical_plan/call_procedure_exec.rs`
- Planner: `crates/sail-iceberg/src/physical/call_procedure_planner.rs`
- Resolver: `crates/sail-plan/src/resolver/command/call.rs`
- Logical node: `crates/sail-logical-plan/src/call_procedure.rs`
- IO + types: `crates/sail-iceberg/src/io/mod.rs`, `spec/manifest_list.rs`,
  `spec/manifest/{mod,entry}.rs`, `spec/manifest/data_file.rs`,
  `spec/metadata/statistic_file.rs`, `spec/metadata/table_metadata.rs`
- Metadata reload: `crates/sail-iceberg/src/table/metadata_loader.rs`
  (`find_latest_metadata_file`), `TableMetadata::from_json`
- Codec/proto: `crates/sail-execution/src/proto/codec.rs`,
  `crates/sail-execution/proto/sail/plan/physical.proto`
- Iceberg reference (mirrored under `/tmp/opencode/iceberg/`): `RemoveSnapshots.java`,
  `spark/.../actions/ExpireSnapshotsSparkAction.java`,
  `core/.../ReachableFileCleanup.java`, `core/.../FileCleanupStrategy.java`,
  `spark/.../actions/BaseSparkAction.java`, `api/.../actions/ExpireSnapshots.java`,
  `spark/.../procedures/ExpireSnapshotsProcedure.java`
