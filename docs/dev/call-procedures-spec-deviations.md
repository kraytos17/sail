# CALL procedures — spec deviations vs Apache Iceberg

**Status:** Analysis / known limitations
**Scope:** `CALL <catalog>.system.<procedure>(...)` implemented in Sail (`feat/v0.6.6`)
**Spec reference:** [Apache Iceberg Spark procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)

---

## 1. What is implemented

Sail supports three Iceberg system procedures, resolved by
`crates/sail-plan/src/resolver/command/call.rs` and executed by
`CallProcedureExec` (`crates/sail-iceberg/src/physical_plan/call_procedure_exec.rs`):

| Procedure | Arguments (positional or named) |
|---|---|
| `rollback_to_snapshot` | `table` (string), `snapshot_id` (long) |
| `set_current_snapshot` | `table` (string), `snapshot_id` (long) **or** `ref` (string, named only) |
| `expire_snapshots` | `table` (string), `[older_than]` (timestamp), `[retain_last]` (int) |

Both commit paths are supported:
- **catalog-managed tables (Polaris / Iceberg REST)** via
  `IcebergCatalogCommitCoordinator::commit` with `TableUpdate::{SetSnapshotRef,
  RemoveSnapshots, RemoveSnapshotRef}`;
- **filesystem tables** via `IcebergTableFormat::retry_metadata_commit`.

The commit is guarded by a `RefSnapshotIdMatch` requirement on `main`, enforced on
**both** commit paths: the catalog path passes it to `IcebergCatalogCommitCoordinator::commit`,
and the filesystem path validates it against the freshly re-read metadata inside
`retry_metadata_commit` (via `validate_procedure_requirements`), mirroring
`IcebergCommitExec::validate_requirements`.

Procedure arguments are resolved by name (case-insensitive) or by position among the
**unnamed** arguments only. Constants are extracted directly from the evaluated
`ScalarValue` — `older_than` matches the timestamp variants (per the time-travel idiom)
instead of round-tripping through a formatted string, so `TIMESTAMP '...'` literals work
and `snapshot_id` accepts any integer scalar.

---

## 2. Deviation matrix

| # | Area | Apache Iceberg spec | Sail v1 current | Severity |
|---|---|---|---|---|
| D1 | `rollback_to_snapshot` / `set_current_snapshot` output | Returns `previous_snapshot_id`, `current_snapshot_id` (long) | **Resolved** — returns `previous_snapshot_id`, `current_snapshot_id` | ~~High~~ ✅ |
| D2 | `expire_snapshots` output | Returns 6 columns: `deleted_data_files_count`, `deleted_position_delete_files_count`, `deleted_equality_delete_files_count`, `deleted_manifest_files_count`, `deleted_manifest_lists_count`, `deleted_statistics_files_count` | **Resolved (schema)** — returns the 6 columns, all `0` (metadata-only) | ~~High~~ ✅ (values 0) |
| D3 | `expire_snapshots` file deletion | Physically deletes snapshots **and data/manifest files** uniquely required by expired snapshots | Metadata-only: removes snapshots/refs; **no** data-file GC | **High** (behavior) |
| D4 | `set_current_snapshot` `ref` argument | Accepts either `snapshot_id` **or** `ref` (branch/tag), not both | **Resolved** — accepts `snapshot_id` (positional or named) or `ref` (named only); exactly one required | ~~Medium~~ ✅ |
| D5 | `expire_snapshots` `retain_last` (default 1) | Preserves the last N ancestor snapshots regardless of `older_than` | **Resolved** — spec retain-set algorithm: per-branch ancestry walk (head-inclusive) keeps first N or `>= older_than`; `older_than`/`retain_last` optional with `history.expire.*` property defaults | ~~Medium~~ ✅ |
| D6 | `expire_snapshots` branch/tag preservation | Snapshots referenced by branches/tags are **not** removed; `main` never expires | **Resolved** — retained refs (main always; others within `max-ref-age-ms`, default never) keep their target snapshots and per-branch ancestry; non-main refs pointing at expired ids are dropped | ~~High~~ ✅ |
| D7 | `rollback_to_snapshot` ancestry | Target snapshot must be an **ancestor** of the current state (`set_current_snapshot` explicitly relaxes this) | **Resolved** — `rollback_to_snapshot` walks `parent_snapshot_id` from the current snapshot (inclusive) and rejects non-ancestors with `Cannot roll back to snapshot, not an ancestor of the current state: <id>`; `set_current_snapshot` remains existence-only | ~~Medium~~ ✅ |

---

## 3. Details per deviation

### D1 — output of `rollback_to_snapshot` / `set_current_snapshot` ✅ resolved

**Spec:**
```
Output
  previous_snapshot_id  long  The current snapshot ID before the rollback
  current_snapshot_id   long  The new current snapshot ID
```

**Sail (resolved):** `CallProcedureOutput::SnapshotRef` produces a single row with
`previous_snapshot_id` and `current_snapshot_id`. `previous_snapshot_id` is the `main`
ref's snapshot id at plan time (the same value the `RefSnapshotIdMatch` requirement
asserts); `current_snapshot_id` is the procedure's target snapshot. The `CallProcedureExec`
is routed to the driver (via `is_driver_stage_plan` and `plan_job_graph_stages`) and
serialized over the worker boundary via `CallProcedureExecNode` (proto field 56).

**Why it matters for heimdall:** heimdall's rollback/rollforward operations record the
before/after snapshot ids (`previous_snapshot_id`, `current_snapshot_id`) in its own
operations table. The CALL result now carries exactly this information.

### D2 — output of `expire_snapshots` ✅ schema resolved (values remain 0)

**Spec:** six `deleted_*_count` long columns (data files, position deletes, equality
deletes, manifests, manifest lists, statistics files).

**Sail (schema resolved):** `CallProcedureOutput::ExpireSnapshots` produces a single row
with the six `deleted_*_count` columns. Because v1 `expire_snapshots` is metadata-only
(see D3), every count is `0` — the column set matches the spec, but the values do not
reflect physical deletions.

### D3 — `expire_snapshots` file deletion

**Spec:**
> "The `expire_snapshots` procedure can be used to remove older snapshots **and their
> files** which are no longer needed. This procedure will remove old snapshots and data
> files which are uniquely required by those old snapshots."

**Sail:** `compute_procedure_updates` only produces `TableUpdate::RemoveSnapshots` (+
`RemoveSnapshotRef` for refs pointing at expired ids). Data files uniquely referenced by
the expired snapshots remain on the object store forever. This is a deliberate v1 scope
cut (metadata-only expiry), but it is **not** what the spec procedure does.

### D4 — `set_current_snapshot` `ref` argument ✅ resolved

**Spec:** `snapshot_id` **or** `ref` (a branch or tag name) — exactly one of the two.

**Sail (resolved):** the resolver accepts `snapshot_id` (positional or named) or `ref`
(named only), enforcing exactly-one via `PlanError::invalid("Either snapshot_id or ref
must be provided, not both")`. At plan time `resolve_target_snapshot_id` resolves a `ref`
through `metadata.refs.get(ref_name).snapshot_id`, failing with
`Cannot find matching snapshot ID for ref <ref>` when the ref is absent. No ancestry
requirement is imposed, matching the spec.

### D5 — `expire_snapshots` `retain_last` ✅ resolved

**Spec:** defaults to `1`; preserves the last N ancestor snapshots even if they are older
than `older_than`.

**Sail (resolved):** `compute_procedure_updates` implements the spec retain-set algorithm:
- `older_than` / `retain_last` are optional; defaults come from
  `history.expire.max-snapshot-age-ms` (5 days) and `history.expire.min-snapshots-to-keep`
  (1) table properties.
- For each retained branch, walk `parent_snapshot_id` from the head (head-inclusive),
  keeping snapshots while `kept < min_snapshots_to_keep || timestamp_ms >= cutoff`.
- Unreferenced snapshots with `timestamp_ms >= older_than` are also retained.
- A branch's own `min_snapshots_to_keep` / `max_snapshot_age_ms` override the defaults.

### D6 — `expire_snapshots` branch/tag preservation ✅ resolved

**Spec:**
> "Snapshots that are still referenced by branches or tags won't be removed. The main
> branch never expires."

**Sail (resolved):** the same retain-set algorithm retains `main` unconditionally and any
non-`main` ref whose snapshot is within `max-ref-age-ms` (default: never). Retained ref
targets and their branch ancestries are kept; `RemoveSnapshotRef` is issued only for
non-`main` refs whose target snapshot is actually being expired (aged-out/dangling refs).

### D7 — `rollback_to_snapshot` ancestry ✅ resolved

**Spec:** `set_current_snapshot` states *"Unlike rollback, the snapshot is not required to
be an ancestor of the current table state."* — implying rollback *does* require an
ancestor.

**Sail (resolved):** `rollback_to_snapshot` validates the target is an ancestor of the
current snapshot via `is_current_ancestor` (walking `parent_snapshot_id`, current
inclusive) and fails with `Cannot roll back to snapshot, not an ancestor of the current
state: <id>` otherwise. `set_current_snapshot` keeps its existence-only check, preserving
the spec's distinction between the two procedures.

---

## 4. Impact on heimdall / clients

- **Result schema:** heimdall currently treats CALL results as `rowsLoaded`/count
  style. The spec output columns are what heimdall's operation-tracking code wants for
  rollback/rollforward, so aligning D1 is the highest-value fix.
- **Metadata table reads are unaffected:** `.refs` / `.snapshots` (Phase A) already expose
  the post-procedure state, so verification via `SELECT ... FROM ns.tbl.refs` /
  `.snapshots` works today.
- **`expire_snapshots` correctness:** the retain-set algorithm now matches spec retention
  semantics (branch/tag-referenced snapshots kept, `main` never expires, `retain_last`
  honored). It is still metadata-only — orphaned data files are not physically deleted
  (D3), so storage is not reclaimed.

---

## 5. Suggested follow-ups

### ✅ A. Align the exec output with the spec — DONE
`CallProcedureExec` now returns spec-shaped result batches:
- `rollback_to_snapshot` / `set_current_snapshot` → `previous_snapshot_id`,
  `current_snapshot_id` (the `main` ref snapshot at plan time, and the target).
- `expire_snapshots` → the six `deleted_*_count` columns (all `0` for metadata-only v1).

To make this work in cluster mode, `CallProcedureExec` was also:
- routed to the **driver** via `is_driver_stage_plan` and `plan_job_graph_stages`
  (matching `IcebergCommitExec` / `CatalogCommandExec`), and
- serialized over the worker boundary via the new `CallProcedureExecNode` proto
  (field 56) + codec encode/decode + a roundtrip test.

### ✅ B. Preserve branch/tag-referenced snapshots in `expire_snapshots` — DONE (D6)
The retain-set algorithm retains every retained ref's target and its branch ancestry;
`RemoveSnapshotRef` is emitted only for non-`main` refs whose target snapshot is being
expired.

### ✅ C. Implement `retain_last` — DONE (D5)
Per-branch, head-inclusive ancestry walk keeping the first N snapshots (or any with
`timestamp_ms >= older_than`); `older_than`/`retain_last` optional with
`history.expire.*` property defaults.

### ✅ D. `set_current_snapshot` `ref` argument — DONE (D4)
Accepts `ref => '<branch-or-tag>'`, resolved through `metadata.refs` at plan time with the
spec error message.

### ✅ E. `rollback_to_snapshot` ancestry check — DONE (D7)
The target must be an ancestor of the current snapshot (walk `parent_snapshot_id`,
current inclusive); `set_current_snapshot` keeps its existence-only check.

### F. `expire_snapshots` physical file GC (D3, large, defer)
After removing snapshots, delete data/manifest files uniquely owned by the expired
snapshots and report real (non-zero) counts. This is the full spec behavior and the
biggest scope item.

---

## 6. Reference

- Spec: https://iceberg.apache.org/docs/latest/spark-procedures/
- Resolver: `crates/sail-plan/src/resolver/command/call.rs`
- Logical node: `crates/sail-logical-plan/src/call_procedure.rs`
- Exec: `crates/sail-iceberg/src/physical_plan/call_procedure_exec.rs`
- Planner: `crates/sail-iceberg/src/physical/call_procedure_planner.rs`
- Commit machinery: `crates/sail-iceberg/src/catalog_support/commit.rs`
