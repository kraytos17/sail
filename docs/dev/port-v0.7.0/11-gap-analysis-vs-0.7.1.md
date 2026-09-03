# Porting feat/0.7.0 → feat/0.7.1 — Doc 11: Gap Analysis vs feat/0.7.1 & Port Strategy

> Part of the `docs/dev/port-v0.7.0/` inventory. For each cluster documented in docs 01–10,
> this file records what **already exists independently on `feat/0.7.1`** (so the port is a
> re-implementation/merge, not a copy), what is genuinely new (pure additions), the
> overlap/conflict risk, and the recommended port order. Evidence was gathered by grepping the
> 0.7.1 working tree (this checkout) at `9544c925`.
>
> Ground truth: source `feat/0.7.0` tip `c07ad0c8`; target `feat/0.7.1` tip `9544c925`.

---

## 0. Branch relationship recap

`feat/0.7.0` = user's fork line on top of the shared v0.7.0 prepare commit `f0b137d6`
(11 commits unique: `ee78c38d` big-bang … `c07ad0c8`). `feat/0.7.1` is the upstream v0.7.1
line (82 commits ahead of the same base, incl. all upstream post-0.7.0 work: storage-shuffle,
checkpoint, DV writers, etc.). The 0.7.0 branch is **missing** all 82 upstream commits, so
porting is not cherry-picking — each feature must be re-laid onto 0.7.1's evolved code, and for
a few subsystems 0.7.1 already ships its *own* solution.

---

## 1. Per-cluster gap table

Legend — **PURE ADD** (0.7.1 has nothing; copy/adapt), **OVERLAP** (0.7.1 has an independent
implementation; merge or supersede), **ADD-on-diverged-base** (0.7.1 lacks the feature but the
host files changed heavily upstream → 3-way merge).

| Doc / cluster | 0.7.1 state (evidence) | Verdict |
|---|---|---|
| **01 session/runtime/config** | No `ServerConfig`/`session_id`, no `object_store.*` config keys, no `SessionConfigFactory` (`session_config.rs` ABSENT), no `SessionIdleDuration` actor event | **PURE ADD** (except `SessionConfigFactory` replaces the same inline code that also exists on 0.7.1's `ServerSessionFactory` — refactor the target's inline methods instead of deleting blindly). Keepalive default 10s→120s is a behavior change to adopt. |
| **02 spark-connect multiplexer + combo** | No `multiplexer.rs`, no `combo.rs`, no `run_combo_server`, no `with_default_session`, no `session_idle_duration` | **PURE ADD**. Depends on doc-01 additions + `SailFlightSqlService` default-session seam. |
| **03 distributed exec** | 0.7.1 worker pool/job graph/RPC files exist but heavily diverged (storage-shuffle, checkpoint exec, codec grew). `delete_worker`/`running_worker_count`/prune not present. `IcebergLoadDataFastExecNode`/`file_path_column` proto fields not present | **ADD-on-diverged-base** (execution layer). Small isolated additions (`delete_worker`, peer-tagged `rpc_error`, keepalive constants, subquery-index tracking, stage plan `OnceLock`) apply cleanly; proto message numbering must be re-checked against 0.7.1 (`physical.proto`). |
| **04 object-store registry** | Registry exists; no `ObjectStoreConfig` plumbing | **PURE ADD** on top of doc-01 config type. |
| **05 SQL frontend** | Parser/AST/spec for `CALL`, `TRUNCATE`, `SHOW TBLPROPERTIES`, `DESCRIBE VIEW`, expanded `ALTER TABLE` ops — check (0.7.1 upstream added SHOW FUNCTIONS/DESCRIBE FUNCTION etc. and some ALTER grammar; assume not present for these statements) | **PURE ADD** (verify keyword collisions; `syntax.json` gold data regenerated). `Identifier`/`ObjectName` `Ord` derive ripple is additive. |
| **06 catalog DDL & providers** | REST catalog provider exists but its `alter_table` was `NotSupported`; no `CallProcedureOptions`; `CatalogObject::Column` absent; `IcebergRestAccessDelegation` absent | **PURE ADD** (REST `update_table` DDL, CREATE-OR-REPLACE, delegation) — but check 0.7.1 `sail-catalog`/`sail-common-datafusion` contracts (`TableFormat` trait may already have gained `AlterColumn*` variants upstream — grep before adding). |
| **07 iceberg row-level ops** | **0.7.1 already has its own DELETE and MERGE for Iceberg** via delete writers: `delete_apply_exec.rs`, `delete_writer_common.rs`, `equality_delete_writer_exec.rs`, `position_delete_writer.rs`, `merge_metadata_exec.rs`, `merge_row_projection.rs`, `IcebergCommitExec` (commit/), `create_deleter`+`create_merger` in `table_format.rs`, `logical/merge.rs`; **no `create_updater`, no `logical/update.rs`, no `physical_plan/planner/`, no `file_path_column`** | **OVERLAP (highest risk)**. The 0.7.0 branch *replaces* the upstream delete-writer approach with targeted rewrite + parent-manifest filtering. Porting requires a strategic decision: (a) supersede 0.7.1's DELETE/MERGE with the 0.7.0 rewrite machinery (large, touches shared `RowLevelWriteNode`), or (b) port only the gaps (UPDATE — genuinely absent on 0.7.1 — plus commit fields `touched_file_paths`/`overwrite_*`/`reported_row_count`, empty-table DELETE, `Delete`/`Replace` commit arms) on top of 0.7.1's existing row-level flow. |
| **08 LOAD DATA + write path** | **No** `LoadDataNode`, `load_classifier.rs`, `load_data_planner.rs`, `IcebergLoadDataFastExec`, or codec node on 0.7.1. `IcebergWriterExec` exists but lacks `compression_codec`/`target_file_size`/`commit_operation`/`touched_file_paths` fields and hash-partition requirement; writer uses default parquet props | **PURE ADD for LOAD**; write-path additions (zstd default, rolling, metrics, table-property-driven `build_writer_properties`, `AsyncShareableBuffer`) are additive on a diverged `writer_exec.rs`. Writer behavior change (zstd + 128MB rolling + hash-distribution for partitioned) affects all INSERT/CTAS — decide scope. |
| **09 procedures / metadata tables / GC** | **Absent entirely** on 0.7.1 (no `CallProcedureOptions`, no `expire_snapshots_gc.rs`, no metadata tables, no `CallProcedureOutput`) | **PURE ADD**. Depends on 06 (`TableFormatProcedureOperation`/catalog command), 07 (`commit`/`retry_metadata_commit`/spec accessors) and 03 (if fast LOAD GC is used). |
| **10 config/docker/k8s/python** | 0.7.1 has older Dockerfiles (no chef, `pyspark-client`), `sail-spark-server` k8s manifest, no `build.sh`, no heimdall flight tests | **PURE ADD / REPLACE** (k8s service rename + port layout must accompany doc 02). |

---

## 2. Recommended port order (bottom-up; each step compiles)

1. **Frontend spec/parser/analyzer** (05) — pure AST/spec additions.
2. **Config types** (`ServerConfig`, `ObjectStoreConfig`, `IcebergRestAccessDelegation`,
   `application.yaml`) + `SessionConfigFactory` refactor on the existing `ServerSessionFactory`
   and the worker factory (01). Adopt 120 s keepalive + task timeout/attempt defaults.
3. **Shared `sail-common-datafusion` contracts** (06 §4): `IcebergMetadataTableType`,
   `SourceInfo.metadata_table` + mechanical `metadata_table: None` destructures repo-wide,
   `UpdateInfo`/`UpdateAssignment`, new `TableFormatAlterTableOperation` variants,
   `TableFormatProcedureOperation`, `TableFormat::create_updater`/`call_procedure` defaults.
4. **Object-store config plumbing** (04) + RPC/keepalive/peer hardening & worker-pool lifecycle
   additions (03), skipping proto numbers that collide.
5. **Catalog command layer + REST provider DDL** (06) — new options types, ALTER/DESCRIBE/
   SHOW TBLPROPERTIES/CALL commands, CREATE-OR-REPLACE, access delegation.
6. **Iceberg write path** (08 §3–§5) — writer properties/zstd/rolling/options/metrics;
   then **row-level ops decision** (07) and **LOAD DATA** (08 §6) once the write path exists.
7. **Procedures/metadata tables/GC** (09) on top of 06/07.
8. **Docker/k8s/python/build** (10).

---

## 3. Key strategic decisions to confirm before porting

1. **Row-level ops**: supersede 0.7.1's delete-writer DELETE/MERGE with the 0.7.0 targeted
   rewrite (doc 07) — or keep 0.7.1's writers and add only UPDATE + commit-exec extensions?
   (Recommendation: prototype UPDATE first — it is unambiguously missing on 0.7.1 — using the
   0.7.0 planner module; evaluate DELETE/MERGE migration separately because upstream
   deliberately invested in DV delete writers.)
2. **`IcebergWriterExec` behavior change** (hash partitioning for partitioned tables, zstd,
   128 MB rolling) affects every write path on 0.7.1 — port as a deliberate behavioral change,
   not silently.
3. **`metadata_files_for_version` signature change** (returns timestamps) ripples through
   `commit_exec.rs`/`table_format.rs` — both files exist on 0.7.1 in divergent form; carry the
   stale-file semantics through a 3-way merge.
4. **k8s `sail-spark-server` → `sail-server` rename** + new ports + canonical-session env is
   externally visible; port only with the combined-server code (02).
5. **Parser keyword additions** (`CALL`, `TRUNCATE`, etc.) regenerate grammar gold data —
   confirm none of them already became keywords on 0.7.1's fork of `sail-sql-parser`.

---

## 4. Known 0.7.1 pre-existing machinery the port depends on (do NOT re-port)

- `sail-common-datafusion` datasource contracts that already exist upstream on 0.7.1:
  `DeleteInfo`, `MergeInfo`/`MergeCapableSource`, `RowLevelWriteNode`
  (`new_delete`/`new_merge`), `MERGE_FILE_COLUMN`/`OPERATION_COLUMN`, `LakehouseExecutionContext`,
  `CommitAuthority`, `OptionLayer`, `PhysicalSinkMode`, `TableFormatRegistry`,
  `resolve_lakehouse_table_context` in `sail-plan`.
- `sail-iceberg` core: `IcebergTableProvider`/`IcebergTableSource`, `IcebergScanByDataFilesExec`
  (without the file-path column), `IcebergCommitExec` (append-oriented), `SnapshotProducer`,
  `StoreContext`, `find_latest_metadata_file`, `metadata_location_from_options`,
  `catalog_managed_iceberg_from_options`, `split_iceberg_write_options_and_table_properties`
  (verify), `IcebergWriteOptions`/`IcebergWriterExecOptions` (without the new fields).
- 0.7.1's own iceberg DELETE/MERGE delete-writer stack (see decision §3.1).
- Config loading (`AppConfig`, `application.yaml` template mechanism) and the object-store
  registry entry points (extended, not replaced).

---

## 5. Coverage note

This gap analysis was produced by targeted greps on the 0.7.1 tree; where a symbol was not
found (`CALL`, metadata tables, procedures, `SessionConfigFactory`, multiplexer, LOAD), absence
is asserted only for the searched identifiers. Any area you plan to re-base should be verified
file-by-file against the live 0.7.1 tree at port time.
