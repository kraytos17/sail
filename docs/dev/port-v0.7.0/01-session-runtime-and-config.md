# Porting feat/0.7.0 → feat/0.7.1 — Doc 01: Session, Runtime & Configuration

> Part of the `docs/dev/port-v0.7.0/` inventory. This file documents everything the
> `feat/0.7.0` branch implemented in the **session manager**, the **runtime environment**,
> and the **application configuration** surface, relative to the shared v0.7.0 base
> `f0b137d6`. It is the port blueprint for re-implementing this code on `feat/0.7.1`.
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`. Files are listed with their full path under
> `crates/` unless noted.

---

## 1. Scope of this document

Files covered (net delta `f0b137d6..feat/0.7.0`):

| File | Change |
|---|---|
| `sail-common/src/config/application.rs` | new `ServerConfig`, `ObjectStoreConfig`, `IcebergRestAccessDelegation`; `AppConfig.server`/`object_store` fields; `access_delegation` on REST catalog config |
| `sail-common/src/config/application.yaml` | new config keys: `server.http2_keepalive_timeout_secs`, `server.session_id`, `object_store.*` (6 keys); tweaks to `cluster.task_stream_creation_timeout_secs` (60→120) and `cluster.task_max_attempts` (3→5) |
| `sail-common/tests/server_config.rs` | NEW — `ServerConfig` deserialization unit tests |
| `sail-session/src/session_config.rs` | NEW — `SessionConfigFactory` extracted from `ServerSessionFactory`, now shared by server **and** worker session factories |
| `sail-session/src/session_factory/server.rs` | inlined `apply_execution_config`/`apply_optimizer_config`/`apply_execution_parquet_config` removed; delegates to `SessionConfigFactory` |
| `sail-session/src/session_factory/worker.rs` | worker session now applies the SAME execution/parquet/optimizer config as the driver via `SessionConfigFactory` |
| `sail-session/src/session_manager/event.rs` | new `SessionIdleDuration` actor event |
| `sail-session/src/session_manager/mod.rs` | new public `SessionManager::session_idle_duration()` RPC |
| `sail-session/src/session_manager/actor/core.rs` | dispatch for `SessionIdleDuration` |
| `sail-session/src/session_manager/actor/handler.rs` | `handle_session_idle_duration()`; clone `session_id` before moving into inserts in `handle_get_or_create_session` |
| `sail-session/src/lib.rs` | `pub mod session_config;` |
| `sail-session/src/catalog.rs` | REST catalog now threads `access_delegation` into `IcebergRestCatalogOptions` |
| `sail-session/src/runtime.rs` | `RuntimeEnvFactory` builds the object-store registry with `new_with_config(...object_store)` |
| `sail-flight/src/entrypoint.rs` | flight server passes `http2_keepalive_timeout` into `ServerBuilderOptions` |
| `sail-server/src/builder.rs` | default `ServerBuilderOptions.http2_keepalive_timeout` 10s → **120s** |

Closely related and documented in their own files: `sail-cli` combo server (`combo.rs`,
`runner.rs`, `Cargo.toml`) and `SailFlightSqlService::with_default_session` are in
`02-spark-connect-multiplexer.md`; the spark-connect/worker keepalive plumbing and RPC
peer tagging are in `03-distributed-execution-worker-pool.md`; the object-store registry
internals are in `04-object-store-registry.md`.

---

## 2. New configuration structures (`sail-common`)

### 2.1 `ServerConfig` (`application.rs`)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub http2_keepalive_timeout_secs: u64,
    #[serde(default,
            serialize_with = "serialize_non_empty_string",
            deserialize_with = "deserialize_non_empty_string")]
    pub session_id: Option<String>,
}
```

- `http2_keepalive_timeout_secs` — how long a gRPC server waits for a peer to ack an
  HTTP/2 keepalive ping before closing the connection. Config key default **120**.
  Motivation (from `application.yaml` description): a too-short timeout makes the server
  drop temporarily-stalled peers (e.g. busy workers), surfacing as
  `h2 protocol error: error reading a body from connection` client-side.
- `session_id` — **stable canonical session id** for the Spark Connect session
  multiplexer. When non-empty, every multiplexed client is routed to this session id and
  it survives pod restarts. When empty, a random UUID is minted per process. Deserialized
  via `deserialize_non_empty_string` so `""` becomes `None` (the unit tests pin this).

Consumers: every `ServerBuilder`-based server (spark-connect, flight, worker, combo), the
multiplexer canonical-id resolution, `WorkerOptions`.

### 2.2 `ObjectStoreConfig` (`application.rs`)

```rust
pub struct ObjectStoreConfig {
    pub connect_timeout_secs: u64,        // default 5
    pub request_timeout_secs: u64,        // default 30
    pub pool_idle_timeout_secs: u64,      // default 90
    pub pool_max_idle_per_host: usize,    // default 0 (unlimited)
    pub http2_keep_alive_interval_secs: u64, // default 0 (disabled)
    pub http2_keep_alive_timeout_secs: u64,  // default 0 (disabled)
}
```

`serde(deny_unknown_fields)`; `Default` impl provided. Only applied to **S3** stores today
(all six keys documented as "S3 only" in `application.yaml`). Wired through
`RuntimeEnvFactory` → `DynamicObjectStoreRegistry::new_with_config` → S3 builder/region
resolution (see doc 04). `0` doubles as "unlimited"/"disabled" sentinel to mirror the
`object_store` crate defaults.

### 2.3 `IcebergRestAccessDelegation`

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergRestAccessDelegation {
    #[default]
    VendedCredentials,
    None,
}
```

New optional field on the `CatalogType::IcebergRest` variant:
`access_delegation: Option<IcebergRestAccessDelegation>`
(`skip_serializing_if = "Option::is_none"`). Threaded through
`sail-session/src/catalog.rs` into
`IcebergRestCatalogOptions { credentials, properties, access_delegation }`
(`access_delegation.unwrap_or_default()`), i.e. the catalog behavior defaults to
**VendedCredentials** unless the user opts into `None`. (Related REST-catalog changes live
in the catalog/doc-06 cluster; this is the config-wiring portion.)

### 2.4 `AppConfig`

Gains two non-optional members, `server: ServerConfig` and
`object_store: ObjectStoreConfig`, which forces the new YAML keys to exist in
`application.yaml` (AppConfig loads from that default file).

### 2.5 Cluster defaults tweaked (`application.yaml`)

- `cluster.task_stream_creation_timeout_secs`: 60 → **120** (grace period for a task
  launched simultaneously with its dependency tasks).
- `cluster.task_max_attempts`: 3 → **5**.

### 2.6 New YAML keys registered

`server.http2_keepalive_timeout_secs` (number, 120), `server.session_id` (string, empty,
experimental), `object_store.connect_timeout_secs` (5), `object_store.request_timeout_secs`
(30), `object_store.pool_idle_timeout_secs` (90), `object_store.pool_max_idle_per_host`
(number, 0), `object_store.http2_keep_alive_interval_secs` (0),
`object_store.http2_keep_alive_timeout_secs` (0) — all `object_store.*` experimental.

### 2.7 Tests: `sail-common/tests/server_config.rs`

Three integration tests: `test_server_config_session_id_deserializes` (present → kept),
`test_server_config_empty_session_id_is_none` (`""` → `None`),
`test_server_config_missing_session_id_is_none`.

---

## 3. `SessionConfigFactory` — unified driver/worker session settings

`crates/sail-session/src/session_config.rs` (NEW, 228 LOC + 3 unit tests) extracts the
per-session DataFusion option massaging that previously lived inline in
`ServerSessionFactory` so the **worker** session factory can apply identical settings.

Public surface:

```rust
pub struct SessionConfigFactory { config: Arc<AppConfig> }
impl SessionConfigFactory {
    pub fn new(config: Arc<AppConfig>) -> Self;
    pub fn apply_execution_config(&self, config: &mut SessionConfig);
    pub fn apply_optimizer_config(&self, config: &mut SessionConfig);
    pub fn apply_execution_parquet_config(&self, config: &mut SessionConfig);
}
```

### 3.1 `apply_execution_config`

- `batch_size = config.execution.batch_size`
- `target_partitions = default_parallelism` **iff** `default_parallelism > 0`
- `collect_statistics`, `use_row_number_estimates_to_optimize_partitioning` copied
- `listing_table_ignore_subdirectory = false`
- **`enable_file_stream_work_stealing = false`** — new, and behavior-critical: Sail runs
  each partition as an independent task that decodes its own physical plan instance.
  DataFusion's sibling file-stream work stealing shares ONE file queue across partitions,
  which is only correct when those partitions run in one process on one plan instance.
  With per-task plan decoding every partition would drain the whole shared queue and
  re-read the entire source `target_partitions` times (Nx rows) for any byte-range split
  scan (e.g. the LOAD DATA fallback path). The comment stresses this must stay identical
  on driver and worker sessions.

### 3.2 `apply_execution_parquet_config`

Maps every `AppConfig.parquet.*` knob onto `config.options_mut().execution.parquet`, plus
hard-coded `created_by = "sail version {CARGO_PKG_VERSION}"` and `coerce_int96 = "us"`.
Fields: enable_page_index, pruning, skip_metadata, metadata_size_hint, pushdown_filters,
reorder_filters, schema_force_view_types, binary_as_string, max_predicate_cache_size,
data_pagesize_limit, write_batch_size, writer_version (parsed via
`DFParquetWriterVersion::from_str(..).unwrap_or_default()`), skip_arrow_metadata,
compression, dictionary_enabled, dictionary_page_size_limit, statistics_enabled,
max_row_group_size, column_index_truncate_length, statistics_truncate_length,
data_page_row_count_limit, encoding, bloom_filter_on_read/write, bloom_filter_fpp/ndv,
allow_single_file_parallelism, maximum_parallel_row_group_writers,
maximum_buffered_record_batches_per_stream, content_defined_chunking
{enabled,min_chunk_size,max_chunk_size,norm_level}.

### 3.3 `apply_optimizer_config`

`expand_views_at_output` copied from `AppConfig.optimizer`.

### 3.4 Unit tests

- `execution_config_matches_application_config`
- `parquet_config_matches_application_config`
- `optimizer_config_matches_application_config`

All three load the real `application.yaml` and assert the resulting `SessionConfig`
mirrors `AppConfig`.

---

## 4. Server / worker session factories

### 4.1 `server.rs`

`ServerSessionFactory` gains `session_config: SessionConfigFactory` (constructed in
`new`). In `create_session`, the three private `apply_*` methods are replaced by
`self.session_config.apply_execution_config/apply_execution_parquet_config/apply_optimizer_config`
**before** `self.mutator.mutate_config(config, info)` runs. The mutator therefore remains
an intentional server-only extension point that can still override driver-side settings
after this factory (that override is explicitly **not** applied on workers).

### 4.2 `worker.rs`

`WorkerSessionFactory` gains `session_config: SessionConfigFactory`. Its `create(())`
(previously `SessionConfig::default()` + two extensions) now builds
`SessionConfig::default()` + the `DeltaTableCache` and `RepartitionBufferConfig`
extensions, then runs all three `apply_*` methods. Rationale (in-code): workers decode
and execute serialized physical plans with this session config, so worker sessions must
carry the same execution/parquet/optimizer settings as the driver session — including the
file-stream work-stealing disable, for the Nx re-read reason above.

New unit test `worker_session_mirrors_server_execution_config` asserts the worker session's
execution options (target_partitions, batch_size, collect_statistics, work-stealing flag)
and parquet options (binary_as_string) match `AppConfig`.

### 4.3 `RuntimeEnvFactory` (`runtime.rs`)

`create_runtime_env` (closure building `RuntimeEnvBuilder`) now constructs the object
store registry via

```rust
DynamicObjectStoreRegistry::new_with_config(self.runtime.clone(), self.config.object_store.clone())
```

instead of `DynamicObjectStoreRegistry::new(...)`, so all S3 client tuning keys apply.

---

## 5. Session idle-duration introspection (actor plumbing)

Purpose: let the spark-connect session multiplexer (doc 02) decide when it is safe to tear
down the shared canonical backend session, deferring to activity produced by **any**
protocol (Spark Connect and Flight SQL both touch the session through `get_or_create`,
which refreshes the session's `ActivityTracker`).

New pieces:

- `SessionManagerEvent::SessionIdleDuration { session_id: String, result: oneshot::Sender<SessionResult<Option<Duration>>> }`
  — added to the enum in `session_manager/event.rs`, to the `SpanAssociation` name match,
  and to the arm that derives the span context from `session_id`.
- `SessionManagerActor::handle_session_idle_duration(...)` (in `actor/handler.rs`):
  looks up `self.sessions.get(&session_id)`; only a session in
  `ServerSessionState::Running { context, .. }` yields a value —
  `context.extension::<ActivityTracker>()` → `Ok(tracker.active_at().ok().map(|at| at.elapsed()))`.
  Any other state, a missing extension, or a missing session returns `Ok(None)`
  ("unknown — callers should avoid destructive actions").
- Dispatch arm in `actor/core.rs` `handle()` → `SessionManagerEvent::SessionIdleDuration`.
- `SessionManager::session_idle_duration(&self, session_id: String) -> SessionResult<Option<Duration>>`
  (in `session_manager/mod.rs`) — async RPC that sends the event over the actor handle and
  awaits the oneshot, wrapping failures in `SessionError::internal`.

`ActiveTracker::active_at()` is in `sail-common-datafusion` (unchanged on this branch);
the `.elapsed()` yields the idle duration since the last activity. Note the activity
extension lookup requires the session's `context`; sessions created but never handed a
context (failed/other states) report `None`.

### 5.1 Related behavior change in `handler.rs`

Three `self.sessions.insert(session_id, session)` sites inside
`handle_get_or_create_session` were changed to `self.sessions.insert(session_id.clone(), session)`
(so `session_id` remains usable in the spawned async shutdown/registration tasks that
follow). Pure ownership fix, no semantic change.

---

## 6. Catalog wiring (`session/catalog.rs`)

`create_catalog_manager`'s `IcebergRest { oauth_access_token, bearer_access_token,
bearer_access_token_file, access_delegation, cache }` arm now builds the REST provider with

```rust
IcebergRestCatalogOptions {
    credentials,
    properties,
    access_delegation: access_delegation.clone().unwrap_or_default(),
}
```

(the `IcebergRestAccessDelegation` config key from §2.3). This is the seam that turns the
config knob into catalog behavior (vended-credentials vs. none) implemented in
`sail-catalog-iceberg`/REST options.

---

## 7. HTTP/2 keepalive on servers (`sail-server` + flight/spark-connect entrypoints)

- `sail-server/src/builder.rs`: default `ServerBuilderOptions.http2_keepalive_timeout`
  changed from `Some(Duration::from_secs(10))` to `Some(Duration::from_secs(120))`.
- `sail-flight/src/entrypoint.rs`: reads `config.server.http2_keepalive_timeout_secs` and
  passes it via `ServerBuilderOptions { http2_keepalive_timeout: Some(...), ..Default::default() }`.
- Spark-connect `entrypoint.rs` and the worker gRPC server do the same (see docs 02/03).

---

## 8. Port notes / risks

1. **Behavioral defaults that 0.7.1 must adopt**: `task_stream_creation_timeout_secs`
   60→120 and `task_max_attempts` 3→5 are *default changes* (in `application.yaml`), not
   feature code. Decide whether the port intentionally carries them.
2. **`enable_file_stream_work_stealing = false` is load-bearing** for correctness of
   byte-range split scans under per-task plan decoding. It must land on BOTH factory
   paths. If 0.7.1's worker-side decode path differs (e.g. it still rewrites file scans at
   task-run time rather than at job-graph build time — see doc 03 §3.2/§4), verify whether
   the flag is redundant or required there.
3. **`ServerConfig`/`ObjectStoreConfig` are non-optional members of `AppConfig`**; the
   `application.yaml` template must carry every new key or `AppConfig::load()` fails.
4. **`SessionIdleDuration` depends on `ActivityTracker` being installed** as a context
   extension on server sessions (already true upstream). The multiplexer (doc 02) relies
   on it; without it the teardown path always sees `None` and never destroys the session.
5. Unit-test drift: `sail-common/tests/server_config.rs`,
   `session_config.rs` tests, and `worker.rs` tests must move with the code.
