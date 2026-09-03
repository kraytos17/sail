# Porting feat/0.7.0 → feat/0.7.1 — Doc 02: Multiplexed Spark Connect Server & Combined CLI Server

> Part of the `docs/dev/port-v0.7.0/` inventory. Documents the **session-multiplexing
> Spark Connect front end** (`multiplexer.rs`) and the **single-process combined server**
> (`sail-cli` combo) introduced on `feat/0.7.0` (commits `b984b8bc` WIP session fixing,
> `46b73948` revert hacky session ID fix + add multiplexed spark connect server,
> `11d729f1` some more fixes), relative to base `f0b137d6`.
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`.

---

## 1. Scope

| File | Change |
|---|---|
| `sail-spark-connect/src/multiplexer.rs` | NEW (~726 LOC) — `MultiplexedSparkConnectServer` |
| `sail-spark-connect/src/lib.rs` | `pub mod multiplexer;`, `pub use crate::error::SparkError;`, `pub use crate::session_manager::create_spark_session_manager;` |
| `sail-spark-connect/src/entrypoint.rs` | serve() passes `server.http2_keepalive_timeout_secs` via `ServerBuilderOptions` |
| `sail-spark-connect/src/service/plan_executor.rs` | clearer "operation not found" error on `handle_reattach_execute` (mentions previous session/restart/released) |
| `sail-cli/src/combo.rs` | NEW (~192 LOC) — `run_combo_server` |
| `sail-cli/src/runner.rs` | new `Server` subcommand wiring `run_combo_server` |
| `sail-cli/src/lib.rs` | `mod combo;` |
| `sail-cli/Cargo.toml` | +`sail-server`, +`arrow-flight`, +`tonic` deps |
| `sail-flight/src/service.rs` | `SailFlightSqlService::with_default_session()` + `default_session_id` field |
| `sail-spark-connect/src/server.rs` | *not* in the net delta (modified then reverted across commits) — context only |

Dependencies: `SessionManager::session_idle_duration` and the `ActivityTracker` semantics
(doc 01 §5); the server keepalive default (doc 01 §7); `ServerBuilder` from `sail-server`.

---

## 2. Why: the problem being solved

Sail's worker fleet is scoped per `session_id` within a `SessionManager`. Before this work,
each front-end protocol (Spark Connect, Flight SQL) spun up its own session manager /
factory, so a Spark Connect client and a Flight SQL client could never share one driver +
worker fleet even if they wanted to. Additionally, every Spark Connect client defaulted to
its own session, so `N` clients ⇒ `N` fleets ⇒ `N×` memory/workers.

The branch solution has two layers:

1. **One session manager, both protocols** — the combined server builds a *single*
   `SessionManager` and registers both the Spark Connect service and the Flight SQL service
   on it, so a shared `session_id` maps to one driver + worker fleet.
2. **A multiplexing Spark Connect front end** — clients keep their own `session_id`, but the
   server internally stamps every request with **one canonical session id**, materializing a
   single backend fleet for all multiplexed clients. Responses echo each client's own id back
   so clients remain fully protocol-compliant.

Flight SQL has no per-client sessions (one `SailFlightSqlService` instance already shares a
single session across all its clients); `with_default_session` merely chooses **which**
session id it uses — normally `flight-default`, but the combined server injects the
multiplexer's canonical id so both protocols share one fleet.

---

## 3. `multiplexer.rs` — design

Module doc (verbatim intent):

> Every client keeps sending its own `session_id`, and every response echoes that same
> client-supplied id back, keeping clients fully protocol-compliant. On the backend side, all
> requests are stamped with ONE canonical session id, so the `SessionManager` materializes a
> single driver + worker fleet that every multiplexed client shares.
>
> The canonical session is never created explicitly: it comes into existence lazily through
> the ordinary `get_or_create_session_context(canonical, ...)` path on the first stamped
> request, and self-heals the same way after a server restart or idle eviction.
>
> Delegation happens in-process: requests are decoded once at the edge, the session id field
> is swapped, and the already-decoded message is handed to the wrapped `SparkConnectServer`.
> No extra network hop, no re-encoding.
>
> Per-RPC reverse mapping is stateless: the client id captured before the outbound swap is
> restored on each inbound response/stream item. The `ClientRegistry` only tracks last-seen
> timestamps for observability and `release_session` bookkeeping; it never routes anything.
>
> `server_side_session_id` is deliberately left untouched (= the canonical id): it truthfully
> identifies the shared backend session and its stability satisfies the client-side idempotency
> checks.
>
> Note: `add_artifacts` cannot delegate through the `SparkConnectService` trait because tonic
> provides no public way to rebuild a `Streaming<T>` request stream. Its handler body is
> therefore replicated here against the crate-internal `service::handle_add_artifacts` (keep
> in sync with `server.rs`).

### 3.1 Constants

| Constant | Value | Meaning |
|---|---|---|
| `CLIENT_REGISTRY_TTL` | 3600 s | how long a client id may stay unseen before registry eviction (bounds registry memory only) |
| `CLIENT_REGISTRY_MAX_ENTRIES` | 1024 | hard cap on tracked client ids; crossing it first sweeps stale entries, then evicts the least-recently-seen |
| `CANONICAL_SESSION_RELEASE_GRACE` | 30 s | wait after the last live client releases before tearing down the canonical session (lets a quickly reconnecting client cancel teardown) |
| `CANONICAL_SESSION_ACTIVITY_HOLD_WINDOW` | 60 s | if the canonical session was used this recently (per its `ActivityTracker`) teardown holds off — covers other protocols sharing the fleet |

### 3.2 `ClientRegistry` (private)

`HashMap<String, Instant>` of client-id → last-seen, plus `HashSet<String> released`.

- `touch(&mut self, client_id)` — enforces the 1024 cap (sweep, then evict the min-by-`last_seen`
  entry if still full), refreshes the timestamp, and clears any `released` mark (a reconnecting
  client becomes live again).
- `sweep(&mut self, now)` — drop entries older than TTL.
- `release_client(&self?, ...) -> bool` — removes the entry, inserts into `released`, returns
  whether an entry existed.
- `has_live_clients(&self) -> bool` — any tracked client not in `released`. **Conservative**:
  entries whose connection dropped without a release still count as live — never tear the
  canonical session out from under a client the registry cannot prove is gone.

### 3.3 Session-id stamping

- `stamp_session_id(registry, canonical, field) -> String` — takes the client id out of the
  field (`std::mem::take`), `touch`es it, writes the canonical id into the field, returns the
  client id.
- `resolve_canonical_session_id(Option<String>) -> String` (pub) — trims/blanks → fallback to
  `Uuid::new_v4()`. Public so embedders (combo server) resolve once and hand the SAME value to
  both the multiplexer and Flight SQL.
- `validate_observed_server_side_session_id(canonical, Option<&str>) -> Result<(), Status>` —
  rejects (Status::failed_precondition "session no longer valid; create a new session") any
  *non-empty* observed server session that differs from canonical; `None`/empty accepted. This
  is how a stale client (reconnected after a restart minted a fresh canonical id) is detected.

### 3.4 `MultiplexedSparkConnectServer`

```rust
pub struct MultiplexedSparkConnectServer {
    inner: SparkConnectServer,
    session_manager: SessionManager,
    canonical_session_id: String,
    registry: Arc<Mutex<ClientRegistry>>,
}
impl MultiplexedSparkConnectServer {
    pub fn new(session_manager: SessionManager, canonical_session_id: Option<String>) -> Self;
    pub fn canonical_session_id(&self) -> &str;
    fn stamp(&self, session_id_field: &mut String,
             observed_server_side_session_id: Option<&str>) -> Result<String, Status>;
}
```

- `new` resolves the canonical id (explicit or UUID); wraps `SparkConnectServer::new(session_manager.clone())`.
- `stamp` = `validate_observed_server_side_session_id` then registry swap. On a poisoned
  registry lock it falls back to `std::mem::replace` — bookkeeping is best-effort, never fail a
  request over a poisoned lock. The observed-session validation is applied on every
  request-bearing RPC except `config` and `release_session` (see below).

### 3.5 Per-RPC behavior (all `SparkConnectService` methods)

For **request/response RPCs** the pattern is identical:

1. `stamp(&mut request.session_id, request.client_observed_server_side_session_id.as_deref())`
   → client_id (rejects stale clients with `failed_precondition`).
2. Delegate to `self.inner.<method>(Request::new(request))`.
3. Restore `response.session_id = client_id` before returning.

RPCs handled this way: `analyze_plan`, `artifact_status`, `interrupt`, `release_execute`,
`fetch_error_details`, `clone_session`, `get_status`.

**Streaming RPCs** (`execute_plan`, `reattach_execute`):
stamp request, delegate, then wrap the upstream stream with `rewrite_execute_stream` which
sets `item.session_id = client_id` on every `ExecutePlanResponse` item.

```rust
fn rewrite_execute_stream(upstream: ExecutePlanResponseStream, client_id: String)
    -> RewrittenExecutePlanStream  // Pin<Box<dyn Stream<Item=Result<ExecutePlanResponse,Status>> + Send>>
```
(implemented with `async_stream::try_stream!`).

**`config`** (session-(re)establishment path): a divergent observed session must NOT kill the
client's session, so instead of rejecting, it logs a `warn!` and proceeds — the response's
`server_side_session_id` (left at the canonical id) re-syncs the client. Then normal stamp +
delegate + restore response.session_id.

**`add_artifacts`** (client-streaming): cannot tunnel through the trait (tonic can't rebuild a
`Streaming<T>`), so the body is replicated against `service::handle_add_artifacts(&ctx, stream)`:
1. pull the first request (error if none);
2. stamp it (using `client_observed_server_side_session_id`);
3. derive `user_id` from its `user_context`;
4. materialize the canonical session context directly:
   `session_manager.get_or_create_session_context(canonical.clone(), user_id)` (SparkError::from on failure);
5. rebuild a `stream` that yields the first payload then consumes the rest, **enforcing
   `item.session_id == stream_client_id`** (parity with `server.rs`'s consistency check) while
   rewriting each to the canonical id;
6. respond with `AddArtifactsResponse { session_id: client_id, server_side_session_id: canonical, artifacts }`.

**`release_session`** (intercepted, does NOT delegate):
- `allow_reconnect = true` ⇒ `SparkError::unsupported("reconnect session")` (the multiplexer
  cannot support per-client reconnect semantics on a shared backend).
- stamp (no observed-session field on this request type); then `registry.release_client(&client_id)`.
- If no live clients remain, spawn a background task that sleeps
  `CANONICAL_SESSION_RELEASE_GRACE` (30 s), then re-checks `has_live_clients()` under the lock
  (reconnected client cancels), then calls
  `session_manager.session_idle_duration(canonical)`:
  - `Ok(None)` → session already gone; return (skip noisy delete).
  - `Ok(Some(idle)) if idle < 60 s` → in use (possibly by Flight SQL); hold off.
  - `Ok(Some(_))` → `session_manager.delete_session(canonical)` (warn on error).
  - `Err(_)` → never tear down on uncertainty.
- Response: `ReleaseSessionResponse { session_id: client_id, server_side_session_id: canonical }`.

### 3.6 Unit tests (in-module)

Registry: `registry_touch_and_release`, `registry_sweep_evicts_only_stale_entries`,
`registry_cap_bounds_memory_under_fresh_id_flood`,
`registry_retouch_at_capacity_does_not_evict`, `release_client_marks_released_and_removes_entry`,
`released_client_reconnecting_becomes_live_again`, `live_clients_are_not_detected_after_release`.
Stamping/resolution/validation: `stamp_swaps_field_and_returns_original`,
`resolve_canonical_generates_uuid_when_absent`, `resolve_canonical_rejects_blank_input`,
`resolve_canonical_honors_explicit_value`, `observed_session_matching_canonical_is_accepted`,
`absent_or_blank_observed_session_is_accepted`, `divergent_observed_session_is_rejected`.

---

## 4. Combined server (`sail-cli`)

### 4.1 `combo.rs` — `run_combo_server(ip, spark_port, flight_port, mux_port, canonical_session_id)`

Single long-running process serving Spark Connect **and** Flight SQL off ONE shared session
manager (docstring: worker fleet scoped per session id within a `SessionManager`; both
protocols against the same manager ⇒ a Spark Connect client and a Flight SQL client pinning
the same `session_id` reuse the *same* driver + worker set).

Flow:

1. `AppConfig::load()`, `RuntimeManager::try_new(&config.runtime)`, `init_telemetry`.
2. `http2_keepalive_timeout = Duration::from_secs(config.server.http2_keepalive_timeout_secs)`.
3. One session manager: `create_spark_session_manager(config.clone(), runtime_manager.handle().clone())`.
4. Bind three `TcpListener`s (`spark_port` 50051 default, `flight_port` 32010 default, `mux_port` 0 = disabled).
5. Spark Connect service (standard `SparkConnectServer`) with gzip+zstd accept/send compression.
6. **Canonical session resolved once**:
   `resolve_canonical_session_id(canonical_session_id.or_else(|| config.server.session_id.clone()))`
   — precedence: explicit CLI flag > `server.session_id` config > random UUID.
7. Flight SQL service via `SailFlightSqlService::with_default_session(session_manager, (mux_port>0).then(|| canonical.clone()))`.
8. If `mux_port > 0`: bind a `MultiplexedSparkConnectServer` on it with the same canonical id.
9. All three services run under `ServerBuilder` (names `sail_spark_connect_mux` /
   `sail_spark_connect` / `flight_sql`), each with the keepalive timeout; spark-connect
   services register `FILE_DESCRIPTOR_SET`.
10. `tokio::join!` the three serve futures, then `session_manager.shutdown()`, then
    `shutdown_telemetry()`. Any serve error propagates.

Session-manager note (docstring): each protocol normally builds its own manager with its own
`ServerSessionFactory` (`SessionMutator`) and session timeout, but worker-pool allocation is
driven by the cluster config (not the mutator), so the pool is identical either way — the
combined server builds ONE manager and wires both services to it.

### 4.2 `runner.rs` — `Server` subcommand

`Command::Server` with flags `--ip` (127.0.0.1), `--spark-port` (50051), `--flight-port`
(32010), `--mux-port` (0 → disabled), `--canonical-session-id` (optional). Dispatches to
`run_combo_server`.

### 4.3 `sail-cli/Cargo.toml`

Adds `sail-server`, `arrow-flight`, `tonic`; reorders `tokio`.

### 4.4 `lib.rs` — `pub use` of `SparkError` + `create_spark_session_manager`

Exports the shared error type and the session-manager constructor from the crate root so the
CLI (and embedders) can build/wire services without reaching into private modules.

---

## 5. `SailFlightSqlService::with_default_session` (`sail-flight/src/service.rs`)

- New field `default_session_id: String`.
- `new(session_manager)` ⇒ `with_default_session(session_manager, None)`.
- `with_default_session(session_manager, default_session_id: Option<String>)` — all Flight SQL
  clients of one service instance share a single session; this picks WHICH one:
  `default_session_id.filter(|id| !id.trim().is_empty()).unwrap_or(Self::DEFAULT_SESSION_ID)`
  where `DEFAULT_SESSION_ID = "flight-default"`, `DEFAULT_USER_ID = "flight-user"`.
  The combined server injects the multiplexer canonical session here.
- `get_session_context` now uses `self.default_session_id` instead of the constant.

---

## 6. `plan_executor.rs` reattach error message

`handle_reattach_execute`: "operation not found: {operation_id}" now appends
"; the operation may belong to a previous session (e.g. the server restarted) or may have been
released; re-run the query" — improving client diagnosability under multiplexed canonical
sessions that self-heal across restarts.

---

## 7. Server & entrypoint changes

`sail-spark-connect/src/entrypoint.rs` serve() passes the configured HTTP/2 keepalive timeout
through `ServerBuilderOptions { http2_keepalive_timeout: Some(...), ..Default::default() }`
(as the flight entrypoint does — doc 01 §7). Worker RPC server same treatment in doc 03 §8.

---

## 8. Port notes / risks

1. **File is self-contained** but imports `service::handle_add_artifacts` (must exist and stay
   in sync with `server.rs`) and `SessionManager`/`SparkConnectServer`. The `SparkConnectService`
   trait method set is the same on 0.7.1 (verify field names:
   `client_observed_server_side_session_id`, `allow_reconnect`, `server_side_session_id`).
2. `SparkError::unsupported` and `SparkError` re-export must exist.
3. `sail-cli` dependency additions (sail-server, tonic, arrow-flight) are additive.
4. **Canonical teardown only triggers on `release_session`** with no live clients AND depends
   on `session_idle_duration` (doc 01 §5). If 0.7.1 lacks the `ActivityTracker` extension or
   doesn't refresh it on `get_or_create` for every protocol, idle detection silently degrades
   to never-tear-down (safe direction).
5. Combo-server story ties Flight SQL to the canonical session only when `mux_port > 0`; the
   `server.session_id` YAML knob makes the canonical id stable across pod restarts (config key
   documented experimental).
