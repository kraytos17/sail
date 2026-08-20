# 07 — Distributed Execution: Worker Pool Accounting, Readiness Gate, Spawn Retry, Session Health

> All changes under `sail-execution`, `sail-server`, `sail-session`, `sail-spark-connect`,
> `sail-flight`, and `sail-common` between `f090e646..b8804803`. These make fleet
> provisioning budget-accurate, add a fleet-readiness barrier, back off worker
> re-spawning, keep idle workers from being reaped too early, keep sessions from being
> reaped during long streaming, and make servers self-heal stale sessions.

Files (this doc):
- `crates/sail-execution/src/driver/worker_pool/{mod,core,options}.rs`
- `crates/sail-execution/src/driver/task_assigner/{core,state}.rs`
- `crates/sail-execution/src/driver/actor/{handler,core,rpc,mod}.rs`, `driver/event.rs`, `driver/options.rs`
- `crates/sail-execution/src/worker_manager/{kubernetes,local,mod,options}.rs`
- `crates/sail-execution/src/worker/peer_tracker/options.rs`, `worker/options.rs`
- `crates/sail-execution/src/proto/codec.rs`, `crates/sail-execution/proto/sail/plan/physical.proto`
- `crates/sail-server/src/builder.rs`, `crates/sail-server/src/retry.rs`
- `crates/sail-session/src/session_manager/actor/handler.rs`
- `crates/sail-common-datafusion/src/session/activity.rs` (new)
- `crates/sail-spark-connect/src/executor.rs`, `service/plan_executor.rs`, `entrypoint.rs`
- `crates/sail-flight/src/lib.rs`
- `crates/sail-common/src/config/application.rs` + `application.yaml`

---

## 1. Worker pool — `driver/worker_pool/mod.rs` (+193)

### 1.1 New state

```rust
pub struct WorkerPool {
    ...
    spawn_retry_delays: Option<Box<dyn Iterator<Item = Duration> + Send>>,
    spawn_retry_armed: bool,
}
```

### 1.2 `reserve_worker_ids`

```rust
pub fn reserve_worker_ids(&mut self, n: usize) -> ExecutionResult<Vec<WorkerId>>
```

Reserves `n` fresh consecutive ids from `worker_id_generator`. The caller must mark each
id `Pending` in the task assigner **before** `start_worker_with_id`. Id exhaustion
propagates the error (never silently fewer).

### 1.3 Spawn-retry backoff

```rust
pub fn next_spawn_retry_delay(&mut self) -> Option<Duration>
pub fn has_pending_spawn_retry(&self) -> bool
pub fn fire_spawn_retry(&mut self)
pub fn reset_spawn_retry(&mut self)
```

- `next_spawn_retry_delay` lazily builds the delay iterator from
  `options.spawn_retry_strategy.delay()` (`RetryStrategy::delay` made `pub` in
  `sail-server/src/retry.rs`); advances one delay per failed spawn; **arms** the flag when
  a delay is returned.
- `has_pending_spawn_retry`: while armed, pending tasks must not fail on the scheduling
  timeout (a replacement worker may still register).
- The armed flag is **never cleared on `None`**: when several workers fail close together,
  the first failure arms a retry while later ones may exhaust the iterator and get `None`;
  clearing would make pending tasks stop waiting while a retry is still in flight. Cleared
  by `fire_spawn_retry` (the delayed event fired) or `reset_spawn_retry` (a worker
  registered successfully → fresh cycle).
- Tests: `reserve_worker_ids_returns_unique_consecutive_ids`,
  `reserve_worker_ids_zero_returns_empty`, `spawn_retry_delay_advances_then_exhausts`,
  `concurrent_failures_do_not_clear_an_armed_retry`, `spawn_retry_resets_after_fire`,
  `spawn_retry_resets_after_registration`, `spawn_retry_with_no_attempts_returns_none`.

### 1.4 `worker_pool/core.rs`

- `start_worker(ctx)` → **`start_worker_with_id(ctx, worker_id)`** (id passed in; the
  caller already reserved + charged it `Pending`).
- `WorkerLaunchOptions` gains `http2_keepalive_interval` / `http2_keepalive_timeout` from
  `WorkerPoolOptions`.
- `register_worker` (Pending→Running): on success calls `reset_spawn_retry()`.
- `worker_pool/options.rs` — `WorkerPoolOptions` gains `http2_keepalive_interval`,
  `http2_keepalive_timeout`, `spawn_retry_strategy: RetryStrategy`, `runtime:
  RuntimeHandle`, plus `for_test()` / `for_test_with_spawn_retry()` builders.

---

## 2. Task assigner — `driver/task_assigner/core.rs` (+376)

### 2.1 Derived counts (never separate counters)

```rust
pub fn pending_worker_count(&self) -> usize      // WorkerResource::Pending
fn active_worker_count(&self) -> usize           // WorkerResource::Active { .. }
fn total_live_worker_count(&self) -> usize       // pending + active
```

### 2.2 `request_workers` — head-of-queue demand

Old behavior scaled on the slot deficit only. New behavior:

- `head_demand` = the max, over all queued regions, of the number of `Worker`-placed task
  sets in the region — provision so the widest region can spread across workers even when
  existing workers have vacant slots.
- `max_count` = `worker_max_count` (`0` ⇒ unbounded).
- `head_workers = max_count.min(head_demand).saturating_sub(total_live_worker_count())`.
- `deficit_workers = required_slots.div_ceil(worker_task_slots)` (kept for
  many-small-regions scaling).
- return `head_workers.max(deficit_workers).min(allowed_workers)` where
  `allowed_workers = max_count - total_live_worker_count()`.

### 2.3 `request_initial_workers`

```rust
pub fn request_initial_workers(&mut self) -> usize
```

Pre-warm count = `worker_initial_count` (capped by remaining budget vs `worker_max_count`),
ignoring the task queue (empty at startup).

### 2.4 Worker transitions

```rust
pub fn add_pending_worker(&mut self, worker_id)     // insert Pending (warn if already tracked)
pub fn track_worker_failed_to_start(&mut self, worker_id)  // shift_remove; releases budget
pub fn activate_worker(&mut self, worker_id)        // Pending -> Active{slots, local_streams}; idempotent
pub fn deactivate_worker(&mut self, worker_id)      // shift_remove entirely (not Inactive); releases budget
pub fn is_task_queue_empty(&self) -> bool
```

`deactivate_worker` removes the worker entirely so reaped workers stop consuming the
budget (previously a lingering `Inactive` state).

### 2.5 Slot selection — fairness

Task-set placement now picks **the worker with the most vacant slots**
(`max_by_key(|(_, slots)| slots.len())`), so task sets spread across the fleet instead of
packing onto the first worker; ties alternate naturally.

### 2.6 State-model change — `WorkerResource::Inactive` → `Pending`

`crates/sail-execution/src/driver/task_assigner/state.rs`: the `Inactive` variant was
**repurposed into `Pending`** — a worker whose spawn was requested but which has not yet
registered. It consumes the `worker_max_count` budget (via `total_live_worker_count`)
but has no slots, so it is never assigned tasks or streams. The four
"cannot add/remove tasks / track/untrack local streams" warnings were updated from
`inactive worker` to `pending worker`.

`crates/sail-execution/src/driver/task_assigner/options.rs`: `TaskAssignerOptions` gains
`worker_initial_count: usize` (from `DriverOptions`) plus a `#[cfg(test)]
for_test(initial_count, task_slots, max_count)` constructor.

`crates/sail-execution/src/driver/task_assigner/mod.rs`: the old
`requested_worker_count: usize` field (and its `0` init) was **removed** — worker counts
are now always derived from state, never a separate counter.

### 2.7 Tests

`request_workers_respects_max_count_against_live_workers`,
`request_workers_scales_for_single_wide_region[_respects_live_workers]`,
`request_workers_for_single_region_respects_max_count`,
`task_assigner_spreads_region_across_workers`, `..._across_full_fleet`,
`pending_worker_count_tracks_pending_and_active`, `activate_then_deactivate_releases_budget`,
`initial_workers_are_charged_against_max_count`, `failed_to_start_releases_budget`,
`pending_worker_is_not_idle`, `is_task_queue_empty_reflects_enqueued_regions`,
`activate_is_idempotent_and_does_not_double_count`.

---

## 3. Driver actor — `driver/actor/handler.rs` (+90) + `core.rs`/`event.rs`

### 3.1 Fleet-readiness barrier

`run_tasks` now defers assignment while `task_assigner.pending_worker_count() > 0` —
otherwise the first workers to register grab all queued work while the rest of the fleet
is still launching. The barrier is re-triggered on every worker registration and on
pending-worker failure, so assignment resumes as soon as the fleet is settled.

### 3.2 Spawn helpers

```rust
fn spawn_initial_workers(&mut self, ctx)   // request_initial_workers() -> spawn_workers
fn spawn_workers(&mut self, ctx, count) {
    let ids = self.worker_pool.reserve_worker_ids(count)?;   // error -> Shutdown
    for id in ids {
        self.task_assigner.add_pending_worker(id);
        self.worker_pool.start_worker_with_id(ctx, id);
    }
}
```

Replaces the old `for _ in 0..initial_count { start_worker(ctx) }` loops.

### 3.3 Pending-worker failure (launch timeout)

`handle_probe_pending_worker`:
1. `fail_worker_if_pending` marks the worker `Failed`.
2. `track_worker_failed_to_start(worker_id)` — releases budget.
3. `worker_manager.delete_worker(worker_id).await` — deletes the pod (by name) so a stuck
   `Pending` pod (e.g. `Insufficient cpu`) doesn't linger.
4. Re-runs `run_tasks` (the readiness barrier may now be clear — queued tasks proceed on
   remaining active workers).
5. `if let Some(delay) = self.worker_pool.next_spawn_retry_delay() {
   ctx.send_with_delay(DriverEvent::RetryWorkerSpawn, delay); }` — **backoff**, not an
   immediate re-spawn.

### 3.4 `handle_retry_worker_spawn` (new)

```rust
self.worker_pool.fire_spawn_retry();
self.scale_up_workers(ctx);
```

### 3.5 Idle-worker reap guard

`handle_probe_idle_worker` now reaps only when **all** of:
- `is_task_queue_empty()` (queued work the worker could take),
- `!worker_pool.has_pending_workers()` (pool not mid-scale-up — a pending worker may still
  register and use this worker's vacant slots),
- `is_worker_idle(worker_id)` (slots vacant, no local streams, not touched since probe).

### 3.6 Scheduling-timeout guard

`handle_probe_pending_task` keeps waiting while
`has_pending_workers() || has_pending_spawn_retry()` (reschedules after
`min(worker_launch_timeout, task_launch_timeout)`); otherwise fails the task with
`"task scheduling timeout"`.

### 3.7 Wiring

- `driver/actor/core.rs` — `DriverActor::start`/`new` plumbing for the new options/events.
- `driver/event.rs` — new `DriverEvent::RetryWorkerSpawn`.
- `driver/options.rs` — `http2_keepalive_*`, `worker_spawn_retry_strategy` flows into
  `WorkerPoolOptions`.

---

## 4. Worker managers

### 4.1 Kubernetes — `worker_manager/kubernetes.rs` (+56)

- `worker_pod_name(id)` helper = `{prefix}{name}-{id}` — shared by **launch** and the new
  **`delete_worker(id)`** (delete by the same deterministic name; previously only `stop`
  deleted pods). This lets a reaped/failed-pending worker be deleted individually.
- `build_pod_env` emits `HTTP2_KEEPALIVE_INTERVAL_SECS` / `HTTP2_KEEPALIVE_TIMEOUT_SECS`
  env vars (`ClusterConfigEnv`).
- `KubernetesWorkerManager` gains `runtime: RuntimeHandle`.
- Test: `test_worker_pod_name_matches_launch_naming`.

### 4.2 Local — `worker_manager/local.rs` (+8)

`delete_worker(id)`: dropping the last `ActorHandle` closes the worker actor's channel →
the runner terminates.

### 4.3 `worker_manager/mod.rs` (+6)

`WorkerManager` trait gains `async fn delete_worker(&self, id: WorkerId)`; struct gains
`runtime`.

### 4.4 `worker/peer_tracker/options.rs` (+4), `worker/options.rs` — `runtime` plumbing.

---

## 5. Codec + proto — remote execution

`proto/sail/plan/physical.proto` (+35):
```proto
message IcebergLoadDataFastExecNode { ... }   // NodeKind iceberg_load_data_fast = 55
message CallProcedureExecNode { ... }         // NodeKind call_procedure = 56
optional string file_path_column = 4;         // added to the scan-by-data-files node
```

`src/proto/codec.rs` (+445):
- Encode/decode for `IcebergLoadDataFastExec` (data files, operation, requirements,
  table properties, lakehouse table, reported row count — JSON fields).
- Encode/decode for `CallProcedureExec` (procedure, updates, requirements, output,
  pre-commit metadata — JSON).
- Both with round-trip tests.

---

## 6. Session manager self-heal — `sail-session/src/session_manager/actor/handler.rs` (+276)

`get_or_create_session_context(session_id, user_id)`:
- Running session → reuse its `SessionContext`.
- Any other state (stale/reaped/failed/deleted) → **drop it** (`shift_remove`) and
  **recreate** via the extracted `create_session(ctx, session_id, user_id)` — the server
  self-heals on the next request instead of erroring until a process restart.
- `create_session` builds a `ServerSessionInfo` (with `ctx.handle()`), calls
  `self.factory.create(info)`, records `ServerSession { user_id, created_at,
  deleted_at: None, state: Running { context } }`.

`set_session_failure` (or equivalent event handler): only a session in the **Deleting**
state may transition to `Failed`; a stale failure event must not clobber a session that
has since been recreated and is running (warn otherwise).

Tests (`CountingFactory` counting session creations):
- `reuse_running_session`, `recreate_deleting_session_does_not_leak`,
  `recreate_deleted_session`, `recreate_failed_session`,
  `set_session_failure_only_applies_when_deleting`, plus `test_manager`/`test_options`
  helpers.

---

## 7. Activity tracker — `sail-common-datafusion/src/session/activity.rs` (new)

```rust
pub struct ActivityTracker { active_at: Mutex<Instant> }
impl ActivityTracker {
    pub fn new() -> Self
    pub fn track_activity(&self) -> Result<Instant>   // refreshes active_at
    pub fn active_at(&self) -> Result<Instant>
}
```

A `SessionExtension` registered on the session context.

### `sail-spark-connect` integration

- `executor.rs` (+18): `ExecutorTaskContext::new(stream, heartbeat_interval,
  activity_tracker: Option<Arc<ActivityTracker>>)`; **on every stream poll** calls
  `tracker.track_activity()` so a long-running operation is never reaped by the session
  idle timeout (`spark.session_timeout_secs`), which previously only refreshed on client
  RPC entry.
- `service/plan_executor.rs` (+5): reads `ctx.extension::<ActivityTracker>()` and passes
  it into the executor task context.

---

## 8. RPC layer: server keepalive + client hardening (`sail-server`, `sail-execution/src/rpc.rs`)

### 8.1 `sail-server/src/builder.rs` (+30)

```rust
pub struct ServerBuilderOptions { ..., http2_keepalive_interval: Option<Duration>,
    http2_keepalive_timeout: Option<Duration> }
impl ServerBuilderOptions {
    pub fn from_keepalive(interval, timeout) -> Self
}
impl From<&ClusterConfig> for ServerBuilderOptions { ... }   // from cluster config secs
```

### 8.2 `sail-execution/src/rpc.rs` (+37) — client/server runtime + keepalives

**`ServerMonitor::start`** now takes a `tokio::runtime::Handle` and spawns the server
future on it (`handle.spawn(f)` instead of `tokio::spawn(f)`).

**`ClientOptions`** gains `runtime: RuntimeHandle` (the gRPC client's control-plane
runtime).

**`ClientBuilder::connect`** (macro `impl_client_builder!`) hardens every client
(driver→worker, worker→driver, session→driver, ...):
- `connect_timeout(CLIENT_CONNECT_TIMEOUT = 30s)` — the default Tonic timeout is infinite,
  which could hang a worker/driver forever when the peer is unreachable.
- `tcp_keepalive(Some(60s))`
- `http2_keep_alive_interval(30s)`, `keep_alive_timeout(20s)`, `keep_alive_while_idle(true)`
- The connect future is spawned on **`options.runtime.io()`** and awaited — the HTTP/2
  connection task (and its keep-alive ping handling) is spawned by Tonic via
  `tokio::spawn` inside connect, so awaiting on the `io` runtime ensures control-plane
  keep-alive pings are never starved by CPU-bound execution on the `primary` runtime.

Constants: `CLIENT_MAX_HEADER_LIST_SIZE = 1 MiB` (pre-existing),
`CLIENT_CONNECT_TIMEOUT = 30s`, `CLIENT_TCP_KEEPALIVE = 60s`,
`CLIENT_HTTP2_KEEPALIVE_INTERVAL = 30s`, `CLIENT_HTTP2_KEEPALIVE_TIMEOUT = 20s`.

### 8.3 Driver server — `driver/actor/rpc.rs` + `driver/actor/core.rs`

- `DriverActor::serve` takes a `ServerBuilderOptions` and builds the driver tonic +
  Flight server with `ServerBuilder::new("sail_driver", options)` instead of `Default::default()`.
- `DriverActor::start` (in `driver/actor/core.rs`) constructs
  `ServerBuilderOptions::from_keepalive(Some(driver.http2_keepalive_interval), Some(driver.http2_keepalive_timeout))`
  and starts the server with
  `self.server.start(self.options.runtime.io().clone(), Self::serve(...).in_span(span))`
  — the same io-runtime pattern as the worker (§8.4).
- `DriverActor::receive` (also `driver/actor/core.rs`) dispatches the new
  `DriverEvent::RetryWorkerSpawn => self.handle_retry_worker_spawn(ctx)`.

### 8.4 Worker server — `worker/actor/rpc.rs` + `worker/actor/core.rs`

- `WorkerActor::serve` takes `ServerBuilderOptions` and builds
  `ServerBuilder::new("sail_worker", options)`.
- `WorkerActor::start` constructs
  `ServerBuilderOptions::from_keepalive(Some(worker.http2_keepalive_interval), Some(worker.http2_keepalive_timeout))`
  and starts the server with
  `self.server.start(self.options.runtime.io().clone(), Self::serve(...).in_span(span))`.
- `WorkerActor::new` passes `runtime: options.runtime.clone()` into its
  `DriverClientSet` (`ClientOptions.runtime`).

### 8.5 `sail-flight/src/lib.rs` (+3) and `sail-spark-connect/src/entrypoint.rs` (+5)

Both build `ServerBuilderOptions::from(&config.cluster)` instead of `Default::default()`,
so the Flight-SQL and Spark-Connect servers inherit `cluster.http2_keepalive_*`.

### 8.6 `sail-server/src/retry.rs`

`RetryStrategy::delay(&self) -> Box<dyn Iterator<Item=Duration>+Send>` made `pub`
(consumed by the worker-pool spawn-retry).

### 8.7 Keepalive/runtime plumbing through options structs

- `worker_manager/options.rs`: `KubernetesWorkerManagerOptions` gains
  `http2_keepalive_interval` / `http2_keepalive_timeout` (forwarded into pod env).
- `worker/peer_tracker/core.rs`: `PeerTracker::new` passes `runtime: self.options.runtime.clone()`.
- `worker/options.rs`: `WorkerOptions` gains the two keepalive durations (from
  `config.cluster.http2_keepalive_*_secs`) and forwards them into the worker manager.

### 8.8 Driver placement — `job_graph/planner.rs`

`CallProcedureExec` is added to `plan_job_graph_stages`'s driver-stage detection and to
`is_driver_stage_plan` (alongside `IcebergCommitExec`, `DeltaCommitExec`,
`FileDeleteExec`, `BarrierExec`, ...). CALL commits must run on the **driver**. (See
also `02-call-procedures.md`.)

### 8.9 `driver/actor/mod.rs`

`DriverActor` now holds `worker_manager: Arc<dyn WorkerManager>` (added field, built in
`DriverActor::new`).

---

## 9. Config — `sail-common/src/config/application.rs` + `application.yaml`

New `cluster.*` keys (all exposed as `SAIL_CLUSTER__*` env vars):

| key | default | meaning |
|---|---|---|
| `cluster.http2_keepalive_interval_secs` | 60 | HTTP/2 keep-alive ping interval on driver, worker, Spark Connect, and Flight servers |
| `cluster.http2_keepalive_timeout_secs` | 30 | ping-ack timeout; connection closed if a peer doesn't acknowledge within the window |
| `cluster.worker_spawn_retry_strategy.type` | `fixed` | `fixed` or `exponential_backoff` (experimental) |
| `cluster.worker_spawn_retry_strategy.fixed.max_count` | 1 | re-spawn attempts |
| `cluster.worker_spawn_retry_strategy.fixed.delay_secs` | 180 | delay between re-spawns |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.max_count` | 3 | re-spawn attempts |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.initial_delay_secs` | 60 | initial delay |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.max_delay_secs` | 180 | max delay |
| `cluster.worker_spawn_retry_strategy.exponential_backoff.factor` | 2 | multiplier per attempt |

---

## 10. Data-plane / misc execution changes

- `sail-iceberg/src/datasource/provider.rs` (+91): `IcebergTableProvider::aggregate_statistics`
  reports **exact 0 rows/bytes** for an empty data-file set (per-column `null_count =
  Exact(0)`, bounds Absent) so DataFusion's `AggregateStatistics` folds `COUNT(*)` → 0
  without scanning — avoiding worker spawn for empty reads. Tests:
  `aggregate_statistics_empty_data_files_reports_zero_rows`,
  `aggregate_statistics_non_empty_data_files_are_aggregated`.
- `sail-data-source/src/formats/{rate,socket}/mod.rs`, `listing/source.rs` — small
  formatting/source adjustments (listed for completeness).
- `sail-iceberg/src/physical_plan/plan_builder.rs` (+63) — wires the new exec nodes
  (`CallProcedureExec`, `IcebergLoadDataFastExec`, row-level write) into `IcebergPlanBuilder`.

---

## 11. Behavior contracts to preserve

- `worker_max_count` is enforced against `pending + active` (never a separate counter);
  initial workers are charged against it.
- `deactivate_worker` and `track_worker_failed_to_start` both release budget fully.
- The readiness barrier defers assignment until no worker is `Pending`; re-triggered on
  registration/failure.
- Worker re-spawn is backoff-bounded (retry strategy), never a tight loop; the armed flag
  protects pending tasks during an in-flight retry.
- Idle workers are never reaped while work is queued or the pool is scaling.
- Stale sessions self-heal; stale failure events can't clobber recreated sessions.
- Streaming refreshes session activity on every poll.
