# Plan: Driver worker-pool accounting & idle-reaping fixes (P0 + P2)

**Status:** Draft — not yet implemented
**Branch:** `feat/v0.6.6`
**Scope:** Two focused fixes to the Sail distributed-execution driver's worker lifecycle, in
`crates/sail-execution`:

- **P0** — Eliminate the `TaskAssigner.requested_worker_count` accounting drift that causes
  the "workers spawned but never utilized → idle-reaped → driver can't spawn again" loop.
- **P2** — Make idle-worker reaping queue-aware so a worker is never reaped while there is
  pending work that could use its vacant slots.

Both fixes are self-contained, single-actor, and unit-testable. They target the root cause
of the production symptom (a `dbt run`/`dbt test` that leaves `Pending` worker pods and
stalls); they are **not** config tuning and require no `values-connect.yaml` change.

**Spec reference / idioms:** Akka actor-model lifecycle (actors do not auto-stop; a parent
explicitly stops its children; resource accounting lives in the owning actor's state, not in
scattered counters). Sail's `ActorRunner` is already idiomatic (single-threaded message
processing, `Stop` → `actor.stop()` → `ctx.reap()`), so the fixes stay inside the
`TaskAssigner`/`WorkerPool` state, which is exactly where the drift lives.

---

## 1. Root cause (code-verified)

### 1.1 Worker accounting is split across two structures that can drift

Worker "who exists / how many may I spawn" is tracked in **two places** inside the
per-session `DriverActor`:

| Concern | Owner | State |
|---|---|---|
| How many spawns are "in flight" (requested, not yet registered) | `TaskAssigner.requested_worker_count: usize` | mutable counter |
| Registered workers + slot occupancy | `TaskAssigner.workers: IndexMap<WorkerId, WorkerResource>` | `Active` / `Inactive` |
| Pod lifecycle (pending → running → completed/failed) | `WorkerPool.workers: IndexMap<WorkerId, WorkerDescriptor>` | `WorkerState::{Pending,Running,Completed,Failed}` |

The counter and the map are mutated by **three different handlers** that are not kept in
sync:

- `TaskAssigner::request_workers` (task_assigner/core.rs:15-56) — **increments**
  `requested_worker_count` by `required_workers` (line 54).
- `TaskAssigner::activate_worker` (task_assigner/core.rs:62-75) — **decrements** by 1
  (line 63).
- `TaskAssigner::track_worker_failed_to_start` (task_assigner/core.rs:58-60) — **decrements**
  by 1 (line 59).
- `TaskAssigner::deactivate_worker` (task_assigner/core.rs:77-90) — **neither decrements the
  counter nor removes the worker**; it only flips `Active → Inactive` (verified via MCP:
  `callees: 0`, pure state flip).

### 1.2 The invariant that is violated

The intended invariant is:

```
requested_worker_count == (# workers requested via request_workers that have NOT yet
                          activated or failed to start)
```

with `allowed_workers = max_count - requested_worker_count - active_workers`
(task_assigner/core.rs:47-50). Two concrete breaks:

1. **`worker_initial_count` spawns bypass `request_workers` entirely.** `handle_server_ready`
   (driver/actor/handler.rs:47-49) calls `WorkerPool::start_worker` directly
   `worker_initial_count` times, without ever incrementing `requested_worker_count`. So the
   initial N workers are **invisible to the max_count budget**. When a job then calls
   `request_workers`, it requests up to `max_count` more on top of the initial N → the real
   fleet can exceed `max_count` by `N` (e.g. `initial_count 2 + max_count 4` can spawn 6).

2. **Idle/lost reaping (`deactivate_worker`) leaves the worker in the map as `Inactive`
   forever and never decrements the counter.** `handle_probe_idle_worker`
   (driver/actor/handler.rs:105-125) and `handle_probe_lost_worker` (handler.rs:127-160)
   call `stop_worker` + `deactivate_worker`. The worker stays in `self.workers` as
   `Inactive`; `request_workers` counts only `Active` workers (`active_workers`, core.rs:38-42)
   and ignores `Inactive` ones, so reaped workers accumulate invisibly in the map and are not
   charged to `max_count`. Over reap cycles, `self.workers` grows unboundedly and the
   effective fleet cap is never enforced.

### 1.3 The resulting failure loop (matches the observed production symptom)

With `workerMaxIdleTimeSecs` low (e.g. 10s) and a multi-stage job:

1. Session init pre-spawns N workers (uncounted).
2. Stage 1 → `request_workers` requests up to `max_count` more (counter +).
3. Workers register → `activate_worker` (counter −1 each) → fleet active.
4. Inter-stage gap > idle timeout → `deactivate_worker` (map grows with `Inactive`, counter
   unchanged).
5. Stage 2 → `request_workers`: `active_workers` is small (many `Inactive` invisible), the
   counter may be inflated by in-flight requests that then fail/race, and `allowed_workers`
   drifts toward 0 → the driver **stops requesting new workers even though no healthy worker
   exists** → tasks stay `Created` → `Pending` worker pods that never get tasks → they idle
   and die → repeat. This is the "spawned but never utilized, then can't spawn" loop.

### 1.4 Why the actor model itself is sound

- `ActorRunner::run` (sail-server/src/actor.rs:197-232) processes messages
  **single-threaded** (`recv()` → `receive()` → `reap()`), so all `TaskAssigner`/`WorkerPool`
  mutations happen on one thread per `DriverActor`. Any recompute-from-state fix is therefore
  **race-free** by construction.
- Each `ClusterJobRunner` owns its own `DriverActor` + `WorkerPool` + `WorkerManager`
  (per-session; verified server.rs:158-200), so the fix is per-driver and needs no global
  coordination.
- `ActorRunner::run` already calls `actor.stop()` on `Stop` (PostStop equivalent), and
  `WorkerPool::close` (worker_pool/core.rs:29-38) stops workers + `worker_manager.stop()`
  (deletes pods) on teardown. **The lifecycle idiom is correct; only the state accounting is
  wrong.**

---

## 2. P0 — Fix `TaskAssigner` worker accounting (single source of truth)

### 2.1 Design

Replace the mutable `requested_worker_count: usize` with a **state-derived accounting** in
`TaskAssigner`, so the three handlers cannot drift.

Introduce a third `WorkerResource` variant so the assigner can distinguish "spawn requested,
pod not yet registered" from "registered" and "reaped":

```rust
// task_assigner/state.rs
pub enum WorkerResource {
    Pending,                        // NEW: requested, pod not yet registered
    Active {
        task_slots: Vec<TaskSlot>,
        local_streams: IndexSet<TaskKey>,
    },
    Inactive,                       // reaped (idle/lost/failed); still occupies an id
}
```

Rules:

- **`Pending`** = a spawn we asked for that has not yet called `RegisterWorker`.
- **`Active`** = registered, has slots.
- **`Inactive`** = reaped; **removed from the map entirely** by `deactivate_worker` (see
  below), so it no longer consumes the `max_count` budget.

Derived counts (recomputed on demand — no drift possible):

```rust
// task_assigner/core.rs
fn in_flight_worker_count(&self) -> usize {
    self.workers.values().filter(|w| matches!(w, WorkerResource::Pending)).count()
}
fn active_worker_count(&self) -> usize {
    self.workers.values().filter(|w| matches!(w, WorkerResource::Active { .. })).count()
}
fn total_live_worker_count(&self) -> usize {  // charged against max_count
    self.in_flight_worker_count() + self.active_worker_count()
}
```

> **Note on two parallel "Pending" notions (intentional, not redundant).** `TaskAssigner`'s
> new `WorkerResource::Pending` (this plan) and `WorkerPool`'s existing `WorkerState::Pending`
> (worker_pool/state.rs:40-52) serve different purposes and are cross-referenced, not
> duplicates:
> - `TaskAssigner::Pending` → the **scheduling budget**: a spawn counted against `max_count`
>   that hasn't registered yet. Drives `request_workers`/`allowed_workers`.
> - `WorkerPool::WorkerState::Pending` → the **pod lifecycle**: inserted in `start_worker`
>   (worker_pool/core.rs:45-56), drives `has_pending_workers()` (core.rs:184-188), the
>   `ProbePendingWorker` timeout, and the `WorkerSnapshot` status string.
> Both are created at the same event (a spawn is requested) and both must transition together
> (`add_pending_worker`/`start_worker` → `activate_worker`/`register_worker` →
> `WorkerState::Running`). Implementation must keep them in step; the P2 guard reuses
> `WorkerPool::has_pending_workers()` (the pod-lifecycle truth) while P0's budget uses
> `TaskAssigner`'s `Pending` (the scheduling truth).

### 2.2 `request_workers` — cap against the live budget, not a counter

```rust
pub fn request_workers(&mut self) -> usize {
    let enqueued_slots = /* unchanged: sum of Worker-placed tasks in task_queue */;
    let vacant_slots = /* unchanged: sum of vacant slots across Active workers */;
    let required_slots = enqueued_slots.saturating_sub(vacant_slots);
    let required_workers = required_slots.div_ceil(self.options.worker_task_slots);

    let allowed_workers = if self.options.worker_max_count == 0 {
        usize::MAX
    } else {
        self.options
            .worker_max_count
            .saturating_sub(self.total_live_worker_count())   // was requested_worker_count + active_workers
    };

    let to_spawn = required_workers.min(allowed_workers);
    // No counter mutation here: `Pending` workers are recorded via `add_pending_worker`
    // as `to_spawn` are actually reserved (see §2.3), so `total_live_worker_count` is
    // always recomputed from state and can never drift.
    to_spawn
}
```

**Remove** the old `requested_worker_count` field entirely (task_assigner/mod.rs:19,39).

### 2.3 Reserve worker ids at request time so `Pending` is tracked

Today `WorkerPool::start_worker` generates the worker id internally
(worker_pool/core.rs:44-96, `worker_id_generator.next()`), so the `TaskAssigner` never learns
the id before registration. Two options — **prefer (a)**:

**(a) Pre-reserve ids in `WorkerPool`, pass down.** Add
`WorkerPool::reserve_worker_ids(n) -> ExecutionResult<Vec<WorkerId>>` that advances
`worker_id_generator` and returns ids. **Note:** `IdGenerator::next` returns
`ExecutionResult<T>` (id.rs:86-90), and the current `start_worker` sends
`DriverEvent::Shutdown { history: None }` on id exhaustion (core.rs:45-48). `reserve_worker_ids`
must propagate that error (return `Err` on the first `next()` failure) rather than silently
returning fewer ids than requested; the handler can then fall back to the existing shutdown
behavior. The handler marks them `Pending` in `TaskAssigner` before spawning:

```rust
// driver/actor/handler.rs: scale_up_workers
fn scale_up_workers(&mut self, ctx: &mut ActorContext<Self>) {
    let to_spawn = self.task_assigner.request_workers();
    let ids = self.worker_pool.reserve_worker_ids(to_spawn);
    for worker_id in ids {
        self.task_assigner.add_pending_worker(worker_id);
        self.worker_pool.start_worker_with_id(ctx, worker_id);
    }
}
```

with `WorkerPool::start_worker_with_id(ctx, worker_id)` using the pre-generated id instead of
calling `worker_id_generator.next()`.

**(b) Keep id generation internal, reconcile on register/fail.** Simpler but weaker: keep
`start_worker` as-is, and make `activate_worker` / `track_worker_failed_to_start` transition
an existing `Pending` entry (inserting one lazily if missing). This still requires knowing the
id at request time, so (a) is cleaner.

### 2.4 `worker_initial_count` goes through the same budget

`handle_server_ready` (handler.rs:47-49) currently spawns directly. Change it to:

```rust
for _ in 0..self.options.worker_initial_count {
    self.scale_up_workers(ctx);
}
```

so initial workers are also counted against `max_count` via `request_workers` →
`add_pending_worker`. (Note: `request_workers` returns 0 when `enqueued_slots == 0`, so to
honor "spawn N at start" the initial loop should call a variant that ignores enqueued_slots;
see §2.6.)

### 2.5 Lifecycle transitions

- `add_pending_worker(worker_id)`: insert `WorkerResource::Pending`.
- `activate_worker(worker_id)` (core.rs:62-75): `Pending → Active { slots: vec![TaskSlot::default(); worker_task_slots], local_streams: IndexSet::new() }`. Remove the counter decrement. **Also fix the pre-existing double-registration bug:** the current code decrements `requested_worker_count` *before* checking `contains_key` (core.rs:62-66), so a duplicate `RegisterWorker` for an already-active id decrements the budget incorrectly. The state-transition version must be idempotent — if the worker is already `Active`, no-op (warn), not decrement anything.
- `track_worker_failed_to_start` (core.rs:58-60): change signature to
  `track_worker_failed_to_start(&mut self, worker_id: WorkerId)` and remove the `Pending` entry
  (worker pod failed before registration). The single caller
  (`handle_probe_pending_worker`, handler.rs:93-103) must pass the worker id it probed.
- `deactivate_worker(worker_id)` (core.rs:77-90): **remove the worker from the map** (it no longer consumes budget). Callers (`handle_probe_idle_worker`, `handle_probe_lost_worker`, and `WorkerPool::stop` teardown) still work because they only need the worker gone from scheduling.
- `handle_register_worker` (handler.rs:53-71): unchanged shape — `register_worker` (WorkerPool) + `activate_worker` (TaskAssigner) + `run_tasks`.

### 2.6 `worker_initial_count` exact semantics

To keep "N workers ready at start" without being gated on `enqueued_slots == 0`, add:

```rust
pub fn request_initial_workers(&mut self) -> usize {
    let budget = if self.options.worker_max_count == 0 {
        usize::MAX
    } else {
        self.options.worker_max_count.saturating_sub(self.total_live_worker_count())
    };
    budget.min(self.options.worker_initial_count.saturating_sub(self.total_live_worker_count()))
}
```

and have `handle_server_ready` use it. This guarantees exactly `worker_initial_count` live
workers at startup, capped by `worker_max_count`, with correct budget accounting.

### 2.7 Files touched (P0)

| File | Change |
|---|---|
| `crates/sail-execution/src/driver/task_assigner/state.rs` | add `WorkerResource::Pending`; adjust `add_task_set`/`remove_task`/`track_local_streams`/`untrack_local_streams` to handle `Pending` (return early / warn), matching the existing `Inactive` arm |
| `crates/sail-execution/src/driver/task_assigner/core.rs` | remove `requested_worker_count`; add derived count helpers; rewrite `request_workers`; rewrite `activate_worker`/`track_worker_failed_to_start`/`deactivate_worker` to state transitions; add `add_pending_worker`, `request_initial_workers` |
| `crates/sail-execution/src/driver/task_assigner/mod.rs` | remove the field |
| `crates/sail-execution/src/driver/worker_pool/mod.rs` | add `reserve_worker_ids` / `start_worker_with_id` |
| `crates/sail-execution/src/driver/worker_pool/core.rs` | refactor `start_worker` to accept a pre-assigned id |
| `crates/sail-execution/src/driver/actor/handler.rs` | `handle_server_ready`, `scale_up_workers` use the new API; `handle_probe_pending_worker` / `handle_probe_lost_worker` / `handle_probe_idle_worker` callers of `track_worker_failed_to_start` / `deactivate_worker` updated |

---

## 3. P2 — Queue-aware idle reaping

### 3.1 Problem (code-verified)

`handle_probe_idle_worker` (driver/actor/handler.rs:105-125) reaps a worker whenever:

```rust
self.task_assigner.is_worker_idle(worker_id)          // all slots vacant && no local streams
    && self.worker_pool.get_worker_last_update(worker_id)
        .is_some_and(|x| x <= instant)                // idle for >= worker_max_idle_time
```

It does **not** check whether there is queued work (`task_assigner.task_queue` non-empty) or a
pending worker that would use the reaped worker's vacant slots. So a worker can be destroyed
even though the driver is actively trying to assign tasks to it — which both wastes the spawn
and, combined with P0's accounting bug, poisons the scaling budget.

`track_worker_activity` (worker_pool/core.rs:509-519) bumps `updated_at` only when a task is
**assigned** to the worker (`run_task`/`stop_task`/`fetch_task_stream`/`clean_up_job`; MCP
`callers: 4`). A registered-but-unassigned worker keeps its registration-time `updated_at`
and is reaped quickly even if the queue is full.

### 3.2 Design

Sail already has an idiomatic "pending worker" predicate: **`WorkerPool::has_pending_workers()`
(worker_pool/core.rs:184-188)** returns true when any `WorkerDescriptor` is in
`WorkerState::Pending`. Reuse it rather than adding a parallel check to `TaskAssigner`.

`TaskAssigner`'s `task_queue` is private with no accessor, so add a minimal one:

```rust
// task_assigner/core.rs
pub fn is_task_queue_empty(&self) -> bool {
    self.task_queue.is_empty()
}
```

Guard the idle reap:

```rust
// driver/actor/handler.rs: handle_probe_idle_worker
if self.task_assigner.is_task_queue_empty()
    && !self.worker_pool.has_pending_workers()
    && self.task_assigner.is_worker_idle(worker_id)
    && self.worker_pool.get_worker_last_update(worker_id)
        .is_some_and(|x| x <= instant)
{
    self.worker_pool.stop_worker(ctx, worker_id, Some("worker has been idle for too long".to_string()));
    self.task_assigner.deactivate_worker(worker_id);
}
```

Rationale:

- `task_queue` non-empty → there is work that could be assigned to this worker's vacant
  slots → do not reap.
- Any `Pending` worker → the driver is mid-scale-up; reaping a live worker while new ones are
  still spawning is counterproductive → do not reap.
- If neither holds and the worker is idle, reaping is safe and correct.

### 3.3 Files touched (P2)

| File | Change |
|---|---|
| `crates/sail-execution/src/driver/task_assigner/core.rs` | add `is_task_queue_empty` |
| `crates/sail-execution/src/driver/actor/handler.rs` | add the guard to `handle_probe_idle_worker` |

---

## 4. Unit tests

There are currently **no** tests for `TaskAssigner` or `WorkerPool`
(`rg request_workers ... *_test.rs` → empty). Add a new test module.

### 4.1 `TaskAssigner` accounting tests (`task_assigner/core.rs` `#[cfg(test)]`)

Use the real `TaskAssigner` + `TaskRegion`/`TaskSet` types (pure, no network). Cover the exact
reap/re-spawn loop:

1. **`request_workers_respects_max_count_against_live_workers`** — with `worker_max_count=4`,
   `worker_task_slots=2`: enqueue 10 worker tasks → `request_workers()==4`; add 4 `Pending`;
   second call → `==0` (budget exhausted).
2. **`activate_then_deactivate_releases_budget`** — request 4, `add_pending_worker` ×4,
   `activate_worker` ×4 → `total_live_worker_count()==4`, `request_workers()==0`; then
   `deactivate_worker` ×4 (map empties) → `total_live_worker_count()==0`,
   `request_workers()==4` again. **This is the regression test for the leak.**
3. **`initial_workers_are_charged_against_max_count`** — `worker_max_count=4`,
   `worker_initial_count=2`: `request_initial_workers()==2`, `total_live_worker_count()==2`,
   then `request_workers()` for 10 tasks returns `==2` (4−2), not 4.
4. **`failed_to_start_releases_budget`** — request 2, `track_worker_failed_to_start` ×2 →
   `request_workers()` can request again.
5. **`idle_reap_guard_respects_pending_work`** — with a non-empty `task_queue`
   (`is_task_queue_empty() == false`) or a `WorkerPool` pending worker
   (`has_pending_workers() == true`), `handle_probe_idle_worker` must **not** reap an
   otherwise-idle worker.

Test helpers: a small `enqueue_region(assigner, worker_task_count)` that builds a
`TaskRegion` with N `TaskPlacement::Worker` task sets (mirror `TaskSet::new` usage in
`assign_tasks`).

### 4.2 `WorkerPool` id-reservation tests (`worker_pool/mod.rs` `#[cfg(test)]`)

- `reserve_worker_ids_returns_unique_consecutive_ids`.
- `start_worker_with_id_uses_given_id` (inject a fake `WorkerManager` that records the id; the
  existing `WorkerManager` trait is already mockable).

### 4.3 `DriverActor` idle-reap guard test (handler-level, optional)

If a lightweight harness exists, assert `handle_probe_idle_worker` does **not** reap when
`task_queue` is non-empty. If no harness exists, rely on the `is_task_queue_empty` /
`has_pending_workers` unit tests + an integration run.

---

## 5. Verification

1. `cargo test -p sail-execution` — new tests green, no regressions.
2. `cargo test --workspace` — broader safety net.
3. `cargo clippy -p sail-execution` — clean under workspace lints
   (`allow_attributes`, `unwrap_used`, `expect_used`, `panic` are denied).
4. `cargo fmt --check`.
5. **Deployment sanity (on the server):**
   - Apply a `values-connect.yaml` with `workerMaxIdleTimeSecs: "120"` (not 10),
     `workerInitialCount: "3"`, `workerMaxCount: "5"`, `executionDefaultParallelism: "10"`,
     `workerLaunchTimeoutSecs/taskLaunchTimeoutSecs: "180"` (config-side mitigation; P0/P2
     make these less load-bearing).
   - During one `run_dbt_project.sh stg/l1`, watch:
     `kubectl get pods -n smartreg -w` → worker count should reach `<= workerMaxCount`, stay
     alive across intra-session stages, and **not** cycle Pending→Running→(reap)→Pending.
   - `kubectl describe pod <pending-worker>` → if `Insufficient cpu` appears, that is a
     separate node-capacity issue; the accounting fix prevents over-spawning but cannot create
     free CPU.

---

## 6. Rollout notes

- P0 is a **behavior change** for any job that relied on the old (leaky) counter; after it,
  `worker_max_count` is enforced strictly against live workers, so the fleet can no longer
  silently exceed the configured cap. This is the intended fix.
- **One benign warning to expect:** because `deactivate_worker` now *removes* the worker from
  the `TaskAssigner` map, a late `unassign_task`/`find_worker_tasks` for a reaped worker id
  logs `warn!("worker {id} not found")` and returns a safe no-op. Idle-reap never triggers this
  (only vacant-slot workers are reaped); lost-worker does (its tasks are failed before
  deactivation, then unassigned). Do not misread it as an error during the sanity check.
- P2 changes idle-reap timing (workers are kept when work is pending). This is strictly safer;
  the only downside is that a truly-idle worker may survive slightly longer during a scale-up,
  which is the desired behavior.
- Both changes are confined to `crates/sail-execution`; no protocol/proto change (the
  `WorkerResource` enum is internal, not serialized in `gen`).
- Re-grep all `requested_worker_count` / `deactivate_worker` / `track_worker_failed_to_start`
  call sites at implementation time — the current set is: `task_assigner/core.rs`,
  `driver/actor/handler.rs` (`handle_probe_pending_worker`, `handle_probe_lost_worker`,
  `handle_probe_idle_worker`, `scale_up_workers`, `handle_server_ready`).
