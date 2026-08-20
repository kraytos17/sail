# Sail Distributed Execution: Actors, Worker Lifecycle, and Task Scheduling

This document explains, end-to-end, how Sail executes a query on a distributed cluster:
the actor framework that everything runs on, how the driver and worker actors are created,
how workers are launched and tracked, how a query is turned into tasks, how those tasks are
enqueued and assigned to slots, and how the whole thing is managed and torn down.

All line references are to `crates/sail-server/src/actor.rs` and
`crates/sail-execution/src/**` at the time of writing.

---

## 1. The Actor Framework (`sail-server/src/actor.rs`)

Sail implements a lightweight **actor model** on top of Tokio. Every concurrent component
(driver, worker, session manager) is an actor: a single-threaded event loop that processes
messages sequentially from an mpsc channel.

### 1.1 The `Actor` trait (actor.rs:13-33)

```rust
pub trait Actor: Sized + Send + 'static {
    type Message: Send + SpanAssociation + 'static;
    type Options;

    fn name() -> &'static str;
    fn new(options: Self::Options) -> Self;
    async fn start(&mut self, ctx: &mut ActorContext<Self>) {}   // optional, runs once
    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction;
    async fn stop(self, ctx: &mut ActorContext<Self>) {}         // optional, runs at shutdown
}
```

Key contract (actor.rs:26-29): `receive` is **synchronous and non-blocking**. All messages
for one actor are processed sequentially in a single thread. If an actor needs to perform
async work (network I/O, sleeping, waiting on a oneshot), it must **spawn a task via
`ActorContext::spawn`** and get the result back as a new message.

`ActorAction` is simply `Continue | Stop` (actor.rs:35-38).

### 1.2 `ActorContext` (actor.rs:40-99)

The context gives an actor access to:

| Method | Purpose |
|---|---|
| `handle()` | A cloneable handle used to send messages **to itself or externally** |
| `send(msg)` | Spawn a task that sends `msg` to the actor itself |
| `send_with_delay(msg, d)` | Spawn a task that sleeps `d` then sends `msg` to itself (used for all timeouts/probes) |
| `spawn(fut)` | Spawn a fire-and-forget task; the handle is kept so it can be aborted at shutdown |
| `reap()` | Join completed spawned tasks and log any panics; called by the runner after each message |

`send`/`send_with_delay` are how the driver schedules its own future work — e.g.
`ProbePendingWorker`, `ProbeIdleWorker`, `ProbeLostWorker`, `RetryWorkerSpawn` are all
self-messages sent with `send_with_delay`.

### 1.3 `ActorSystem`, `ActorHandle`, `ActorRunner` (actor.rs:101-233)

- `ActorSystem::spawn(options)` (actor.rs:120-131) creates an mpsc channel (size 8), builds
  the `ActorRunner { actor: T::new(options), ctx, receiver, start }`, and spawns the runner
  as a Tokio task on a `JoinSet`. It returns an `ActorHandle`.
- `ActorHandle::send(msg)` (actor.rs:166-187) is the **only way to talk to an actor from
  outside**. It is `async`, `Clone`, and wraps each message in a `MessageEnvelop` carrying a
  tracing `SpanContext` so spans cross the channel correctly.
- `ActorRunner::run` (actor.rs:197-232) is the event loop:
  1. Runs `actor.start(ctx)` once.
  2. Loops: `receiver.recv().await` → open a tracing span → `actor.receive(ctx, msg)` →
     `Continue` keeps looping, `Stop` breaks.
  3. `ctx.reap()` after every message.
  4. On exit: closes the receiver, runs `actor.stop(ctx)`, reaps, and aborts any still-pending
     spawned tasks.

Because the runner is single-threaded, **all state mutations inside an actor are race-free
without locks** — this is what makes the driver's accounting logic (worker budgets, task
assignment) trivially correct.

---

## 2. Actors in the Execution Engine

There are two main execution actors plus the session manager:

| Actor | Message type | Created by | Role |
|---|---|---|---|
| `DriverActor` | `DriverEvent` | `ClusterJobRunner::new` → `ActorSystem::spawn` | Schedules jobs, spawns/reaps workers, assigns tasks |
| `WorkerActor` | `WorkerEvent` | the worker binary started in the worker pod | Executes tasks, streams results, reports status/heartbeats |
| `SessionManagerActor` | (session mgmt) | `sail-session` | Owns sessions and their `JobRunner`s |

### 2.1 Driver actor construction

`crates/sail-execution/src/job_runner.rs:79-88`:

```rust
pub struct ClusterJobRunner { driver: ActorHandle<DriverActor> }

impl ClusterJobRunner {
    pub fn new(system: &mut ActorSystem, options: DriverOptions) -> Self {
        let driver = system.spawn(options);
        Self { driver }
    }
}
```

`DriverActor::new` (actor/core.rs:26-45) builds its collaborators:
`WorkerPool` (owns the fleet), `JobScheduler` (job/task/region state machine),
`TaskAssigner` (slot accounting + assignment), `TaskRunner` (local task execution),
`StreamManager` (task-stream bookkeeping), and a `ServerMonitor`.

`ActorSystem::spawn` is called per `JobRunner`, and there is one `JobRunner` **per session**
(`server.rs` / `session_factory/server.rs:158-200`). This means each session has its own
driver actor and therefore its own worker pool.

### 2.2 `DriverActor::start` — serving the driver RPC server

`DriverActor::start` (actor/core.rs:49-57) starts a tonic gRPC server (`Self::serve`) on
`driver_listen_host:driver_listen_port` and wraps it in a `ServerMonitor`. When the server is
up it emits `DriverEvent::ServerReady { port, signal }` to itself.

`handle_server_ready` (actor/handler.rs:31-52):
1. Finalizes the server via `server.ready(signal)`.
2. Calls `worker_pool.set_driver_server_port(port)` — workers need to know how to reach the
   driver.
3. Calls `spawn_initial_workers(ctx)`, which pre-spawns `worker_initial_count` workers
   (capped by `worker_max_count`) via the task assigner's `Pending` accounting.

---

## 3. Worker Creation and Lifecycle

### 3.1 Spawning a worker (`WorkerPool::start_worker_with_id`, worker_pool/core.rs:44-95)

`spawn_workers` (actor/handler.rs:622-632) is the single entry point used by both
`spawn_initial_workers` and `scale_up_workers`:

```rust
fn spawn_workers(&mut self, ctx, count) {
    let ids = self.worker_pool.reserve_worker_ids(count)?;       // fresh WorkerIds
    for worker_id in ids {
        self.task_assigner.add_pending_worker(worker_id);        // charge the budget
        self.worker_pool.start_worker_with_id(ctx, worker_id);
    }
}
```

`start_worker_with_id`:
1. Inserts a `WorkerDescriptor { state: Pending, created_at, .. }` into the pool's map.
2. **Schedules the launch-timeout watchdog**: `send_with_delay(ProbePendingWorker, worker_launch_timeout)`.
3. Builds `WorkerLaunchOptions` (driver address, heartbeat interval, stream buffer, RPC retry
   strategy, tracing context).
4. Spawns a task calling `worker_manager.launch_worker(worker_id, options)` — this is the
   only async part; errors are logged.

### 3.2 The `WorkerManager` abstraction (worker_manager/mod.rs)

```rust
#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    async fn launch_worker(&self, id: WorkerId, options: WorkerLaunchOptions) -> ExecutionResult<()>;
    async fn delete_worker(&self, id: WorkerId) -> ExecutionResult<()>;  // reaped/failed pod
    async fn stop(&self) -> ExecutionResult<()>;                         // stop all, at pool close
}
```

Two implementations:

**`KubernetesWorkerManager`** (worker_manager/kubernetes.rs)
- `launch_worker` builds a `Pod` with a deterministic name
  `{worker_pod_name_prefix}{manager-name}-{worker_id}` (kubernetes.rs:238-289), injects the
  full worker config as env vars via `build_pod_env` (kubernetes.rs:113-222), sets
  `restart_policy: Never`, an owner reference to the driver pod, and calls
  `Api::create`. Pod labels include `sail.lakesail.com/worker-manager=<name>`.
- `delete_worker` deletes the pod by the same computed name.
- `stop` deletes all pods in the namespace matching the worker-manager label
  (`delete_collection`, kubernetes.rs:298-306).

**`LocalWorkerManager`** (worker_manager/local.rs) — used by `local-cluster` mode. It spawns
`WorkerActor` in a local `ActorSystem` and stores its handle; `delete_worker` drops the
handle, closing the actor's channel so the runner terminates.

### 3.3 The worker pod boot sequence

The worker pod runs `sail worker` with the env vars from `build_pod_env`. Those env vars
(ClusterConfigEnv / ConfigEnv) are read by `AppConfig`, which drives `WorkerActor::new`
(worker/actor/core.rs:26-43): it builds a `DriverClientSet` (gRPC client to the driver),
a `PeerTracker`, a `StreamManager`, and a `TaskRunner`.

`WorkerActor::start` (worker/actor/core.rs:45-55) starts the worker's own tonic server
(`Self::serve`), then emits `WorkerEvent::ServerReady`.

`handle_server_ready` (worker/actor/handler.rs:20-63):
1. Computes the externally reachable host/port (pod IP from env / `--ip`).
2. Spawns a task that **retries `register_worker(worker_id, host, port)` to the driver**
   using `RetryStrategy::run`, then sends itself `StartHeartbeat`.
3. On failure: sends `WorkerEvent::Shutdown`, terminating the worker.

### 3.4 Worker registration at the driver

Worker gRPC call → `DriverServer::register_worker` (driver/server.rs:29-58) → sends
`DriverEvent::RegisterWorker { worker_id, host, port, result }` and awaits the oneshot reply.

`handle_register_worker` (actor/handler.rs:54-72):
1. `worker_pool.register_worker(ctx, id, host, port)`:
   - Transitions `Pending → Running { host, port, updated_at, heartbeat_at, client }`
     (worker_pool/core.rs:97-140). The gRPC/flight client is created **lazily** on first use
     via `get_client_set` (core.rs:431-454).
   - Resets the spawn-retry state (any cluster is schedulable again).
   - Schedules the **lost-worker watchdog** (`ProbeLostWorker` after `worker_heartbeat_timeout`)
     and the **idle-worker watchdog** (`ProbeIdleWorker` after `worker_max_idle_time`).
2. `task_assigner.activate_worker(id)` — `Pending → Active` with
   `worker_task_slots` empty `TaskSlot`s, releasing nothing (budget is already charged, it
   just changes kind). See task_assigner/core.rs:111-125.
3. `run_tasks(ctx)` — re-runs assignment so the new worker's slots pick up any queued regions.
4. Replies over the oneshot so the worker's registration RPC returns success.

### 3.5 Heartbeats

`handle_start_heartbeat` (worker/actor/handler.rs:65-80) spawns a loop that every
`worker_heartbeat_interval` calls `report_worker_heartbeat(worker_id)`.

At the driver, `DriverServer::report_worker_heartbeat` → `DriverEvent::WorkerHeartbeat` →
`handle_worker_heartbeat` → `update_worker_heartbeat` (worker_pool/core.rs:214-227), which
refreshes `heartbeat_at` and re-arms the lost-worker probe. The heartbeat timeout is
`worker_heartbeat_timeout` (default 120s; configured to 600s in the deployment).

### 3.6 Reaping workers

**Idle reap** — `handle_probe_idle_worker` (actor/handler.rs:131-156). The watchdog fires at
`worker_max_idle_time`. A worker is reaped only when ALL of:
- the task queue is empty (`is_task_queue_empty`),
- there are no `Pending` workers (pool not mid-scale-up),
- the worker's slots are all vacant and it holds no local streams (`is_worker_idle`),
- it was not touched since the probe was armed.

Then `stop_worker` (sends the worker a stop RPC / marks completed) and
`deactivate_worker` (removes it from the task assigner so its budget is released).

**Lost-worker reap** — `handle_probe_lost_worker` (actor/handler.rs:158-195). If the last
heartbeat is older than the probe instant, the worker is considered dead: stop it, mark all
its tasks `Failed`, `deactivate_worker`, then `refresh_job` + `run_tasks` + `scale_up_workers`
for the affected jobs so their regions are rescheduled.

**Pending-worker failure** — `handle_probe_pending_worker` (actor/handler.rs:94-120). When the
launch-timeout watchdog fires and the worker is still `Pending`:
1. `fail_worker_if_pending` marks it `Failed`.
2. `track_worker_failed_to_start` removes it from the task assigner (releases budget).
3. `delete_worker` deletes the pod so a stuck `Pending` (e.g. `Insufficient cpu`) pod does
   not linger.
4. **Backoff**: instead of immediately re-spawning, consume the next spawn-retry delay; if
   any remains, schedule `RetryWorkerSpawn` after that delay. When the retries are exhausted,
   re-spawning stops entirely and pending tasks fail via the scheduling timeout (see §6.5).

---

## 4. From Query to Tasks: the Job Graph

### 4.1 `JobGraph::try_new` (job_graph/planner.rs:42-61)

The driver receives a **physical plan** (already planned by the session). `JobGraph::try_new`
splits it into a DAG of **stages** in topological order:

```rust
pub struct Stage {
    inputs: Vec<StageInput>,       // (stage, InputMode)
    plan: Arc<dyn ExecutionPlan>,
    group: String,                 // "slot sharing group"
    mode: OutputMode,              // Pipelined | Blocking
    distribution: OutputDistribution, // Hash | RoundRobin | RoundRobinRow
    placement: TaskPlacement,      // Driver | Worker
}
```

Key concepts (job_graph/mod.rs):
- **Stage**: a unit of execution; one stage may have many **partitions**
  (`plan.output_partitioning().partition_count()`).
- **Task**: the execution of one partition of one stage. A task may have multiple
  **attempts** (retries).
- **TaskPlacement**: `Driver` (e.g. scalar-subquery stages, the final output stage) or
  `Worker`.
- **InputMode** describes how a stage reads its inputs:
  `Forward` / `Merge` / `Shuffle` / `Broadcast` / `Rescale`.
- The final stage is always placed on `Worker` (planner.rs:57) with `RoundRobin { channels: 1 }`.

### 4.2 `JobDescriptor` (job_scheduler/state.rs)

`JobDescriptor::try_new(graph, state)` stores the graph plus per-stage/per-partition
**`TaskAttemptDescriptor`s** (state machine: `Created → Scheduled → Running → Succeeded/Failed/Canceled`)
and per-stage `StageState` / per-region `TaskRegionState`.

### 4.3 Task regions (`task/scheduling.rs:1-75`)

A **`TaskRegion`** groups task sets that are scheduled together:

> "A task region represents multiple task sets that should be scheduled together... Failure of
> any tasks in the region should trigger a rescheduling of the entire region."

A region is built from a stage-group (`TaskRegionTopology`). Since our scheduling changes, a
region may now be assigned across **multiple passes** (see §6.3).

A **`TaskSet`** is the unit assigned to a single slot; it contains tasks from different stages
of the same "slot sharing group". Each entry is `(TaskKey, TaskOutputKind)` where output is
`Local` (streamed to the consumer) or `Remote` (written to object storage).

---

## 5. Job Execution and Task Enqueueing

### 5.1 `ClusterJobRunner::execute` → `ExecuteJob`

`job_runner.rs:113-131`: the session calls `execute(plan)` — the `ClusterJobRunner::execute`
impl is at job_runner.rs:114-131 — which builds a oneshot channel and
sends `DriverEvent::ExecuteJob { plan, context, result }` to the driver. The future awaits
the oneshot, which is fulfilled when the driver has built the job output stream.

### 5.2 `handle_execute_job` (actor/handler.rs:197-212)

```rust
let out = self.job_scheduler.accept_job(ctx, plan, context);
if let Ok((job_id, _)) = &out {
    self.refresh_job(ctx, *job_id);   // schedules regions, enqueues task regions
    self.run_tasks(ctx);              // assigns what fits
    self.scale_up_workers(ctx);       // spawn workers for the deficit
}
result.send(out.map(|(_, stream)| stream));
```

### 5.3 `JobScheduler::accept_job` (job_scheduler/core.rs:40-60)

1. Allocates a `JobId`.
2. Builds the `JobGraph` from the plan.
3. Builds the job output: `build_job_output(ctx, job_id, schema)` returns a stream the client
   will poll, plus an output handle wired into the driver.
4. Creates the `JobDescriptor` and inserts it.

### 5.4 `refresh_job` → `schedule_task_regions` → enqueue

`refresh_job` (handler.rs) iterates `JobScheduler::schedule_task_regions(job_id, job)`
(job_scheduler/core.rs:272-319). For every region whose dependencies have succeeded and whose
tasks have no live attempt, it:
- pushes a new `TaskAttemptDescriptor { state: Created }` per task,
- produces a `JobAction::ScheduleTaskRegion { region }`.

`run_job_action` (handler.rs) handles `ScheduleTaskRegion`:
- schedules a `ProbePendingTask` watchdog per task (after `task_launch_timeout`),
- calls `task_assigner.enqueue_tasks(region)` → pushed onto the `VecDeque<TaskRegion>`
  (task_assigner/mod.rs:29, core.rs:135-137).

So "enqueueing" means: **the job scheduler materializes task attempts, and the resulting
`TaskRegion` is pushed into the task assigner's FIFO queue.**

---

## 6. Task Scheduling and Assignment

### 6.1 The assigner state (task_assigner/state.rs)

- `DriverResource`: unbounded `task_slots` on the driver, plus local/remote stream sets.
- `WorkerResource`: `Pending` (spawn requested, not yet registered) or `Active { task_slots,
  local_streams }`.
- `TaskSlot`: a set of `TaskKey`s. Vacant ⇔ empty. A slot can hold tasks from different
  stages of the same group but not different partitions of the same stage.
- `task_assignments: IndexMap<TaskKey, TaskAssignment>`: the authoritative "where is this task
  attempt assigned" map. Each attempt is assigned at most once; entries are retained
  historically after completion.

### 6.2 `request_workers` — how many workers to spawn (task_assigner/core.rs:37-70)

```
enqueued_slots = number of Worker-placed task sets across all queued regions
vacant_slots   = number of vacant slots across Active workers (Pending contribute 0)
required_slots = enqueued_slots − vacant_slots
allowed        = worker_max_count − total_live_worker_count      // max_count==0 ⇒ unbounded
request_workers = ceil(required_slots / worker_task_slots) , capped by allowed
```

`total_live_worker_count` = `pending + active` — this is the budget that `worker_max_count`
enforces (both counts are derived from state, never a separate counter).

`request_initial_workers` (core.rs:75-88) is the pre-warm variant that ignores the queue and
returns `worker_initial_count` capped by the remaining budget.

### 6.3 `assign_tasks` — assign what fits (task_assigner/core.rs:147-192)

`run_tasks` (actor/handler.rs) calls `assign_tasks`, which:
1. Builds a `TaskSlotAssigner` from all **Active** workers' slots
   (`build_worker_task_slot_assigner`, core.rs:286-309). `Pending` workers contribute no slots.
2. Pops regions from the queue front. For each, `try_assign_task_region`
   (core.rs:342-378):
   - `TaskPlacement::Driver` sets → always assigned to the driver.
   - `TaskPlacement::Worker` sets → assigned greedily to a `(worker_id, slot)` until slots are
     exhausted; the unassigned remainder is kept.
3. If there is a remainder (slots insufficient for the whole region), it is pushed back to the
   **front** of the queue and the loop `break`s — preserving FIFO ordering (head-of-line
   blocking, so a partially-assigned region still keeps later regions waiting). This is the
   **partial assignment** behavior: a query makes progress with the slots it has, and the
   remainder is picked up on the next `run_tasks` pass (e.g. when a new worker registers).
4. Records each assignment in `task_assignments` and installs the task set into the target
   slot; returns the assignments.

`run_tasks` then walks the assignments:
- Driver assignments → `task_runner.run_task(ctx, key, definition, context)` (local execution).
- Worker assignments → `worker_pool.run_task(ctx, worker_id, key, definition)` (dispatched over
  gRPC, §7).

### 6.4 `scale_up_workers` (actor/handler.rs:608-620)

```rust
let count = self.task_assigner.request_workers();
self.spawn_workers(ctx, count);
```

Called after job execution, worker registration, task success/failure, lost-worker reaping,
and the spawn-retry event. It spawns exactly `count` new workers (each charged as `Pending`).

### 6.5 Scheduling timeout — `handle_probe_pending_task` (actor/handler.rs:280-324)

Every enqueued task arms a `ProbePendingTask` watchdog. When it fires and the task is still
`Created`:
- If there are `Pending` workers **or** a spawn retry is armed
  (`has_pending_workers() || has_pending_spawn_retry()`), the probe is rescheduled after
  `min(worker_launch_timeout, task_launch_timeout)` — a pending worker may still register.
- Otherwise the task fails with `"task scheduling timeout"`.

This is bounded: a `Pending` worker is failed at `worker_launch_timeout`, and once the spawn
retry budget is exhausted the task fails promptly.

---

## 7. Task Dispatch and Execution

### 7.1 To a worker (`WorkerPool::run_task`, worker_pool/core.rs:280-353)

1. Lists running workers (for peer discovery) and gets/lazily-creates the worker's client.
2. Refreshes worker activity (`track_worker_activity` → re-arms the idle probe).
3. Verifies the worker is `Running`, else fails the task.
4. Spawns a task calling `client.run_task(key, definition, peers)` over gRPC; on error it
   sends `DriverEvent::UpdateTask { Failed }` back to itself.

### 7.2 Inside the worker (`WorkerActor::handle_run_task`, worker/actor/handler.rs:100-111)

The worker tracks the peers, then hands the task to its own `TaskRunner::run_task`
(task_runner/core.rs:39-66):
- `execute_plan` decodes the physical plan and streams inputs/outputs; a `TaskMonitor` is
  spawned to drive the stream and report status.
- Task output is split into **channels** and consumed via the stream accessor; outputs are
  either `Local` (the consuming task fetches the stream) or `Remote` (written to object
  storage via `ShuffleWriteExec` / `StageInputExec`).

### 7.3 Status reporting back to the driver

`TaskMonitor` calls the worker's `report_task_status`; `WorkerActor::handle_report_task_status`
(worker/actor/handler.rs:122-165) stamps a per-worker `sequence` number and **retries** the RPC
to the driver. `DriverServer::report_task_status` (driver/server.rs:102-142) forwards it as
`DriverEvent::UpdateTask { status, sequence, .. }`.

`handle_update_task` (actor/handler.rs:223-278):
- **Stale-sequence guard**: an update whose `sequence <=` the last seen one is dropped
  (out-of-order protection).
- `Running` → update state, `refresh_job`.
- `Succeeded` → update state, `unassign_task`, `refresh_job`, `run_tasks`, `scale_up_workers`.
- `Failed` → update state, `unassign_task`, `refresh_job`, `run_tasks`, `scale_up_workers`
  (the region reschedules because the failed attempt is terminal).
- `Canceled` → update state only.

`unassign_task` (task_assigner/core.rs:194-210) removes the key from the slot and driver
resource, leaving the historical `task_assignments` entry intact.

### 7.4 Job output

The final stage's output stream is consumed by `extend_job_output` / `JobOutputHandle`; the
client polls it via the oneshot stream returned by `execute`. When the last task succeeds, the
job's output stream terminates and the query completes.

---

## 8. Shutdown and Cleanup

### 8.1 `ClusterJobRunner::stop` → `Shutdown`

`job_runner.rs:133-140` sends `DriverEvent::Shutdown { history }`. `handle_shutdown` builds the
`JobRunnerHistory` and replies, causing the driver actor to return `ActorAction::Stop`.

`DriverActor::stop` (actor/core.rs:138-150):
1. `job_scheduler.stop()`
2. `stream_manager.stop().await`
3. `worker_pool.close(ctx)` — stops every worker (best-effort stop RPC) then
   `worker_manager.stop()` (k8s: delete all pods by label; local: join the actor system).
4. Sends the `JobRunnerHistory` (job/stage/task/worker snapshots).
5. Stops the driver server.

### 8.2 Worker shutdown

The worker's `stop_worker` gRPC is handled by `WorkerServer::stop_worker`, which sends
`WorkerEvent::Shutdown` → `ActorAction::Stop`. `WorkerActor::stop` stops its server.
If the driver dies without a clean stop, the k8s **owner-reference / cascading deletion**
cleans up worker pods (`WorkerManager` doc, worker_manager/mod.rs).

---

## 9. Summary: End-to-End Flow

```
Session create (per session)
  └─ JobRunner = ClusterJobRunner
       └─ ActorSystem.spawn(DriverActor)          # driver actor event loop starts
            └─ DriverActor::start → gRPC server
            └─ handle_server_ready
                 └─ spawn_initial_workers → reserve ids → add_pending_worker → launch pods

Query executes
  └─ ClusterJobRunner::execute(plan)
       └─ DriverEvent::ExecuteJob
            └─ accept_job → JobGraph (stages/partitions) + JobDescriptor + output stream
            └─ refresh_job → schedule_task_regions → JobAction::ScheduleTaskRegion
                 └─ enqueue_tasks(TaskRegion) → task_queue
            └─ run_tasks → assign_tasks (greedy, partial, FIFO)
            └─ scale_up_workers → request_workers → spawn_workers

Worker boot (each pod)
  └─ WorkerActor::start → gRPC server → ServerReady
       └─ register_worker(host, port) [retried]  → DriverEvent::RegisterWorker
            └─ Pending→Running, activate_worker, run_tasks (new slots pick up queue)
            └─ StartHeartbeat loop

Task execution
  └─ assign_tasks → Worker assignment → WorkerPool::run_task → gRPC RunTask
  └─ Worker TaskRunner executes plan, TaskMonitor streams + reports status
  └─ DriverEvent::UpdateTask(Succeeded/Failed) → unassign/refresh/run/scale

Fleet management
  └─ Heartbeats re-arm lost-worker watchdog (worker_heartbeat_timeout)
  └─ Idle watchdog (worker_max_idle_time) → stop_worker + deactivate (releases budget)
  └─ Launch watchdog (worker_launch_timeout) → fail + delete pod + backoff respawn
  └─ Lost worker → fail its tasks, refresh/run/scale
```

---

## 10. Configuration Surface (relevant to this flow)

All under `cluster.*` in `crates/sail-common/src/config/application.yaml`, exposed as
`SAIL_CLUSTER__*` env vars:

| Key | Default | Meaning |
|---|---|---|
| `worker_initial_count` | 4 | Pre-warmed workers at session start |
| `worker_max_count` | 0 (unbounded) | Hard cap on live (pending+active) workers |
| `worker_task_slots` | 8 | Slots (tasks) per worker; drives `request_workers` |
| `worker_max_idle_time_secs` | 60 | Idle reap watchdog |
| `worker_heartbeat_interval_secs` | 10 | Worker heartbeat cadence |
| `worker_heartbeat_timeout_secs` | 120 | Lost-worker watchdog |
| `worker_launch_timeout_secs` | 120 | Pending-worker (pod boot) watchdog |
| `task_launch_timeout_secs` | 120 | Pending-task watchdog |
| `worker_spawn_retry_strategy` | fixed{1,180s} | Backoff before re-spawning a failed pod |
| `rpc_retry_strategy` | fixed{3,5s} | Retry strategy for driver↔worker RPC |
| `execution.default_parallelism` | 0 (auto) | Partitions for plan nodes → task count |

---

## 11. Related Documents

- `docs/dev/driver-worker-pool-accounting-plan.md` — the accounting/idle-reap fixes (P0/P2).
- `docs/dev/sail-implementation-patterns.md` — general Sail codebase conventions.
- Code: `crates/sail-server/src/actor.rs` (framework),
  `crates/sail-execution/src/driver/**`, `crates/sail-execution/src/worker/**`,
  `crates/sail-execution/src/job_graph/**`, `crates/sail-execution/src/task_runner/**`,
  `crates/sail-execution/src/worker_manager/**`.
