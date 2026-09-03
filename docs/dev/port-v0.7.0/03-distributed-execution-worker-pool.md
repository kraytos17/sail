# Porting feat/0.7.0 → feat/0.7.1 — Doc 03: Distributed Execution — Worker Pool Lifecycle, Job Graph & RPC Hardening

> Part of the `docs/dev/port-v0.7.0/` inventory. Covers the `sail-execution` (and one
> `sail-server`) deltas on `feat/0.7.0` vs base `f0b137d6`: worker lifecycle/cleanup,
> job-graph stage planning (scalar-subquery indexing, file-scan rewrite, lazy plan
> encoding), the remote codec, RPC client/server hardening, and the task assigner.
> Produced mostly by commits `b72a956e` (WIP), `9e455322` (scalar subquery fix WIP),
> `b603f9ff` (revert faulty fix), `49eba34b` (ScalarSubqueryExpr page-filter fix),
> `11d729f1`/`b984b8bc` (session fixes) and the `ee78c38d` big-bang.
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`.

---

## 1. Files in this cluster (net delta)

| File | Nature of change |
|---|---|
| `sail-execution/proto/sail/plan/physical.proto` | new `IcebergLoadDataFastExecNode` (#56); `IcebergScanByDataFilesExecNode.file_path_column` (optional string #4) |
| `sail-execution/src/job_graph/mod.rs` | `Stage.encoded_plan: OnceLock<Arc<[u8]>>` + `Stage::encoded_plan()` |
| `sail-execution/src/job_graph/planner.rs` | SubqueryIndex-set scalar-subquery tracking; `rewrite_file_scans` moved here from task runner; lazy plan-encode wiring |
| `sail-execution/src/proto/codec.rs` | `contains_scalar_subquery_expr`; ParquetSource page-filter predicate dropped on encode; decode/encode for `IcebergLoadDataFastExecNode`; `file_path_column`; round-trip test |
| `sail-execution/src/task_runner/core.rs` | removed per-task `rewrite_file_scans` (moved into job-graph planner) |
| `sail-execution/src/driver/worker_pool/core.rs` | worker delete-on-failure, `running_worker_count`, `prune_terminal_workers`, `fail_worker_if_pending(ctx,…)`, tests |
| `sail-execution/src/driver/worker_pool/options.rs` | `#[cfg(test)] WorkerPoolOptions::new_for_test(...)` |
| `sail-execution/src/driver/actor/handler.rs` | idle scale-down gated by `running_worker_count() > worker_initial_count`; `fail_worker_if_pending(ctx,…)` |
| `sail-execution/src/driver/task_assigner/core.rs` | `deactivate_worker` now `shift_remove`s the worker (no tombstone) |
| `sail-execution/src/driver/task_assigner/options.rs` | `#[cfg(test)] new_for_test` |
| `sail-execution/src/driver/task_assigner/state.rs` | `WorkerResource::Inactive` kept only as dead-code vocabulary |
| `sail-execution/src/driver/job_scheduler/core.rs` | uses `stage.encoded_plan(codec)` |
| `sail-execution/src/rpc.rs` | peer label in `ClientOptions`, client keepalive settings, `rpc_error()` helper, `ClientHandle::peer()` |
| `sail-execution/src/driver/client.rs` | all calls wrapped with `rpc_error(peer, …)` |
| `sail-execution/src/worker/client.rs` | same wrapping |
| `sail-execution/src/stream_service/client.rs` | peer-tagged `do_get` errors incl. stream-item `TaskStreamError` remap |
| `sail-execution/src/worker/actor/core.rs` | thread `http2_keepalive_timeout` through serve |
| `sail-execution/src/worker/actor/rpc.rs` | worker server uses configured keepalive timeout |
| `sail-execution/src/worker/options.rs` | `http2_keepalive_timeout` field (+60 s in test ctor) |
| `sail-execution/src/worker/peer_tracker/core.rs` | peer label on worker clients |
| `sail-execution/src/worker_manager/mod.rs` | `WorkerManager::delete_worker(id)` default no-op |
| `sail-execution/src/worker_manager/kubernetes.rs` | `delete_worker` deletes the pod (404 ⇒ Ok) |

---

## 2. Job-graph planner & stage encoding

### 2.1 Lazy per-stage plan encoding (`job_graph/mod.rs`)

`Stage` gains `encoded_plan: OnceLock<Arc<[u8]>>`. New method:

```rust
impl Stage {
    pub fn encoded_plan(&self, codec: &dyn PhysicalExtensionCodec) -> ExecutionResult<Arc<[u8]>>;
}
```

The serialized stage plan is encoded **lazily, once**, and shared by every task — and every
task re-attempt — of that stage (previously each `JobScheduler::launch_task`/stage-launch
path called `encode_remote_physical_plan(...)` per task). `push_stage` initializes the
`OnceLock`; `JobScheduler` calls `stage.encoded_plan(codec)` to build `TaskDefinition.plan`
(now `Arc<[u8]>` directly, no extra `Arc::from`). Also `Stage.plan` stays for display.

### 2.2 Scalar-subquery index tracking (`job_graph/planner.rs`)

The previous tracking was a single `bool has_pending_scalar_subquery_expr` per planned
subtree. Multiple or nested scalar subqueries could not be distinguished, so
`wrap_pending_scalar_subqueries` wrapped subtrees with *all* known links. Rework:

- `PlannedSubtree` carries `pending_scalar_subquery_indices: HashSet<SubqueryIndex>`.
- `RebuiltSubtree` carries `child_pending_indices: Vec<HashSet<SubqueryIndex>>`,
  `node_pending_indices: HashSet<SubqueryIndex>`, `subtree_pending_indices: HashSet<SubqueryIndex>`
  (children ∪ node).
- `plan_scalar_subquery_indices(plan)` replaces `plan_node_has_scalar_subquery_expr` and the
  per-node-type helpers (`collect_aggregate_scalar_subquery_indices`,
  `collect_hash_join_scalar_subquery_indices`, `collect_window_expr_scalar_subquery_indices`,
  `collect_scalar_subquery_indices`) recursively collect **`ScalarSubqueryExpr::index()`**
  values from: `FilterExec.predicate`, `ProjectionExec.expr`, `AggregateExec`
  (group_expr incl. null_expr, aggr_expr expressions + order_bys, filter_expr),
  `SortExec`/`SortPreservingMergeExec` exprs, `HashJoinExec` on+filter,
  `NestedLoopJoinExec.filter`, `PiecewiseMergeJoinExec.on`, `WindowAggExec`/`BoundedWindowAggExec`
  (args/partition_by/order_by).
- `build_barrier_job_graph` unions precondition + node pending indices into the barrier.
- `wrap_pending_scalar_subqueries` filters `scalar_context.links` to only those whose
  `link.index` is in the subtree's pending index set before wrapping in `ScalarSubqueryExec`,
  so a stage boundary only pulls the subquery results it actually needs.
- Final single-stage path in `JobGraph::try_new` now goes through `push_stage(...,
  RoundRobin{1}, Worker, Pipelined)` instead of building the `Stage` literal inline.

### 2.3 File-scan rewrite moved to plan time (`rewrite_file_scans`)

`push_stage` now runs `rewrite_file_scans(plan)` after `rewrite_inputs`, and
`task_runner/core.rs` **removed** its per-task `rewrite_file_scans`. Rationale: DataFusion
file scans use process-local sibling state letting partitions steal work from a shared queue
of all file groups. In cluster mode each partition runs as an isolated task with its own
deserialized plan, so that queue would be recreated per task and every task would scan every
file. The rewrite sets:

- `FileScanConfigBuilder::from(base_config).with_preserve_order(true)` — disable sibling work
  sharing, keep each task on its own file group; and
- for `ParquetSource` (via `downcast_to_file_source::<ParquetSource>()`) where no
  expr-adapter factory is set, attach `SchemaEvolutionPhysicalExprAdapterFactory`
  (`sail_common_datafusion::schema_evolution`).

Doing it once at job-graph build means the whole stage shares the rewritten plan (and since
the rewrite is idempotent this is safe even for 0.7.1 if the task runner already rewrites).
New imports: `datafusion::catalog::memory::DataSourceExec`, `FileScanConfig{,Builder}`,
`ParquetSource`, `PhysicalExprAdapterFactory`, `SubqueryIndex`.

### 2.4 New proto nodes & codec support

**`physical.proto`** (message numbers must not collide):
- `ExtendedPhysicalPlanNode.iceberg_load_data_fast = 56` →
  `message IcebergLoadDataFastExecNode { string data_files_json = 1; string table_url = 2;
  string operation = 3; string requirements_json = 4; string table_properties_json = 5;
  string lakehouse_table_json = 6; }` (all JSON-encoded to keep the wire generic; empty
  `lakehouse_table_json` = None).
- `IcebergScanByDataFilesExecNode` gains `optional string file_path_column = 4`.

**`proto/codec.rs`:**
- Decode branch `NodeKind::IcebergLoadDataFast`: decodes JSON `Vec<DataFile>`, `Url`,
  `Operation`, `Vec<TableRequirement>`, `Vec<(String,String)>`, lakehouse via
  `try_decode_lakehouse_table`, builds `IcebergLoadDataFastExec::new(...)`.
- Encode branch mirrors it (`serde_json::to_string` on the spec objects;
  `try_encode_lakehouse_table`).
- `IcebergScanByDataFilesExec` decode/encode now use
  `IcebergScanByDataFilesExec::new_with_file_path_column(input, table_url, output_schema, file_path_column)`.
- New unit test `test_round_trip_iceberg_load_data_fast_exec` round-trips a node with a full
  `DataFile` (asserts record_count, operation, table_properties survive).

### 2.5 ScalarSubqueryExpr page-filter guard (encode side)

`contains_scalar_subquery_expr(expr)` — recursive `downcast_ref::<ScalarSubqueryExpr>()`
walk. When encoding a `ParquetSource` whose `filter()` predicate contains one, the predicate
is **dropped** before encoding:

```rust
.filter(|predicate| !contains_scalar_subquery_expr(predicate))
```

Reason (in-code doc): `ScalarSubqueryExpr` needs `ScalarSubqueryResults` context to
deserialize, which only DataFusion-proto's own codec provides inside a `ScalarSubqueryExec`;
Sail's `RemoteExecutionCodec` only receives a `TaskContext`, so a worker would fail to decode
the stage plan. (This is the `ScalarSubqueryExpr in ParquetSource page filter for remote
execution` fix.) Decode side unaffected — the dropped predicate only costs page-level
pruning on the worker, which is safe.

---

## 3. Worker pool lifecycle (`driver/worker_pool/core.rs`)

### 3.1 Delete worker resources on unreachable/failed workers

New invariant: whenever a worker ends in `Pending`→stop, fails to register in time, or its
graceful stop cannot complete, the pool asks the `WorkerManager` to `delete_worker(id)` so the
backing resource (Kubernetes pod) is actually gone. Changes inside `stop_worker`:

- `WorkerState::Pending`: mark `Completed`, spawn `worker_manager.delete_worker(id)`.
- `WorkerState::Running`: build client; on `get_client_set` error → `Failed` + delete; else
  spawn background task calling `client.stop_worker()` and, **only on stop failure**, delete.
  State becomes `Completed` immediately (as before).
- `fail_worker_if_pending(ctx, worker_id)` (new signature, takes `ctx`) — on registration
  timeout: state `Failed` with message "worker registration timeout", then spawn
  `worker_manager.delete_worker(id)`.

### 3.2 Terminal-worker pruning & count helpers

- `const MAX_TERMINAL_WORKERS_RETAINED: usize = 100`.
- `start_worker` begins with `prune_terminal_workers()`: if `Completed|Failed` descriptors
  exceed 100, `shift_remove` the oldest excess (keeps diagnostics bounded).
- `running_worker_count() -> usize` counts only `Running` workers (used by the driver's idle
  scale-down gate, §4).

### 3.3 Tests added (in-module, cfg(test))

`StubWorkerManager` (no-op launch/stop), `WorkerPoolOptions::new_for_test(...)` and
`TaskAssignerOptions::new_for_test(...)`; helpers `descriptor`, `running`, `insert`.
Tests: `test_running_worker_counts_only_running`, `test_prune_keeps_cap_and_drops_oldest_terminal`,
`test_prune_noop_under_cap`, `test_deactivate_worker_removes_entry`.

---

## 4. Driver idle scale-down gate (`driver/actor/handler.rs`)

The idle-worker teardown branch (a worker whose `get_worker_last_update <= instant`) now also
requires

```rust
&& self.worker_pool.running_worker_count() > self.options.worker_initial_count
```

so the pool never scales **below** `worker_initial_count` via idle reaping. And the
worker-start-failure path calls the new
`self.worker_pool.fail_worker_if_pending(ctx, worker_id)` (which now triggers the
resource-delete) followed by `scale_up_workers`.

---

## 5. Task assigner (`driver/task_assigner/*`)

- `deactivate_worker(worker_id)`: previously set the entry to `WorkerResource::Inactive`;
  now `self.workers.shift_remove(&worker_id)` (log `warn!` when absent). Rationale: an
  `Inactive` tombstone broke re-activation bookkeeping; removing keeps `activate_worker` able
  to insert a fresh `Active` entry (unit-tested in `test_deactivate_worker_removes_entry`).
- `WorkerResource::Inactive` variant retained but marked `#[allow(dead_code)]` as vocabulary
  for defensive matches.
- `TaskAssignerOptions::new_for_test(worker_task_slots, worker_max_count)` (test-only).

---

## 6. RPC client hardening (`rpc.rs`, clients)

### 6.1 `ClientOptions.peer`

New field `peer: String` — a human-readable label of the remote peer, e.g.
`"worker 5 at 10.0.0.3:33771"`. Populated at every construction site:
- `worker_pool/core.rs` (`WorkerClientSet::new` inside `register_worker` client build):
  `format!("worker {worker_id} at {host}:{port}")`,
- `peer_tracker/core.rs`: same pattern for peer worker clients,
- `worker/actor/core.rs`: `format!("driver {driver_id} at {host}:{port}")`.

### 6.2 Keepalive & connection tuning (client side)

`impl_client_builder!` `connect()` now applies:
`http2_keep_alive_interval(60 s)`, `keep_alive_timeout(120 s)`, `keep_alive_while_idle(true)`,
`tcp_keepalive(Some(60 s))` (constants `CLIENT_HTTP2_KEEPALIVE_INTERVAL/TIMEOUT`,
`CLIENT_TCP_KEEPALIVE`), plus existing max-header-list-size.

### 6.3 `rpc_error` + `ClientHandle::peer`

```rust
pub fn rpc_error(peer: &str, e: impl fmt::Display) -> ExecutionError {
    ExecutionError::InternalError(format!("{peer}: {e}"))
}
pub fn peer(&self) -> &str   // on ClientHandle<T>
```

Every failure through the RPC client layers is now wrapped with the peer label:
- `rpc.rs` `ClientHandle::get()` connect errors;
- `driver/client.rs` `register_worker`, `report_worker_heartbeat`,
  `report_worker_known_peers`, `report_task_status`;
- `worker/client.rs` `run_task`, `stop_task`, `clean_up_job`, `stop_worker`.

### 6.4 Flight task-stream client (`stream_service/client.rs`)

`do_get` connection + RPC errors are peer-tagged. In-stream errors are remapped: a
`FlightError::Tonic(status)` converted via `TaskStreamError` — `TaskStreamError::Unknown(msg)`
becomes `Tonic(Status::unknown("{peer}: {msg}"))`, other task-stream errors become
`ExternalError(Box::new(...))`. Goal: any task-stream failure clearly names the worker that
failed mid-stream.

---

## 7. Worker server keepalive (`worker/options.rs`, `worker/actor/*`)

- `WorkerOptions` gains `http2_keepalive_timeout: Duration`; populated from
  `config.server.http2_keepalive_timeout_secs` in the standard constructor and hard-coded
  `60 s` in the test constructor.
- `WorkerActor::on_start` passes the option into `serve(...)`, whose gRPC server
  (`worker/actor/rpc.rs`) now builds `ServerBuilderOptions { http2_keepalive_timeout:
  Some(...), ..Default::default() }` (both the worker RPC service and the Arrow Flight
  service are hosted on the same builder as before).

---

## 8. Worker manager `delete_worker`

- Trait (default, no-op): `async fn delete_worker(&self, _id: WorkerId) -> ExecutionResult<()>`.
- `KubernetesWorkerManager`: deletes pod named
  `{worker_pod_name_prefix}{name}-{id}` with `DeleteParams::default()`; a `404` API error is
  treated as success (`Ok(())`).

---

## 9. Interaction with other docs / pre-existing machinery

- The `IcebergLoadDataFastExec` / `IcebergScanByDataFilesExec` proto/codec work is exercised
  by the iceberg LOAD/write clusters (docs 07/08): the fast-register LOAD path ships pre-built
  `DataFile`s as JSON on the wire and commits them on a worker, and row-level rewrite scans
  expose a `file_path_column`.
- RPC/keepalive + session-idle-duration plumbing pairs with doc 01 §5/§7 and doc 02 §7.
- `server.session_id`/`server.http2_keepalive_timeout_secs` config keys are defined in
  doc 01 §2.1.

---

## 10. Port notes / risks

1. **Behavioral**: moving `rewrite_file_scans` from the task runner to job-graph build time
   requires 0.7.1's job-graph planner to own the rewrite (or keep the task-runner rewrite and
   skip the planner one) — verify against 0.7.1's file-scan/writer pipeline, which has diverged
   (e.g. storage-shuffle/checkpoint work).
2. **Scalar-subquery index tracking** must align with the DataFusion version on 0.7.1
   (`SubqueryIndex`, `ScalarSubqueryExpr::index()` API availability).
3. **Proto field numbers**: 56 must not collide with 0.7.1's own extension additions; check
   `physical.proto` on 0.7.1 first.
4. `file_path_column` on `IcebergScanByDataFilesExecNode` is an `optional` string in proto but
   the codec passes it through; if 0.7.1 already has a different field layout for this node,
   the whole node needs a 3-way merge.
5. Client keepalive constants (60 s ping/120 s timeout) interact with the server-side 120 s
   timeout; keep the pair consistent during the port.
6. `WorkerResource::Inactive` removal changes `TaskAssigner` semantics — check 0.7.1 assigner
   callers for `deactivate_worker`/`is_worker_idle` assumptions.
