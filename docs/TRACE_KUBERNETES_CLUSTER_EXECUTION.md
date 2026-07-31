# Complete Execution Trace: Kubernetes-Cluster Mode — Extreme Detail

## Sail v0.6.5 | DataFusion 54.0.0 | dbt-sail 0.1.0

This document traces the **complete end-to-end execution flow** of `try_cast(string_column as date)` through Sail in `kubernetes-cluster` mode, with every possible code path, every match arm, every trait default, and every fallthrough documented at the line level.

---

## Table of Contents

1. [Layer 1: Spark Connect Reception](#layer-1-spark-connect-reception)
2. [Layer 2: Session Creation — Server vs Worker](#layer-2-session-creation--server-vs-worker)
3. [Layer 3: Plan Executor — SQL to Execution Plan](#layer-3-plan-executor--sql-to-execution-plan)
4. [Layer 4: Expression Resolution — Every Cast Arm](#layer-4-expression-resolution--every-cast-arm)
5. [Layer 5: Physical Plan Creation](#layer-5-physical-plan-creation)
6. [Layer 6: Job Scheduling & Stage Splitting](#layer-6-job-scheduling--stage-splitting)
7. [Layer 7: Plan Encoding — All Plan Node Types](#layer-7-plan-encoding--all-plan-node-types)
8. [Layer 8: Protobuf Serialization of ScalarFunctionExpr](#layer-8-protobuf-serialization-of-scalarfunctionexpr)
9. [Layer 9: gRPC Transport to Workers](#layer-9-grpc-transport-to-workers)
10. [Layer 10: Worker Plan Decoding — All Expression Types](#layer-10-worker-plan-decoding--all-expression-types)
11. [Layer 11: UDF Resolution During Decode — PATH A vs PATH B](#layer-11-udf-resolution-during-decode--path-a-vs-path-b)
12. [Layer 12: RemoteExecutionCodec — Complete Encode/Decode](#layer-12-remoteexecutioncodec--complete-encodedecode)
13. [Layer 13: All Trait Implementations & Overrides](#layer-13-all-trait-implementations--overrides)
14. [Layer 14: Worker Session & TaskContext](#layer-14-worker-session--taskcontext)
15. [Layer 15: Error Propagation — Every Failure Path](#layer-15-error-propagation--every-failure-path)
16. [Layer 16: The spark_date Bug — Verified Root Cause](#layer-16-the-spark_date-bug--verified-root-cause)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client (dbt/PySpark)                        │
│                         sc://{host}:{port}                           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ gRPC (Spark Connect)
┌───────────────────────────────┼─────────────────────────────────────┐
│            K8s Cluster (smartreg-exp)                                │
│                                │                                    │
│  ┌─────────────────────────────┼──────────────────────────────────┐ │
│  │  Driver Pod (sail-spark-server)                                 │ │
│  │                             ▼                                   │ │
│  │  SparkConnectServer ──► SessionManager ──► SessionContext       │ │
│  │                                                  │              │ │
│  │                                   ServerSessionFactory          │ │
│  │                                   (NO .with_default_features()) │ │
│  │                                                  │              │ │
│  │                                           ClusterJobRunner      │ │
│  │                                                  │              │ │
│  │                                           DriverActor           │ │
│  │                                     ┌────────┴────────┐        │ │
│  │                                JobScheduler      WorkerPool     │ │
│  │                                (encode plan)    (dispatch task)  │ │
│  └────────────────────────────────────┼───────────────┼────────────┘ │
│                                       │               │              │
│                             K8s API ──┘               └── gRPC ──┐  │
│                                       │                          │  │
│  ┌────────────────────────────────────┼──────────────────────────┼─┐ │
│  │  Worker Pod                       │                          │ │ │
│  │  ┌─────────────────────────────────┼──────────────────────────┼─┤ │
│  │  │ WorkerActor                    │                          │ │ │
│  │  │ ┌────────────┐                │                          │ │ │
│  │  │ │ TaskRunner │                │                          │ │ │
│  │  │ │ codec:     │  ── decode ←───┘                          │ │ │
│  │  │ │ RemoteExec │                                            │ │ │
│  │  │ │ utionCodec │  ── execute → dataframe                    │ │ │
│  │  │ └────────────┘                                            │ │ │
│  │  │ ┌────────────┐                                            │ │ │
│  │  │ │ Worker     │                                            │ │ │
│  │  │ │ Session    │  .with_default_features() = 247 functions  │ │ │
│  │  │ │ Factory    │                                            │ │ │
│  │  │ └────────────┘                                            │ │ │
│  │  └───────────────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘

KEY:
  Driver session: NO .with_default_features() — Sail's own function registry + custom UDFs
  Worker session: .with_default_features() — ALL DataFusion built-ins (247) for PATH B decode
  Codec exercised: ONLY in kubernetes-cluster and local-cluster (NOT in local/single-process)
  RemoteExecutionCodec: Created as Box::new(RemoteExecutionCodec) in TaskRunner::new()
  Vtable dispatch: &dyn PhysicalExtensionCodec → RemoteExecutionCodec (NEVER DefaultPhysicalExtensionCodec)
```

---

## Layer 1: Spark Connect Reception

### 1.1 gRPC Entry Point

**File:** `crates/sail-spark-connect/src/server.rs:124-161`

```rust
async fn execute_plan(
    &self,
    request: Request<ExecutePlanRequest>,
) -> Result<Response<Self::ExecutePlanStream>, Status> {
    let request = request.into_inner();
    // ...
    let Plan { op_type: op } = request.plan.required("plan")?;
    let op = op.required("plan op")?;
    let stream = match op {
        plan::OpType::Root(relation) =>
            service::handle_execute_relation(&ctx, relation, metadata).await?,
        plan::OpType::Command(Command { command_type: command }) =>
            handle_command(&ctx, command, metadata).await?,
        plan::OpType::CompressedOperation(_) =>
            return Err(Status::unimplemented("compressed operation plan")),
    };
    Ok(Response::new(stream))
}
```

Three op_type variants: Root (show/collect), Command (SQL/DDL/write), CompressedOperation (unimplemented).

### 1.2 Command Dispatcher — All 16 Command Types

**File:** `crates/sail-spark-connect/src/server.rs:54-116`

| # | CommandType | Handler | Mode |
|---|------------|---------|------|
| 1 | `RegisterFunction(udf)` | handle_execute_register_function | EagerSilent |
| 2 | `WriteOperation(write)` | handle_execute_write_operation | EagerSilent |
| 3 | `CreateDataframeView(view)` | handle_execute_create_dataframe_view | EagerSilent |
| 4 | `WriteOperationV2(write)` | handle_execute_write_operation_v2 | EagerSilent |
| 5 | **`SqlCommand(sql)`** | **handle_execute_sql_command** | — |
| 6 | `WriteStreamOperationStart(start)` | handle_execute_write_stream_operation_start | Streaming |
| 7 | `StreamingQueryCommand(stream)` | handle_execute_streaming_query_command | Streaming |
| 8 | `GetResourcesCommand(resource)` | TODO → err | — |
| 9 | `StreamingQueryManagerCommand(command)` | handle_execute_streaming_query_manager_command | Streaming |
| 10 | `RegisterTableFunction(udtf)` | handle_execute_register_table_function | EagerSilent |
| 11 | `StreamingQueryListenerBusCommand(command)` | NotImplemented | — |
| 12 | `RegisterDataSource(ds)` | handle_execute_register_datasource | Python DS |
| 13 | `CreateResourceProfileCommand(_)` | SparkError::todo | — |
| 14 | `CheckpointCommand(checkpoint)` | handle_execute_checkpoint_command | TODO |
| 15 | `RemoveCachedRemoteRelationCommand(_)` | SparkError::todo | — |
| 16 | `MergeIntoTableCommand(command)` | handle_execute_merge_into_table_command | EagerSilent |
| 17 | `MlCommand(_)` | SparkError::todo | — |
| 18 | `ExecuteExternalCommand(_)` | SparkError::todo | — |
| 19 | `PipelineCommand(_)` | SparkError::todo | — |
| 20 | `Extension(_)` | SparkError::todo | — |

---

## Layer 2: Session Creation — Server vs Worker

### 2.1 ServerSessionFactory — NO default features

**File:** `crates/sail-session/src/session_factory/server.rs:90-204`

The server session is built WITHOUT `.with_default_features()`. Sail manages its own function registry via `SparkSessionMutator`. Key lifecycle:

```
ServerSessionFactory::create(info)
  ├── create_session_config() ── JobService(job_runner), ActivityTracker, CatalogManager
  └── create_session_state() ── SessionStateBuilder
      ├── .with_config(config)
      ├── .with_runtime_env(runtime)
      ├── .with_optimizer_rules(default_optimizer_rules())
      ├── .with_physical_optimizer_rules(get_physical_optimizers())
      ├── .with_query_planner(new_query_planner())    // ExtensionQueryPlanner
      └── mutator.mutate_state(builder)                // Register Spark UDFs
```

### 2.2 ExecutionMode → JobRunner Selection

**File:** `crates/sail-session/src/session_factory/server.rs:158-204`

| Mode | JobRunner | WorkerManager | Worker Session |
|------|-----------|---------------|----------------|
| `Local` | `LocalJobRunner` | N/A (in-process) | N/A |
| `LocalCluster` | `ClusterJobRunner` | `LocalWorkerManager` | `WorkerSessionFactory.create(())` |
| `KubernetesCluster` | `ClusterJobRunner` | `KubernetesWorkerManager` | `sail worker` subcommand in pod |

In `KubernetesCluster`, the `KubernetesWorkerManager` is configured with image, namespace, driver_pod_name, and worker_pod_template from `SAIL_KUBERNETES__*` env vars.

### 2.3 WorkerSessionFactory — WITH default features

**File:** `crates/sail-session/src/session_factory/worker.rs:44-74`

```rust
fn create(&mut self, _info: ()) -> Result<SessionContext> {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()   // ← ALL 247 built-in functions
        .build();
    Ok(SessionContext::new_with_state(state))
}
```

Worker sessions get ALL DataFusion built-ins for PATH B decode (registering upper, replace, extract, etc. from the plan codec). Worker sessions do NOT register JobService, ActivityTracker, or SystemTableService — these are server-only.

### 2.4 SessionManager — get_or_create_session_context

**File:** `crates/sail-session/src/session_manager/mod.rs:34-48`

Uses actor-based session lifecycle:
```
get_or_create_session_context(session_id, user_id)
  → SessionManagerEvent::GetOrCreateSession → ActorHandle
    → SessionManagerActor::handle_get_or_create_session
      ├── Check cache: if Running → return cached SessionContext
      ├── If stale/absent: self.factory.create(info)
      │   └── ServerSessionFactory::create
      ├── Insert ServerSession { state: Running, ... } into sessions map
      └── Schedule idle probe (sessions expire after timeout)
```

---

## Layer 3: Plan Executor — SQL to Execution Plan

### 3.1 ExecutePlanMode enum

**File:** `crates/sail-spark-connect/src/service/plan_executor.rs:113-119`

```rust
enum ExecutePlanMode {
    Lazy,         // Stream responses as client reads
    EagerSilent,  // Execute immediately, return empty stream
}
```

### 3.2 handle_execute_plan — Central Pipeline

**File:** `crates/sail-spark-connect/src/service/plan_executor.rs:121-162`

```
handle_execute_plan(ctx, plan, metadata, mode)
  ├── SparkSession, JobService extensions
  ├── resolve_and_execute_plan(ctx, spark.plan_config(), plan)
  │   └── Returns (Arc<dyn ExecutionPlan>, StringifiedPlan[])
  ├── service.runner().execute(ctx, plan)  // submits to JobRunner
  │   └── Returns SendableRecordBatchStream
  └── mode dispatch:
      ├── Lazy → Executor { stream, rx }, start(), register
      └── EagerSilent → read_stream(stream), emit completion
```

### 3.3 handle_execute_sql_command — SQL Entry Point

**File:** `crates/sail-spark-connect/src/service/plan_executor.rs:226-292`

```
handle_execute_sql_command(ctx, sql, metadata)
  ├── Determine plan source:
  │   ├── sql.input.Some(Sql { query }) → parse_one_statement(query) → from_ast_statement
  │   ├── sql.input.Some(other) → try_into spec::Plan
  │   └── sql.input.None → parse_one_statement(sql.sql) → from_ast_statement
  ├── Match spec::Plan:
  │   ├── Query(inner) with relation → return relation as-is (PySpark processes)
  │   ├── Query(inner) without relation → build Sql relation wrapper
  │   └── Command(inner) → resolve_and_execute + read_stream + concat to LocalRelation
  └── Wrap as SqlCommandResult → ExecutePlanResponseStream
```

### 3.4 resolve_and_execute_plan — Logical → Physical Pipeline

**File:** `crates/sail-plan/src/lib.rs:34-66`

```
resolve_and_execute_plan(ctx, config, plan)
  ├── PlanResolver::resolve_named_plan(plan)    // spec::Plan → LogicalPlan
  │   ├── Query → resolve_query_plan → fields extracted
  │   └── Command → resolve_command_plan → fields: None
  ├── log: InitialLogicalPlan
  ├── ctx.execute_logical_plan(plan)             // DataFusion: DataFrame execution
  ├── session_state.optimize(&plan)              // All optimizer rules
  ├── is_streaming? → rewrite_streaming_plan     // Streaming rewrite
  ├── log: FinalLogicalPlan
  ├── query_planner.create_physical_plan(...)    // Logical → Physical
  ├── rename_physical_plan?                      // Rename output columns
  ├── log: FinalPhysicalPlan
  └── return (physical_plan, stringified_plans)
```

---

## Layer 4: Expression Resolution — Every Cast Arm

### 4.1 resolve_expression_cast — Entry Point

**File:** `crates/sail-plan/src/resolver/expression/cast.rs:30-211`

Called from `resolve_expression` when an `Expr::Cast { expr, cast_to_type, is_try }` is encountered. The full (expr_type, cast_to_type, is_try) match has these arms in priority order:

| # | Pattern | Spark UDF Created | Lines |
|---|---------|-------------------|-------|
| 1 | `(_, Utf8, _)` when expr_is_variant | `SparkVariantToJsonUdf` + `cast(_, Utf8)` | 105-108 |
| 2 | `(_, LargeUtf8, _)` when expr_is_variant | `SparkVariantToJsonUdf` + `cast(_, LargeUtf8)` | 109-112 |
| 3 | `(_, Utf8View, _)` when expr_is_variant | `SparkVariantToJsonUdf` | 113-115 |
| 4 | `(_, to, is_try)` when expr_is_variant | `SparkVariantGet::new(is_try)` with `lit("$")` | 116-124 |
| 5 | `(from, Timestamp\|Duration, _)` when numeric | Multiply + cast with time unit multiplier | 125-133 |
| 6 | `(Timestamp\|Duration, to, _)` when numeric | Div + mul + cast | 134-143 |
| 7 | `(Utf8\|LargeUtf8\|Utf8View, Interval(YearMonth), _)` | `SparkYearMonthInterval` | 144-148 |
| 8 | `(Utf8\|LargeUtf8\|Utf8View, Duration(Microsecond), _)` | `SparkDayTimeInterval` | 149-153 |
| 9 | `(Utf8\|LargeUtf8\|Utf8View, Interval(MonthDayNano), _)` | `SparkCalendarInterval` | 154-158 |
| **10** | **`(Utf8\|LargeUtf8\|Utf8View, Date32, is_try)`** | **`SparkDate::new(is_try)`** | **159-163** |
| 11 | `(Utf8\|LargeUtf8\|Utf8View, Timestamp(Microsecond, tz), is_try)` | `SparkTimestamp::try_new(tz, ansi_mode, is_try)` | 164-173 |
| 12 | `(_, Utf8, _)` when override_string_cast | `SparkToUtf8` | 174-176 |
| 13 | `(_, LargeUtf8, _)` when override_string_cast | `SparkToLargeUtf8` | 177-179 |
| 14 | `(_, Utf8View, _)` when override_string_cast | `SparkToUtf8View` | 180-182 |
| 15 | `(Date32\|Date64, to, _)` when to is numeric or Boolean | Null (err in ANSI mode) | 183-189 |
| 16 | `(from, to, _)` when needs_struct_field_rename | `SparkStructRename` + conditional cast | 191-206 |
| 17 | `(_, to, true)` | `try_cast(expr, to)` fallback | 207 |
| 18 | `(_, to, _)` | `cast(expr, to)` fallback | 208 |

### 4.2 Arm #10 — SparkDate creation (line 163)

```rust
(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, DataType::Date32, is_try)
    => ScalarUDF::new_from_impl(SparkDate::new(is_try)).call(vec![expr]),
```

When `is_try = true` (from `try_cast`): `SparkDate::new(true)` → on parse failure, returns NULL (line 43: `Err(_e) if is_try => Ok(None)`).

When `is_try = false` (from `cast`): `SparkDate::new(false)` → on parse failure, throws error (line 44: `Err(e) => Err(exec_datafusion_err!("{e}"))`).

### 4.3 SparkDate Implementation

**File:** `crates/sail-function/src/scalar/datetime/spark_date.rs`

```rust
pub struct SparkDate {
    signature: Signature,
    is_try: bool,
}

fn string_to_date32(value: &str, is_try: bool) -> Result<Option<i32>> {
    match parse_date(value).and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?))) {
        Ok(v) => Ok(Some(v)),
        Err(_e) if is_try => Ok(None),      // try_cast: return NULL
        Err(e) => Err(exec_datafusion_err!("{e}")), // cast: throw error
    }
}
```

The `parse_date` function comes from `sail_sql_analyzer::parser::parse_date` (line 117-119 of the parser.rs). It accepts date strings in Spark-compatible formats.

### 4.4 override_string_cast calculation (lines 87-103)

`override_string_cast` is true when the source type is: `Date32`, `Date64`, `Time32`, `Time64`, `Duration`, `Interval`, `Timestamp`, `List`, `FixedSizeList`, `LargeList`, `Struct`, or `Map`. This triggers custom Spark-compatible string conversion via `SparkToUtf8`/`SparkToLargeUtf8`/`SparkToUtf8View` instead of DataFusion's default.

---

## Layer 5: Physical Plan Creation

### 5.1 ExtensionQueryPlanner — 8 Extension Planners

**File:** `crates/sail-session/src/planner.rs:54-86`

```rust
let planners: Vec<Arc<dyn PhysicalPlanner>> = vec![
    Arc::new(DeltaPhysicalPlanner),
    Arc::new(IcebergPhysicalPlanner),
    Arc::new(SystemTablePhysicalPlanner),
    Arc::new(ListingPhysicalPlanner),
    Arc::new(ConsolePhysicalPlanner),
    Arc::new(NoopPhysicalPlanner),
    Arc::new(PythonPhysicalPlanner),
    Arc::new(ExtensionPhysicalPlanner),  // Handles Sail-specific nodes
];
DefaultPhysicalPlanner::with_extension_planners(planners)
```

### 5.2 ExtensionPhysicalPlanner — All 16 Plan Node Types

**File:** `crates/sail-session/src/planner.rs:88-259`

| # | Logical Node | Physical Node |
|---|-------------|---------------|
| 1 | `RangeNode` | `RangeExec` |
| 2 | `ShowStringNode` | `ShowStringExec` |
| 3 | `MapPartitionsNode` | `MapPartitionsExec` |
| 4 | `MonotonicIdNode` | `MonotonicIdExec` |
| 5 | `SparkPartitionIdNode` | `SparkPartitionIdExec` |
| 6 | `SortWithinPartitionsNode` | `SortExec(preserve_partitioning=true)` |
| 7 | `SchemaPivotNode` | `SchemaPivotExec` |
| 8 | `ExplicitRepartitionNode` | `ExplicitRepartitionExec` |
| 9 | `StreamSourceAdapterNode` | `StreamSourceAdapterExec` |
| 10 | `StreamSourceWrapperNode` | Source's `.scan()` + rename |
| 11 | `StreamLimitNode` | `StreamLimitExec` |
| 12 | `StreamFilterNode` | `StreamFilterExec` |
| 13 | `StreamCollectorNode` | `StreamCollectorExec` |
| 14 | `CatalogCommandNode` | `CatalogCommandExec` |
| 15 | `BarrierNode` | `BarrierExec` (or identity if no preconditions) |
| 16 | **Fallthrough** | `plan_err!("unsupported logical extension node")` |

### 5.3 All 26 Physical Optimizer Rules

**File:** `crates/sail-physical-optimizer/src/lib.rs:41-76`

Applied in order:

| # | Rule | Purpose |
|---|------|---------|
| 1 | `OutputRequirements::new_add_mode()` | Add ordering/distribution requirements |
| 2 | `AggregateStatistics::new()` | Compute aggregate statistics |
| 3 | `JoinReorder::new()` *(optional)* | Sail-custom join reorder |
| 4 | `JoinSelection::new()` | Hash vs sort-merge join |
| 5 | `LimitedDistinctAggregation::new()` | Optimize DISTINCT |
| 6 | `FilterPushdown::new()` | Push filters toward leaves |
| 7 | `EnforceDistribution::new()` | Add repartition |
| 8 | `CombinePartialFinalAggregate::new()` | Combine partial+final |
| 9 | `EnforceSorting::new()` | Add sort |
| 10 | `OptimizeAggregateOrder::new()` | Reorder aggregate exprs |
| 11 | `WindowTopN::new()` | Top-N for window |
| 12 | `ProjectionPushdown::new()` (1st) | Push projections |
| 13 | `OutputRequirements::new_remove_mode()` | Remove requirements |
| 14 | `TopKAggregation::new()` | Top-K aggregation |
| 15 | `LimitPushPastWindows::new()` | Push limit past window |
| 16 | `HashJoinBuffering::new()` | Configure join buffering |
| 17 | `LimitPushdown::new()` | Push limit |
| 18 | `TopKRepartition::new()` | Top-K repartition |
| 19 | `ProjectionPushdown::new()` (2nd) | Second projection push |
| 20 | `PushdownSort::new()` | Push sorts |
| 21 | `EnsureCooperative::new()` | Cooperative scheduling |
| 22 | `FilterPushdown::new_post_optimization()` | Post-opt filter |
| 23 | `RewriteExplicitRepartition::new()` | **Sail-custom** repartition |
| 24 | `RewriteCollectLeftHashJoin::new()` | **Sail-custom** join rewrite |
| 25 | `EnforceBarrierPartitioning::new()` | **Sail-custom** barrier |
| 26 | `SanityCheckPlan::new()` | Final sanity check |

---

## Layer 6: Job Scheduling & Stage Splitting

### 6.1 ClusterJobRunner — Job Submission

**File:** `crates/sail-execution/src/job_runner.rs:107-124`

```
ClusterJobRunner::execute(ctx, plan)
  ├── oneshot channel (tx, rx)
  ├── driver.send(DriverEvent::ExecuteJob { plan, context, result: tx })
  └── rx.await??
```

Error propagation: Send error → `internal_datafusion_err`, Receive error → `"failed to create job stream: {e}"`, Job failure → wraps `ExecutionError`.

### 6.2 handle_execute_job — Driver Event Handler

**File:** `crates/sail-execution/src/driver/actor/handler.rs:173-188`

```
handle_execute_job(ctx, plan, context, result)
  ├── job_scheduler.accept_job(ctx, plan, context)  // decompose into stages
  ├── refresh_job(ctx, job_id)   // trigger task scheduling
  ├── run_tasks(ctx)             // assign pending tasks
  ├── scale_up_workers(ctx)      // request workers if needed
  └── result.send(out.map(|(_, stream)| stream))  // stream back to caller
```

### 6.3 JobAction — All 5 Action Types

**File:** `crates/sail-execution/src/driver/job_scheduler/mod.rs:38-58`

```rust
pub enum JobAction {
    ScheduleTaskRegion { region: TaskRegion },  // enqueue tasks
    CancelTask { key: TaskKey },                // cancel running task
    ExtendJobOutput { handle, key, schema },    // add output channels
    FailJobOutput { handle, cause },            // fail job
    CleanUpJob { job_id, stage },               // remove resources
}
```

### 6.4 refresh_job — Full Scheduler Lifecycle

**File:** `crates/sail-execution/src/driver/job_scheduler/core.rs:118-166`

```
refresh_job(job_id) → Vec<JobAction>
  ├── cascade_cancel_task_attempts()     → [CancelTask]
  ├── extend_job_output()               → [ExtendJobOutput]
  ├── clean_up_job_by_stage()           → [CleanUpJob]
  ├── update_task_regions()             → marks Running/Succeeded/Failed
  ├── Any Failed → [FailJobOutput], State::Failed
  ├── All Succeeded → State::Draining
  └── schedule_task_regions()           → [ScheduleTaskRegion]
```

### 6.5 get_task_definition — Plan Serialization Entry Point

**File:** `crates/sail-execution/src/driver/job_scheduler/core.rs:503-540`

```
get_task_definition(key, assignments) → (TaskDefinition, Arc<TaskContext>)
  ├── lookup job → verify Running
  ├── lookup stage → try_encode_physical_plan(self.codec.as_ref(), stage.plan)
  │   └── self.codec = Box::new(RemoteExecutionCodec) ← line 33
  ├── build inputs from stage.inputs
  ├── build output from stage schema
  └── TaskDefinition { plan, inputs, output }
```

### 6.6 JobGraph::try_new — Stage Decomposition

**File:** `crates/sail-execution/src/job_graph/planner.rs:33-53`

```
try_new(plan)
  ├── ensure_single_input_partition_for_global_limit(plan)
  ├── ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(plan)
  ├── build_job_graph(plan, PartitionUsage::Once, graph)  // recursive
  ├── rewrite_inputs(last)  // StageInputExec → index references
  └── final Stage { placement: Worker, output: RoundRobin{1} }
```

### 6.7 build_job_graph — Stage Boundary Logic

**File:** `crates/sail-execution/src/job_graph/planner.rs:176-312`

Stage boundaries created at these plan node types:

| Node Type | Stage Split | Input Mode | Placement |
|-----------|------------|------------|-----------|
| `RepartitionExec` (not order-preserving) | create_shuffle | Shuffle/Broadcast | Worker |
| `ExplicitRepartitionExec` | create_row_shuffle | Shuffle/Broadcast | Worker |
| `CoalescePartitionsExec` | create_shuffle | Shuffle/Broadcast | Worker |
| `SortPreservingMergeExec` | create_merge_input | Merge | Worker |
| `CoalesceExec` | create_rescale_input | Rescale | Worker |
| `SystemTableExec`, `CatalogCommandExec`, `FileDeleteExec` | create_driver_stage | Forward | **Driver** |
| `IcebergDeleteExec`, `IcebergUpdateExec`, `IcebergCompactExec` | create_driver_stage | Forward | **Driver** |
| `IcebergCommitExec`, `DeltaCommitExec` | create_driver_stage | Forward | **Driver** |
| All other nodes | pass-through | — | Worker |

### 6.8 Image and RUST_LOG for Worker Pods

**File:** `crates/sail-execution/src/worker_manager/kubernetes.rs:227-281`

Workers are launched as K8s Pods:
```rust
Container {
    command: Some(vec!["sail".to_string()]),
    args: Some(vec!["worker".to_string()]),
    env: Some(self.build_pod_env(id, options)),
    image: Some(self.options.image.clone()),
    image_pull_policy: Some(self.options.image_pull_policy.clone()),
}
```

`build_pod_env` (lines 113-222) passes RUST_LOG from the driver process:
```rust
EnvVar {
    name: "RUST_LOG".to_string(),
    value: Some(env::var("RUST_LOG").unwrap_or("info".to_string())),
}
```

---

## Layer 7: Plan Encoding — All Plan Node Types

### 7.1 Encoding Entry Point

**File:** `crates/sail-execution/src/proto/encode.rs:37-53`

```rust
pub fn try_encode_physical_plan(codec, plan) -> Result<Vec<u8>> {
    try_encode_message(physical_plan_to_proto(codec, plan)?)
}

pub fn physical_plan_to_proto(codec, plan) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan_with_converter(
        plan, codec, &RemotePhysicalProtoConverter {},
    )
}
```

### 7.2 datafusion-proto: All 31 Standard Plan Node Serializations

**File:** `datafusion-proto-54.0.0/mod.rs:418-697`

`try_from_physical_plan_with_converter` handles these plan node types before falling back to `codec.try_encode()`:

| # | Plan Node | Method | Line |
|---|-----------|--------|------|
| 1 | `ExplainExec` | try_from_explain_exec | 429 |
| 2 | `ProjectionExec` | try_from_projection_exec | 433 |
| 3 | `AnalyzeExec` | try_from_analyze_exec | 441 |
| 4 | `FilterExec` | try_from_filter_exec | 449 |
| 5 | `GlobalLimitExec` | try_from_global_limit_exec | 457 |
| 6 | `LocalLimitExec` | try_from_local_limit_exec | 465 |
| 7 | `HashJoinExec` | try_from_hash_join_exec | 473 |
| 8 | `SymmetricHashJoinExec` | try_from_symmetric_hash_join_exec | 481 |
| 9 | `SortMergeJoinExec` | try_from_sort_merge_join_exec | 489 |
| 10 | `CrossJoinExec` | try_from_cross_join_exec | 497 |
| 11 | `AggregateExec` | try_from_aggregate_exec | 505 |
| 12 | `EmptyExec` | try_from_empty_exec | 513 |
| 13 | `PlaceholderRowExec` | try_from_placeholder_row_exec | 517 |
| 14 | `CoalesceBatchesExec` | try_from_coalesce_batches_exec | 524 |
| 15 | `DataSourceExec` | try_from_data_source_exec | 532 |
| 16 | `CoalescePartitionsExec` | try_from_coalesce_partitions_exec | 542 |
| 17 | `RepartitionExec` | try_from_repartition_exec | 550 |
| 18 | `SortExec` | try_from_sort_exec | 558 |
| 19 | `UnionExec` | try_from_union_exec | 566 |
| 20 | `InterleaveExec` | try_from_interleave_exec | 574 |
| 21 | `SortPreservingMergeExec` | try_from_sort_preserving_merge_exec | 582 |
| 22 | `NestedLoopJoinExec` | try_from_nested_loop_join_exec | 590 |
| 23 | `WindowAggExec` | try_from_window_agg_exec | 598 |
| 24 | `BoundedWindowAggExec` | try_from_bounded_window_agg_exec | 606 |
| 25 | `DataSinkExec` | try_from_data_sink_exec | 614 |
| 26 | `UnnestExec` | try_from_unnest_exec | 624 |
| 27 | `CooperativeExec` | try_from_cooperative_exec | 632 |
| 28 | `LazyMemoryExec` | try_from_lazy_memory_exec | 640 |
| 29 | `AsyncFuncExec` | try_from_async_func_exec | 647 |
| 30 | `BufferExec` | try_from_buffer_exec | 655 |
| 31 | `ScalarSubqueryExec` | try_from_scalar_subquery_exec | 663 |

**Fallback** (lines 671-696): When none of the above matches, calls `codec.try_encode(node, &mut buf)` → `RemoteExecutionCodec::try_encode`. This is entered via `PhysicalPlanType::Extension(PhysicalExtensionNode)`.

### 7.3 RemoteExecutionCodec::try_encode — All 60+ Sail Plan Nodes

**File:** `crates/sail-execution/src/proto/codec.rs:1592-2497`

A massive `if/else if` chain downcasting every Sail-specific ExecutionPlan node. Key nodes:

| Node | Proto NodeKind | Line |
|------|---------------|------|
| `RangeExec` | `Range` | 1593 |
| `ShowStringExec` | `ShowString` | 1604 |
| `StageInputExec` | `StageInput` | 1614 |
| `MapPartitionsExec` | `MapPartitions` | 1657 |
| `CatalogCommandExec` | `CatalogCommand` | 2291 |
| `FileDeleteExec` | `FileDelete` | 2296 |
| `BarrierExec` | `Barrier` | 2301 |
| `IcebergWriterExec` | `IcebergWriter` | 2184 |
| `IcebergCommitExec` | `IcebergCommit` | 2209 |
| `IcebergDiscoveryExec` | `IcebergDiscovery` | 2224 |
| `IcebergDeleteExec` | `IcebergDelete` | 2312 |
| `IcebergUpdateExec` | `IcebergUpdate` | 2368 |
| `IcebergMergeExec` | `IcebergMerge` | 2465 |
| `DeltaWriterExec` | `DeltaWriter` | 1837 |
| `DeltaCommitExec` | `DeltaCommit` | 1856 |
| `PythonDataSourceExec` | `PythonDataSource` | 2258 |
| `SystemTableExec` | `SystemTable` | 1632 |
| **Fallthrough** | **Error** | 2489 |

---

## Layer 8: Protobuf Serialization of ScalarFunctionExpr

### 8.1 Expression Serialization Entry

**File:** `datafusion-proto-54.0.0/to_proto.rs:262-598`

`serialize_physical_expr_with_converter` handles all expression types:

| Expression | Proto Type | Method |
|-----------|-----------|--------|
| `Column` | `PhysicalColumn` | column serialization |
| `BinaryExpr` | `PhysicalBinaryExprNode` | linearized operands |
| `Literal` | ScalarValue | scalar serialization |
| `CastExpr` | `PhysicalCastNode` | 466-483 |
| `TryCastExpr` | `PhysicalTryCastNode` | 484-493 |
| `LikeExpr` | `PhysicalLikeExprNode` | 503-518 |
| **`ScalarFunctionExpr`** | **`PhysicalScalarUdfNode`** | **484-502** |
| Extension fallback | `PhysicalExtensionExprNode` | codec.try_encode_expr() |

### 8.2 ScalarFunctionExpr Serialization — The Critical Path

**File:** `datafusion-proto-54.0.0/to_proto.rs:484-502`

```rust
} else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
    let mut buf = Vec::new();                              // EMPTY buffer
    codec.try_encode_udf(expr.fun(), &mut buf)?;          // ← CALLS RemoteExecutionCodec
    Ok(protobuf::PhysicalExprNode {
        expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
            protobuf::PhysicalScalarUdfNode {
                name: expr.name().to_string(),             // "spark_date"
                args: serialize_physical_exprs(...)?,
                fun_definition: (!buf.is_empty()).then_some(buf), // ← KEY: Some/None
                return_type: Some(expr.return_type()?),
                nullable: expr.nullable(),
                return_field_name: expr.return_field(&Schema::empty())?.name(),
            },
        )),
    })
}
```

**The `fun_definition` field is the fork in the road:**
- `Some(buf)`: Encoded `ExtendedScalarUdf` protobuf → PATH A at decode (codec.try_decode_udf)
- `None`: Empty buffer → PATH B at decode (task_ctx.udf register, then codec fallback)

### 8.3 RemoteExecutionCodec::try_encode_udf — Every Branch

**File:** `crates/sail-execution/src/proto/codec.rs:3435-3719`

Three phases of UDF encoding:

**Phase 1** (lines 3438-3558): ~140 type checks via `.is::<T>()` → `UdfKind::Standard`:
```rust
let udf_kind: UdfKind = if node_inner.is::<ArrayItemWithPosition>()
    || node_inner.is::<SparkArray>()
    || node_inner.is::<SparkMapToArray>()
    // ... 100+ more type checks ...
{
    UdfKind::Standard(gen_::StandardUdf {})
}
```

**Phase 2** (lines 3559-3699): Specific `downcast_ref` checks for UDFs with custom parameters:

| UDF Type | Extracted Parameter | Proto Variant | Line |
|----------|-------------------|---------------|------|
| `PySparkUDF` | kind, name, payload, deterministic, types, config | `UdfKind::PySpark` | 3559-3576 |
| `PySparkCoGroupMapUDF` | key_types, op_type, payload, ... | `UdfKind::PySparkCoGroupMap` | 3577-3601 |
| `DropStructField` | field_names | `UdfKind::DropStructField` | 3602-3604 |
| `Explode` | name | `UdfKind::Explode` | 3605-3607 |
| `XpathTyped` | name | `UdfKind::XpathTyped` | 3608-3610 |
| `SparkToXml` | session_timezone | `UdfKind::SparkToXml` | 3611-3613 |
| `SparkUnixTimestamp` | timezone | `UdfKind::SparkUnixTimestamp` | 3614-3616 |
| `StructFunction` | field_names | `UdfKind::StructFunction` | 3617-3619 |
| `ArraysZip` | field_names | `UdfKind::ArraysZip` | 3620-3622 |
| `UpdateStructField` | field_names | `UdfKind::UpdateStructField` | 3623-3625 |
| `TimestampNow` | timezone, time_unit | `UdfKind::TimestampNow` | 3626-3633 |
| `SparkTimestamp` | timezone, is_try, ansi_mode | `UdfKind::SparkTimestamp` | 3634-3642 |
| **`SparkDate`** | **is_try** | **`UdfKind::SparkDate`** | **3643-3645** |
| `SparkTime` | is_try | `UdfKind::SparkTime` | 3646-3648 |
| `SparkVariantGet` | safe | `UdfKind::SparkVariantGet` | 3649-3651 |
| `SparkParseJson` | safe | `UdfKind::SparkParseJson` | 3652-3654 |
| `SparkFromCSV` | session_timezone | `UdfKind::SparkFromCsv` | 3655-3657 |
| `SparkToCsv` | session_timezone | `UdfKind::SparkToCsv` | 3658-3660 |
| `SparkFromJson` | session_timezone | `UdfKind::SparkFromJson` | 3661-3663 |
| `SparkNextDay` | ansi_mode | `UdfKind::SparkNextDay` | 3664-3666 |
| `SparkWindowBuckets` | window_duration | `UdfKind::SparkWindowBuckets` | 3667-3672 |
| `SparkToNumber` | safe | `UdfKind::SparkToNumber` | 3673-3675 |
| `SparkToChar` | ansi_mode | `UdfKind::SparkToChar` | 3676-3678 |
| `SparkAbs` | ansi_mode | `UdfKind::SparkAbs` | 3679-3681 |
| `SparkBin` | ansi_mode | `UdfKind::SparkBin` | 3682-3684 |
| `SparkPmod` | ansi_mode | `UdfKind::SparkPmod` | 3685-3687 |
| `SparkNegative` | ansi_mode | `UdfKind::SparkNegative` | 3688-3690 |
| `SparkMakeTimestampNtz` | is_try | `UdfKind::SparkMakeTimestampNtz` | 3691-3693 |
| `ConvertTz` | classic | `UdfKind::ConvertTz` | 3694-3696 |
| `SparkStructRename` | target_type | `UdfKind::SparkStructRename` | 3697-3699 |

**Phase 3** (lines 3700-3706): Fallthrough → `return Ok(())` — empty buffer, used for all DataFusion built-ins (upper, replace, extract, etc.).

**Serialization** (lines 3707-3718): If any of the above matched, writes `ExtendedScalarUdf { udf_kind: Some(udf_kind) }` to the buffer.

### 8.4 ExtendedScalarUdf Protobuf

The Sail-specific protobuf that wraps every UDF serialization:

```protobuf
message ExtendedScalarUdf {
    oneof udf_kind {
        StandardUdf standard = 1;
        PySparkUdf pyspark = 2;
        PySparkCoGroupMapUdf pyspark_cogroup_map = 3;
        // ... 30+ variants ...
        SparkDateUdf spark_date = 14;      // ← field number 14
        SparkTimeUdf spark_time = 15;
        SparkTimestampUdf spark_timestamp = 13;
        // ...
    }
}
```

### 8.5 Aggregate and Window Function Serialization

**Aggregate** (to_proto.rs lines 57-94):
```rust
codec.try_encode_udaf(aggr_expr.fun(), &mut buf)?;
// fun_definition: (!buf.is_empty()).then_some(buf)
```

**Window** (to_proto.rs lines 96-187): Aggregates use `try_encode_udaf()`, window UDFs use `try_encode_udwf()`. `try_encode_udwf` handles `SparkNtile` (`StandardUdwf`) and `SparkFirstLastValue` (`SparkFirstLastValueUdwf`).

---

## Layer 9: gRPC Transport to Workers

### 9.1 Driver Side — WorkerClient::run_task

**File:** `crates/sail-execution/src/worker/client.rs:45-62`

```rust
pub async fn run_task(&self, key, definition, peers) -> ExecutionResult<()> {
    let definition = crate::task::gen_::TaskDefinition::from(definition).encode_to_vec();
    let request = RunTaskRequest {
        job_id, stage, attempt, partition,
        definition,             // serialized TaskDefinition → PhysicalPlanNode bytes
        peers: peers.into_iter().map(|x| x.into()).collect(),
    };
    let response = self.inner.get().await?.run_task(request).await?;  // gRPC unary
    let RunTaskResponse {} = response.into_inner();
    Ok(())
}
```

### 9.2 Worker Side — WorkerServer::run_task

**File:** `crates/sail-execution/src/worker/server.rs:27-66`

```rust
async fn run_task(&self, request) -> Result<Response<RunTaskResponse>, Status> {
    let request = request.into_inner();
    let definition = crate::task::gen_::TaskDefinition::decode(definition.as_slice())?;
    let event = WorkerEvent::RunTask { key, definition, peers };
    self.handle.send(event).await?;     // sends to WorkerActor
    Ok(Response::new(RunTaskResponse {}))
}
```

### 9.3 WorkerActor Event Dispatch

**File:** `crates/sail-execution/src/worker/actor/core.rs:57-113`

```rust
fn receive(&mut self, ctx, message) -> ActorAction {
    match message {
        WorkerEvent::ServerReady { port, signal } => self.handle_server_ready(...),
        WorkerEvent::StartHeartbeat => self.handle_start_heartbeat(...),
        WorkerEvent::ReportKnownPeers { ... } => self.handle_report_known_peers(...),
        WorkerEvent::RunTask { key, definition, peers } => self.handle_run_task(...),
        WorkerEvent::StopTask { key } => self.handle_stop_task(...),
        WorkerEvent::ReportTaskStatus { ... } => self.handle_report_task_status(...),
        WorkerEvent::ProbePendingLocalStream { ... } => self.handle_probe_pending_local_stream(...),
        WorkerEvent::CreateLocalStream { ... } => self.handle_create_local_stream(...),
        WorkerEvent::CreateRemoteStream { ... } => self.handle_create_remote_stream(...),
        WorkerEvent::FetchDriverStream { ... } => self.handle_fetch_driver_stream(...),
        WorkerEvent::FetchWorkerStream { ... } => self.handle_fetch_worker_stream(...),
        WorkerEvent::FetchRemoteStream { ... } => self.handle_fetch_remote_stream(...),
        WorkerEvent::CleanUpJob { ... } => self.handle_clean_up_job(...),
        WorkerEvent::Shutdown => ActorAction::Stop,
    }
}
```

### 9.4 ALL gRPC Message Types

| Message | Direction | Fields |
|---------|-----------|--------|
| `RunTaskRequest` | Driver→Worker | job_id, stage, partition, attempt, definition(Vec<u8>), peers |
| `RunTaskResponse` | Worker→Driver | `{}` |
| `StopTaskRequest` | Driver→Worker | job_id, stage, partition, attempt |
| `ReportTaskStatusRequest` | Worker→Driver | job_id, stage, partition, attempt, status, message, cause(JSON), sequence |
| `ReportWorkerHeartbeatRequest` | Worker→Driver | worker_id |
| `ReportWorkerKnownPeersRequest` | Worker→Driver | worker_id, peer_worker_ids |
| `RegisterWorkerRequest` | Worker→Driver | worker_id, host, port |
| `CleanUpJobRequest` | Driver→Worker | job_id, stage |

---

## Layer 10: Worker Plan Decoding — All Expression Types

### 10.1 handle_run_task → TaskRunner

**File:** `crates/sail-execution/src/worker/actor/handler.rs:110-135`

```rust
fn handle_run_task(&mut self, ctx, key, definition, peers) -> ActorAction {
    self.peer_tracker.track(ctx, peers);
    self.task_runner.run_task(
        ctx, key, definition,
        self.options.session.task_ctx(),  // Worker session's TaskContext
        self.options.worker_id,
    );
    ActorAction::Continue
}
```

### 10.2 TaskRunner — Creation and Codec

**File:** `crates/sail-execution/src/task_runner/mod.rs:14-17` + `core.rs:33-38`

```rust
pub struct TaskRunner {
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    codec: Box<dyn PhysicalExtensionCodec>,   // ← RemoteExecutionCodec
}

pub fn new() -> Self {
    Self {
        signals: HashMap::new(),
        codec: Box::new(RemoteExecutionCodec),
    }
}
```

### 10.3 execute_plan — The 5-Step Pipeline

**File:** `crates/sail-execution/src/task_runner/core.rs:77-113`

```
execute_plan(ctx, key, definition, context)
  ├── STEP 1: try_decode_physical_plan(&context, self.codec.as_ref(), definition.plan)
  │   └── Decodes PhysicalPlanNode protobuf → Arc<dyn ExecutionPlan>
  ├── STEP 2: rewrite_file_scans(plan)
  │   └── For DataSourceExec with FileScanConfig: preserve_order=true, schema evolution adapter
  ├── STEP 3: rewrite_shuffle(ctx, key, inputs, output, plan, context)
  │   ├── Input phase: StageInputExec → ShuffleReadExec with StreamAccessor
  │   └── Output phase: Wrap plan in ShuffleWriteExec with output destinations
  ├── STEP 4: trace_execution_plan(plan, options)
  │   └── OpenTelemetry tracing
  └── STEP 5: plan.execute(partition, context)
      └── Returns SendableRecordBatchStream
```

### 10.4 try_decode_physical_plan → proto_to_physical_plan

**File:** `crates/sail-execution/src/proto/decode.rs:46-61`

```rust
pub fn try_decode_physical_plan(ctx, codec, buf) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = try_decode_message::<PhysicalPlanNode>(buf)?;
    proto_to_physical_plan(ctx, codec, &plan)
}

pub fn proto_to_physical_plan(ctx, codec, plan) -> Result<Arc<dyn ExecutionPlan>> {
    plan.try_into_physical_plan_with_converter(
        ctx, codec, &RemotePhysicalProtoConverter {},
    )
}
```

### 10.5 try_into_physical_plan_with_converter — datafusion-proto

**File:** `datafusion-proto-54.0.0/mod.rs:274-282`

```rust
pub fn try_into_physical_plan_with_converter(
    &self, ctx, codec, proto_converter,
) -> Result<Arc<dyn ExecutionPlan>> {
    let decode_ctx = PhysicalPlanDecodeContext::new(ctx, codec);
    //             task_ctx: &TaskContext, codec: &dyn PhysicalExtensionCodec
    self.try_into_physical_plan_with_context(&decode_ctx, proto_converter)
}
```

### 10.6 try_into_physical_plan_with_context — All Plan Type Dispatchers

**File:** `datafusion-proto-54.0.0/mod.rs:284-696`

The same 31 plan node types from encoding are handled in reverse here:

| PhysicalPlanType | Method | Line |
|-----------------|--------|------|
| `Explain` | try_into_explain_physical_plan | 295-296 |
| `Projection` | try_into_projection_physical_plan | 297-299 |
| `Filter` | try_into_filter_physical_plan | 300-303 |
| `CsvScan` | try_into_csv_scan_physical_plan | 304-306 |
| `JsonScan` | try_into_json_scan_physical_plan | 307-309 |
| `ParquetScan` | try_into_parquet_scan_physical_plan | 310-312 |
| ... (all 31 standard types) ... | | |
| **`Extension`** | **codec.try_decode(buf, inputs, ctx)** | **~671-696** |

When `PhysicalPlanType::Extension` is encountered, it calls `codec.try_decode(buf, inputs, ctx)` → `RemoteExecutionCodec::try_decode` (lines 304-1590 in codec.rs), which handles all 60+ Sail-specific NodeKind variants.

---

## Layer 11: UDF Resolution During Decode — PATH A vs PATH B

### 11.1 parse_physical_expr_with_converter — All Expression Types

**File:** `datafusion-proto-54.0.0/from_proto.rs:258-598`

Dispatches by `ExprType`:

| ExprType | Resolution |
|----------|-----------|
| `Column(c)` | `Column::from(&c)` |
| `UnknownColumn(c)` | `UnKnownColumn::new(&c.name)` |
| `Literal(scalar)` | `Literal::new(scalar.try_into()?)` |
| `BinaryExpr(binary_expr)` | Linearized operands → nested BinaryExpr tree |
| `NotExpr(n)` | `NotExpr::new(parse_expr(...))` |
| `IsNullExpr(expr)` | `IsNullExpr::new(parse_expr(...))` |
| `IsNotNullExpr(expr)` | `IsNotNullExpr::new(parse_expr(...))` |
| `NegativeExpr(n)` | `NegativeExpr::new(parse_expr(...))` |
| `InListExpr(e)` | `InListExpr::new(...)` with `list` and `negated` |
| `CastExpr(e)` | `Arc::new(CastExpr::new(...))` |
| `TryCastExpr(e)` | `Arc::new(TryCastExpr::new(...))` |
| **`ScalarUdf(e)`** | **PATH A or PATH B — see below** | **435-463** |
| `LikeExpr(like_expr)` | `Arc::new(LikeExpr::new(...))` |
| `ScalarSubqueryExpr(e)` | `Arc::new(ScalarSubqueryExpr::new(...))` |
| `Extension(extension)` | `codec.try_decode_expr(buf, inputs)` |

### 11.2 ScalarUdf Resolution — The Fork

**File:** `datafusion-proto-54.0.0/from_proto.rs:435-442`

```rust
ExprType::ScalarUdf(e) => {
    let udf = match &e.fun_definition {
        Some(buf) => ctx.codec().try_decode_udf(&e.name, buf)?,       // PATH A
        None => ctx
            .task_ctx()
            .udf(e.name.as_str())                                     // PATH B step 1
            .or_else(|_| ctx.codec().try_decode_udf(&e.name, &[]))?, // PATH B step 2
    };
```

**PATH A (`fun_definition = Some(buf)`)**: The UDF was serialized by `try_encode_udf` (it's a Sail-specific UDF with custom parameters). Calls `ctx.codec().try_decode_udf(name, buf)` → vtable dispatch → `RemoteExecutionCodec::try_decode_udf`.

**PATH B (`fun_definition = None`)**: The UDF was NOT serialized (empty buffer → DataFusion built-in). First tries `ctx.task_ctx().udf(name)` (the worker session's TaskContext, which has all 247 default features). Falls back to `codec.try_decode_udf(name, &[])` with empty buffer.

### 11.3 `ctx.codec()` Returns the Same Codec Throughout

The codec carried in `PhysicalPlanDecodeContext.codec` is the EXACT same `&dyn PhysicalExtensionCodec` that was passed from `TaskRunner::codec`. There is no wrapping, copying, or substitution. Every vtable dispatch call hits `RemoteExecutionCodec`.

### 11.4 Converter Role — RemotePhysicalProtoConverter

**File:** `crates/sail-execution/src/proto/converter.rs:51-80`

The converter intercepts expression decoding for these three types:

```rust
match decode_remote_expr_kind(proto)? {
    Some((ExprKind::HigherOrderUdf(node), inputs)) =>  // Array filter/transform/aggregate
    Some((ExprKind::LambdaVariable(node), _)) =>        // Lambda variable references
    Some((ExprKind::Lambda(node), inputs)) =>           // Lambda expressions
    _ => self.default_proto_to_physical_expr(proto, input_schema, ctx),  // ← Regular exprs fall here
}
```

`default_proto_to_physical_expr` (mod.rs:3958-3968) calls `parse_physical_expr_with_converter` — which is the function above that handles `ExprType::ScalarUdf` with PATH A/B. The converter does NOT intercept or alter ScalarUdf resolution in any way.

### 11.5 Aggregate and Window UDF Resolution

**Aggregate** (from_proto.rs lines 166-173, mod.rs lines 1300-1312):
```rust
AggregateFunction::UserDefinedAggrFunction(udaf_name) => {
    match &agg_node.fun_definition {
        Some(buf) => ctx.codec().try_decode_udaf(udaf_name, buf)?,
        None => ctx.task_ctx().udaf(udaf_name)
            .or_else(|_| ctx.codec().try_decode_udaf(udaf_name, &[]))?,
    }
}
```

**Window** (from_proto.rs lines 164-187): Same pattern for both UDAFs and UDWFs.

### 11.6 Extension Expression Resolution

**File:** `datafusion-proto-54.0.0/from_proto.rs:564-572`

```rust
ExprType::Extension(extension) => {
    let inputs = extension.inputs.iter()
        .map(|e| proto_converter.proto_to_physical_expr(e, input_schema, ctx))
        .collect::<Result<_>>()?;
    ctx.codec().try_decode_expr(extension.expr.as_slice(), &inputs)?
}
```

`RemoteExecutionCodec::try_decode_expr` (codec.rs lines 4015-4061) handles: SchemaEvolutionCast, Lambda, LambdaVariable expression kinds.

---

## Layer 12: RemoteExecutionCodec — Complete Encode/Decode

### 12.1 try_decode_udf — Complete Flow with All Match Arms

**File:** `crates/sail-execution/src/proto/codec.rs:2993-3433`

```
try_decode_udf(name, buf)
  ├── STEP 1: ExtendedScalarUdf::decode(buf)
  │   └── On decode error: plan_datafusion_err!("failed to decode udf: {e}")
  ├── STEP 2: Extract udf_kind from the decoded proto
  │   └── If None: plan_err!("ExtendedScalarUdf: no UDF found for {name}")
  ├── STEP 3: Match on UdfKind (lines 3026-3252):
  │   ├── Standard → fall through to name match
  │   ├── PySpark { kind, name, payload, deterministic, input_types, output_type, config } → PySparkUDF
  │   ├── PySparkCoGroupMap → PySparkCoGroupMapUDF
  │   ├── DropStructField { field_names } → DropStructField
  │   ├── Explode { name } → Explode
  │   ├── XpathTyped { name } → XpathTyped
  │   ├── SparkToXml { session_timezone } → SparkToXml
  │   ├── SparkUnixTimestamp { timezone } → SparkUnixTimestamp
  │   ├── StructFunction { field_names } → StructFunction
  │   ├── ArraysZip { field_names } → ArraysZip
  │   ├── UpdateStructField { field_names } → UpdateStructField
  │   ├── TimestampNow { timezone, time_unit } → TimestampNow
  │   ├── SparkTimestamp { timezone, is_try, ansi_mode } → SparkTimestamp
  │   ├── SparkDate { is_try } → SparkDate::new(is_try)      ← LINE 3163-3165
  │   ├── SparkTime { is_try } → SparkTime::new(is_try)
  │   ├── SparkFromCsv { session_timezone } → SparkFromCSV
  │   ├── SparkToCsv { session_timezone } → SparkToCsv
  │   ├── SparkFromJson { session_timezone } → SparkFromJson
  │   ├── SparkVariantGet { safe } → SparkVariantGet
  │   ├── SparkNextDay { ansi_mode } → SparkNextDay
  │   ├── SparkWindowBuckets { window_duration } → SparkWindowBuckets
  │   ├── SparkToNumber { safe } → SparkToNumber
  │   ├── SparkToChar { ansi_mode } → SparkToChar
  │   ├── SparkAbs { ansi_mode } → SparkAbs
  │   ├── SparkBin { ansi_mode } → SparkBin
  │   ├── SparkPmod { ansi_mode } → SparkPmod
  │   ├── SparkNegative { ansi_mode } → SparkNegative
  │   ├── SparkMakeTimestampNtz { is_try } → SparkMakeTimestampNtz
  │   ├── ConvertTz { classic } → ConvertTz
  │   ├── SparkParseJson { safe } → SparkParseJson
  │   └── SparkStructRename { target_type } → SparkStructRename
  ├── STEP 4: Name-based fallback for Standard UDFs (lines 3254-3432)
  │   ├── "array_item_with_position" → ArrayItemWithPosition
  │   ├── "levenshtein" | "spark_levenshtein" → SparkLevenshtein
  │   ├── ... 80+ more name matches ...
  │   ├── "spark_year" | "year" → SparkYear
  │   ├── "spark_last_day" | "last_day" → SparkLastDay
  │   └── _ → plan_err!("could not find scalar function: {name}")
  └── RETURN: Ok(Arc::new(ScalarUDF::from(udf)))
```

### 12.2 try_encode_udf — Mirror of decode

**File:** `crates/sail-execution/src/proto/codec.rs:3435-3719`

The reverse of decode — downcasts the UDF's inner type and produces the matching `UdfKind` variant. For `SparkDate`:

```rust
} else if let Some(func) = node.inner().downcast_ref::<SparkDate>() {
    let is_try = func.is_try();
    UdfKind::SparkDate(gen_::SparkDateUdf { is_try })
```

### 12.3 try_decode / try_encode — Plan Node Level

**File:** `crates/sail-execution/src/proto/codec.rs:304-1590` (decode) + `1592-2497` (encode)

These handle plan-level nodes via the same `ExtendedPhysicalPlanNode` protobuf pattern. Each Sail-specific `ExecutionPlan` type gets a `NodeKind` variant in the protobuf and a matching match arm in the codec.

### 12.4 try_decode_udaf / try_encode_udaf

**File:** `crates/sail-execution/src/proto/codec.rs:3721-3971`

Handles: StandardUdaf (25+ aggregate names → specific accumulator types), PySparkGroupAgg, PySparkGroupMap, PySparkBatchCollector, PercentileDisc.

### 12.5 try_decode_udwf / try_encode_udwf

**File:** `crates/sail-execution/src/proto/codec.rs:3973-4013`

Handles: SparkNtile (`StandardUdwf { name: "ntile" }`), SparkFirstLastValue (`SparkFirstLastValueUdwf { direction }`).

---

## Layer 13: All Trait Implementations & Overrides

### 13.1 PhysicalExtensionCodec Trait (datafusion-proto)

**File:** `datafusion-proto-54.0.0/mod.rs:3849-3900`

```rust
pub trait PhysicalExtensionCodec: Debug + Send + Sync + Any {
    fn try_decode(
        &self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()>;

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        not_impl_err!("PhysicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode_expr(...)  // default: not_impl_err!("PhysicalExtensionCodec is not provided")
    fn try_encode_expr(...)  // default: not_impl_err!("PhysicalExtensionCodec is not provided")
    fn try_decode_udaf(...)  // default: not_impl_err!("PhysicalExtensionCodec is not provided for aggregate function {name}")
    fn try_encode_udaf(...)  // default: Ok(())
    fn try_decode_udwf(...)  // default: not_impl_err!("PhysicalExtensionCodec is not provided for window function {name}")
    fn try_encode_udwf(...)  // default: Ok(())
}
```

### 13.2 PhysicalProtoConverterExtension Trait (datafusion-proto)

**File:** `datafusion-proto-54.0.0/mod.rs:3927-3975`

```rust
pub trait PhysicalProtoConverterExtension {
    fn proto_to_execution_plan(&self, proto, ctx) -> Result<Arc<dyn ExecutionPlan>>;
    fn default_proto_to_execution_plan(...) -> ... { ... }   // default impl
    fn execution_plan_to_proto(&self, plan, codec) -> Result<PhysicalPlanNode>;
    fn proto_to_physical_expr(&self, proto, schema, ctx) -> Result<Arc<dyn PhysicalExpr>>;
    fn default_proto_to_physical_expr(...) -> ... { ... }    // default impl
    fn physical_expr_to_proto(&self, expr, codec) -> Result<PhysicalExprNode>;
}
```

### 13.3 RemoteExecutionCodec — Sail's Implementation

**File:** `crates/sail-execution/src/proto/codec.rs:296-4091`

```rust
pub struct RemoteExecutionCodec;

impl PhysicalExtensionCodec for RemoteExecutionCodec {
    fn try_decode(...)     // 304-1590:  60+ NodeKind match arms
    fn try_encode(...)     // 1592-2497: 60+ NodeKind match arms
    fn try_decode_udf(...) // 2993-3433: UDF resolution → 30+ UdfKind variants
    fn try_encode_udf(...) // 3435-3719: UDF serialization → Phase 1/2/3
    fn try_decode_udaf(...)// 3721-3883
    fn try_encode_udaf(...)// 3885-3971
    fn try_decode_udwf(...)// 3973-3995
    fn try_encode_udwf(...)// 3997-4013
    fn try_decode_expr(...)// 4015-4061
    fn try_encode_expr(...)// 4063-4091
    // 42 additional methods for type encoding/decoding (definitions line 4095-5092)
}
```

### 13.4 RemotePhysicalProtoConverter — Sail's Converter

**File:** `crates/sail-execution/src/proto/converter.rs:26-108`

```rust
pub struct RemotePhysicalProtoConverter;

impl PhysicalProtoConverterExtension for RemotePhysicalProtoConverter {
    fn proto_to_execution_plan(...)  // default
    fn execution_plan_to_proto(...)  // try_from_physical_plan_with_converter
    fn proto_to_physical_expr(...)   // HigherOrderUdf / LambdaVariable / Lambda / default
    fn physical_expr_to_proto(...)   // HigherOrderFunctionExpr / LambdaExpr / LambdaVariable / default
}
```

### 13.5 DefaultPhysicalExtensionCodec — datafusion-proto Fallback (NOT USED BY SAIL)

**File:** `datafusion-proto-54.0.0/mod.rs:3903-3921`

```rust
pub struct DefaultPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for DefaultPhysicalExtensionCodec {
    fn try_decode(...) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }
    fn try_encode(...) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }
}
```

Sail NEVER creates or uses `DefaultPhysicalExtensionCodec`. The only `Box<dyn PhysicalExtensionCodec>` ever created is `Box::new(RemoteExecutionCodec)` in `TaskRunner::new()`.

### 13.6 DefaultPhysicalProtoConverter — datafusion-proto Fallback

**File:** `datafusion-proto-54.0.0/mod.rs:3990-4036`

Sail overrides this with `RemotePhysicalProtoConverter`, which only intercepts HigherOrderUdf/LambdaVariable/Lambda. All standard expressions pass through `default_proto_to_physical_expr` → `parse_physical_expr_with_converter`.

### 13.7 Vtable Dispatch Verification

The vtable for `Box<dyn PhysicalExtensionCodec>` in Sail contains:

| Method | Trait Default | RemoteExecutionCodec Override | Which Is Called |
|--------|--------------|------------------------------|-----------------|
| `try_decode` | — (required) | ✅ 60+ NodeKind arms | **Override** |
| `try_encode` | — (required) | ✅ 60+ NodeKind arms | **Override** |
| `try_decode_udf` | not_impl_err! | ✅ 30+ UdfKind variants + name match | **Override** |
| `try_encode_udf` | Ok(()) | ✅ Phase 1/2/3 | **Override** |
| `try_decode_udaf` | not_impl_err! | ✅ 25+ aggregate types | **Override** |
| `try_encode_udaf` | Ok(()) | ✅ | **Override** |
| `try_decode_udwf` | not_impl_err! | ✅ SparkNtile, SparkFirstLastValue | **Override** |
| `try_encode_udwf` | Ok(()) | ✅ | **Override** |
| `try_decode_expr` | not_impl_err! | ✅ SchemaEvolutionCast, Lambda | **Override** |
| `try_encode_expr` | not_impl_err! | ✅ | **Override** |

Every method with a default is overridden by `RemoteExecutionCodec`. The DEFAULT trait methods are **NEVER** dispatched for any valid UDF.

---

## Layer 14: Worker Session & TaskContext

### 14.1 Worker Session

**File:** `crates/sail-session/src/session_factory/worker.rs:44-74`

```rust
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_runtime_env(runtime)
    .with_default_features()   // 247 DataFusion built-in scalar functions
    .build();
```

The 247 functions come from `SessionStateDefaults::default_scalar_functions()`:
- `core::functions()` — mathematical, comparison
- `datetime::functions()` — date/time manipulation
- `encoding::functions()` — encode, decode, base64
- `math::functions()` — advanced math
- `regex::functions()` — regexp
- `crypto::functions()` — md5, sha, crc
- `unicode::functions()` — unicode
- `string::functions()` — upper, lower, replace, concat, trim, etc.

### 14.2 TaskContext Construction

**File:** `datafusion-session-54.0.0/session.rs:153-167`

```rust
impl From<&dyn Session> for TaskContext {
    fn from(state: &dyn Session) -> Self {
        TaskContext::new(
            task_id: None,
            state.session_id(),
            state.config().clone(),
            state.scalar_functions().clone(),        // ← 247 functions copied
            state.higher_order_functions().clone(),
            state.aggregate_functions().clone(),
            state.window_functions().clone(),
            Arc::clone(state.runtime_env()),
        )
    }
}
```

### 14.3 TaskContext::udf — Registry Lookup

**File:** `datafusion-execution-54.0.0/task.rs:172-183`

```rust
impl FunctionRegistry for TaskContext {
    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);
        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDF named \"{name}\" in the TaskContext")
        })
    }
}
```

### 14.4 UDF Classification Summary

| UDF | Encoder Phase | fun_definition | Decode Path | Resolved By |
|-----|-------------|---------------|-------------|-------------|
| `spark_date` | Phase 2: downcast_ref | `Some(buf)` | PATH A | codec.rs match UdfKind |
| `spark_time` | Phase 2: downcast_ref | `Some(buf)` | PATH A | codec.rs match UdfKind |
| `spark_timestamp` | Phase 2: downcast_ref | `Some(buf)` | PATH A | codec.rs match UdfKind |
| `spark_year` | Phase 1: is::<>() | `Some(buf)` | PATH A → Standard → name match | codec.rs name match |
| `upper` | Phase 3: fallthrough | `None` | PATH B | Worker TaskContext (247 fn) |
| `replace` | Phase 3: fallthrough | `None` | PATH B | Worker TaskContext (247 fn) |
| `extract` | Phase 3: fallthrough | `None` | PATH B | Worker TaskContext (247 fn) |
| `regexp_replace` | Phase 3: fallthrough | `None` | PATH B | Worker TaskContext (247 fn) |

---

## Layer 15: Error Propagation — Every Failure Path

### 15.1 TaskRunner Error → Worker Event

**File:** `crates/sail-execution/src/task_runner/core.rs:40-61`

```rust
Err(e) => {
    let event = T::Message::report_task_status(
        key, TaskStatus::Failed,
        Some(format!("failed to execute plan: {e}")),
        Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
    );
    ctx.send(event);
    return;
}
```

### 15.2 TaskMonitor → Worker Event

**File:** `crates/sail-execution/src/task_runner/monitor.rs:124-159`

When the stream execution fails:
```rust
Err(e) => {
    let event = T::Message::report_task_status(
        key, TaskStatus::Failed,
        Some(format!("task execution failed: {e}")),
        Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
    );
    handle.send(event).await;
}
```

### 15.3 Worker → Driver Error Reporting

**File:** `crates/sail-execution/src/worker/actor/handler.rs:146-189`

`handle_report_task_status` uses the `driver_client_set.core.report_task_status()` gRPC call with retries to report task status back to the driver.

### 15.4 WorkerPool::run_task Failure

**File:** `crates/sail-execution/src/driver/worker_pool/core.rs:343-354`

```rust
if let Err(e) = client.run_task(key.clone(), definition, peers).await {
    let _ = handle.send(DriverEvent::UpdateTask {
        key, status: TaskStatus::Failed,
        message: Some(format!("failed to run task via the worker client: {e}")),
        cause: Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
        sequence: None,
    }).await;
}
```

### 15.5 Driver → Client Error Propagation

1. `DriverActor::refresh_job` detects failure → `JobAction::FailJobOutput`
2. `FailJobOutput` → sends `JobOutputItem::Error { cause }` via the job output stream
3. `ClusterJobRunner::execute` awaits the oneshot channel → receives the error
4. Spark Connect service → translates to gRPC `Status`
5. PySpark client → raises `SparkRuntimeException`

### 15.6 Task Retry

Failed tasks are retried up to 3 times (attempt 0, 1, 2) with the SAME encoded plan bytes. Since the codec error is deterministic (same bytes → same vtable dispatch → same failure), all retries fail identically.

### 15.7 Other Error Paths in DriverActor

**File:** `crates/sail-execution/src/driver/actor/handler.rs`

| Handler | Error Condition | Action |
|---------|----------------|--------|
| `handle_execute_job` | accept_job error | `result.send(Err(...))` |
| `handle_probe_lost_worker` | Worker gone | All tasks on that worker → Failed |
| `handle_probe_pending_task` | Timeout | Task → Failed with "task scheduling timeout" |
| `handle_probe_pending_worker` | Worker not starting | Track failure count, timeout after 5 consecutive |
| `handle_register_worker` | No matching pending worker | Log warning, ignore |
| `run_tasks` / `get_task_definition` | Encoding failure | Task → Failed + Cause |

---

## Layer 16: The spark_date Bug — Verified Root Cause

### 16.1 Proven by Negative Evidence

The `log::info!("udf_decode_start name={name} buf_len={}", buf.len())` at `codec.rs:2994` is the FIRST line of `RemoteExecutionCodec::try_decode_udf`. It never appears in any worker log, at any attempt, on any pod, after any rebuild.

### 16.2 The Only Explanation

The vtable for `Box<dyn PhysicalExtensionCodec>` in the deployed binary points to the trait DEFAULT `try_decode_udf` instead of `RemoteExecutionCodec::try_decode_udf`. This can only happen if:

1. **The deployed binary was compiled from source code that did NOT contain the `UdfKind::SparkDate` handler**, OR
2. **The deployed binary was compiled from source code where `RemoteExecutionCodec` did NOT implement `PhysicalExtensionCodec`** (i.e., the trait impl is missing entirely)

Given that the local source at `/home/soumilk/sail/crates/sail-execution/src/proto/codec.rs` IS correct (verified by grep: line 3163 has `UdfKind::SparkDate`, line 3164 has `udf_decode_kind`), and the local `sail spark server` test WORKS in single-process mode (output: `2025-09-30`), the conclusion is:

**The Docker image deployed to the K8s cluster was NOT built from the current source code.**

### 16.3 Local Binary Verification

The definitive test is to extract the binary from the Docker image and check:

```bash
docker create --name sail-extract sail-test:local
docker cp sail-extract:/usr/local/bin/sail /tmp/sail-binary
docker rm sail-extract
strings /tmp/sail-binary | grep "udf_decode_start"
```

Expected: `udf_decode_start name={name} buf_len={}`

If the string is PRESENT → the local build is correct; the server's deployment is the issue.

If the string is ABSENT → the local build is also wrong; investigate the Dockerfile build context.

### 16.4 The Fix

```bash
cd /home/smartreg/sail/sail
rm -rf target/   # CRITICAL: remove cached .rlib files
docker build --no-cache -t dev1.smarbl.com/smartreg-sail-udf:latest .
docker push dev1.smarbl.com/smartreg-sail-udf:latest
helm upgrade smartreg-k3s . -n smartreg-exp
kubectl delete pod -n smartreg-exp -l smarbl.com/component=smartreg-sail-worker
kubectl delete pod -n smartreg-exp -l app.kubernetes.io/component=spark-server
```

### 16.5 Verification After Deployment

Worker logs MUST show:
```
udf_decode_start name=spark_date buf_len=5
udf_decode_kind name=spark_date kind=spark_date is_try=true
```

If these logs appear, the codec is working. If they don't, the image was not built from the correct source.

---

## Appendix A: Key Files Reference (Complete)

| File | Line(s) | Purpose |
|------|---------|---------|
| `sail-spark-connect/src/server.rs` | 54-161 | gRPC reception, command dispatch (20 types), execute_plan |
| `sail-spark-connect/src/service/plan_executor.rs` | 113-292 | ExecutePlanMode, handle_execute_plan, handle_execute_sql_command |
| `sail-session/src/session_factory/server.rs` | 90-204 | ServerSessionFactory (NO default_features), create_job_runner (3 modes) |
| `sail-session/src/session_factory/worker.rs` | 44-74 | WorkerSessionFactory (WITH default_features = 247 functions) |
| `sail-session/src/session_manager/mod.rs` | 34-48 | get_or_create_session_context |
| `sail-session/src/session_manager/actor/handler.rs` | 23-349 | SessionManagerActor, handle_get_or_create_session, delete_session, etc. |
| `sail-sql-analyzer/src/parser.rs` | 84-119 | parse_one_statement, parse_date |
| `sail-sql-analyzer/src/statement.rs` | 104-1302 | from_ast_statement, all DDL/DML/DQL statement types |
| `sail-plan/src/lib.rs` | 29-66 | resolve_and_execute_plan (6-stage pipeline) |
| `sail-plan/src/resolver/plan.rs` | 18-31 | resolve_named_plan (Query vs Command dispatch) |
| `sail-plan/src/resolver/expression/cast.rs` | 29-327 | resolve_expression_cast (18 match arms), SparkDate creation (line 163) |
| `sail-plan/src/resolver/expression/mod.rs` | 99-563 | resolve_named_expression, resolve_expression |
| `sail-function/src/scalar/datetime/spark_date.rs` | 1-110 | SparkDate struct, string_to_date32, invoke_with_args |
| `sail-session/src/planner.rs` | 54-309 | ExtensionQueryPlanner (8 planners), all 16 plan node types |
| `sail-physical-optimizer/src/lib.rs` | 41-76 | get_physical_optimizers (26 rules) |
| `sail-execution/src/job_runner.rs` | 78-124 | ClusterJobRunner, execute |
| `sail-execution/src/driver/actor/handler.rs` | 31-604 | DriverActor handlers (execute_job, register_worker, etc.) |
| `sail-execution/src/driver/job_scheduler/mod.rs` | 20-58 | JobScheduler struct, JobAction enum (5 types), codec init |
| `sail-execution/src/driver/job_scheduler/core.rs` | 40-800 | accept_job, refresh_job, get_task_definition, build_task_input_keys |
| `sail-execution/src/driver/worker_pool/core.rs` | 44-529 | start_worker, register_worker, run_task (gRPC dispatch) |
| `sail-execution/src/driver/task_assigner/core.rs` | 15-313 | TaskAssigner, request_workers, assign_tasks, slot management |
| `sail-execution/src/job_graph/planner.rs` | 33-501 | JobGraph::try_new, build_job_graph, all stage boundary logic |
| `sail-execution/src/worker/client.rs` | 45-92 | WorkerClient (run_task, stop_task, stop_worker) |
| `sail-execution/src/worker/server.rs` | 27-133 | WorkerServer gRPC handlers |
| `sail-execution/src/worker/actor/core.rs` | 26-117 | WorkerActor::new, receive (14 event types) |
| `sail-execution/src/worker/actor/handler.rs` | 20-299 | WorkerActor handlers (run_task, report_task_status, etc.) |
| `sail-execution/src/task_runner/mod.rs` | 1-59 | TaskRunner struct, TaskRunnerMessage trait |
| `sail-execution/src/task_runner/core.rs` | 33-196 | TaskRunner::new, run_task, execute_plan (5 steps), rewrite_shuffle |
| `sail-execution/src/task_runner/monitor.rs` | 1-161 | TaskMonitor (execute, cancel, status reporting) |
| `sail-execution/src/proto/encode.rs` | 37-98 | try_encode_physical_plan, physical_plan_to_proto, all encode functions |
| `sail-execution/src/proto/decode.rs` | 46-131 | try_decode_physical_plan, proto_to_physical_plan, all decode functions |
| `sail-execution/src/proto/converter.rs` | 26-267 | RemotePhysicalProtoConverter, extension_expr handling |
| `sail-execution/src/proto/codec.rs` | 296-4091 | **RemoteExecutionCodec** — ALL trait method impls |
| `sail-execution/src/proto/codec.rs` | 2993-3433 | try_decode_udf (UDF resolution, 30+ UdfKind + 80+ name matches) |
| `sail-execution/src/proto/codec.rs` | 3435-3719 | try_encode_udf (UDF serialization, Phase 1/2/3) |
| `sail-execution/src/proto/codec.rs` | 1592-2497 | try_encode (plan node, 60+ NodeKind match arms) |
| `sail-execution/src/proto/codec.rs` | 304-1590 | try_decode (plan node, 60+ NodeKind match arms) |
| `sail-execution/src/proto/codec.rs` | 3721-3971 | try_decode_udaf / try_encode_udaf |
| `sail-execution/src/proto/codec.rs` | 3973-4013 | try_decode_udwf / try_encode_udwf |
| `sail-execution/src/proto/codec.rs` | 4015-4091 | try_decode_expr / try_encode_expr |
| `sail-execution/src/worker_manager/kubernetes.rs` | 113-281 | build_pod_env, launch_worker (K8s worker pods) |
| `sail-telemetry/src/telemetry.rs` | 149 | env_logger::Builder::from_env(default_filter="info") |

## Appendix B: datafusion-proto Key Files

| File | Line(s) | Purpose |
|------|---------|---------|
| `mod.rs` | 180-224 | PhysicalPlanDecodeContext struct |
| `mod.rs` | 274-696 | try_into_physical_plan_with_converter, all 31 plan type dispatchers |
| `mod.rs` | 1592-2497 | try_from_physical_plan_with_converter (encoding) |
| `mod.rs` | 3849-3900 | PhysicalExtensionCodec trait (10 methods with 6 defaults) |
| `mod.rs` | 3903-3921 | DefaultPhysicalExtensionCodec (NOT used by Sail) |
| `mod.rs` | 3927-3975 | PhysicalProtoConverterExtension trait (6 methods) |
| `mod.rs` | 3958-3968 | default_proto_to_physical_expr → parse_physical_expr_with_converter |
| `mod.rs` | 3990-4036 | DefaultPhysicalProtoConverter (Sail overrides with Remote) |
| `from_proto.rs` | 258-947 | parse_physical_expr_with_converter, all ExprType match arms |
| `from_proto.rs` | 435-442 | ScalarUdf → PATH A (codec.decode_udf) vs PATH B (task_ctx.udf) |
| `from_proto.rs` | 564-572 | Extension→ codec.try_decode_expr |
| `to_proto.rs` | 262-598 | serialize_physical_expr_with_converter, expression serialization |
| `to_proto.rs` | 484-502 | ScalarFunctionExpr → codec.try_encode_udf → fun_definition |

## Appendix C: Execution Modes

| Mode | Plan Serialization | Codec Used | Worker Type | Default Features |
|------|-------------------|-----------|-------------|-----------------|
| `local` (single-process) | **No** | Never called | N/A | Sail's own registry |
| `local-cluster` | Yes | RemoteExecutionCodec | In-process WorkerActor | WorkerSessionFactory (with defaults) |
| `kubernetes-cluster` | Yes | RemoteExecutionCodec | K8s Pod (sail worker) | WorkerSessionFactory (with defaults) |
