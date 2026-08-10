# Plan: Heimdall Flight SQL audit fixes (per-statement batch, dead code, timeouts)

**Status:** Implemented — all three fixes landed; `go build`/`vet`/`gofmt`/`staticcheck`/
`go test ./...` clean.
**Repo:** `smartreg-dbt-base` (heimdall, Go), branch `dev/soumil`
**Scope:** Fix the remaining Flight SQL robustness/parsing issues found after the count-parsing
bug fix (which restored `SELECT COUNT(*)` row-count capture). Three items: (1) `ExecuteBatchSQL`
fail-fast loses per-statement granularity, (2) `QueryTestTableSnapshot` is dead/duplicated code,
(3) no per-query timeouts on Flight calls.
**Depends on:** the already-landed `parseCountValue` fix (heimdall `internal/db/iceberg.go`) that
unblocked row-count capture.

---

## 1. Background & verified findings

The Flight SQL migration (E2/E3) replaced heimdall's REST iceberg interface with an Arrow
Flight SQL client (`internal/db/flight.go`, `internal/db/iceberg.go`). End-to-end testing via
`run_dbt_project.sh` exposed several latent issues that were never exercised under the old REST
path:

- **Row-count capture failed** (`count=-1`, step `failed`): Sail returns `COUNT(*)` as
  `UInt64` → `cellValue` → Go `int64`, but `ParseCountResult` asserted `.(float64)`. **Already
  fixed** by `parseCountValue` (type-switch helper), along with `QueryIcebergRowCount` and
  `ParseTestSnapshotResult`.
- `FlightClient.Close()` **is already invoked** (`cmd/heimdall/main.go:50`, `defer
  flight.Close()`) — item 4 of the earlier audit is a non-issue; no change needed.
- `CALL <catalog>.system.<procedure>` syntax is correct for Sail (verified in
  `sail-plan/src/resolver/command/call.rs`).
- `ICEBERG_CATALOG` default is already `testcat` (`internal/config/config.go:25`).

The remaining issues are:

---

## 2. Issue 1 — `ExecuteBatchSQL` fail-fast loses per-statement granularity

### 2.1 Current behavior

`internal/db/iceberg.go:325-339`:

```go
func (r *Repository) ExecuteBatchSQL(ctx context.Context, statements []string) ([]icebergQueryResp, error) {
	if r.flight == nil {
		return nil, fmt.Errorf("flight client not configured")
	}
	results := make([]icebergQueryResp, 0, len(statements))
	for _, stmt := range statements {
		resp, err := r.flight.Execute(ctx, stmt)
		if err != nil {
			return nil, fmt.Errorf("batch statement %q: %w", stmt, err)
		}
		results = append(results, *resp)
	}
	return results, nil
}
```

On the **first** failing statement it returns `(nil, err)`. Both callers then treat **every**
subsequent statement as failed:

- Row-count capture (`internal/db/repository.go:727` + loop `:728-756`): when `batchErr != nil`,
  every table gets `count=-1` / step `failed`, even tables whose count would have succeeded.
- Test-snapshot capture (`internal/cli/dbt.go:465` + loop `:466-480`): when `err != nil`, every
  test gets `qErr = err`, so a single missing audit table marks all tests as failed and records
  no snapshots.

### 2.2 Fix (per-statement results)

Return **partial results alongside the first error**, so callers can record accurate per-table
outcomes. Two options:

**Option A (minimal, recommended):** return `results` accumulated so far **plus** the first
error, and have callers index into `results` per statement (they already do `i < len(results)`).

```go
func (r *Repository) ExecuteBatchSQL(ctx context.Context, statements []string) ([]icebergQueryResp, error) {
	if r.flight == nil {
		return nil, fmt.Errorf("flight client not configured")
	}
	results := make([]icebergQueryResp, 0, len(statements))
	var firstErr error
	for _, stmt := range statements {
		resp, err := r.flight.Execute(ctx, stmt)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("batch statement %q: %w", stmt, err)
			}
			// Keep going so per-statement outcomes are preserved for the other statements.
			// (A statement error produces no result row for that index; callers must
			//  guard with `i < len(results)` — they already do.)
			continue
		}
		results = append(results, *resp)
	}
	return results, firstErr
}
```

**Option B (explicit error markers):** return `[]icebergQueryResp` with a `*error` (or a
parallel error slice) per statement. More explicit but larger churn; not needed since callers
already guard by index.

**Choose Option A.** Then update the two callers minimally:
- `repository.go` `CaptureRowCountsWithStep`: keep the `batchErr != nil` path but only fall back
  to `err = batchErr` when `i >= len(results)` (missing result); otherwise `ParseCountResult`
  on the partial result as today. Net effect: tables that succeed get real counts; tables that
  failed keep `count=-1`.
- `dbt.go` test-snapshot loop: same — only assign `qErr = err` when `i >= len(results)`,
  otherwise parse the partial result.

### 2.3 Tests

- Unit test for `ExecuteBatchSQL` with mixed success/failure (requires a mock Flight client —
  see §5 on testability). At minimum, assert partial results are returned alongside the error.
- Integration: row-count capture over a mix of existing + missing tables returns real counts
  for existing and `-1` only for missing (non-fatal), rather than blanket-failing.

---

## 3. Issue 2 — `QueryTestTableSnapshot` is dead/duplicated code

### 3.1 Current state

- `internal/db/iceberg.go:240-276` defines `QueryTestTableSnapshot(schema, table) (snapshotID
  int64, rowCount int64, err error)` — **never called** (verified: only its definition exists).
- The real test-snapshot query is duplicated **inline** in `internal/cli/dbt.go:455-463` (builds
  the `SELECT snap.sid, cnt.cnt ... .refs ... CROSS JOIN COUNT(*)` SQL), then parsed by
  `db.ParseTestSnapshotResult`.
- `QueryTestTableSnapshot` has a **fragile inline count switch** (iceberg.go:266-274: float64 /
  int64 / string) that should use `parseCountValue` for consistency.

### 3.2 Fix

Two options:

**Option A (recommended): delete `QueryTestTableSnapshot`.** It's dead code and its inline count
parse is a divergence risk. The live path (`dbt.go` + `ParseTestSnapshotResult`) already uses the
robust `parseCountValue`/`parseSnapshotID` helpers.

**Option B: route `dbt.go` through it.** Extract the SQL-building + parse into one shared method
so there's a single source of truth. Larger change; only worth it if more callers are planned.

**Choose Option A** (delete), and add a small unit test for `ParseTestSnapshotResult` (already
covered by the count fix's tests) to lock the shape.

---

## 4. Issue 3 — No per-query timeouts on Flight calls

### 4.1 Current state

Every Flight call uses `context.Background()`:

- `iceberg.go:70` `postIcebergQuery` → `r.flight.Execute(context.Background(), sql)`
- `iceberg.go:143,149,155` `RollbackToSnapshot`/`SetCurrentSnapshot`/`ExpireSnapshots` →
  `ExecuteSQL(context.Background(), ...)`
- `repository.go:727` row-count batch
- `dbt.go:465` test-snapshot batch
- `flight.go` `FlightClient.Execute` uses the caller's ctx directly (no internal deadline)

A hung Sail query (driver busy, worker loss, network partition) blocks heimdall indefinitely —
no cancellation, no timeout, no progress.

### 4.2 Fix

Add a **configurable per-query timeout** at the `FlightClient.Execute` boundary so every SQL call
gets a bounded deadline, while still respecting a caller-provided context (e.g. SIGINT
cancellation in `dbt.go:190`).

**Design:**
- Add `queryTimeout time.Duration` to `FlightClient` (default e.g. 60s; configurable via env
  `HEIMDALL_FLIGHT_TIMEOUT` or a constant). Use `const DefaultFlightQueryTimeout = 60 * time.Second`.
- In `FlightClient.Execute`:
  ```go
  func (f *FlightClient) Execute(ctx context.Context, sql string) (*icebergQueryResp, error) {
      ctx, cancel := context.WithTimeout(ctx, f.queryTimeout)
      defer cancel()
      // ... existing body unchanged (uses ctx for Execute + DoGet) ...
  }
  ```
- `NewFlightClient` accepts an optional timeout; keep the signature simple by defaulting
  internally (or add a `WithQueryTimeout` option). Prefer a plain field set in `NewFlightClient`
  with a default, to avoid touching every constructor call.
- Caveat: `SELECT * FROM <t> VERSION AS OF <id>` for **test export** (`test.go:524`) and big
  audit tables could exceed a short timeout; 60s default is generous but if needed, make the
  timeout per-operation (longer for export) or raise the default. Flag this in the doc.

### 4.3 Tests

- Unit: `FlightClient.Execute` applies a deadline — construct with a tiny timeout against a
  non-routable/blackhole endpoint and assert it returns within the deadline with a
  `context.DeadlineExceeded`-ish error (best-effort; network tests may be flaky, so keep it
  lenient or gate behind an env).

---

## 5. Testability note (mock Flight client)

`Repository` holds `flight *FlightClient` (concrete type, `repository.go:86`). To unit-test
`ExecuteBatchSQL` and the row-count/test-snapshot callers without a live Sail server, introduce a
minimal interface:

```go
// internal/db/flight.go
type FlightSQLExecutor interface {
    Execute(ctx context.Context, sql string) (*icebergQueryResp, error)
    Close() error
}
```

- `FlightClient` satisfies it.
- `Repository.flight` becomes `FlightSQLExecutor` (or keep a `*FlightClient` and add a seam via a
  small interface used only in tests — decide based on churn).
- Enables table-driven tests for `ExecuteBatchSQL` partial results and `CaptureRowCountsWithStep`
  per-table outcomes.

If interface extraction is deemed too invasive for now, at minimum add unit tests for the pure
helpers (`parseCountValue`, `ParseCountResult`, `ParseTestSnapshotResult`) and a focused
`ExecuteBatchSQL` test using a lightweight fake that implements the same method set.

---

## 6. Implementation plan

1. **`internal/db/iceberg.go`**:
   - `ExecuteBatchSQL`: per-statement results + first error (Option A).
   - Delete `QueryTestTableSnapshot` (dead code).
2. **`internal/db/repository.go`** `CaptureRowCountsWithStep`:
   - Adjust loop so `ParseCountResult` runs on partial results when `i < len(results)`;
     fall back to `err` only for missing indices.
3. **`internal/cli/dbt.go`** test-snapshot loop:
   - Same per-index adjustment.
4. **`internal/db/flight.go`**:
   - Add `queryTimeout` field (default 60s) + `context.WithTimeout` in `Execute`.
5. **Tests**:
   - `internal/db/iceberg_parser_test.go`: extend with `ExecuteBatchSQL` partial-result test
     (via a fake executor or a small interface seam) and keep the parse-helper tests.
   - `internal/db/flight_test.go`: timeout application test (lenient).
6. **Regression**: `go build ./...`, `go vet ./...`, `gofmt -l`, `staticcheck ./...`,
   `go test ./...`.
7. **E2E**: `run_dbt_project.sh stg gl_balances -s landing.gl_balances -b 2025-09-30 -m 1` —
   confirm row-count capture returns real counts for `landing.gl_balances`, test-snapshot
   capture records snapshots for both data tests, and only genuinely-missing tables
   (`stg.gl_balances` before the model runs) show `-1` non-fatally.

---

## 7. Edge cases & risks

| # | Risk | Mitigation |
|---|------|------------|
| 1 | **`ExecuteBatchSQL` partial results + error** — callers must guard `i < len(results)` consistently; a missed guard could index out of range. | Both callers already guard `i < len(results)`; add a defensive `else { qErr = missing-result }` as today. |
| 2 | **Timeout too short for big `SELECT *` exports.** | 60s default; document that `writeCSV`/test export may need a larger value. Consider a longer timeout for export operations specifically if it bites. |
| 3 | **`context.WithTimeout` overrides caller cancellation.** | Use `context.WithTimeout(ctx, ...)` (child of caller ctx), so SIGINT still propagates; the deadline is an upper bound, not a replacement. |
| 4 | **Interface extraction churn for tests.** | If `FlightSQLExecutor` is too invasive, keep `*FlightClient` and test `ExecuteBatchSQL` with a minimal fake struct in the same package (`package db`) that duplicates the needed method — Go's structural typing makes this low-risk. |
| 5 | **Deleting `QueryTestTableSnapshot`** could remove something relied on by a future caller. | It's never called today; git history preserves it. Document the removal. |

---

## 8. Files touched

- `heimdall/internal/db/iceberg.go` — `ExecuteBatchSQL` per-statement results; delete
  `QueryTestTableSnapshot`.
- `heimdall/internal/db/repository.go` — row-count loop uses partial results.
- `heimdall/internal/cli/dbt.go` — test-snapshot loop uses partial results.
- `heimdall/internal/db/flight.go` — query timeout.
- `heimdall/internal/db/iceberg_parser_test.go` / `flight_test.go` — new tests.

---

## 9. Rollout & verification checklist

- [ ] `ExecuteBatchSQL` returns partial results + first error; both callers use per-index results.
- [ ] `QueryTestTableSnapshot` removed; no dangling references.
- [ ] `FlightClient.Execute` applies a bounded timeout (default 60s), preserving caller ctx.
- [ ] `go build`, `go vet`, `gofmt`, `staticcheck`, `go test ./...` all clean.
- [ ] E2E: row-count capture real counts for existing tables, `-1` only for missing (non-fatal);
      test-snapshot capture records per-test snapshots.
