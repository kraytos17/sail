# Plan: heimdall → Sail Arrow Flight SQL migration (Phase E + F)

**Status:** Draft / to be implemented
**Target repo:** `smartreg-dbt-base` (branch `dev/soumil`), Go module at `heimdall/` (Go 1.26)
**Scope:** Replace heimdall's REST consumption of `sail-rest-service` with a native Arrow
Flight SQL client against Sail's `sail flight server` (default port 32010). Covers E1–E6.
**Prerequisite (Sail):** Complete — engine features (`LOAD DATA`, `.refs`/`.snapshots`,
`CALL system.*` with physical GC, `TRUNCATE`, `VERSION AS OF`) are implemented and committed
on `feat/v0.6.6`; `sail flight server` routes all Query + Command plans via
`CommandStatementQuery`.

---

## 1. Context

heimdall orchestrates the SmartReg ETL: loads CSV landing data, runs dbt models, captures
before/after row counts, and performs snapshot rollback/rollforward/cleanup against Iceberg
tables (via Polaris). Today it talks to a REST API (`POST /engine/dbt/query`,
`/engine/dbt/load`, `/engine/dbt/batch`) through `internal/db/iceberg.go` using `net/http`.

This plan replaces that transport with **Arrow Flight SQL**: a single
`flightsql.Client.Execute(ctx, sql)` + `DoGet(ticket)` path for every statement, preserving
heimdall's public function signatures and response structs so `internal/cli/*.go` and
`internal/db/repository.go` callers are unchanged.

---

## 2. Transport decision — `CommandStatementQuery` (`Execute` + `DoGet`) for everything

Arrow Flight SQL exposes two statement RPC paths:

| Path | RPC calls | Result | Use in this plan |
|---|---|---|---|
| `CommandStatementQuery` | `Execute(ctx, sql)` → `*flight.FlightInfo` (ticket + schema); then `DoGet(ctx, ticket)` → `*flight.Reader` (Arrow record batches) | **Full result set**: column names/types + rows | **Yes — every statement** |
| `CommandStatementUpdate` | `ExecuteUpdate(ctx, sql)` → `DoPutUpdateResult.recordCount` | Integer affected-row count only | **No** |

### Why `Execute` + `DoGet` (never `ExecuteUpdate`)

1. **Sail does not implement the update RPC.** The Flight server implements only
   `get_flight_info_statement` + `do_get_statement` (`crates/sail-flight/src/service.rs:90,175`);
   `do_put_statement_update` / `ExecuteUpdate` is unimplemented. Calling `ExecuteUpdate` would
   hit an unimplemented RPC and fail.
2. **Sail eagerly executes Command plans inside `get_flight_info_statement`** and returns
   their result rows through `DoGet`:
   - `LOAD DATA INPATH ... INTO TABLE t` → a **`count` row batch** (produced by
     `IcebergCommitExec`),
   - `CALL <cat>.system.rollback_to_snapshot(...)` /
     `set_current_snapshot(...)` → row `(previous_snapshot_id, current_snapshot_id)`,
   - `CALL <cat>.system.expire_snapshots(...)` → row of six `deleted_*_count` columns,
   - `TRUNCATE TABLE t` → a count row.
3. **Heimdall reads result rows, not just a count.** `GetCurrentIcebergSnapshotID`,
   `GetLatestIcebergSnapshotID`, `CheckSnapshotExists`, `GetSnapshotParent`,
   `QueryIcebergRowCount` all parse `resp.Rows[0][0]` (`internal/db/iceberg.go:177,191,252,290`).
   Only a full result set provides these. `ExecuteUpdate` cannot.

### Per-statement client flow (used for everything)

```
flightsql.NewClient(ctx, opts...)                    // dial grpc://<host>:32010
info, err := client.Execute(ctx, sql)                // CommandStatementQuery
rdr,  err := client.DoGet(ctx, info.Endpoint[0].Ticket)  // stream Arrow batches
// read rdr (array.RecordReader) → collect rows + schema → icebergQueryResp
```

Round-trips: one `Execute` + one `DoGet` per statement — the same number of network calls as
today's single REST `POST`. `ExecuteUpdate` is never called.

### API signatures (arrow-go v18)

`github.com/apache/arrow-go/v18@v18.7.0/arrow/flight/flightsql/client.go`:
- `func (c *Client) Execute(ctx, query, opts...) (*flight.FlightInfo, error)` — `:132`
- `func (c *Client) DoGet(ctx, in *flight.Ticket, opts...) (*flight.Reader, error)` — `:365`
- `func (c *Client) ExecuteUpdate(ctx, query, opts...) (n int64, err error)` — `:171` (unused)

---

## 3. Current heimdall surface → Flight mapping

Response structs to preserve (`internal/db/iceberg.go:39-69`):
```go
type icebergQueryResp struct {
    Status   string
    Columns  []struct{ Name, Type string }
    RowCount int64
    Rows     [][]any
}
type engineLoadResp struct {
    Status, Schema, Table, FilePath, FileFormat string
    RowsLoaded int64
    Message    string
}
```

| heimdall fn | REST today | Flight replacement |
|---|---|---|
| `LoadFile(ctx, schema, table, s3aPath)` (`:71`) | `POST /engine/dbt/load` per file | `Execute(ctx, "LOAD DATA INPATH '<s3a>' INTO TABLE <schema>.<table>")`; read count row → `engineLoadResp.RowsLoaded`. See §7 for the per-business-date glob variant. |
| `postIcebergQuery(sql)` (`:120`) | `POST /engine/dbt/query` | `Execute` + `DoGet` → `icebergQueryResp{Columns, Rows, RowCount}` |
| `ExecuteSQLWithRowCount(sql)` (`:301`) | query | same, `RowCount` from rows |
| `ExecuteSQL(ctx, sql)` (`:354`) | query (ignore rows) | same, check `Status=="ok"` |
| `ExecuteBatchSQL(ctx, statements)` (`:405`) | `POST /engine/dbt/batch` | loop `Execute` per statement, preserve order + per-statement status |
| `GetCurrentIcebergSnapshotID` (`:169`) | `.refs` query | same SQL via `Execute` |
| `GetLatestIcebergSnapshotID` (`:183`) | `.snapshots` query | same |
| `CheckSnapshotExists` (`:230`) | `.snapshots WHERE snapshot_id = N` | same |
| `GetSnapshotParent` (`:244`) | `.snapshots WHERE snapshot_id = N` → parent_id | same |
| `GetSnapshotAncestry` (`:268`) | walk parent_id | same (loop `Execute` or one scan) |
| `RollbackToSnapshot` (`:212`) | `CALL <cat>.system.rollback_to_snapshot(...)` | same CALL via `Execute` |
| `SetCurrentSnapshot` (`:218`) | `CALL ...set_current_snapshot(...)` | same |
| `ExpireSnapshots` (`:224`) | `CALL ...expire_snapshots(...)` | same; read six `deleted_*_count` (optional logging) |
| `TruncateTable` (`:366`) | `TRUNCATE TABLE` | same via `Execute` |
| `ListTables` (`:381`) | `SHOW TABLES IN <schema>` | same |

**Config:** `Repository{icebergURL, icebergTok, icebergCatalog}` (`repository.go:83-88`),
wired from `ICEBERG_URL` / `ICEBERG_TOKEN` / `ICEBERG_CATALOG` in `cmd/heimdall/main.go:35-42`.

---

## 4. E1 — Add the Flight SQL dependency

- Add `github.com/apache/arrow-go/v18 v18.7.0` to `heimdall/go.mod`. It is already present
  in the Go module cache (`~/go/pkg/mod/github.com/apache/arrow-go/v18@v18.7.0`), so it
  resolves offline.
- Run `go mod tidy` to pin transitive deps (`google.golang.org/grpc`, `go.uber.org/multierr`,
  `github.com/stretchr/testify`, etc.). Go 1.26 is compatible.
- Client import path: `github.com/apache/arrow-go/v18/arrow/flight/flightsql`.

---

## 5. E2 — New Flight client layer (`internal/db/flight.go`)

New file wrapping the arrow-go client and converting results to heimdall's response shape, so
`iceberg.go` changes are mechanical:

```go
package db

import (
    "context"
    "fmt"
    "sync"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/flight"
    "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
    "google.golang.org/grpc"
)

// FlightClient wraps an arrow-go flightsql client and converts results to
// heimdall's icebergQueryResp shape.
type FlightClient struct {
    client *flightsql.Client
    conn   grpc.ClientConnInterface
    close  func() error
    mu     sync.Mutex // guards client calls
}

// NewFlightClient dials an Arrow Flight SQL endpoint (e.g. "grpc://host:32010").
func NewFlightClient(ctx context.Context, endpoint string) (*FlightClient, error)

// Close tears down the underlying connection.
func (f *FlightClient) Close() error

// Execute runs any statement (SELECT / LOAD DATA / CALL / TRUNCATE / DDL) and returns
// columns + rows, mirroring Sail's CommandStatementQuery behavior.
func (f *FlightClient) Execute(ctx context.Context, sql string) (*icebergQueryResp, error)
```

### `Execute` implementation (detailed)

1. `info, err := f.client.Execute(ctx, sql)`
   - On error → return `&icebergQueryResp{Status: "error", Message: err.Error()}, nil` OR a
     wrapped `error` — **match current semantics**: existing callers check both `err` and
     `resp.Status != "ok"`. Choose one convention and document it; recommend returning
     `(nil, fmt.Errorf("flight execute: %w", err))` for transport errors (like the current
     `http.NewRequestWithContext` errors) and `Status:"error"` only for logical failures.
2. `rdr, err := f.client.DoGet(ctx, info.Endpoint[0].Ticket)`
   - On error → same error convention as step 1.
3. Build the response:
   - `Columns`: iterate `rdr.Schema().Fields()` → `{Name: f.Name(), Type: arrowTypeToString(f.Type)}`.
   - `Rows`: read every `record.Record` from `rdr`; for each column, convert the Arrow array
     to Go values (see `cellValue` below) and append a row `[]any`.
   - `RowCount = len(Rows)`.
4. `defer rdr.Release()` and release each `record` after use (arrow-go requires manual
   memory release).
5. Return `&icebergQueryResp{Status: "ok", Columns: cols, Rows: rows, RowCount: n}, nil`.

### Helper conversions

- `arrowTypeToString(t arrow.DataType) string`: map the common Arrow types to heimdall's
  column type strings (best-effort; heimdall uses column *names* for snapshot queries, so an
  approximate map is sufficient):
  - `arrow.Int64`/`Int32`/`Int16`/`Int8` → `"bigint"`/`"int"`/`"smallint"`/`"tinyint"`
  - `arrow.Uint*` → `"bigint"`/`"int"` (etc.)
  - `arrow.Float64`/`Float32` → `"double"`/`"float"`
  - `arrow.Boolean` → `"boolean"`
  - `arrow.Binary`/`LargeBinary` → `"binary"`
  - `arrow.String`/`LargeString`/`StringView` → `"string"`
  - `arrow.Timestamp`/`Date32`/`Date64` → `"timestamp"`/`"date"`
  - `arrow.Decimal128`/`Decimal256` → `"decimal"`
  - default → `t.String()`
- `cellValue(arr array.Array, i int) any`: convert a single cell to a Go value:
  - numeric arrays → `arr.Value(i)` (int64/float64)
  - string arrays → `arr.Value(i)`
  - boolean → `arr.Value(i)`
  - timestamp → RFC3339 string or `int64` (heimdall doesn't parse timestamps from queries
    today; return the Arrow value's raw form or a stable string)
  - `IsNull(i)` → `nil`
- Reuse/extend `parseSnapshotID` (`iceberg.go:194`) — it already handles
  `string/float64/int64/json.Number`. Ensure the flight layer emits these concrete Go types
  for snapshot-id columns (int64 for `bigint`), so `parseSnapshotID` keeps working unchanged.

---

## 6. E3 — Rewire `internal/db/iceberg.go`

- Replace the `http.Client`s (`sharedHTTPClient`, `loadHTTPClient`) and the bodies of
  `postIcebergQuery`, `LoadFile`, and `ExecuteBatchSQL` with `FlightClient.Execute`.
- **Keep function signatures unchanged** so `repository.go`, `cli/*.go`, and
  `operations.go` callers are untouched.
- `icebergURL` now holds a `grpc://` endpoint (e.g. `grpc://sail-flight-service:32010`).
- **`icebergTok` is dropped** (decision: keep `do_handshake` permissive — see §10). Options:
  - remove the field from `Repository` + `NewRepositoryWithIceberg` + `cmd/heimdall/main.go`,
    or
  - keep the field but stop using it (least churn). **Recommend: remove** — it is only set
    from `ICEBERG_TOKEN` and never used elsewhere.
- Error handling: keep `fmt.Errorf(...)` wrapping and the `Status=="ok"` convention so
  `cli/load.go:204-208` and `iceberg.go:238,252,290,306,328,386,446,458` continue to work.

### `LoadFile` (detailed)

```go
func (r *Repository) LoadFile(ctx context.Context, schema, table, s3aPath string) (*engineLoadResp, error) {
    if r.icebergURL == "" { return nil, fmt.Errorf("iceberg URL not configured") }
    sql := fmt.Sprintf("LOAD DATA INPATH '%s' INTO TABLE %s.%s", s3aPath, schema, table)
    resp, err := r.flight.Execute(ctx, sql)
    if err != nil { return nil, fmt.Errorf("load %s.%s: %w", schema, table, err) }
    out := &engineLoadResp{
        Status: resp.Status, Schema: schema, Table: table,
        FilePath: s3aPath, FileFormat: "csv",
        RowsLoaded: resp.RowCount, Message: "",  // or resp.Message on error
    }
    return out, nil
}
```
Note: the existing `LoadFile` returns `Status != "ok"` + `Message` on non-200. The flight
version should set `Status: resp.Status` and `Message: resp.Message` (empty on success).
The `count` row from Sail is single-column; `resp.RowCount` is the loaded row count. If the
`count` value itself is needed (e.g. `IcebergCommitExec` reports `reported_row_count`), read
`resp.Rows[0][0]` when present and use it for `RowsLoaded`.

---

## 7. E4 — One `LOAD DATA` glob per business date

Today `internal/cli/load.go` `loadCSVFiles` (`:189-217`) loops per CSV key:
```go
for _, key := range csvKeys {
    s3aPath := "s3a://" + path.Join(bucket, key)
    resp, err := repo.LoadFile(ctx, schema, table, s3aPath)
    ...
}
return g.Wait()
```

Replace with a **single `LOAD DATA` glob** per table + business date:
```go
glob := "s3a://" + path.Join(bucket, table, businessDate) + "/*.csv"
resp, err := repo.LoadFile(ctx, "landing", table, glob)
```
This uses Sail's `LOAD DATA` fast/glob path → **one atomic Iceberg commit** instead of N
commits (faster, one snapshot). Keep the per-file `LoadFile` only where a single versioned
CSV must be loaded (mapper version loads, `internal/cli/mapper.go:306`).

---

## 8. E5 — Config / scripts

- `ICEBERG_URL` → `grpc://<sail-flight-host>:32010` (was `http://…/engine/dbt`).
- `ICEBERG_TOKEN` → **remove** (Flight handshake stays permissive; see §10).
- `ICEBERG_CATALOG` unchanged (e.g. `sail`) — used to build `CALL sail.system.<proc>`.
- Update `run.sh`, `scripts/_common.sh`, README/docs, and any Docker/k8s env
  (`build.sh`, `Dockerfile`, `charts/`) to point at the Flight endpoint.

---

## 9. E6 — Deploy (Phase F)

1. **k8s (`k8s/sail.yaml`)**: add `sail flight server --ip 0.0.0.0 --port 32010` as a second
   container in the same pod (or a separate Deployment) with the same env as the Spark
   server (`SAIL_MODE=kubernetes-cluster`, `SAIL_CLUSTER__*`, `SAIL_KUBERNETES__*` so LOAD
   DATA spawns worker pods). Add a Service entry for 32010 (`sail-flight-service`).
2. **heimdall** points at `grpc://sail-flight-service:32010`.
3. Retire `sail-rest-service` from use (it exists only on `feat/iceberg-ops`; the whole point
   is to not use REST).

---

## 10. Open decisions (confirmed)

1. **Scope:** E1–E6 in one plan. ✅
2. **Flight auth:** keep `do_handshake` permissive; drop `ICEBERG_TOKEN`. ✅
3. **Client shape:** `flightsql.Client.Execute` (`CommandStatementQuery`) + `DoGet` for every
   statement, including LOAD DATA / CALL / TRUNCATE. `ExecuteUpdate` is **not** used (Sail
   doesn't implement it; heimdall needs rows). ✅

---

## 11. Suggested sequencing

1. **E1** (dep) + **E2** (flight client layer) — the core.
2. **E3** (rewire `iceberg.go`) — drop-in; existing cli/repository untouched.
3. **E4** (glob load in `cli/load.go`) — behavior change, separate commit.
4. **E5** (config/scripts) + **E6** (deploy) — cutover.
5. **E2E:** heimdall flows (load → dbt → rollback → rollforward → expire → truncate) against
   a running `sail flight server`; validate `icebergQueryResp` shapes
   (columns/rows/rowCount) match what callers parse.

---

## 12. Reference

- heimdall: `internal/db/iceberg.go`, `internal/db/repository.go`,
  `internal/cli/load.go`, `internal/cli/mapper.go`, `cmd/heimdall/main.go`,
  `run.sh`, `scripts/_common.sh`
- Sail Flight server: `crates/sail-flight/src/service.rs` (only `CommandStatementQuery`
  implemented), `crates/sail-cli/src/flight/server.rs`
- arrow-go: `github.com/apache/arrow-go/v18@v18.7.0/arrow/flight/flightsql/client.go`
  (`Execute` `:132`, `DoGet` `:365`, `ExecuteUpdate` `:171`)
- Sail parity docs: `docs/dev/heimdall-parity-plan.md`,
  `docs/dev/call-procedures-spec-deviations.md`, `docs/dev/expire-snapshots-file-gc-plan.md`
