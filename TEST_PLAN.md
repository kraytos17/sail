# Sail Iceberg Test Plan

Copy and paste each command into the PySpark shell (`sail spark shell`).

**Environment**: catalog-managed Iceberg (Polaris REST, `commit = IcebergRestCommit`), data on MinIO (`s3://work/`), tables under namespace `test1`.

> **Syntax notes (Sail parser gaps / conventions)**
> - `DESCRIBE` requires a keyword after it. Bare `DESCRIBE <t>` is **not** parsed. Use `DESCRIBE TABLE <t>`, `DESCRIBE EXTENDED <t>`, `DESCRIBE VIEW <v>`, or `DESCRIBE TABLE <t> <col>`.
> - `DESCRIBE VIEW [EXTENDED] <v>` — `VIEW` comes **before** `EXTENDED`. `DESCRIBE EXTENDED view <v>` is mis-parsed as a table+column describe.
> - `ALTER TABLE ... SET/UNSET TBLPROPERTIES`, `ADD/DROP COLUMNS`, `RENAME TO`, `SHOW TBLPROPERTIES`, and column-level `DESCRIBE TABLE t col` target **catalog-managed (Iceberg REST)** tables.

---

## 0. Setup namespace

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS test1").show()
```

---

## 1. CREATE TABLE

### 1.1 Basic table
```python
spark.sql("CREATE TABLE test1.events (id INT, name STRING, value DOUBLE) USING iceberg").show()
```

### 1.2 Partitioned table (identity partition)
```python
spark.sql("""
CREATE TABLE test1.events_part (id INT, event_date DATE, val DOUBLE) USING iceberg
PARTITIONED BY (event_date)
""").show()
```

### 1.3 Partitioned by time transforms
```python
spark.sql("""
CREATE TABLE test1.events_ts (id INT, ts TIMESTAMP, val DOUBLE) USING iceberg
PARTITIONED BY (years(ts), months(ts))
""").show()
```

### 1.4 Bucketed table
```python
spark.sql("""
CREATE TABLE test1.events_bucket (id INT, user_id STRING) USING iceberg
CLUSTERED BY (user_id) INTO 16 BUCKETS
""").show()
```

### 1.5 Partitioned + bucketed
```python
spark.sql("""
CREATE TABLE test1.events_part_bucket (id INT, event_date DATE, user_id STRING) USING iceberg
PARTITIONED BY (event_date)
CLUSTERED BY (user_id) INTO 8 BUCKETS
""").show()
```

### 1.6 Bucket transform in partition spec
```python
spark.sql("""
CREATE TABLE test1.events_bucket_part (id INT, user_id STRING) USING iceberg
PARTITIONED BY (bucket(16, user_id))
""").show()
```

---

## 2. INSERT INTO (Append)

```python
spark.sql("INSERT INTO test1.events VALUES (1, 'alice', 10.5)").show()
spark.sql("INSERT INTO test1.events VALUES (2, 'bob', 20.0), (3, 'charlie', 30.0)").show()
spark.sql("SELECT * FROM test1.events ORDER BY id").show()
```

---

## 3. INSERT OVERWRITE

### 3.1 Full table overwrite
```python
spark.sql("INSERT OVERWRITE test1.events VALUES (10, 'new_user', 99.9)").show()
spark.sql("SELECT * FROM test1.events ORDER BY id").show()
```

### 3.2 Dynamic partition overwrite
```python
spark.sql("INSERT INTO test1.events_part VALUES (1, '2024-01-15', 10.0), (2, '2024-01-15', 20.0), (3, '2024-01-16', 30.0)").show()
spark.sql("INSERT OVERWRITE test1.events_part VALUES (10, '2024-01-15', 99.0)").show()
spark.sql("SELECT * FROM test1.events_part ORDER BY id").show()
```

---

## 4. REPLACE WHERE (Predicate Overwrite)

```python
spark.sql("""
CREATE TABLE test1.events_replace (id INT, event_date DATE, name STRING) USING iceberg
PARTITIONED BY (event_date)
""").show()

spark.sql("INSERT INTO test1.events_replace VALUES (1, '2024-01-15', 'alice'), (2, '2024-01-15', 'bob'), (3, '2024-01-16', 'charlie')").show()

spark.sql("""
INSERT INTO test1.events_replace REPLACE WHERE (event_date = '2024-01-15')
VALUES (10, '2024-01-15', 'new_user')
""").show()

spark.sql("SELECT * FROM test1.events_replace ORDER BY id").show()
```

---

## 5. DELETE FROM

### Setup
```python
spark.sql("""
CREATE TABLE test1.events_del (id INT, event_date DATE, name STRING, score DOUBLE)
USING iceberg PARTITIONED BY (event_date)
""").show()

spark.sql("INSERT INTO test1.events_del VALUES (1, '2024-01-15', 'alice', 10.0), (2, '2024-01-15', 'bob', 20.0), (3, '2024-01-16', 'charlie', 30.0), (4, '2024-01-17', 'dave', 40.0)").show()
```

### 5.1 DELETE with partition predicate
```python
spark.sql("DELETE FROM test1.events_del WHERE event_date = '2024-01-15'").show()
spark.sql("SELECT * FROM test1.events_del ORDER BY id").show()
```

### 5.2 DELETE with non-partition predicate
```python
spark.sql("DELETE FROM test1.events_del WHERE score > 35").show()
spark.sql("SELECT * FROM test1.events_del ORDER BY id").show()
```

### 5.3 TRUNCATE
```python
spark.sql("DELETE FROM test1.events_del").show()
spark.sql("SELECT COUNT(*) FROM test1.events_del").show()
```

---

## 6. UPDATE (targeted rewrite)

### Setup
```python
spark.sql("""
CREATE TABLE test1.events_upd (id INT, event_date DATE, name STRING, score DOUBLE)
USING iceberg PARTITIONED BY (event_date)
""").show()

spark.sql("INSERT INTO test1.events_upd VALUES (1, '2024-01-15', 'alice', 10.0), (2, '2024-01-15', 'bob', 20.0), (3, '2024-01-16', 'charlie', 30.0)").show()
```

### 6.1 UPDATE with partition predicate
```python
spark.sql("UPDATE test1.events_upd SET score = 100.0 WHERE event_date = '2024-01-15'").show()
# Expected: count = 2  (rows matched/affected: alice + bob)
spark.sql("SELECT * FROM test1.events_upd ORDER BY id").show()
# Expected: 1 alice 100.0 | 2 bob 100.0 | 3 charlie 30.0
```

> **Count semantics**: the UPDATE result `count` is the number of rows **matched** by the predicate (here 2), **not** the total rows written (3 — the untouched charlie row is carried through to a new file so the commit can rewrite the manifest). `SELECT count(*)` on the table still returns 3.

### 6.2 UPDATE with non-partition predicate
```python
spark.sql("UPDATE test1.events_upd SET score = 999.0 WHERE score > 50").show()
spark.sql("SELECT * FROM test1.events_upd WHERE score = 999.0").show()
```

### 6.3 UPDATE multiple columns
```python
spark.sql("UPDATE test1.events_upd SET score = score * 2, name = 'updated' WHERE event_date = '2024-01-16'").show()
spark.sql("SELECT * FROM test1.events_upd WHERE id = 3").show()
```

### 6.4 UPDATE all rows (no predicate)
```python
spark.sql("UPDATE test1.events_upd SET score = 0").show()
spark.sql("SELECT score FROM test1.events_upd").show()
```

### 6.5 UPDATE matching nothing
```python
spark.sql("UPDATE test1.events_upd SET score = 5.0 WHERE id = 999").show()
# Expected: count = 0 (no-op; table unchanged)
```

---

## 7. Temp views + DESCRIBE

### 7.1 Create a temporary view
```python
spark.sql("create temp view test_v as select '2025-09-30' as buss_date").show()
# Expected: true
```

### 7.2 DESCRIBE TABLE
```python
spark.sql("DESCRIBE TABLE test1.events_upd").show()
# Expected: columns id, event_date, name, score
```

### 7.3 DESCRIBE EXTENDED
```python
spark.sql("DESCRIBE EXTENDED test1.events_upd").show(truncate=False)
# Expected: columns + "# Detailed Table Information" incl. Location, Table Properties, etc.
```

### 7.4 DESCRIBE VIEW
```python
spark.sql("DESCRIBE VIEW test_v").show()
# Expected: buss_date | string | NULL
spark.sql("DESCRIBE VIEW EXTENDED test_v").show()
# Expected: same single column row (EXTENDED adds no extra rows for a view)
```

### 7.5 DESCRIBE TABLE ... column (column-level)
```python
spark.sql("DESCRIBE TABLE test1.events_upd score").show()
# Expected: score | double | NULL  (single row)

spark.sql("DESCRIBE TABLE test_v buss_date").show()
# Expected: buss_date | string | NULL

spark.sql("DESCRIBE TABLE test1.events_upd nope").show()
# Expected error: Column 'nope' not found
```

> **Gotcha**: bare `DESCRIBE test1.events_upd` (no keyword) is a **parser error** ("found test1 ... expected 'TABLE'..."). Always use `DESCRIBE TABLE`, `DESCRIBE VIEW`, or `DESCRIBE EXTENDED`.

---

## 8. ALTER TABLE (catalog-managed / Iceberg REST)

> All `ALTER TABLE` operations below target a catalog-managed table and are executed via the Iceberg REST commit (`update_table`), not the storage layer.

### 8.1 RENAME TO
```python
spark.sql("CREATE TABLE test1.old_name (id INT) USING iceberg").show()
spark.sql("ALTER TABLE test1.old_name RENAME TO test1.new_name").show()
# Expected: true
spark.sql("SHOW TABLES IN test1").show()
# Expected: new_name present, old_name gone
spark.sql("DESCRIBE TABLE test1.new_name").show()
# Data/metadata intact after rename (Location unchanged)
```

### 8.2 SET TBLPROPERTIES
```python
spark.sql("ALTER TABLE test1.new_name SET TBLPROPERTIES ('description' = 'test1 table')").show()
# Expected: true
spark.sql("SHOW TBLPROPERTIES test1.new_name").show(truncate=False)
# Expected: a row with key 'description' value 'test1 table' among the properties
```

### 8.3 UNSET TBLPROPERTIES
```python
spark.sql("ALTER TABLE test1.new_name UNSET TBLPROPERTIES ('description')").show()
# Expected: true
spark.sql("SHOW TBLPROPERTIES test1.new_name").show(truncate=False)
# Expected: 'description' no longer present

spark.sql("ALTER TABLE test1.new_name UNSET TBLPROPERTIES ('never_set')").show()
# Expected error: cannot remove property 'never_set' because it is not set on the table
spark.sql("ALTER TABLE test1.new_name UNSET TBLPROPERTIES IF EXISTS ('never_set')").show()
# Expected: true (no-op)
```

### 8.4 ADD COLUMNS
```python
spark.sql("ALTER TABLE test1.new_name ADD COLUMNS (new_col INT, comment STRING)").show()
# Expected: true
spark.sql("DESCRIBE TABLE test1.new_name").show()
# Expected: new columns new_col, comment visible
```

### 8.5 DROP COLUMNS
```python
spark.sql("ALTER TABLE test1.new_name DROP COLUMNS (new_col)").show()
# Expected: true
spark.sql("DESCRIBE TABLE test1.new_name").show()
# Expected: new_col gone, comment still present
```

### 8.6 DROP COLUMNS IF EXISTS
```python
spark.sql("ALTER TABLE test1.new_name DROP COLUMNS IF EXISTS (nonexistent_col)").show()
# Expected: true (silent no-op — no schema commit issued)
spark.sql("DESCRIBE TABLE test1.new_name").show()
# Expected: unchanged

spark.sql("ALTER TABLE test1.new_name DROP COLUMNS (nonexistent_col)").show()
# Expected error: Column 'nonexistent_col' not found in Iceberg table schema
```

---

## 9. SHOW TBLPROPERTIES

```python
spark.sql("SHOW TBLPROPERTIES test1.new_name").show(truncate=False)
# Expected: all properties as (key, value) rows, sorted by key

spark.sql("SHOW TBLPROPERTIES test1.new_name ('description')").show()
# Expected: single row if the key exists

spark.sql("SHOW TBLPROPERTIES test1.new_name ('missing_key')").show()
# Expected: empty result (no error)
```

---

## 10. MERGE INTO

### Setup
```python
spark.sql("""
CREATE TABLE test1.events_merge (id INT, event_date DATE, name STRING, score DOUBLE)
USING iceberg PARTITIONED BY (event_date)
""").show()

spark.sql("INSERT INTO test1.events_merge VALUES (1, '2024-01-15', 'alice', 10.0), (2, '2024-01-15', 'bob', 20.0)").show()

spark.sql("CREATE TEMP VIEW src_v AS SELECT 1 AS id, '2024-01-15' AS event_date, 'alice_2' AS name, 11.0 AS score").show()
```

### 10.1 UPDATE + INSERT clauses
```python
spark.sql("""
MERGE INTO test1.events_merge AS t
USING src_v AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET score = s.score
WHEN NOT MATCHED THEN INSERT (id, event_date, name, score) VALUES (s.id, s.event_date, s.name, s.score)
""").show()

spark.sql("SELECT * FROM test1.events_merge ORDER BY id").show()
# Expected: id 1 -> alice, score 11.0 ; id 2 unchanged (bob, 20.0)
```

---

## 11. End-to-End Workflow

```python
spark.sql("""
CREATE TABLE test1.workflow (
    id INT,
    event_date DATE,
    user_id STRING,
    score DOUBLE
) USING iceberg
PARTITIONED BY (event_date)
CLUSTERED BY (user_id) INTO 4 BUCKETS
""").show()

spark.sql("INSERT INTO test1.workflow VALUES (1, '2024-01-15', 'alice', 85.0), (2, '2024-01-15', 'bob', 92.0), (3, '2024-01-16', 'alice', 78.0)").show()

spark.sql("UPDATE test1.workflow SET score = 100 WHERE event_date = '2024-01-16'").show()
# Expected: count = 1 (matched rows)

spark.sql("INSERT OVERWRITE test1.workflow SELECT * FROM test1.workflow WHERE event_date = '2024-01-15' LIMIT 0").show()

spark.sql("DELETE FROM test1.workflow WHERE score < 50").show()

spark.sql("INSERT INTO test1.workflow VALUES (4, '2024-01-17', 'charlie', 95.0)").show()

spark.sql("ALTER TABLE test1.workflow ADD COLUMNS (status STRING)").show()
spark.sql("ALTER TABLE test1.workflow SET TBLPROPERTIES ('owner' = 'sail')").show()

spark.sql("UPDATE test1.workflow SET status = 'active' WHERE score >= 90").show()
# Expected: count = 2 (ids 2 and 4)

spark.sql("SELECT * FROM test1.workflow ORDER BY id").show()

spark.sql("SHOW TBLPROPERTIES test1.workflow ('owner')").show()
# Expected: owner | sail

spark.sql("DESCRIBE TABLE test1.workflow status").show()
# Expected: status | string | NULL

spark.sql("ALTER TABLE test1.workflow RENAME TO test1.workflow_final").show()
spark.sql("SHOW TABLES IN test1").show()
```

---

## 12. LOAD DATA (optimized file → Iceberg)

`LOAD DATA INPATH '<path>' [OVERWRITE] INTO TABLE <ns>.<tbl>` loads files from object storage
into an Iceberg table. **Parquet files whose schema name+type-matches the table are
registered directly** (footer-only read, no rewrite — the source file path appears in the
manifest). CSV/JSON and schema-mismatched parquet go through the rewrite fallback.

> **Path format (important)**: `<path>` must be a **full absolute object-store URL** —
> `s3a://<bucket>/<key>`. A bare key like `data/test/test.csv` has no scheme/bucket and is
> rejected at plan time (`invalid source location`). Include the bucket:
> `s3a://work/data/test/test.csv` (if the bucket is `work`). A directory/glob needs a
> trailing slash (`s3a://work/data/test/`) or wildcard (`s3a://work/data/test/*.csv`).

### 12.0 Quick start — load a single CSV into an Iceberg table (your use case)

```python
# Source: MinIO object data/test/test.csv inside bucket <bucket> (e.g. work).
# Header row column names must match the table columns.

# 1. Create the table (once)
spark.sql("CREATE TABLE test.test_tbl (id INT, name STRING) USING iceberg").show()

# 2. Load the CSV (use the FULL s3a:// URL, not the bare key)
spark.sql("LOAD DATA INPATH 's3a://<bucket>/data/test/test.csv' INTO TABLE test.test_tbl").show()
# Expected: count = number of CSV data rows

# 3. Verify
spark.sql("SELECT COUNT(*) FROM test.test_tbl").show()
spark.sql("SELECT * FROM test.test_tbl").show()
```

### 12.1 Parquet fast path (single file, append)

```python
spark.sql("CREATE TABLE test1.load_t (id INT, name STRING) USING iceberg").show()

# Produce an external parquet file with schema (id INT, name STRING) on s3.
# s3://work/loads/in/data.parquet with rows (1,'a'),(2,'b'),(3,'c')

spark.sql("LOAD DATA INPATH 's3a://work/loads/in/data.parquet' INTO TABLE test1.load_t").show()
# Expected: count = 3

spark.sql("SELECT COUNT(*) FROM test1.load_t").show()
# Expected: 3

# No-rewrite verification (not via SQL metadata tables — Sail doesn't expose
# <table>.files): after the load, the latest snapshot's manifest references the
# SOURCE path s3a://work/loads/in/data.parquet directly (inspect the manifest list
# on the object store), i.e. no new table-owned parquet file was written.
```

### 12.2 Many parquet files via glob / directory (append)

```python
# s3://work/loads/in/ has data1.parquet, data2.parquet, data3.parquet (all matching schema)
spark.sql("LOAD DATA INPATH 's3a://work/loads/in/*.parquet' INTO TABLE test1.load_t").show()
# Expected: count = 9 (3 per file; total_rows reported)

spark.sql("LOAD DATA INPATH 's3a://work/loads/in/' INTO TABLE test1.load_t").show()
# Expected: count = 9 (directory listing)
```

### 12.3 OVERWRITE (full-table replace)

```python
spark.sql("LOAD DATA INPATH 's3a://work/loads/in/data.parquet' OVERWRITE INTO TABLE test1.load_t").show()
# Expected: count = 3
spark.sql("SELECT COUNT(*) FROM test1.load_t").show()
# Expected: 3  (previous 9 rows replaced; new snapshot contains only loaded files)
```

### 12.4 CSV fallback (rewrite)

```python
# s3://work/loads/in/data.csv with header "id,name" and rows (1,'a'),(2,'b')
spark.sql("LOAD DATA INPATH 's3a://work/loads/in/data.csv' INTO TABLE test1.load_t").show()
# Expected: count = 2  (rewritten to parquet; file_path is now a table-owned path)
# Note: before the count-fix this reported 0; the writer's actual row count now flows through.
spark.sql("SELECT COUNT(*) FROM test1.load_t").show()
```

### 12.5 Schema-mismatch parquet → fallback

```python
# s3://work/loads/in/bad.parquet with schema (id INT, extra STRING) — missing `name`
spark.sql("LOAD DATA INPATH 's3a://work/loads/in/bad.parquet' INTO TABLE test1.load_t").show()
# Expected: parquet is rewritten (schema mismatch) OR scan error if column mapping fails
```

### 12.6 Not-supported

```python
spark.sql("LOAD DATA LOCAL INPATH '/local/data.parquet' INTO TABLE test1.load_t").show()
# Expected error: LOAD DATA LOCAL is not supported

spark.sql("LOAD DATA INPATH 's3a://x/y.parquet' INTO TABLE test1.load_t PARTITION (id = 1)").show()
# Expected error: LOAD DATA ... PARTITION is not supported

spark.sql("LOAD DATA INPATH 'data/test/test.csv' INTO TABLE test1.load_t").show()
# Expected error: invalid source location — a bare key (no scheme/bucket) is rejected;
# the path must be a full s3a://<bucket>/<key> URL.
```

### 12.7 Cross-bucket load (path-resolution fix)

```python
# Write a parquet file into a SEPARATE bucket (same MinIO endpoint).
(spark.createDataFrame([(10,"x"),(11,"y")], ["id","name"])
      .coalesce(1).write.mode("overwrite").parquet("s3a://data-bucket/loads/in.parquet/"))

spark.sql("LOAD DATA INPATH 's3a://data-bucket/loads/in.parquet/' INTO TABLE test1.load_t").show()
# Expected: count = 2  (source read from data-bucket, NOT the table's work bucket)
```

### 12.8 Mixed formats in one glob (count-sum across branches)

```python
# s3a://work/loads/mix/ contains a.parquet (3 rows) + b.csv (2 rows)
spark.sql("LOAD DATA INPATH 's3a://work/loads/mix/' INTO TABLE test1.load_t").show()
# Expected: count = 5  (3 fast-register + 2 rewrite — count sums across both branches)
```

### 12.9 Empty source (known edge, harmless)

```python
# s3a://work/loads/empty/ exists but has no matching files
spark.sql("LOAD DATA INPATH 's3a://work/loads/empty/' INTO TABLE test1.load_t").show()
# Expected: succeeds; creates a no-op snapshot (zero data files) — not an error
```

### 12.10 Many CSVs in a directory (parallel fallback writers)

The fallback now builds **one writer per chunk of files** (per-file when the count is small,
bounded to `target_partitions` writers when large), so CSV parse + parquet encode run in
parallel across files.

```python
# s3a://work/loads/many/ has 20 CSVs, each (id,name) with 3 rows → 60 rows total
spark.sql("LOAD DATA INPATH 's3a://work/loads/many/' INTO TABLE test1.load_t").show()
# Expected: count = 60  (summed across the parallel writer branches)

spark.sql("SELECT COUNT(*) FROM test1.load_t").show()
```

### 12.11 Compressed CSV (gzip/zstd detection)

```python
# s3a://work/loads/gz/ has data.csv.gz (gzip-compressed, header "id,name", 2 rows)
spark.sql("LOAD DATA INPATH 's3a://work/loads/gz/data.csv.gz' INTO TABLE test1.load_t").show()
# Expected: count = 2  (gzip auto-detected from the .gz extension and decompressed)

spark.sql("LOAD DATA INPATH 's3a://work/loads/gz/' INTO TABLE test1.load_t").show()
# Expected: count = 2  (directory form; .csv.zst also supported)
```

---

## 13. Known limitations / notes

- **Bare `DESCRIBE <t>`** is not accepted by the Sail parser (requires `TABLE`/`VIEW`/`EXTENDED` keyword).
- **`DESCRIBE EXTENDED view <v>`** is mis-parsed as a table + column describe and errors (`DESCRIBE VIEW EXTENDED <v>` is the supported order).
- **`DESCRIBE TABLE t PARTITION(...) col`** (partition-spec describe) is not yet implemented.
- **Nested columns** in `ADD/DROP COLUMNS` (`a.b.c`) and column-level `DESCRIBE TABLE t a.b` are treated as flat names (top-level only); nested paths produce a "not found" error rather than traversing structs.
- **Dropping a column referenced by the partition spec / sort order, or the last remaining column**, is rejected by the Iceberg REST server (surfaced as a 400 with the server message).
- **`SHOW TBLPROPERTIES`/`ALTER TABLE` metadata ops** apply to catalog-managed (Iceberg REST) tables via the catalog commit; for filesystem-commit tables the storage layer is used.
- **`LOAD DATA LOCAL` and `LOAD DATA ... PARTITION(...)`** are not supported (v1).
- **`LOAD DATA`** is supported for **Iceberg tables only**; other formats error with `NotSupported`.
- **`LOAD DATA` fast path** registers external parquet by column **name+type** match against the table's current schema; the file must contain all table columns with matching types (extra source columns are dropped). Missing/mismatched columns → rewrite fallback (or scan error if the CSV/parquet cannot be mapped to the table schema).
- **`LOAD DATA` typed bounds** (`lower_bounds`/`upper_bounds`) are not yet populated on registered files (stats are present, bounds omitted) — a known v1 limitation.
- **`LOAD DATA` overwrite** is a full-table replace (static overwrite), not partition-aware.
- **`LOAD DATA` `count`** = total rows loaded (fast register + rewrite), summed across all writer branches.
- **Empty source path/glob** → `LOAD DATA` succeeds but creates a **no-op snapshot** (zero data files) — harmless, not an error.
- **Cross-bucket `LOAD DATA`** resolves the object store per source URL; the target bucket must be reachable with the same object-store config (e.g. same MinIO endpoint/credentials).
- **`LOAD DATA` fallback (CSV/JSON/rewrite)** builds **one writer per chunk of files** — per-file up to `target_partitions`, then chunked — so parse + encode run in parallel across many files. Gzip/bzip2/xz/zstd CSV is auto-detected from the extension.
- **`LOAD DATA` fallback compression** is inferred per chunk from the first file; a single statement should use uniform compression (a mixed `.csv` + `.csv.gz` directory is an edge case).
