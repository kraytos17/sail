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

## 12. Known limitations / notes

- **Bare `DESCRIBE <t>`** is not accepted by the Sail parser (requires `TABLE`/`VIEW`/`EXTENDED` keyword).
- **`DESCRIBE EXTENDED view <v>`** is mis-parsed as a table + column describe and errors (`DESCRIBE VIEW EXTENDED <v>` is the supported order).
- **`DESCRIBE TABLE t PARTITION(...) col`** (partition-spec describe) is not yet implemented.
- **Nested columns** in `ADD/DROP COLUMNS` (`a.b.c`) and column-level `DESCRIBE TABLE t a.b` are treated as flat names (top-level only); nested paths produce a "not found" error rather than traversing structs.
- **Dropping a column referenced by the partition spec / sort order, or the last remaining column**, is rejected by the Iceberg REST server (surfaced as a 400 with the server message).
- **`SHOW TBLPROPERTIES`/`ALTER TABLE` metadata ops** apply to catalog-managed (Iceberg REST) tables via the catalog commit; for filesystem-commit tables the storage layer is used.
