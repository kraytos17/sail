# Iceberg Feature Test1 Plan

Copy and paste each command into the PySpark shell (`sail spark shell`).

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

## 6. UPDATE

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
spark.sql("SELECT * FROM test1.events_upd ORDER BY id").show()
```

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

### 6.4 UPDATE all rows
```python
spark.sql("UPDATE test1.events_upd SET score = 0").show()
spark.sql("SELECT score FROM test1.events_upd").show()
```

---

## 7. ALTER TABLE

### 7.1 RENAME TO
```python
spark.sql("CREATE TABLE test1.old_name (id INT) USING iceberg").show()
spark.sql("ALTER TABLE test1.old_name RENAME TO test1.new_name").show()
spark.sql("SHOW TABLES IN test1").show()
```

### 7.2 SET TBLPROPERTIES
```python
spark.sql("ALTER TABLE test1.new_name SET TBLPROPERTIES ('description' = 'test1 table')").show()
spark.sql("DESCRIBE EXTENDED test1.new_name").show()
```

### 7.3 ADD COLUMNS
```python
spark.sql("ALTER TABLE test1.new_name ADD COLUMNS (new_col INT, comment STRING)").show()
spark.sql("DESCRIBE test1.new_name").show()
```

### 7.4 DROP COLUMNS
```python
spark.sql("ALTER TABLE test1.new_name DROP COLUMNS (new_col)").show()
spark.sql("DESCRIBE test1.new_name").show()
```

### 7.5 DROP COLUMNS IF EXISTS
```python
spark.sql("ALTER TABLE test1.new_name DROP COLUMNS IF EXISTS (nonexistent_col)").show()
```

---

## 8. End-to-End Workflow

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

spark.sql("INSERT OVERWRITE test1.workflow SELECT * FROM test1.workflow WHERE event_date = '2024-01-15' LIMIT 0").show()

spark.sql("DELETE FROM test1.workflow WHERE score < 50").show()

spark.sql("INSERT INTO test1.workflow VALUES (4, '2024-01-17', 'charlie', 95.0)").show()

spark.sql("ALTER TABLE test1.workflow ADD COLUMNS (status STRING)").show()

spark.sql("UPDATE test1.workflow SET status = 'active' WHERE score >= 90").show()

spark.sql("SELECT * FROM test1.workflow ORDER BY id").show()
```
