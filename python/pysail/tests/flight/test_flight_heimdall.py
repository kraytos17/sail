"""Flight SQL integration tests covering the heimdall SQL surface.

These tests exercise the engine's heimdall-parity features over the Arrow Flight SQL
transport using Sail's default Memory catalog and local ``file://`` Iceberg tables:

- ``LOAD DATA INPATH ... INTO TABLE``
- ``SELECT ... FROM <t>.refs`` / ``<t>.snapshots`` metadata tables
- ``TRUNCATE TABLE``
- ``CALL <catalog>.system.{rollback_to_snapshot,set_current_snapshot,expire_snapshots}``
- ``SELECT ... VERSION AS OF <snapshot_id>``

No external catalog or object store is required: tables live under a temporary directory
and the server uses the default Memory catalog.
"""

from datetime import datetime, timezone

import pytest
from adbc_driver_flightsql import dbapi

TABLE = "events"


@pytest.fixture
def connection(flight_catalog_uri):
    uri, _ = flight_catalog_uri
    conn = dbapi.connect(uri)
    yield conn
    conn.close()


def _execute(connection, sql: str):
    cur = connection.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def _execute_ignore_result(connection, sql: str):
    _execute(connection, sql)


def _write_csv(warehouse, name: str, rows: list[tuple]) -> str:
    """Write a headerless CSV and return its ``file://`` URI."""
    path = warehouse / f"{name}.csv"
    lines = [",".join(str(v) for v in row) for row in rows]
    path.write_text("\n".join(lines) + "\n")
    return path.absolute().as_uri()


def _create_table(connection, warehouse, table: str) -> str:
    """Create an Iceberg table at a ``file://`` location under the warehouse."""
    location = warehouse / table
    location_uri = location.absolute().as_uri()
    _execute_ignore_result(
        connection,
        f"CREATE TABLE {table} (id BIGINT, event STRING) "
        f"USING iceberg LOCATION '{location_uri}'",
    )
    return location_uri


def _load(connection, csv_uri: str, table: str, *, overwrite: bool = False) -> None:
    clause = "OVERWRITE" if overwrite else ""
    _execute_ignore_result(
        connection,
        f"LOAD DATA INPATH '{csv_uri}' {clause} INTO TABLE {table}",
    )


def _snapshot_ids(connection, table: str) -> list[int]:
    rows = _execute(
        connection,
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY snapshot_id",
    )
    return [int(row[0]) for row in rows]


def test_load_data_and_read_back(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    csv_uri = _write_csv(warehouse, "data", [(1, "alice"), (2, "bob"), (3, "carol")])
    _load(connection, csv_uri, TABLE)

    rows = _execute(connection, f"SELECT id, event FROM {TABLE} ORDER BY id")
    assert rows == [(1, "alice"), (2, "bob"), (3, "carol")]


def test_load_data_overwrite_replaces_rows(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data", [(1, "alice")]), TABLE)
    _load(
        connection,
        _write_csv(warehouse, "data", [(9, "zed")]),
        TABLE,
        overwrite=True,
    )

    rows = _execute(connection, f"SELECT id, event FROM {TABLE} ORDER BY id")
    assert rows == [(9, "zed")]


def test_refs_and_snapshots_metadata_tables(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data", [(1, "alice")]), TABLE)

    # `.refs`: the main branch points at the only snapshot.
    refs = _execute(connection, f"SELECT name, type FROM {TABLE}.refs")
    assert ("main", "branch") in refs

    # heimdall's `current snapshot id` shape.
    current = _execute(
        connection,
        f"SELECT CAST(snapshot_id AS STRING) FROM {TABLE}.refs WHERE name = 'main'",
    )
    assert len(current) == 1

    # `.snapshots`: at least one row, latest-snapshot shape.
    snapshots = _execute(
        connection,
        f"SELECT snapshot_id, committed_at FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1",
    )
    assert len(snapshots) == 1
    snapshot_id = int(snapshots[0][0])

    # heimdall's `snapshot exists` and `parent id` shapes.
    exists = _execute(
        connection,
        f"SELECT 1 FROM {TABLE}.snapshots WHERE snapshot_id = {snapshot_id}",
    )
    assert exists == [(1,)]
    parent = _execute(
        connection,
        f"SELECT CAST(parent_id AS STRING) FROM {TABLE}.snapshots WHERE snapshot_id = {snapshot_id}",
    )
    assert len(parent) == 1


def test_truncate_table(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data", [(1, "alice"), (2, "bob")]), TABLE)

    _execute_ignore_result(connection, f"TRUNCATE TABLE {TABLE}")
    assert _execute(connection, f"SELECT COUNT(*) FROM {TABLE}") == [(0,)]

    # A subsequent LOAD DATA append works after truncate.
    _load(connection, _write_csv(warehouse, "data", [(3, "carol")]), TABLE)
    rows = _execute(connection, f"SELECT id, event FROM {TABLE} ORDER BY id")
    assert rows == [(3, "carol")]


def test_rollback_and_set_current_snapshot(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data", [(1, "alice")]), TABLE)
    _load(connection, _write_csv(warehouse, "data", [(2, "bob")]), TABLE)
    ids = _snapshot_ids(connection, TABLE)
    assert len(ids) >= 2
    older, newer = ids[0], ids[-1]

    # Rollback to the older snapshot: output carries before/after ids.
    rows = _execute(
        connection,
        f"CALL sail.system.rollback_to_snapshot('{TABLE}', {older})",
    )
    assert len(rows) == 1
    previous, current = rows[0]
    assert int(current) == older

    current_main = int(
        _execute(
            connection,
            f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'",
        )[0][0]
    )
    assert current_main == older

    # set_current_snapshot back to the newer snapshot.
    rows = _execute(
        connection,
        f"CALL sail.system.set_current_snapshot('{TABLE}', {newer})",
    )
    assert len(rows) == 1
    current_main = int(
        _execute(
            connection,
            f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = 'main'",
        )[0][0]
    )
    assert current_main == newer


def test_rollback_rejects_non_ancestor(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data", [(1, "alice")]), TABLE)

    # A snapshot id that exists in no metadata and is not an ancestor.
    with pytest.raises(Exception, match="not an ancestor of the current state"):
        _execute(
            connection,
            f"CALL sail.system.rollback_to_snapshot('{TABLE}', 999999)",
        )


def test_expire_snapshots_returns_counts_and_removes_files(
    connection, flight_catalog_uri
):
    _, warehouse = flight_catalog_uri
    location = _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data1", [(1, "alice")]), TABLE)
    _load(connection, _write_csv(warehouse, "data2", [(2, "bob")]), TABLE)
    _load(connection, _write_csv(warehouse, "data3", [(3, "carol")]), TABLE)
    before_ids = _snapshot_ids(connection, TABLE)
    assert len(before_ids) >= 3

    # Expire snapshots older than "now" — keep only the latest by retain_last=1.
    now = datetime.now(timezone.utc)
    rows = _execute(
        connection,
        f"CALL sail.system.expire_snapshots('{TABLE}', TIMESTAMP '{now.isoformat()}')",
    )
    assert len(rows) == 1
    assert len(rows[0]) == 6  # six deleted_*_count columns

    remaining = _snapshot_ids(connection, TABLE)
    assert len(remaining) < len(before_ids)

    # Retained data is still readable.
    rows = _execute(connection, f"SELECT id FROM {TABLE} ORDER BY id")
    assert len(rows) == 3


def test_version_as_of(connection, flight_catalog_uri):
    _, warehouse = flight_catalog_uri
    _create_table(connection, warehouse, TABLE)
    _load(connection, _write_csv(warehouse, "data1", [(1, "alice")]), TABLE)
    _load(connection, _write_csv(warehouse, "data2", [(2, "bob")]), TABLE)
    ids = _snapshot_ids(connection, TABLE)
    assert len(ids) >= 2
    older = ids[0]

    rows = _execute(
        connection,
        f"SELECT id FROM {TABLE} VERSION AS OF {older} ORDER BY id",
    )
    assert rows == [(1,)]
