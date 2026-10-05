from pathlib import Path

import pytest

from pysail.testing.spark.steps.iceberg import _current_row_lineage, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _create_table(spark, table_name: str, location: Path) -> None:
    """Create an unpartitioned Iceberg table so LOAD DATA takes the fast path."""
    escaped_location = escape_sql_string_literal(str(location))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT, value BIGINT)
        USING iceberg
        LOCATION '{escaped_location}'
        """
    )


def _write_source_parquet(spark, directory: Path, name: str, rows: list[tuple[int, int]]) -> None:
    (
        spark.createDataFrame(rows, schema="id BIGINT, value BIGINT")
        .write.mode("overwrite")
        .parquet(str(directory / name))
    )


def _first_row_id_of(location: Path, table_name: str) -> int | None:
    """The table's next row id, i.e. the id the next loaded row should receive."""
    return _find_latest_metadata(location).get("next-row-id", 0) or 0


@pytest.mark.parametrize("overwrite", [False, True])
def test_iceberg_load_data_assigns_v3_row_lineage(spark, tmp_path, overwrite):
    """`LOAD DATA` into a V3 table must number the registered files' rows.

    The fast path registers pre-built data files with `first_row_id = None` and
    relies on the commit to assign ids (`materialize_inherited_entry`). If the
    planner ever sets the field itself, the commit skips the file and row ids
    end up null or non-contiguous — which is what this test pins down.
    """
    table_name = f"iceberg_load_data_v3_{int(overwrite)}"
    location = tmp_path / table_name
    source = tmp_path / f"source_v3_{int(overwrite)}"
    _create_table(spark, table_name, location)

    try:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '3')")
        # Read after the upgrade: V2 metadata may not carry the key yet, which
        # would make the lower-bound assertion below vacuous.
        start_row_id = _first_row_id_of(location, table_name)

        # Two files with different row counts, written into one directory so a
        # single LOAD DATA registers both and exercises cross-file numbering.
        _write_source_parquet(spark, source, "a.parquet", [(1, 10), (2, 20)])
        _write_source_parquet(spark, source, "b.parquet", [(3, 30)])

        escaped_source = escape_sql_string_literal(str(source))
        keyword = "OVERWRITE " if overwrite else ""
        spark.sql(f"LOAD DATA INPATH '{escaped_source}' {keyword}INTO TABLE {table_name}")

        lineage = _current_row_lineage(location)
        assert set(lineage) == {1, 2, 3}

        row_ids = sorted(row_id for row_id, _ in lineage.values())
        assert all(row_id is not None for row_id, _ in lineage.values()), (
            "every loaded row must have a row id"
        )
        # Contiguous: the ids must be a run with no gaps, whatever order the
        # files were registered in. A planner-side assignment bug shows up here
        # as either a gap or a duplicate.
        assert row_ids == list(range(row_ids[0], row_ids[0] + len(row_ids))), row_ids
        assert row_ids[0] >= start_row_id, (row_ids[0], start_row_id)
        assert len(set(row_ids)) == len(row_ids), "row ids must be unique"

        metadata = _find_latest_metadata(location)
        assert all(row_id < metadata["next-row-id"] for row_id in row_ids)
        assert metadata["format-version"] == 3  # noqa: PLR2004

        # The lineage columns must be readable from the table itself.
        selected = spark.sql(
            f"SELECT _row_id FROM {table_name} ORDER BY id"
        ).collect()
        assert [row[0] for row in selected] == sorted(lineage[key][0] for key in (1, 2, 3))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
