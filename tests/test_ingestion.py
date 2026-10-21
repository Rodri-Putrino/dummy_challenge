import csv
from pathlib import Path

import pytest

from sales_pipeline.ingest import COLUMNS, connect, ingest, initialize

pytestmark = pytest.mark.integration


def test_ingestion_is_atomic_and_preserves_bad_width(warehouse, tmp_path):
    source = tmp_path / "input.csv"
    with source.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerow(["a", "c", "2026-01-01", "10", "COMPLETED", "AR"])
        writer.writerow(["bad", "c"])
    result = ingest(source, tmp_path / "archive")
    assert result["row_count"] == 2
    assert ingest(source, tmp_path / "archive")["reused"] is True
    with connect() as conn:
        assert conn.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0] == 2
        assert conn.execute("SELECT raw_payload FROM bronze.transactions_raw WHERE ingestion_error IS NOT NULL").fetchone()[0] == ["bad", "c"]
        assert conn.execute("SELECT count(*) FROM audit.ingestion_batches").fetchone()[0] == 1

    source.write_text(
        ",".join(COLUMNS) + '\nvalid,c,2026-01-01,5,COMPLETED,AR\n"unterminated\n'
    )
    with pytest.raises(csv.Error):
        ingest(source, tmp_path / "archive")
    with connect() as conn:
        assert conn.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM audit.ingestion_batches").fetchone()[0] == 1


@pytest.mark.parametrize("content", ["", ",".join(COLUMNS) + "\n", "wrong,header\n1,2\n"])
def test_empty_or_wrong_header_fails_without_partial_batch(warehouse, tmp_path, content):
    with connect() as conn:
        initialize(conn)
    source = tmp_path / "input.csv"
    source.write_text(content)
    with pytest.raises(ValueError):
        ingest(source, tmp_path / "archive")
    with connect() as conn:
        assert conn.execute("SELECT count(*) FROM audit.ingestion_batches").fetchone()[0] == 0
