import csv
from decimal import Decimal
from pathlib import Path

import pytest

from sales_pipeline.ingest import COLUMNS, ROOT, connect
from sales_pipeline.run import execute

pytestmark = pytest.mark.integration


def fetch(query):
    with connect() as conn:
        return conn.execute(query).fetchall()


def write_csv(path, rows):
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(rows)


def test_recovery_late_data_cross_batch_deduplication_and_full_refresh(warehouse, tmp_path, monkeypatch):
    monkeypatch.setenv("ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    source = ROOT / "data/incoming/2026-01-01/transactions_raw.csv"
    archive = tmp_path / "archive"
    initial = execute(source, archive, "initial")
    assert fetch("SELECT count(*) FROM bronze.transactions_raw") == [(17,)]
    assert fetch("SELECT count(*) FROM silver.silver_transactions") == [(7,)]
    assert fetch("SELECT count(*) FROM quarantine.quarantine_transactions") == [(10,)]
    assert fetch("SELECT total_billed_amount, average_ticket, unique_transaction_count, cancellation_rate_pct FROM gold.daily_country_sales WHERE country_code = 'AR'") == [
        (Decimal("120.00"), Decimal("120.0000"), 3, Decimal("33.3333"))
    ]
    assert fetch("SELECT average_ticket FROM gold.daily_country_sales WHERE country_code = 'BR'") == [(None,)]
    assert fetch("SELECT count(*) FROM silver.silver_transactions WHERE transaction_id = 't9'") == [(1,)]

    late = tmp_path / "late.csv"
    write_csv(late, [
        ["t1", "c1", "2026-01-01", "9999", "CANCELLED", "AR"],
        ["t3", "c3", "2026/01/01", "90", "COMPLETED", "FR"],
        ["t8", "c8", "2026-01-01", "40", "CANCELLED", "AR"],
        ["t14", "c14", "2026-01-01", "45", "PENDING", "BR"],
        ["t15", "c15", "2026-01-02", "200", "COMPLETED", "AR"],
        ["t9", "c9", "2026-01-01T12:00:00Z", "10", "COMPLETED", "US"],
    ])
    with pytest.raises(RuntimeError, match="Fallo simulado"):
        execute(late, archive, "failed", fail_after_silver=True)
    assert fetch("SELECT last_successful_batch_seq FROM audit.pipeline_state") == [(initial["upper_seq"],)]
    assert fetch("SELECT status FROM audit.pipeline_runs WHERE run_id = 'failed'") == [("FAILED",)]
    assert fetch("SELECT total_billed_amount FROM gold.daily_country_sales WHERE country_code = 'ES'") == [(Decimal("80.00"),)]

    # El próximo run recibe OTRO archivo: debe recuperar también el lote fallido.
    next_file = tmp_path / "next.csv"
    write_csv(next_file, [["t16", "c16", "2026-01-02", "0", "PENDING", "AR"]])
    recovered = execute(next_file, archive, "recover")
    assert recovered["lower_seq"] == initial["upper_seq"]
    assert fetch("SELECT count(*) FROM gold.daily_country_sales WHERE country_code = 'ES'") == [(0,)]
    assert fetch("SELECT total_billed_amount FROM gold.daily_country_sales WHERE country_code = 'FR'") == [(Decimal("90.00"),)]
    assert fetch("SELECT amount, status FROM silver.silver_transactions WHERE transaction_id = 't1'") == [(Decimal("120.00"), "COMPLETED")]
    assert fetch("SELECT amount FROM silver.silver_transactions WHERE transaction_id = 't14'") == [(Decimal("45.00"),)]
    assert fetch("SELECT cancellation_rate_pct FROM gold.daily_country_sales WHERE country_code = 'AR' AND business_date = '2026-01-01'") == [(Decimal("66.6667"),)]
    assert fetch("SELECT total_billed_amount, average_ticket, unique_transaction_count FROM gold.daily_country_sales WHERE country_code = 'US'") == [(Decimal("1010.00"), Decimal("505.0000"), 2)]
    assert fetch("SELECT total_billed_amount, average_ticket, unique_transaction_count FROM gold.daily_country_sales WHERE country_code = 'AR' AND business_date = '2026-01-02'") == [(Decimal("200.00"), Decimal("200.0000"), 2)]
    expected = fetch("SELECT * FROM gold.daily_country_sales ORDER BY 1, 2")
    assert fetch("SELECT count(*) FROM bronze.transactions_raw") == [(24,)]
    assert fetch("SELECT count(*) FROM silver.silver_transactions") == [(10,)]
    assert fetch("SELECT count(*) FROM quarantine.quarantine_transactions") == [(14,)]

    # Repetir un archivo antiguo no regresa el checkpoint ni restaura versiones viejas.
    replay = execute(source, archive, "replay")
    assert replay["reused"] is True
    assert fetch("SELECT count(*) FROM bronze.transactions_raw") == [(24,)]
    assert fetch("SELECT * FROM gold.daily_country_sales ORDER BY 1, 2") == expected
    execute(source, archive, "rebuild", full_refresh=True)
    assert fetch("SELECT * FROM gold.daily_country_sales ORDER BY 1, 2") == expected
    assert fetch("SELECT count(*) FROM quarantine.quarantine_transactions") == [(14,)]
