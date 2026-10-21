import importlib.util
from pathlib import Path

import pytest


@pytest.mark.skipif(importlib.util.find_spec("airflow") is None, reason="Airflow se verifica dentro de su imagen")
def test_dag_import_and_dependencies():
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.dags.get("daily_sales_medallion")
    assert dag is not None
    assert dag.max_active_runs == 1
    assert dag.get_task("gold_and_tests").upstream_task_ids == {"bronze_ingest", "silver_and_quarantine"}
    assert dag.get_task("commit_checkpoint").upstream_task_ids == {"bronze_ingest", "gold_and_tests"}
