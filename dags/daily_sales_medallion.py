"""DAG diario; sólo metadatos pequeños pasan por XCom."""
from datetime import date, timedelta
import logging
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.python import PythonSensor


def input_file(context):
    configured = (context["dag_run"].conf or {}).get("source_date")
    source_date = date.fromisoformat(configured) if configured else context["data_interval_start"].date()
    return Path(os.environ.get("DATA_ROOT", "/data")) / "incoming" / source_date.isoformat() / "transactions_raw.csv"


def file_exists():
    return input_file(get_current_context()).is_file()


def report_failure(context):
    from sales_pipeline.runner import fail_run
    logging.error("Falló daily_sales: run_id=%s task_id=%s log=%s",
                  context["run_id"], context["task_instance"].task_id, context["task_instance"].log_url)
    try:
        fail_run(context["run_id"], f"task_failed:{context['task_instance'].task_id}")
    except Exception:
        logging.exception("No se pudo persistir auditoría de fallo")


@dag(
    dag_id="daily_sales_medallion",
    description="CSV → Bronze → dbt Silver/cuarentena → dbt Gold por día y país",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=65),
        "on_failure_callback": report_failure,
    },
    tags=["sales", "medallion", "dbt"],
)
def daily_sales():
    wait = PythonSensor(
        task_id="wait_for_csv", python_callable=file_exists,
        mode="reschedule", poke_interval=60, timeout=3600,
    )

    @task
    def bronze_ingest():
        from sales_pipeline.ingest import ingest
        from sales_pipeline.runner import begin_run, processing_window
        context = get_current_context()
        begin_run(context["run_id"])
        batch = ingest(input_file(context), Path(os.environ.get("DATA_ROOT", "/data")) / "archive")
        return {**batch, **processing_window(context["run_id"], batch["batch_seq"])}

    @task
    def reference_seed(batch):
        from sales_pipeline.runner import run_dbt
        run_dbt("seed", get_current_context()["run_id"], batch)

    @task
    def silver_and_quarantine(batch):
        from sales_pipeline.runner import run_dbt
        run_dbt("silver", get_current_context()["run_id"], batch)

    @task
    def gold_and_tests(batch):
        from sales_pipeline.runner import run_dbt
        run_dbt("gold", get_current_context()["run_id"], batch)

    @task
    def commit_checkpoint(batch):
        from sales_pipeline.runner import finish_run
        finish_run(get_current_context()["run_id"], batch)

    batch = bronze_ingest()
    wait >> batch
    seeded = reference_seed(batch)
    silver = silver_and_quarantine(batch)
    gold = gold_and_tests(batch)
    finished = commit_checkpoint(batch)
    seeded >> silver >> gold >> finished


daily_sales()
