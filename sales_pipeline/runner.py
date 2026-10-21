"""Ventana recuperable y ejecución de dbt sin shell ni credenciales en argumentos."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess

from sales_pipeline.ingest import ROOT, connect, initialize

LOG = logging.getLogger(__name__)


def begin_run(run_id: str) -> None:
    with connect() as conn:
        initialize(conn)
        conn.execute(
            """INSERT INTO audit.pipeline_runs (run_id, status) VALUES (%s, 'RUNNING')
               ON CONFLICT (run_id) DO UPDATE SET status = 'RUNNING', error = NULL,
                   finished_at = NULL""", (run_id,)
        )


def processing_window(run_id: str, batch_seq: int) -> dict:
    with connect() as conn:
        lower = conn.execute(
            "SELECT last_successful_batch_seq FROM audit.pipeline_state WHERE pipeline_name = 'daily_sales'"
        ).fetchone()[0]
        upper = conn.execute("SELECT coalesce(max(batch_seq), 0) FROM audit.ingestion_batches").fetchone()[0]
        conn.execute(
            "UPDATE audit.pipeline_runs SET batch_seq = %s, lower_seq = %s, upper_seq = %s WHERE run_id = %s",
            (batch_seq, lower, upper, run_id),
        )
    return {"lower_seq": lower, "upper_seq": upper}


def run_dbt(command: str, run_id: str, window: dict, *, full_refresh: bool = False) -> None:
    project = Path(os.environ.get("PROJECT_ROOT", str(ROOT))) / "dbt"
    artifact_root = Path(os.environ.get("ARTIFACTS_ROOT", str(ROOT / "artifacts")))
    # run_id puede contener ':' y '/' (Airflow); nunca forma parte de un comando shell.
    run_key = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    folder = artifact_root / run_key / command
    folder.mkdir(parents=True, exist_ok=True)
    args = [os.environ.get("DBT_EXECUTABLE", "dbt"), "seed" if command == "seed" else "build",
            "--project-dir", str(project), "--profiles-dir", str(project),
            "--target", "dev",
            "--target-path", str(folder / "target"), "--log-path", str(folder / "logs"),
            "--vars", json.dumps(window)]
    if command == "seed":
        args += ["--select", "country_codes"]
    elif command in {"silver", "gold"}:
        args += ["--select", f"tag:{command}", "--indirect-selection", "buildable"]
    else:
        raise ValueError(f"Etapa dbt desconocida: {command}")
    if full_refresh:
        args.append("--full-refresh")
    LOG.info("Iniciando dbt %s; artefactos=%s", command, folder)
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    subprocess.run(args, check=True, timeout=3600, env=env)


def finish_run(run_id: str, window: dict) -> None:
    with connect() as conn:
        # El checkpoint y el estado de éxito se confirman en la misma transacción.
        conn.execute(
            """UPDATE audit.pipeline_state
               SET last_successful_batch_seq = greatest(last_successful_batch_seq, %s)
               WHERE pipeline_name = 'daily_sales'""", (window["upper_seq"],)
        )
        conn.execute(
            """UPDATE audit.pipeline_runs SET status = 'SUCCEEDED', finished_at = current_timestamp, error = NULL,
               bronze_rows = (SELECT coalesce(sum(row_count), 0) FROM audit.ingestion_batches
                              WHERE batch_seq <= %s),
               silver_rows = (SELECT count(*) FROM silver.silver_transactions),
               quarantine_rows = (SELECT count(*) FROM quarantine.quarantine_transactions)
               WHERE run_id = %s""", (window["upper_seq"], run_id)
        )


def fail_run(run_id: str, error: str) -> None:
    with connect() as conn:
        initialize(conn)
        # No copiar payloads ni errores SQL con datos personales a la auditoría.
        conn.execute(
            """INSERT INTO audit.pipeline_runs (run_id, status, finished_at, error)
               VALUES (%s, 'FAILED', current_timestamp, %s)
               ON CONFLICT (run_id) DO UPDATE SET status = 'FAILED',
                   finished_at = current_timestamp, error = excluded.error""",
            (run_id, error[:1000]),
        )
