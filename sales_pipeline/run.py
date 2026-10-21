"""CLI equivalente al DAG, útil para reproducir y probar sin Airflow."""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import uuid

from sales_pipeline.ingest import connect, ingest
from sales_pipeline.runner import begin_run, processing_window, run_dbt, finish_run, fail_run


def execute(source: Path, archive: Path, run_id: str, *, full_refresh: bool = False,
            fail_after_silver: bool = False) -> dict:
    # Serializa invocaciones de esta CLI. No ejecutar CLI mientras el DAG esté activo;
    # el DAG serializa sus propias ejecuciones mediante max_active_runs=1.
    with connect() as lock:
        lock.execute("SELECT pg_advisory_lock(71320502)")
        try:
            begin_run(run_id)
            batch = ingest(source, archive)
            window = processing_window(run_id, batch["batch_seq"])
            run_dbt("seed", run_id, window)
            run_dbt("silver", run_id, window, full_refresh=full_refresh)
            if fail_after_silver:
                raise RuntimeError("Fallo simulado después de Silver")
            run_dbt("gold", run_id, window, full_refresh=full_refresh)
            finish_run(run_id, window)
            return {**batch, **window}
        except Exception as exc:
            fail_run(run_id, type(exc).__name__)
            raise
        finally:
            lock.execute("SELECT pg_advisory_unlock(71320502)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--archive", type=Path, default=Path(os.environ.get("DATA_ROOT", "data")) / "archive")
    parser.add_argument("--run-id", default=f"cli-{uuid.uuid4()}")
    parser.add_argument("--full-refresh", action="store_true", help="Reconstruye desde todo Bronze; cambio de reglas")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    logging.info("Resultado: %s", execute(args.file, args.archive, args.run_id, full_refresh=args.full_refresh))


if __name__ == "__main__":
    main()
