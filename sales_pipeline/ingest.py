"""CSV -> COPY streaming, con snapshot inmutable y commit de lote atómico."""
from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import tempfile
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = ("transaction_id", "customer_id", "transaction_date", "amount", "status", "country_code")


def connect():
    # libpq toma PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD (o PGPASSFILE).
    return psycopg.connect("", options="-c timezone=UTC -c statement_timeout=1800000")


def initialize(conn):
    conn.execute((ROOT / "sql/001_bronze.sql").read_text())


def snapshot(source: Path, archive: Path) -> tuple[Path, str]:
    archive.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    temporary = None
    try:
        with source.open("rb") as incoming, tempfile.NamedTemporaryFile(dir=archive, delete=False) as out:
            temporary = Path(out.name)
            while chunk := incoming.read(1024 * 1024):
                digest.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
        checksum = digest.hexdigest()
        destination = archive / f"{checksum}.csv"
        # Reemplazar por los mismos bytes es seguro incluso en un retry concurrente.
        os.replace(temporary, destination)
        return destination, checksum
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def ingest(source: Path, archive: Path) -> dict:
    source = source.resolve()
    archived, checksum = snapshot(source, archive)
    batch_id = hashlib.sha256(f"{source.as_uri()}\n{checksum}".encode()).hexdigest()
    now = datetime.now(timezone.utc)
    with connect() as conn:
        # Ordena commits y secuencias: nunca se adelanta el watermark a un lote sin commit.
        conn.execute("SELECT pg_advisory_xact_lock(71320501)")
        initialize(conn)
        existing = conn.execute(
            "SELECT batch_seq, row_count FROM audit.ingestion_batches WHERE batch_id = %s", (batch_id,)
        ).fetchone()
        if existing:
            return {"batch_seq": existing[0], "row_count": existing[1], "reused": True}

        with archived.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, strict=True)
            header = next(reader, None)
            if header != list(COLUMNS):
                raise ValueError(f"Cabecera inesperada: se requiere {COLUMNS}")
            # Un archivo vacío o sólo con header es un fallo, no un día con cero ventas.
            first = next(reader, None)
            if first is None:
                raise ValueError("El archivo no contiene transacciones")
            batch_seq = conn.execute(
                """INSERT INTO audit.ingestion_batches
                   (batch_id, source_uri, file_sha256, archive_uri, ingested_at, row_count)
                   VALUES (%s, %s, %s, %s, %s, 1) RETURNING batch_seq""",
                (batch_id, source.as_uri(), checksum, archived.as_uri(), now),
            ).fetchone()[0]
            day = now.date()
            conn.execute(sql.SQL(
                "CREATE TABLE IF NOT EXISTS bronze.{} PARTITION OF bronze.transactions_raw "
                "FOR VALUES FROM ({}) TO ({})"
            ).format(sql.Identifier(f"transactions_raw_{day:%Y%m%d}"), sql.Literal(day), sql.Literal(day + timedelta(days=1))))
            count = 0
            with conn.cursor().copy(
                "COPY bronze.transactions_raw (ingest_date, raw_row_id, batch_seq, source_line, "
                "ingested_at, transaction_id, customer_id, transaction_date, amount, status, "
                "country_code, raw_payload, ingestion_error) FROM STDIN"
            ) as copy:
                from itertools import chain
                for count, row in enumerate(chain([first], reader), start=1):
                    values = row if len(row) == len(COLUMNS) else [None] * len(COLUMNS)
                    raw_id = hashlib.sha256(f"{batch_id}:{count}".encode()).hexdigest()
                    copy.write_row((
                        day, raw_id, batch_seq, count, now, *values,
                        json.dumps(row, ensure_ascii=False),
                        None if len(row) == len(COLUMNS) else "INVALID_COLUMN_COUNT",
                    ))
            conn.execute(
                "UPDATE audit.ingestion_batches SET row_count = %s WHERE batch_seq = %s", (count, batch_seq)
            )
        return {"batch_seq": batch_seq, "row_count": count, "reused": False}
