"""Las pruebas de integración crean una base efímera, nunca borran el warehouse."""
import os
import uuid

import psycopg
from psycopg import sql
import pytest


@pytest.fixture
def warehouse(monkeypatch):
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Configurar RUN_INTEGRATION=1 y PostgreSQL 16 para integrar")
    database = f"sales_test_{uuid.uuid4().hex}"
    with psycopg.connect("", autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        monkeypatch.setenv("PGDATABASE", database)
        try:
            yield database
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
