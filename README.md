# Pipeline de ventas: Airflow + dbt + PostgreSQL

Solución ejecutable del challenge. Incluye ingesta por streaming, modelos incrementales,
cuarentena, pruebas SQL de calidad, recuperación de lotes fallidos y un entorno Docker.
El CSV del repositorio es una **muestra sintética**: no se recibió el archivo original.

Repositorio: [Rodri-Putrino/dummy_challenge](https://github.com/Rodri-Putrino/dummy_challenge).

## Ejecutar la demostración

Requiere Docker Engine y Docker Compose v2; reservar aproximadamente 4 GB de RAM.
Las versiones del entorno son Airflow 2.10.5 / Python 3.11, dbt-core 1.9.4,
dbt-postgres 1.9.0 y PostgreSQL 16.8. dbt corre en un virtualenv separado de Airflow.
Las versiones elegidas se fijan para reproducir la solución, no como recomendación de
versiones para producción.

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f airflow
```

Cuando Airflow termine de inicializar, abrir <http://localhost:18080> con el usuario
`admin` y la contraseña generada que aparece en los logs. También se puede consultar:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags test daily_sales_medallion 2026-01-02T03:00:00+00:00 \
  --conf '{"source_date":"2026-01-01"}'
```

Los archivos se entregan como `data/incoming/YYYY-MM-DD/transactions_raw.csv`.
`source_date` permite backfills y pruebas manuales; se valida como fecha y no acepta
rutas arbitrarias. No implica que todas las transacciones pertenezcan a ese día.
`dags test` ejecuta todas las tareas y registra el resultado sin habilitar el cron.
Activar el DAG en la UI cuando estén disponibles los archivos diarios actuales.
Para producción, el proveedor debe publicar por rename atómico o indicar que el objeto
está completo antes de que el sensor lo detecte.

Consultar los resultados:

```bash
docker compose exec warehouse psql -U warehouse -d warehouse -c \
  'SELECT * FROM gold.daily_country_sales ORDER BY business_date, country_code;'
docker compose exec warehouse psql -U warehouse -d warehouse -c \
  'SELECT reason_codes, count(*) FROM quarantine.quarantine_transactions GROUP BY 1;'
docker compose exec warehouse psql -U warehouse -d warehouse -c \
  'SELECT * FROM audit.pipeline_runs ORDER BY started_at DESC;'
```

La muestra produce 17 filas Bronze, 7 Silver y 10 en cuarentena (8 inválidas + 2
duplicados). Resultado esperado para `2026-01-01`:

| País | Facturado | Ticket promedio | Transacciones únicas | Canceladas % |
|---|---:|---:|---:|---:|
| AR | 120.00 | 120.0000 | 3 | 33.3333 |
| BR | 0.00 | NULL | 1 | 0.0000 |
| CL | 0.00 | 0.0000 | 1 | 0.0000 |
| ES | 80.00 | 80.0000 | 1 | 0.0000 |
| US | 1000.00 | 1000.0000 | 1 | 0.0000 |

El modo `standalone` es para desarrollo. Los volúmenes conservan PostgreSQL, Airflow
y los snapshots originales; `docker compose down` detiene los servicios sin borrarlos.
Las credenciales de `.env.example` son exclusivamente locales. No se necesita ninguna
cuenta cloud para ejecutar el entregable.

## Documentación

El [diseño de arquitectura y contrato de datos](docs/architecture.md) describe el modelo
dimensional, reglas de deduplicación, fórmulas, cuarentena, idempotencia, recuperación,
escalabilidad y preguntas para el Lead. La implementación usa PostgreSQL 16; el documento
explica qué adaptar para Snowflake.
La [evidencia de validación](docs/validation.md) resume la ejecución realizada.

## Calidad, pruebas y evidencia

`dbt build --select tag:silver` verifica tipos, nulos, estados permitidos, países,
claves únicas, ausencia de negativos y reconciliación: cada fila staging pertenece
exactamente a Silver o cuarentena. Otra prueba detecta filas Bronze pendientes sin
staging. Gold se compara contra una agregación independiente de Silver, incluyendo
ticket, conteos y tasa; la clave compuesta tiene un índice único físico.
La selección indirecta `buildable` impide ejecutar pruebas de Gold antes de construir
su tabla. Los rechazos esperados no hacen fallar el pipeline: quedan disponibles para
auditoría; sí lo hace una violación de los invariantes.

Los artefactos `manifest.json`, `run_results.json` y logs dbt se guardan por run y
etapa en `/opt/airflow/artifacts/<hash-run-id>/`, evitando escribir sobre el proyecto
montado de sólo lectura. `.env`, snapshots, artefactos y virtualenv no se versionan.

Pruebas locales (Python 3.11 recomendado; no necesitan instalar Airflow):

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
docker compose up -d warehouse
set -a
source .env
set +a
export DBT_EXECUTABLE="$PWD/.venv/bin/dbt"
RUN_INTEGRATION=1 .venv/bin/pytest -q
```

La suite crea y elimina bases efímeras `sales_test_<uuid>` usando el usuario local,
sin borrar el warehouse. Verifica rollback de CSV truncado, cuarentena de filas
malformadas, header/vacíos, snapshot exacto, deduplicación entre archivos, prioridad
de estado, timestamps equivalentes, mismo ID a distintas horas, datos tardíos,
cambio de país, fallo después de Silver, recuperación con otro archivo, replay y
equivalencia de `--full-refresh`. La prueba de importación del DAG se omite si Airflow
no está instalado; se puede ejecutar la validación equivalente en su imagen:

```bash
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags test daily_sales_medallion 2026-01-02T03:00:00+00:00 \
  --conf '{"source_date":"2026-01-01"}'
```
