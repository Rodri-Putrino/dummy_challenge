# Validación ejecutada

Verificado el 21 de octubre de 2026 contra PostgreSQL 16.8 real en Docker,
usando la muestra sintética incluida en el repositorio.

| Comprobación | Resultado |
|---|---|
| Suite pytest con integración | **6 passed, 1 skipped**, 67 segundos. |
| Importación en Airflow 2.10.5 / Python 3.11 | Sin errores. |
| `airflow dags test` con `source_date=2026-01-01` | Las seis tareas terminaron en éxito; DagRun `success`. |
| dbt Silver / cuarentena | Tres modelos y **19 pruebas**, sin errores. |
| dbt Gold | Un modelo y **5 pruebas**, sin errores. |
| Auditoría final | `SUCCEEDED`; Bronze **17**, Silver **7**, cuarentena **10**. |
| Imagen final con volumen de archivo nuevo | Snapshot e importación del DAG correctos como usuario `airflow`. |

La omisión de pytest corresponde al test de importación de Airflow en el virtualenv
local, que instala sólo dbt y las dependencias del loader. La importación y ejecución
de Airflow se verificaron por separado en su imagen Docker con Python 3.11. La suite
local de dbt/loader se ejecutó con Python 3.13.2.

El escenario de integración verifica prioridad `COMPLETED`, timestamp equivalente,
deduplicación histórica, cambio de país que elimina un grupo Gold, fallo tras Silver,
recuperación con otro archivo, replay sin duplicar Bronze y equivalencia entre resultado
incremental y reconstrucción completa. El contrato SQL verifica fechas imposibles,
offsets, bisiestos, importes fuera de rango y formatos numéricos ambiguos.

Para Argentina, la consulta real devolvió `120.00` facturados, `120.0000` de ticket,
`3` transacciones y `33.3333` de tasa de cancelación, coincidiendo con el README.
El estado quedó disponible en `audit.pipeline_runs` con run ID
`manual__2026-01-02T03:00:00+00:00`.

Se incluye un [workflow de CI](../.github/workflows/test.yml) para repetir la integración
y la importación del DAG al publicar cambios. El workflow remoto no se ejecutó desde
esta sesión. No se realizó benchmark de 500.000 filas ni se probó Snowflake.
