FROM apache/airflow:2.10.5-python3.11
USER root
RUN mkdir -p /data/archive && chown airflow:root /data/archive
USER airflow
COPY --chown=airflow:root requirements.txt requirements-dbt.txt /tmp/dependencies/
RUN pip install --no-cache-dir -r /tmp/dependencies/requirements.txt \
    && python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dependencies/requirements-dbt.txt
ENV DBT_EXECUTABLE=/home/airflow/dbt-venv/bin/dbt
ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
