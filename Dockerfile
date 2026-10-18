ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

COPY requirements-airflow.txt requirements-dbt.txt /tmp/

# 1) dbt en un virtualenv independiente (/opt/dbt-venv)
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && chmod -R a+rX /opt/dbt-venv

# 2) Dependencias extra de Airflow (con la misma versión de Airflow fijada)
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

ENV DBT_VENV=/opt/dbt-venv \
    DBT_PROJECT_DIR=/opt/dbt \
    DBT_PROFILES_DIR=/opt/dbt \
    PYTHONPATH=/opt/project
