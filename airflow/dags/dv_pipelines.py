"""Crea los DAGs de Airflow a partir de metadata/orchestration/pipeline.yml.

Cada pipeline del yml es un DAG. Las etapas y sus dependencias también salen del yml,
y la etapa de carga tiene una tarea por cada contrato.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, TaskGroup

from dv_lib import tasks
from landing import extract, loader

ROOT = Path(os.environ.get("PROJECT_ROOT", "/opt/project"))
CONFIG = yaml.safe_load((ROOT / "metadata" / "orchestration" / "pipeline.yml").read_text(encoding="utf-8"))
DEFAULTS = CONFIG.get("defaults", {})
PERIODS = extract.list_periods()


def retry_args(config):
    """Reintentos y tiempo máximo de una tarea (lo que no diga `config` sale de `defaults`)."""
    args = {
        "retries": config.get("retries", DEFAULTS.get("retries", 0)),
        "retry_delay": timedelta(minutes=config.get("retry_delay_minutes", DEFAULTS.get("retry_delay_minutes", 1))),
    }
    if config.get("execution_timeout_minutes"):
        args["execution_timeout"] = timedelta(minutes=config["execution_timeout_minutes"])
    return args


def make_task(task_id, function, kwargs=None, **retry):
    return PythonOperator(task_id=task_id, python_callable=function, op_kwargs=kwargs or {}, **retry)


def dbt_layers_after(stage_name):
    """Capas de dbt que van después de esta etapa (para no ejecutar sus tests todavía)."""
    layers = []
    pending = [stage_name]
    while pending:
        current = pending.pop()
        for name, stage in CONFIG["stages"].items():
            if current in stage.get("depends_on", []):
                if stage["type"] == "dbt" and stage["select"] not in layers:
                    layers.append(stage["select"])
                pending.append(name)
    return layers


def make_stage(name, stage):
    """Crea la tarea (o el grupo de tareas) de una etapa del yml."""
    if stage["type"] == "load_landing":
        with TaskGroup(group_id=name) as group:
            for entity in loader.list_entities():
                config = {**CONFIG.get("entity_defaults", {}), **CONFIG.get("entities", {}).get(entity, {})}
                make_task(entity, tasks.load_landing, {"entity": entity}, **retry_args(config))
        return group

    if stage["type"] == "generate_models":
        return make_task(name, tasks.generate_models, **retry_args(stage))

    if stage["type"] == "dbt":
        kwargs = {"stage": name, "select": stage["select"], "exclude": " ".join(dbt_layers_after(name))}
        return make_task(name, tasks.run_dbt, kwargs, **retry_args(stage))

    raise ValueError(f"Etapa '{name}': tipo desconocido '{stage['type']}'")


def make_params():
    """Parámetros que se piden al lanzar el DAG: periodo y full_refresh."""
    if PERIODS:
        period = Param(PERIODS[0], type="string", enum=PERIODS, description="Periodo (YYYYMM) del lote")
    else:
        period = Param("", type="string", pattern=r"^\d{6}$", description="Periodo (YYYYMM)")
    full_refresh = Param(False, type="boolean",
                         description="Reconstruir el vault desde cero. Solo para el primer lote.")
    return {"period": period, "full_refresh": full_refresh}


def make_dag(dag_id, pipeline):
    with DAG(
        dag_id=dag_id,
        description=pipeline.get("description"),
        schedule=pipeline.get("schedule"),
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,  # un lote cada vez
        params=make_params(),
        tags=["metadata-driven", *pipeline.get("tags", [])],
    ) as dag:
        stage_names = pipeline["stages"]
        for name in stage_names:
            if name not in CONFIG["stages"]:
                raise ValueError(f"{dag_id}: la etapa '{name}' no está definida en `stages`")

        # Crear las tareas y luego unirlas según depends_on
        nodes = {name: make_stage(name, CONFIG["stages"][name]) for name in stage_names}
        for name in stage_names:
            for before in CONFIG["stages"][name].get("depends_on", []):
                if before in nodes:
                    nodes[before] >> nodes[name]
    return dag


# Airflow solo encuentra los DAGs que son variables del módulo: por eso se guardan en globals()
for dag_id, pipeline in CONFIG["pipelines"].items():
    globals()[dag_id] = make_dag(dag_id, pipeline)
