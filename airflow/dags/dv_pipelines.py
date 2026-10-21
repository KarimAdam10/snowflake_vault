"""Factoría de DAGs: genera los DAGs de Airflow a partir de metadata/orchestration/pipeline.yml y metadata/ingestion/contracts/.

No hay código por tabla ni por pipeline:
  - cada pipeline de `pipelines:` es un DAG, con las etapas que liste;
  - las dependencias salen de `depends_on` de cada etapa;
  - las tareas de carga a LANDING son una por contrato, con sus reintentos y timeout;
  - el periodo (un lote de CSV) y `full_refresh` son parámetros de cada ejecución.

Cambiar metadata/orchestration/pipeline.yml o los contratos cambia los DAGs en el siguiente refresco del dag-processor.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, TaskGroup

from dv_lib import tasks
from landing import extract, loader

CFG = loader.load_pipeline_config()
DEFAULTS = CFG.get("defaults", {})
PERIODS = extract.list_periods()          # un periodo por cada lote disponible


def _retry_args(source):
    args = {"retries": source.get("retries", DEFAULTS.get("retries", 0)),
            "retry_delay": timedelta(minutes=source.get("retry_delay_minutes", DEFAULTS.get("retry_delay_minutes", 1)))}
    if source.get("execution_timeout_minutes"):
        args["execution_timeout"] = timedelta(minutes=source["execution_timeout_minutes"])
    return args


def _task(task_id, fn, op_kwargs=None, **orch):
    return PythonOperator(task_id=task_id, python_callable=fn, op_kwargs=op_kwargs or {}, **orch)


def _downstream_dbt_selects(name):
    """Selectores dbt de las etapas dbt que dependen (directa o indirectamente) de `name`."""
    found, pending = [], [name]
    while pending:
        current = pending.pop()
        for other, cfg in CFG["stages"].items():
            if current in cfg.get("depends_on", []) and cfg.get("select") not in found:
                if cfg["type"] == "dbt":
                    found.append(cfg["select"])
                pending.append(other)
    return found


def _build_stage(name, cfg):
    kind = cfg["type"]
    if kind == "load_landing":
        with TaskGroup(group_id=name) as group:
            for entity in loader.list_entities():
                orch = {**CFG.get("entity_defaults", {}), **CFG.get("entities", {}).get(entity, {})}
                _task(entity, tasks.load_landing, {"entity": entity}, **_retry_args(orch))
        return group
    if kind == "generate_models":
        return _task(name, tasks.generate_models, **_retry_args(cfg))
    if kind == "dbt":
        exclude = " ".join(_downstream_dbt_selects(name))
        return _task(name, tasks.run_dbt, {"stage": name, "select": cfg["select"], "exclude": exclude}, **_retry_args(cfg))
    raise ValueError(f"Etapa '{name}': tipo desconocido '{kind}'")


def _params():
    period = (Param(PERIODS[0], type="string", enum=PERIODS, description="Periodo (YYYYMM) del lote")
              if PERIODS else Param("", type="string", pattern=r"^\d{6}$", description="Periodo (YYYYMM)"))
    return {
        "period": period,
        "full_refresh": Param(False, type="boolean",
                              description="Reconstruir el vault desde cero (borra el histórico acumulado; "
                                          "úsalo solo con el primer lote)"),
    }


def _build_dag(dag_id, spec):
    with DAG(
        dag_id=dag_id,
        description=spec.get("description"),
        schedule=spec.get("schedule"),
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,       # un lote cada vez: landing se sustituye y el vault lo consume en orden
        params=_params(),
        tags=["metadata-driven", *spec.get("tags", [])],
    ) as dag:
        names = spec["stages"]
        unknown = [n for n in names if n not in CFG["stages"]]
        if unknown:
            raise ValueError(f"{dag_id}: etapas no definidas en `stages`: {unknown}")
        nodes = {n: _build_stage(n, CFG["stages"][n]) for n in names}
        for n in names:
            for dep in CFG["stages"][n].get("depends_on", []):
                if dep in nodes:
                    nodes[dep] >> nodes[n]
    return dag


for _dag_id, _spec in CFG["pipelines"].items():
    globals()[_dag_id] = _build_dag(_dag_id, _spec)
