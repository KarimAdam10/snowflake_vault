"""Lo que hace cada tarea de Airflow: cargar una entidad, generar los modelos y ejecutar una capa de dbt."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from airflow.sdk import get_current_context
from snowflake.connector import DictCursor

from dv_lib import audit
from landing import loader
from landing.snowflake_conn import get_connection

DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/dbt"))
DBT_BIN = os.environ.get("DBT_VENV", "/opt/dbt-venv") + "/bin/dbt"


def get_ctx():
    """Datos de la ejecución actual: DAG, run, periodo y si es full_refresh."""
    c = get_current_context()
    return {"dag_id": c["dag"].dag_id, "run_id": c["run_id"], "period": c["params"]["period"],
            "full_refresh": bool(c["params"].get("full_refresh"))}


def load_landing(entity):
    ctx = get_ctx()
    contract = loader.load_contract(entity)
    with audit.tracked(ctx, "landing", entity) as ev:
        with get_connection() as conn, conn.cursor(DictCursor) as cur:
            rows = loader.load(conn, cur, entity, ctx["period"], ctx["full_refresh"])
        ev["rows"] = rows
        print(f"{entity} {ctx['period']}: {rows} filas cargadas")
        if contract["load"]["type"] == "snapshot" and rows == 0:
            raise ValueError(f"{entity} es snapshot y el lote no trae filas")


def generate_models():
    ctx = get_ctx()
    with audit.tracked(ctx, "generate_models", "metadata -> modelos dbt") as ev:
        result = subprocess.run([sys.executable, str(DBT_DIR / "scripts" / "generate_models.py")],
                                capture_output=True, text=True)
        print(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError("generate_models.py falló")
        ev["message"] = result.stdout.strip().replace("\n", " | ")


def run_dbt(stage, select, exclude=""):
    """Ejecuta `dbt build` de una capa y apunta cada modelo y test en PIPELINE_LOG.
    `exclude` deja fuera las capas que van después."""
    ctx = get_ctx()
    if not (DBT_DIR / "dbt_packages" / "automate_dv").exists():
        subprocess.run([DBT_BIN, "deps"], cwd=DBT_DIR, check=True)

    command = [DBT_BIN, "build", "--select", *select.split()]
    if exclude:
        command += ["--exclude", *exclude.split()]
    if ctx["full_refresh"]:
        command.append("--full-refresh")

    start = time.time()
    result = subprocess.run(command, cwd=DBT_DIR, capture_output=True, text=True)
    print(result.stdout + result.stderr)

    # Un evento por cada modelo y test que dbt ha ejecutado
    events = []
    results_file = DBT_DIR / "target" / "run_results.json"
    if results_file.exists():
        for r in json.loads(results_file.read_text(encoding="utf-8"))["results"]:
            events.append({"stage": stage, "object": r["unique_id"].split(".", 1)[1],
                           "status": r["status"].upper(),
                           "rows": (r.get("adapter_response") or {}).get("rows_affected"),
                           "seconds": round(r.get("execution_time") or 0, 3),
                           "message": (r.get("message") or "")[:500]})

    # Y un evento con el resultado de toda la ejecución
    events.append({"stage": stage, "object": f"dbt build --select {select}",
                   "status": "SUCCESS" if result.returncode == 0 else "FAILED",
                   "seconds": round(time.time() - start, 3),
                   "message": "--full-refresh" if ctx["full_refresh"] else None})
    audit.log_many(ctx, events)

    if result.returncode:
        raise RuntimeError(f"dbt build --select {select} terminó con errores (mira el log de la tarea)")
