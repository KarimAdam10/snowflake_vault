"""Tareas que ejecutan las etapas de metadata/orchestration/pipeline.yml. No contienen nada específico de una tabla:
las entidades salen de los contratos y las capas de dbt, de los metadatos."""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from airflow.sdk import get_current_context
from snowflake.connector import DictCursor

from dv_lib import audit
from landing import loader
from landing.snowflake_conn import get_connection

WORK_ROOT = Path(os.environ.get("DV_WORK_DIR", "/tmp/dv_pipeline"))
DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/dbt"))
DBT_BIN = os.environ.get("DBT_VENV", "/opt/dbt-venv") + "/bin/dbt"


def _ctx():
    c = get_current_context()
    return {"dag_id": c["dag"].dag_id, "run_id": c["run_id"], "period": c["params"]["period"],
            "full_refresh": bool(c["params"].get("full_refresh"))}


def _workdir(period):
    return WORK_ROOT / period


def extract_archive():
    ctx = _ctx()
    workdir = _workdir(ctx["period"])
    shutil.rmtree(workdir, ignore_errors=True)
    with audit.tracked(ctx, "extract", f"zip {ctx['period']}") as ev:
        archive = loader.extract_archive(ctx["period"], workdir)
        files = loader.check_contents(workdir)
        ev["rows"] = len(files)
        ev["message"] = f"{archive.name} sha256={loader.file_checksum(archive)}"
        print(f"{archive.name}: {len(files)} ficheros -> {workdir}")


def load_landing(entity):
    ctx = _ctx()
    contract = loader.load_contract(entity)
    with audit.tracked(ctx, "landing", entity) as ev:
        with get_connection() as conn, conn.cursor(DictCursor) as cur:
            rows = loader.run(conn, cur, entity, ctx["period"], _workdir(ctx["period"]))
        ev["rows"] = rows
        print(f"{entity} {ctx['period']}: {rows} filas cargadas")
        if contract["load"]["type"] == "snapshot" and rows == 0:
            raise ValueError(f"{entity} es de tipo snapshot y el lote no trae filas")


def generate_models():
    ctx = _ctx()
    with audit.tracked(ctx, "generate_models", "metadata -> modelos dbt") as ev:
        p = subprocess.run([sys.executable, str(DBT_DIR / "scripts" / "generate_models.py")],
                           capture_output=True, text=True)
        print(p.stdout + p.stderr)
        if p.returncode:
            raise RuntimeError("generate_models.py falló")
        ev["message"] = p.stdout.strip().replace("\n", " | ")


def run_dbt(stage, select, exclude=""):
    """dbt build de una capa (modelos + sus tests). Registra cada nodo ejecutado en PIPELINE_LOG.
    `exclude` deja fuera las capas posteriores y los tests que dependen de ellas."""
    ctx = _ctx()
    if not (DBT_DIR / "dbt_packages" / "automate_dv").exists():
        subprocess.run([DBT_BIN, "deps"], cwd=DBT_DIR, check=True)
    cmd = [DBT_BIN, "build", "--select", *select.split()]
    if exclude:
        cmd += ["--exclude", *exclude.split()]
    if ctx["full_refresh"]:
        cmd.append("--full-refresh")
    t0 = time.time()
    p = subprocess.run(cmd, cwd=DBT_DIR, capture_output=True, text=True)
    print(p.stdout + p.stderr)

    events = []
    results = DBT_DIR / "target" / "run_results.json"
    if results.exists():
        for r in json.loads(results.read_text(encoding="utf-8"))["results"]:
            events.append({"stage": stage, "object": r["unique_id"].split(".", 1)[1],
                           "status": r["status"].upper(),
                           "rows": (r.get("adapter_response") or {}).get("rows_affected"),
                           "seconds": round(r.get("execution_time") or 0, 3),
                           "message": (r.get("message") or "")[:500]})
    events.append({"stage": stage, "object": f"dbt build --select {select}",
                   "status": "SUCCESS" if p.returncode == 0 else "FAILED",
                   "seconds": round(time.time() - t0, 3),
                   "message": "--full-refresh" if ctx["full_refresh"] else None})
    audit.log_many(ctx, events)
    if p.returncode:
        raise RuntimeError(f"dbt build --select {select} terminó con errores (ver el log de la tarea)")


def cleanup():
    shutil.rmtree(_workdir(_ctx()["period"]), ignore_errors=True)
