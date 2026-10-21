"""Trazabilidad operativa: cada etapa deja constancia en CONTROL.PIPELINE_LOG (Snowflake).

Una fila por objeto procesado (periodo, entidad cargada, modelo o test de dbt): qué ejecución de Airflow
(`DAG_ID`, `RUN_ID`), qué periodo, qué etapa, cuántas filas, cuánto tardó y cómo terminó.
El registro no bloquea el pipeline: si Snowflake no responde al registrar, se avisa en el log de la tarea.
"""
import time
from contextlib import contextmanager

from landing.snowflake_conn import get_connection

DDL_TABLE = """CREATE TABLE IF NOT EXISTS CONTROL.PIPELINE_LOG (
    LOG_TS           TIMESTAMP_NTZ DEFAULT SYSDATE(),
    DAG_ID           VARCHAR,
    RUN_ID           VARCHAR,
    PERIOD           VARCHAR,
    STAGE            VARCHAR,
    OBJECT_NAME      VARCHAR,
    STATUS           VARCHAR,
    ROWS_AFFECTED    NUMBER,
    DURATION_SECONDS NUMBER(12, 3),
    MESSAGE          VARCHAR
)"""

INSERT = ("INSERT INTO CONTROL.PIPELINE_LOG "
          "(DAG_ID, RUN_ID, PERIOD, STAGE, OBJECT_NAME, STATUS, ROWS_AFFECTED, DURATION_SECONDS, MESSAGE) "
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")


def log_many(ctx, events):
    """events: lista de dicts con stage, object, status y, opcionalmente, rows, seconds y message."""
    rows = [(ctx["dag_id"], ctx["run_id"], ctx["period"], e["stage"], e["object"], e["status"],
             e.get("rows"), e.get("seconds"), (e.get("message") or "")[:2000] or None) for e in events]
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(DDL_TABLE)
            cur.executemany(INSERT, rows)
    except Exception as exc:  # noqa: BLE001 - la trazabilidad no debe romper la carga
        print(f"AVISO: no se pudo registrar en CONTROL.PIPELINE_LOG: {exc}")


@contextmanager
def tracked(ctx, stage, object_name):
    """Registra SUCCESS o FAILED (con duración) al terminar el bloque. Rellenar ev['rows'] y ev['message']."""
    ev = {"rows": None, "message": None}
    t0 = time.time()
    try:
        yield ev
    except Exception as exc:
        log_many(ctx, [{"stage": stage, "object": object_name, "status": "FAILED", "rows": ev["rows"],
                        "seconds": round(time.time() - t0, 3), "message": str(exc)}])
        raise
    log_many(ctx, [{"stage": stage, "object": object_name, "status": "SUCCESS", "rows": ev["rows"],
                    "seconds": round(time.time() - t0, 3), "message": ev["message"]}])
