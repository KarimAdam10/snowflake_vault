"""Deja constancia de cada paso en la tabla CONTROL.PIPELINE_LOG.

Una fila por periodo, entidad, modelo o test. Si el registro falla solo se avisa, el pipeline sigue.
"""
import time
from contextlib import contextmanager

from landing.snowflake_conn import get_connection

CREATE_TABLE = """CREATE TABLE IF NOT EXISTS CONTROL.PIPELINE_LOG (
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
    """Cada evento es un dict con stage, object, status y, si se quiere, rows, seconds y message."""
    rows = []
    for e in events:
        message = (e.get("message") or "")[:2000] or None
        rows.append((ctx["dag_id"], ctx["run_id"], ctx["period"], e["stage"], e["object"], e["status"],
                     e.get("rows"), e.get("seconds"), message))
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(CREATE_TABLE)
            cur.executemany(INSERT, rows)
    except Exception as exc:
        print(f"AVISO: no se pudo registrar en CONTROL.PIPELINE_LOG: {exc}")


@contextmanager
def tracked(ctx, stage, object_name):
    """Registra SUCCESS o FAILED al terminar el bloque. Dentro se puede rellenar ev['rows'] y ev['message']."""
    ev = {"rows": None, "message": None}
    start = time.time()
    try:
        yield ev
    except Exception as exc:
        log_many(ctx, [{"stage": stage, "object": object_name, "status": "FAILED", "rows": ev["rows"],
                        "seconds": round(time.time() - start, 3), "message": str(exc)}])
        raise
    log_many(ctx, [{"stage": stage, "object": object_name, "status": "SUCCESS", "rows": ev["rows"],
                    "seconds": round(time.time() - start, 3), "message": ev["message"]}])
