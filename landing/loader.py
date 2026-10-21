"""Carga a LANDING (Snowflake) el fichero de una entidad, según su contrato."""
import yaml

from landing.extract import ROOT, get_file

CONTRACTS_DIR = ROOT / "metadata" / "ingestion" / "contracts"
STAGE = "LANDING.STG_INGEST"


def list_entities():
    return sorted(p.stem for p in CONTRACTS_DIR.glob("*.yml"))


def load_contract(entity):
    path = CONTRACTS_DIR / f"{entity}.yml"
    if not path.is_file():
        raise FileNotFoundError(f"No existe el contrato {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load(conn, cur, entity, period, full_refresh=False):
    """Carga la entidad en su tabla de landing y devuelve cuántas filas ha cargado."""
    contract = load_contract(entity)
    path = get_file(contract, period)

    table = "LANDING." + contract["target"]["landing_table"]
    stage_path = f"@{STAGE}/{period}/{entity}/"
    columns = contract["columns"]

    # Datos del contrato que usamos en el SQL
    column_ddl = ",\n    ".join(f"{c['name']} {c['type']}" for c in columns)
    column_names = ", ".join(c["name"] for c in columns)
    positions = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    delimiter = contract["source"].get("delimiter", ",")
    skip_header = 1 if contract["source"].get("header", True) else 0
    if contract["source"].get("format", "csv").lower() != "csv":
        raise ValueError(f"{entity}: solo se cargan CSV")

    # snapshot: se borra todo. incremental: se borra solo el periodo (así relanzar no duplica).
    # Con full_refresh se borra todo siempre.
    load_type = contract["load"]["type"]
    if load_type == "snapshot" or full_refresh:
        delete_sql = f"DELETE FROM {table}"
    elif load_type == "incremental":
        delete_sql = f"DELETE FROM {table} WHERE _period = '{period}'"
    else:
        raise ValueError(f"{entity}: load.type debe ser snapshot o incremental")

    # Stage y tabla (si no existen)
    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE}")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
        {column_ddl},
        _source_file VARCHAR,
        _period VARCHAR
    )""")

    # Subir el fichero al stage
    cur.execute(f"REMOVE {stage_path}")
    cur.execute(f"PUT 'file://{path.as_posix()}' {stage_path} AUTO_COMPRESS = TRUE OVERWRITE = TRUE")

    # Borrar y cargar en una transacción: si falla algo, la tabla se queda como estaba
    conn.autocommit(False)
    try:
        cur.execute(delete_sql)
        cur.execute(f"""COPY INTO {table} ({column_names}, _source_file, _period)
        FROM (SELECT {positions}, METADATA$FILENAME, '{period}'
              FROM {stage_path})
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '{delimiter}' SKIP_HEADER = {skip_header}
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = ABORT_STATEMENT
        FORCE = TRUE""")
        rows = sum(row["rows_loaded"] for row in cur.fetchall())
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit(True)
