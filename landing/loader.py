"""Extrae y carga a LANDING (Snowflake) los datos de cada entidad.

El contrato de cada entidad (metadata/ingestion/contracts/<entidad>.yml) dice qué dato interesa y de qué origen sale
(`source.name`); metadata/ingestion/source.yml define esos orígenes (su `type` elige el extractor).

Carga de una entidad (`load`):
  1. extract: el extractor del origen deja disponible el fichero de la entidad para el periodo
  2. crear stage y tabla de landing si no existen (el esquema LANDING lo crea snowflake/02_database.sql)
  3. borrar lo que corresponda según load.type (snapshot: todo; incremental: solo el periodo; relanzar no duplica)
  4. PUT del CSV al stage y COPY INTO la tabla, añadiendo columnas técnicas
"""
import os
import re
from contextlib import contextmanager
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])
METADATA_DIR = ROOT / "metadata"
CONTRACTS_DIR = METADATA_DIR / "ingestion" / "contracts"
SOURCE_FILE = METADATA_DIR / "ingestion" / "source.yml"
PIPELINE_FILE = METADATA_DIR / "orchestration" / "pipeline.yml"

SCHEMA = "LANDING"
STAGE = f"{SCHEMA}.STG_INGEST"


# ---- metadatos ----
def list_entities():
    return sorted(p.stem for p in CONTRACTS_DIR.glob("*.yml"))


def load_contract(entity):
    path = CONTRACTS_DIR / f"{entity}.yml"
    if not path.is_file():
        raise FileNotFoundError(f"No existe el contrato {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_pipeline_config():
    return yaml.safe_load(PIPELINE_FILE.read_text(encoding="utf-8"))


# ---- extractores: de dónde sale cada dato ----
# Un extractor es lo único que cambia para traer los datos de otro origen (S3, SharePoint, una API...): contratos,
# carga y dbt no saben de dónde vienen. Un extractor es un gestor de contexto
#   extract(src, contract_source, period)  ->  `with` que entrega la ruta local del fichero de la entidad
# (`src` es el origen en source.yml y `contract_source` el bloque `source` del contrato). Uno local entrega el fichero
# donde está; uno remoto lo descargaría a un directorio temporal y lo borraría al salir del `with`.
# Opcionalmente, `periods(src)` lista los periodos disponibles (solo tiene sentido en orígenes que los exponen).
@contextmanager
def _local_folder_extract(src, contract_source, period):
    folder = ROOT / src["directory"].format(period=period)
    matches = sorted(folder.glob(contract_source["file_pattern"]))
    if len(matches) != 1:
        raise FileNotFoundError(f"Se esperaba un fichero '{contract_source['file_pattern']}' en {folder}; encontrados {len(matches)}")
    yield matches[0]


def _local_folder_periods(src):
    """Periodos (YYYYMM) = carpetas que ocupan el lugar de `{period}` en `directory`."""
    if "{period}" not in src["directory"]:
        return []
    parent = ROOT / src["directory"].split("{period}")[0]
    if not parent.is_dir():
        return []
    return sorted(p.name for p in parent.iterdir() if p.is_dir() and re.fullmatch(r"\d{6}", p.name))


EXTRACTORS = {"local_folder": {"extract": _local_folder_extract, "periods": _local_folder_periods}}


def _load_sources():
    return yaml.safe_load(SOURCE_FILE.read_text(encoding="utf-8"))["sources"]


def _extractor(source_name):
    sources = _load_sources()
    if source_name not in sources:
        raise ValueError(f"Origen '{source_name}' no definido en source.yml; definidos: {sorted(sources)}")
    src = sources[source_name]
    if src["type"] not in EXTRACTORS:
        raise ValueError(f"Origen '{source_name}': type '{src['type']}' no soportado; extractores: {sorted(EXTRACTORS)}")
    return src, EXTRACTORS[src["type"]]


def list_periods():
    """Periodos (YYYYMM) que exponen los orígenes (hoy, las carpetas de `local_folder`); vacío si ninguno los expone."""
    found = set()
    for src in _load_sources().values():
        periods = EXTRACTORS[src["type"]].get("periods")
        if periods:
            found.update(periods(src))
    return sorted(found)


# ---- carga a Snowflake ----
def load(conn, cur, entity, period, full_refresh=False):
    """Extrae la entidad de su origen y la carga a LANDING; devuelve el nº de filas cargadas. `cur` es un cursor de Snowflake."""
    contract = load_contract(entity)
    src, ext = _extractor(contract["source"]["name"])
    with ext["extract"](src, contract["source"], period) as path:
        return _load_file(conn, cur, contract, path, period, full_refresh)


def _load_file(conn, cur, contract, path, period, full_refresh):
    entity = contract["entity"]
    table = f"{SCHEMA}.{contract['target']['landing_table']}"
    stage_path = f"@{STAGE}/{period}/{entity}/"
    columns = contract["columns"]

    # columnas del CSV + columnas técnicas que añadimos nosotros
    column_ddl = ",\n    ".join(f"{c['name']} {c['type']}" for c in columns)
    column_names = ", ".join(c["name"] for c in columns)
    positional = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    delimiter = contract["source"].get("delimiter", ",")
    fmt = contract["source"].get("format", "csv").lower()
    if fmt != "csv":
        raise ValueError(f"{entity}: formato '{fmt}' no soportado (solo csv)")
    skip_header = 1 if contract["source"].get("header", True) else 0

    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE}")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
        {column_ddl},
        _source_file VARCHAR,
        _period VARCHAR
    )""")

    cur.execute(f"REMOVE {stage_path}")
    cur.execute(f"PUT 'file://{path.as_posix()}' {stage_path} AUTO_COMPRESS = TRUE OVERWRITE = TRUE")

    # Según load.type del contrato (borrado y carga, en una transacción: si algo falla, la tabla queda como estaba):
    #   snapshot    -> el fichero es el estado completo: se vacía la tabla y queda solo el último lote
    #   incremental -> el fichero solo trae filas nuevas: se conservan los lotes anteriores y solo se
    #                  sustituye el del mismo periodo (relanzar no duplica); con full_refresh se parte de cero
    load_type = contract["load"]["type"]
    if load_type == "snapshot" or full_refresh:
        delete_sql = f"DELETE FROM {table}"
    elif load_type == "incremental":
        delete_sql = f"DELETE FROM {table} WHERE _period = '{period}'"
    else:
        raise ValueError(f"{entity}: load.type '{load_type}' no soportado (snapshot | incremental)")
    conn.autocommit(False)
    try:
        cur.execute(delete_sql)
        cur.execute(f"""COPY INTO {table} ({column_names}, _source_file, _period)
FROM (SELECT {positional}, METADATA$FILENAME, '{period}'
      FROM {stage_path})
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '{delimiter}' SKIP_HEADER = {skip_header}
               FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = ABORT_STATEMENT
FORCE = TRUE""")
        rows_loaded = sum(row["rows_loaded"] for row in cur.fetchall())
        conn.commit()
        return rows_loaded
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit(True)
