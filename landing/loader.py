"""Carga los CSV de un lote (un zip por periodo) a sus tablas de landing en Snowflake.

El zip se localiza con metadata/ingestion/source.yml; cada CSV, con su contrato
(metadata/ingestion/contracts/<entidad>.yml).

Flujo de un lote:
  1. extract_archive: descomprimir el zip del periodo en un directorio de trabajo
  2. check_contents: comprobar que está el fichero de cada contrato
  3. run (por entidad):
       crear schema, stage y tabla de landing si no existen
       vaciar la tabla (landing = último lote; relanzar no duplica)
       PUT del CSV al stage y COPY INTO la tabla, añadiendo columnas técnicas
"""
import hashlib
import os
import re
import uuid
import zipfile
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


# ---- el zip de cada lote ----
def _source():
    src = yaml.safe_load(SOURCE_FILE.read_text(encoding="utf-8"))
    return ROOT / src["directory"], src["pattern"]


def list_periods():
    """Periodos (YYYYMM) para los que hay un zip, según el patrón de metadata/ingestion/source.yml."""
    directory, pattern = _source()
    any_period = pattern.format(period="[0-9]" * 6)
    return sorted({re.search(r"\d{6}", p.name).group() for p in directory.glob(any_period)})


def find_archive(period):
    directory, pattern = _source()
    matches = sorted(directory.glob(pattern.format(period=period)))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Se esperaba un zip para el periodo {period} ({pattern.format(period=period)}) en {directory}; "
            f"encontrados: {[m.name for m in matches]}")
    return matches[0]


def file_checksum(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_archive(period, dest):
    """Extrae el zip del periodo en `dest` (solo el nombre de cada fichero: evita zip-slip)."""
    archive = find_archive(period)
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            if not info.is_dir():
                (dest / Path(info.filename).name).write_bytes(zf.read(info))
    return archive


def find_source_file(contract, workdir):
    pattern = contract["source"]["file_pattern"]
    matches = sorted(Path(workdir).glob(pattern))
    if len(matches) != 1:
        raise FileNotFoundError(f"{contract['entity']}: se esperaba un fichero '{pattern}' en el lote; encontrados {len(matches)}")
    return matches[0]


def check_contents(workdir):
    """Comprueba que el lote trae el fichero de todos los contratos. Devuelve {entidad: ruta}."""
    found, missing = {}, []
    for entity in list_entities():
        try:
            found[entity] = find_source_file(load_contract(entity), workdir)
        except FileNotFoundError as exc:
            missing.append(str(exc))
    if missing:
        raise FileNotFoundError("El lote no está completo: " + "; ".join(missing))
    return found


# ---- carga a Snowflake ----
def run(conn, cur, entity, period, workdir):
    """Ejecuta la carga y devuelve el nº de filas cargadas. `cur` es un cursor de Snowflake."""
    contract = load_contract(entity)
    path = find_source_file(contract, workdir)

    table = f"{SCHEMA}.{contract['target']['landing_table']}"
    stage_path = f"@{STAGE}/{period}/{entity}/"
    columns = contract["columns"]

    # columnas del CSV + columnas técnicas que añadimos nosotros
    column_ddl = ",\n    ".join(f"{c['name']} {c['type']}" for c in columns)
    column_names = ", ".join(c["name"] for c in columns)
    positional = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    delimiter = contract["source"].get("delimiter", ",")

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE}")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
        {column_ddl},
        _load_id VARCHAR,
        _load_ts TIMESTAMP_NTZ DEFAULT SYSDATE(),
        _source_file VARCHAR,
        _source_row NUMBER,
        _period VARCHAR
    )""")

    cur.execute(f"REMOVE {stage_path}")
    cur.execute(f"PUT 'file://{path.as_posix()}' {stage_path} AUTO_COMPRESS = TRUE OVERWRITE = TRUE")

    # LANDING guarda solo el último lote: se vacía la tabla y se carga el nuevo, en una
    # transacción (si algo falla, la tabla queda como estaba). El histórico vive en el vault.
    conn.autocommit(False)
    try:
        cur.execute(f"DELETE FROM {table}")
        cur.execute(f"""COPY INTO {table} ({column_names}, _load_id, _source_file, _source_row, _period)
FROM (SELECT {positional}, '{uuid.uuid4()}', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '{period}'
      FROM {stage_path})
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '{delimiter}' SKIP_HEADER = 1
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
