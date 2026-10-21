"""Extractores: de dónde sale cada dato.

metadata/ingestion/source.yml define los orígenes y cada contrato nombra el suyo (`source.name`); el `type` del origen
elige el extractor de este fichero. Añadir un origen nuevo (S3, SharePoint, una API...) es escribir su extractor,
registrarlo en EXTRACTORS y declararlo en source.yml: contratos, carga y dbt no cambian.
"""
import os
import re
from contextlib import contextmanager
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])
SOURCE_FILE = ROOT / "metadata" / "ingestion" / "source.yml"


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


def get_extractor(source_name):
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
