"""Saca de cada origen el fichero de una entidad.

El contrato dice de qué origen sale (source.name), source.yml dice de qué tipo es ese origen
y el tipo elige la función de este fichero. Para un origen nuevo se escribe su función aquí.
"""
import os
import re
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])
SOURCE_FILE = ROOT / "metadata" / "ingestion" / "source.yml"


def read_sources():
    return yaml.safe_load(SOURCE_FILE.read_text(encoding="utf-8"))["sources"]


def extract_local_folder(source, file_pattern, period):
    """Devuelve la ruta del fichero dentro de la carpeta del periodo."""
    folder = ROOT / source["directory"].format(period=period)
    files = sorted(folder.glob(file_pattern))
    if len(files) != 1:
        raise FileNotFoundError(f"Se esperaba un fichero '{file_pattern}' en {folder} y hay {len(files)}")
    return files[0]


def periods_local_folder(source):
    """Los periodos son las carpetas con nombre YYYYMM."""
    if "{period}" not in source["directory"]:
        return []
    parent = ROOT / source["directory"].split("{period}")[0]
    if not parent.is_dir():
        return []
    return sorted(p.name for p in parent.iterdir() if p.is_dir() and re.fullmatch(r"\d{6}", p.name))


# tipo de origen -> función
EXTRACTORS = {"local_folder": extract_local_folder}
PERIODS = {"local_folder": periods_local_folder}


def get_file(contract, period):
    """Trae el fichero de la entidad desde el origen que dice su contrato."""
    name = contract["source"]["name"]
    sources = read_sources()
    if name not in sources:
        raise ValueError(f"El origen '{name}' no está en source.yml (hay: {sorted(sources)})")
    source = sources[name]
    if source["type"] not in EXTRACTORS:
        raise ValueError(f"El origen '{name}' es de tipo '{source['type']}', que no existe (hay: {sorted(EXTRACTORS)})")
    return EXTRACTORS[source["type"]](source, contract["source"]["file_pattern"], period)


def list_periods():
    """Periodos que se pueden cargar, según los orígenes que los saben listar."""
    found = set()
    for source in read_sources().values():
        if source["type"] in PERIODS:
            found.update(PERIODS[source["type"]](source))
    return sorted(found)
