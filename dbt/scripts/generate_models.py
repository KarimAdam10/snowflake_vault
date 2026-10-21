"""Genera los modelos de dbt de STAGING y RAW_VAULT a partir de los metadatos.

Lee metadata/ingestion/contracts/*.yml y metadata/vault/vault.yml y escribe:
  - dbt/models/staging: una vista stg_<entidad> por contrato
  - dbt/models/raw_vault: un modelo por hub, link, satélite y tabla de referencia, con sus tests
  - dbt/tests: un test por cada tabla de referencia

Los modelos generados no se suben a git: se generan en cada ejecución del DAG.
Uso: python generate_models.py
"""
import os
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("PROJECT_ROOT", "/opt/project"))
DBT_DIR = Path(__file__).resolve().parents[1]
STAGING_DIR = DBT_DIR / "models" / "staging"
RAW_VAULT_DIR = DBT_DIR / "models" / "raw_vault"
TESTS_DIR = DBT_DIR / "tests"

HEADER = "{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}\n"

STAGING_TEMPLATE = """{{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}}
{{%- set yaml_metadata -%}}
{metadata}{{%- endset -%}}

{{% set metadata_dict = fromyaml(yaml_metadata) %}}

{{{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns']) }}}}
"""

# Columnas que llevan todos los modelos del Raw Vault
COMMON_ARGS = "src_ldts='LOAD_DATETIME',\n   src_source='RECORD_SOURCE',\n   source_model='stg_{source}'"


def hash_name(object_name):
    """hub_customer -> CUSTOMER_HK ; lnk_customer_nation -> CUSTOMER_NATION_HK"""
    return object_name.split("_", 1)[1].upper() + "_HK"


def upper(columns):
    """Pasa las columnas a mayúsculas. Si es una sola, devuelve el texto en vez de la lista."""
    columns = [c.upper() for c in columns]
    return columns[0] if len(columns) == 1 else columns


def link_columns(link):
    """Todas las columnas que forman un link: las claves de sus hubs y la clave hija."""
    columns = [c for cols in link["hubs"].values() for c in cols]
    return columns + link.get("child_key", [])


def key_columns(entity, vault):
    """Columnas de la entidad que son clave de un hub o de un link: no van en el payload del satélite."""
    used = []
    for hub in vault["hubs"].values():
        if hub["source"] == entity:
            used += hub["key"]
    for link in vault["links"].values():
        if link["source"] == entity:
            used += link_columns(link)
    return used


def payload_columns(sat, vault, columns):
    keys = key_columns(sat["source"], vault)
    return [c for c in columns if c not in keys]


def hashed_columns(entity, vault, columns):
    """Las claves hash que hay que calcular en el staging de una entidad."""
    hashed = {}

    # Un hash por cada hub que carga esta entidad
    for hub_name, hub in vault["hubs"].items():
        if hub["source"] == entity:
            hashed[hash_name(hub_name)] = upper(hub["key"])

    # Por cada link de esta entidad: el hash de cada hub que une y el hash del propio link
    for link_name, link in vault["links"].items():
        if link["source"] != entity:
            continue
        for hub_name, cols in link["hubs"].items():
            hashed.setdefault(hash_name(hub_name), upper(cols))
        hashed[hash_name(link_name)] = upper(link_columns(link))

    # El hashdiff (para detectar cambios) si la entidad alimenta un satélite
    sats = [s for s in vault["satellites"].values() if s["source"] == entity]
    if len(sats) > 1:
        raise SystemExit(f"{entity}: una entidad solo puede alimentar un satélite")
    for sat in sats:
        payload = [c.upper() for c in payload_columns(sat, vault, columns)]
        hashed[f"{entity.upper()}_HASHDIFF"] = {"is_hashdiff": True, "columns": payload}
    return hashed


def render(macro, args, source):
    """Escribe el modelo: una llamada a la macro de automate_dv."""
    body = ",\n   ".join(f"{name}={value!r}" for name, value in args)
    body += ",\n   " + COMMON_ARGS.format(source=source)
    return f"{HEADER}{{{{ automate_dv.{macro}({body}) }}}}\n"


def hub_models(vault):
    models = {}
    for name, hub in vault["hubs"].items():
        pk = hash_name(name)
        sql = render("hub", [("src_pk", pk), ("src_nk", upper(hub["key"]))], hub["source"])
        tests = {"columns": [{"name": pk, "tests": ["unique", "not_null"]}]}
        models[name] = (sql, tests)
    return models


def link_models(vault):
    models = {}
    for name, link in vault["links"].items():
        pk = hash_name(name)
        fks = [hash_name(hub) for hub in link["hubs"]]
        args = [("src_pk", pk), ("src_fk", fks)]
        if link.get("child_key"):
            args.append(("src_extra_columns", [c.upper() for c in link["child_key"]]))

        # Tests: la clave del link es única y cada clave de hub existe en su hub
        columns = [{"name": pk, "tests": ["unique", "not_null"]}]
        for hub in link["hubs"]:
            relation = {"relationships": {"arguments": {"to": f"ref('{hub}')", "field": hash_name(hub)}}}
            columns.append({"name": hash_name(hub), "tests": [relation]})
        models[name] = (render("link", args, link["source"]), {"columns": columns})
    return models


def sat_models(vault, contracts):
    models = {}
    for name, sat in vault["satellites"].items():
        columns = [c["name"] for c in contracts[sat["source"]]["columns"]]
        pk = hash_name(sat["parent"])
        args = [("src_pk", pk),
                ("src_hashdiff", f"{sat['source'].upper()}_HASHDIFF"),
                ("src_payload", [c.upper() for c in payload_columns(sat, vault, columns)])]

        # Tests: la clave existe en el padre y no se repite la misma versión
        parent_exists = {"relationships": {"arguments": {"to": f"ref('{sat['parent']}')", "field": pk}}}
        no_repeats = {"dbt_utils.unique_combination_of_columns":
                      {"arguments": {"combination_of_columns": [pk, "LOAD_DATETIME"]}}}
        tests = {"columns": [{"name": pk, "tests": ["not_null", parent_exists]}], "tests": [no_repeats]}
        models[name] = (render("sat", args, sat["source"]), tests)
    return models


def reference_models(vault, contracts):
    models = {}
    for name, ref in vault.get("reference_tables", {}).items():
        attributes = [c["name"] for c in contracts[ref["source"]]["columns"] if c["name"] not in ref["key"]]
        args = [("src_pk", upper(ref["key"])), ("src_extra_columns", [c.upper() for c in attributes])]
        key_tests = [{"name": c.upper(), "tests": ["unique", "not_null"]} for c in ref["key"]]
        models[name] = (render("ref_table", args, ref["source"]), {"columns": key_tests})
    return models


def reference_tests(vault, contracts):
    """Un test por tabla de referencia: devuelve los códigos del staging que faltan en la tabla
    o que tienen otros valores (ref_table no actualiza los códigos que ya tiene)."""
    tests = {}
    for name, ref in vault.get("reference_tables", {}).items():
        all_columns = [c["name"].upper() for c in contracts[ref["source"]]["columns"]]
        key = [c.upper() for c in ref["key"]]
        join = " and ".join(f"r.{c} = s.{c}" for c in key)
        different = "\n   or ".join(f"r.{c} is distinct from s.{c}" for c in all_columns if c not in key)
        tests[f"{name}_matches_staging.sql"] = (
            f"{HEADER}select s.{key[0]}\nfrom {{{{ ref('stg_{ref['source']}') }}}} s\n"
            f"left join {{{{ ref('{name}') }}}} r on {join}\n"
            f"where r.{key[0]} is null\n   or {different}\n")
    return tests


def check(vault, contracts):
    """Comprueba que los metadatos son coherentes antes de generar nada."""
    refs = vault.get("reference_tables", {})
    for name, obj in {**vault["hubs"], **vault["links"], **vault["satellites"], **refs}.items():
        if obj["source"] not in contracts:
            raise SystemExit(f"{name}: la entidad '{obj['source']}' no tiene contrato")
    for name, ref in refs.items():
        missing = set(ref["key"]) - set(c["name"] for c in contracts[ref["source"]]["columns"])
        if missing:
            raise SystemExit(f"{name}: columnas clave inexistentes {sorted(missing)}")
    for name, link in vault["links"].items():
        for hub, cols in link["hubs"].items():
            if hub not in vault["hubs"]:
                raise SystemExit(f"{name}: el hub '{hub}' no existe")
            missing = set(cols) - set(c["name"] for c in contracts[link["source"]]["columns"])
            if missing:
                raise SystemExit(f"{name}: columnas inexistentes {sorted(missing)}")


def empty_folder(folder, pattern="*"):
    """Crea la carpeta si no existe y borra lo que había generado antes."""
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob(pattern):
        old.unlink()


def main():
    metadata_dir = ROOT / "metadata"
    contract_files = sorted((metadata_dir / "ingestion" / "contracts").glob("*.yml"))
    contracts = {p.stem: yaml.safe_load(p.read_text(encoding="utf-8")) for p in contract_files}
    vault = yaml.safe_load((metadata_dir / "vault" / "vault.yml").read_text(encoding="utf-8"))
    check(vault, contracts)

    # Staging: una vista por contrato
    empty_folder(STAGING_DIR)
    for entity, contract in contracts.items():
        columns = [c["name"] for c in contract["columns"]]
        metadata = {
            "source_model": {"landing": contract["target"]["landing_table"]},
            "derived_columns": {
                "RECORD_SOURCE": "_SOURCE_FILE",
                # El periodo YYYYMM pasa a ser el último día de ese mes
                "LOAD_DATETIME": "LAST_DAY(TO_DATE(_PERIOD || '01', 'YYYYMMDD'))",
            },
            "hashed_columns": hashed_columns(entity, vault, columns),
        }
        text = yaml.safe_dump(metadata, sort_keys=False, default_flow_style=None, width=100)
        (STAGING_DIR / f"stg_{entity}.sql").write_text(STAGING_TEMPLATE.format(metadata=text), encoding="utf-8")

    sources = {"version": 2, "sources": [{
        "name": "landing", "schema": "LANDING",
        "tables": [{"name": c["target"]["landing_table"]} for c in contracts.values()]}]}
    (STAGING_DIR / "_sources.yml").write_text(yaml.safe_dump(sources, sort_keys=False), encoding="utf-8")
    print(f"Generados {len(contracts)} modelos de staging en {STAGING_DIR}")

    # Raw Vault: hubs, links, satélites y tablas de referencia
    models = {**hub_models(vault), **link_models(vault), **sat_models(vault, contracts), **reference_models(vault, contracts)}
    empty_folder(RAW_VAULT_DIR)
    for name, (sql, _) in models.items():
        (RAW_VAULT_DIR / f"{name}.sql").write_text(sql, encoding="utf-8")
    schema = {"version": 2, "models": [{"name": name, **tests} for name, (_, tests) in models.items()]}
    (RAW_VAULT_DIR / "_raw_vault.yml").write_text(yaml.safe_dump(schema, sort_keys=False), encoding="utf-8")
    print(f"Generados {len(models)} modelos de raw vault en {RAW_VAULT_DIR}")

    # Tests de las tablas de referencia
    empty_folder(TESTS_DIR, "*_matches_staging.sql")
    for file_name, sql in reference_tests(vault, contracts).items():
        (TESTS_DIR / file_name).write_text(sql, encoding="utf-8")


if __name__ == "__main__":
    main()
