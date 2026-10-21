"""Genera los modelos dbt de STAGING y RAW_VAULT desde metadata/ingestion/contracts/*.yml y metadata/vault/vault.yml.

- stg_<entidad>: añade hash keys, hashdiff, LOAD_DATETIME y RECORD_SOURCE (automate_dv.stage).
- hub_*, lnk_*, sat_*: un modelo por objeto del vault (automate_dv.hub / link / sat) + sus tests.
- ref_*: tablas de referencia (automate_dv.ref_table) + un test que las compara con su staging.
El Business Vault (dbt/models/business_vault) y el mart (dbt/models/mart) son modelos SQL escritos a mano.
Para añadir una entidad: contrato + metadatos del vault y volver a ejecutar.

Uso (en el contenedor):  python /opt/dbt/scripts/generate_models.py

Los modelos generados NO se versionan en git (están en .gitignore): cada entorno los genera en la etapa
`generate_models` del DAG, antes de ejecutar dbt.
"""
import os
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("PROJECT_ROOT", "/opt/project"))
OUT = Path(__file__).resolve().parents[1] / "models" / "staging"
RV = Path(__file__).resolve().parents[1] / "models" / "raw_vault"
TESTS = Path(__file__).resolve().parents[1] / "tests"

TEMPLATE = """{{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}}
{{%- set yaml_metadata -%}}
{metadata}{{%- endset -%}}

{{% set metadata_dict = fromyaml(yaml_metadata) %}}

{{{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns']) }}}}
"""


def hash_name(obj_name):
    """hub_customer -> CUSTOMER_HK ; lnk_customer_nation -> CUSTOMER_NATION_HK"""
    return obj_name.split("_", 1)[1].upper() + "_HK"


def upper(cols):
    cols = [c.upper() for c in cols]
    return cols[0] if len(cols) == 1 else cols


def key_columns(entity, vault):
    """Columnas de la entidad que son claves o fk: no forman parte del payload del satélite."""
    used = []
    for h in vault["hubs"].values():
        if h["source"] == entity:
            used += h["key"]
    for l in vault["links"].values():
        if l["source"] == entity:
            used += [c for cols in l["hubs"].values() for c in cols] + l.get("child_key", [])
    return used


def payload_columns(sat, vault, columns):
    return [c for c in columns if c not in key_columns(sat["source"], vault)]


def hashed_columns(entity, vault, columns):
    """Hash keys que necesita el stage de una entidad, según los metadatos del vault."""
    hashed = {}
    for hub, h in vault["hubs"].items():
        if h["source"] == entity:
            hashed[hash_name(hub)] = upper(h["key"])
    for link, l in vault["links"].items():
        if l["source"] != entity:
            continue
        for hub, cols in l["hubs"].items():          # hash de cada hub referenciado
            hashed.setdefault(hash_name(hub), upper(cols))
        link_cols = [c for cols in l["hubs"].values() for c in cols] + l.get("child_key", [])
        hashed[hash_name(link)] = upper(link_cols)

    sats = [s for s in vault["satellites"].values() if s["source"] == entity]
    if len(sats) > 1:
        raise SystemExit(f"{entity}: una entidad solo puede alimentar un satélite")
    for sat in sats:
        hashed[f"{entity.upper()}_HASHDIFF"] = {
            "is_hashdiff": True, "columns": [c.upper() for c in payload_columns(sat, vault, columns)]}
    return hashed


# ---- Raw Vault: un modelo por hub, link y satélite ----
COMMON = "src_ldts='LOAD_DATETIME',\n   src_source='RECORD_SOURCE',\n   source_model='stg_{source}'"
HEADER = "{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}\n"


def render(macro, args, source):
    body = ",\n   ".join(f"{k}={v!r}" for k, v in args) + ",\n   " + COMMON.format(source=source)
    return f"{HEADER}{{{{ automate_dv.{macro}({body}) }}}}\n"


def raw_vault_models(vault, contracts):
    """Devuelve {nombre_modelo: (sql, tests)}."""
    models = {}
    for name, h in vault["hubs"].items():
        pk = hash_name(name)
        models[name] = (render("hub", [("src_pk", pk), ("src_nk", upper(h["key"]))], h["source"]),
                        {"columns": [{"name": pk, "tests": ["unique", "not_null"]}]})
    for name, l in vault["links"].items():
        pk, fks = hash_name(name), [hash_name(h) for h in l["hubs"]]
        args = [("src_pk", pk), ("src_fk", fks)]
        if l.get("child_key"):
            args.append(("src_extra_columns", [c.upper() for c in l["child_key"]]))
        tests = [{"name": pk, "tests": ["unique", "not_null"]}] + [
            {"name": hash_name(h), "tests": [{"relationships": {"arguments": {"to": f"ref('{h}')", "field": hash_name(h)}}}]}
            for h in l["hubs"]]
        models[name] = (render("link", args, l["source"]), {"columns": tests})
    for name, s in vault["satellites"].items():
        columns = [c["name"] for c in contracts[s["source"]]["columns"]]
        pk = hash_name(s["parent"])
        args = [("src_pk", pk), ("src_hashdiff", f"{s['source'].upper()}_HASHDIFF"),
                ("src_payload", [c.upper() for c in payload_columns(s, vault, columns)])]
        models[name] = (render("sat", args, s["source"]), {
            "columns": [{"name": pk, "tests": ["not_null",
                         {"relationships": {"arguments": {"to": f"ref('{s['parent']}')", "field": pk}}}]}],
            "tests": [{"dbt_utils.unique_combination_of_columns": {"arguments": {"combination_of_columns": [pk, "LOAD_DATETIME"]}}}]})
    for name, r in vault.get("reference_tables", {}).items():
        attrs = [c["name"] for c in contracts[r["source"]]["columns"] if c["name"] not in r["key"]]
        key = [{"name": c.upper(), "tests": ["unique", "not_null"]} for c in r["key"]]
        args = [("src_pk", upper(r["key"])), ("src_extra_columns", [c.upper() for c in attrs])]
        models[name] = (render("ref_table", args, r["source"]), {"columns": key})
    return models


def reference_table_tests(vault, contracts):
    """{nombre_fichero: sql}. ref_table no actualiza códigos ya cargados: este test detecta
    cualquier código del staging ausente o con atributos distintos en la tabla de referencia."""
    tests = {}
    for name, r in vault.get("reference_tables", {}).items():
        cols = [c["name"].upper() for c in contracts[r["source"]]["columns"]]
        key = [c.upper() for c in r["key"]]
        join = " and ".join(f"r.{c} = s.{c}" for c in key)
        diff = "\n   or ".join(f"r.{c} is distinct from s.{c}" for c in cols if c not in key)
        tests[f"{name}_matches_staging.sql"] = (
            f"{HEADER}select s.{key[0]}\nfrom {{{{ ref('stg_{r['source']}') }}}} s\n"
            f"left join {{{{ ref('{name}') }}}} r on {join}\n"
            f"where r.{key[0]} is null\n   or {diff}\n")
    return tests


def check(vault, contracts):
    refs = vault.get("reference_tables", {})
    for name, o in {**vault["hubs"], **vault["links"], **vault["satellites"], **refs}.items():
        if o["source"] not in contracts:
            raise SystemExit(f"{name}: la entidad '{o['source']}' no tiene contrato")
    for name, r in refs.items():
        missing = set(r["key"]) - set(c["name"] for c in contracts[r["source"]]["columns"])
        if missing:
            raise SystemExit(f"{name}: columnas clave inexistentes {sorted(missing)}")
    for name, l in vault["links"].items():
        for hub, cols in l["hubs"].items():
            if hub not in vault["hubs"]:
                raise SystemExit(f"{name}: el hub '{hub}' no existe")
            missing = set(cols) - set(c["name"] for c in contracts[l["source"]]["columns"])
            if missing:
                raise SystemExit(f"{name}: columnas inexistentes {sorted(missing)}")


def main():
    meta = ROOT / "metadata"
    contracts = {p.stem: yaml.safe_load(p.read_text(encoding="utf-8"))
                 for p in sorted((meta / "ingestion" / "contracts").glob("*.yml"))}
    vault = yaml.safe_load((meta / "vault" / "vault.yml").read_text(encoding="utf-8"))
    check(vault, contracts)

    OUT.mkdir(parents=True, exist_ok=True)
    for old in OUT.glob("*"):
        old.unlink()                                  # el directorio es 100 % generado

    for entity, c in contracts.items():
        columns = [col["name"] for col in c["columns"]]
        metadata = {
            "source_model": {"landing": c["target"]["landing_table"]},
            "derived_columns": {
                "RECORD_SOURCE": "_SOURCE_FILE",
                # _period YYYYMM -> último día del mes (fecha reproducible del lote)
                "LOAD_DATETIME": "LAST_DAY(TO_DATE(_PERIOD || '01', 'YYYYMMDD'))",
            },
            "hashed_columns": hashed_columns(entity, vault, columns),
        }
        text = yaml.safe_dump(metadata, sort_keys=False, default_flow_style=None, width=100)
        (OUT / f"stg_{entity}.sql").write_text(TEMPLATE.format(metadata=text), encoding="utf-8")

    sources = {"version": 2, "sources": [{
        "name": "landing", "schema": "LANDING",
        "tables": [{"name": c["target"]["landing_table"]} for c in contracts.values()]}]}
    (OUT / "_sources.yml").write_text(yaml.safe_dump(sources, sort_keys=False), encoding="utf-8")
    print(f"Generados {len(contracts)} modelos de staging en {OUT}")

    RV.mkdir(parents=True, exist_ok=True)
    for old in RV.glob("*"):
        old.unlink()
    models = raw_vault_models(vault, contracts)
    for name, (sql, _) in models.items():
        (RV / f"{name}.sql").write_text(sql, encoding="utf-8")
    schema = {"version": 2, "models": [{"name": n, **tests} for n, (_, tests) in models.items()]}
    (RV / "_raw_vault.yml").write_text(yaml.safe_dump(schema, sort_keys=False), encoding="utf-8")
    print(f"Generados {len(models)} modelos de raw vault en {RV}")

    TESTS.mkdir(parents=True, exist_ok=True)
    for old in TESTS.glob("*_matches_staging.sql"):
        old.unlink()
    for fname, sql in reference_table_tests(vault, contracts).items():
        (TESTS / fname).write_text(sql, encoding="utf-8")


if __name__ == "__main__":
    main()
