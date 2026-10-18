# Modelo del Raw Vault

Fuente de verdad: [`metadata/vault/vault.yml`](../metadata/vault/vault.yml). Los modelos dbt se generan con `dbt/scripts/generate_models.py`.

**15 objetos:** 4 hubs, 3 links, 6 satélites y 2 reference tables (`nation` y `region`, ver [decision_reference_tables.md](decision_reference_tables.md)).

```mermaid
flowchart TB
    classDef hub fill:#E1F5EE,stroke:#0F6E56,color:#085041
    classDef lnk fill:#FAECE7,stroke:#993C1D,color:#712B13
    classDef sat fill:#F1EFE8,stroke:#5F5E5A,color:#444441
    classDef ref fill:#EEEDFE,stroke:#534AB7,color:#3C3489

    hub_customer[hub_customer]:::hub --- lnk_order_customer{{lnk_order_customer}}:::lnk --- hub_order[hub_order]:::hub
    hub_part[hub_part]:::hub --- lnk_partsupp{{lnk_partsupp}}:::lnk --- hub_supplier[hub_supplier]:::hub
    hub_order --- lnk_lineitem{{lnk_lineitem}}:::lnk
    hub_part --- lnk_lineitem
    hub_supplier --- lnk_lineitem

    hub_customer --- sat_customer([sat_customer]):::sat
    hub_supplier --- sat_supplier([sat_supplier]):::sat
    hub_part --- sat_part([sat_part]):::sat
    hub_order --- sat_order([sat_order]):::sat
    lnk_partsupp --- sat_lnk_partsupp([sat_lnk_partsupp]):::sat
    lnk_lineitem --- sat_lnk_lineitem([sat_lnk_lineitem]):::sat

    sat_customer -. c_nationkey .-> ref_nation[(ref_nation)]:::ref
    sat_supplier -. s_nationkey .-> ref_nation
    ref_nation -. n_regionkey .-> ref_region[(ref_region)]:::ref
```

Las líneas punteadas no son links: son un código (atributo) que se resuelve contra la tabla de referencia por su valor, sin hash key.

| Objeto | Clave | Notas |
|---|---|---|
| `hub_*` | `<ENTIDAD>_HK` (MD5 de la clave de negocio) | `hub_order` sale de `orders` |
| `lnk_*` | `<A>_<B>_HK` (MD5 de las claves unidas) | `lnk_lineitem` añade `L_LINENUMBER` como clave hija |
| `sat_*` | `<padre>_HK` + `LOAD_DATETIME` | nueva fila solo si cambia el `HASHDIFF`; `sat_customer` y `sat_supplier` incluyen el código de nación como atributo |
| `ref_*` | código natural (`N_NATIONKEY`, `R_REGIONKEY`) | sin hash; conserva la primera versión de cada código |

`partsupp` y `lineitem` no tienen hub propio: son links con satélite.

**Cómo se genera:** staging y Raw Vault se regeneran desde `vault.yml` en cada ejecución del DAG (etapa `generate_models`); los ficheros de `dbt/models/staging` y `dbt/models/raw_vault` son 100 % generados, no se editan a mano.

**Cada hub se carga desde una sola entidad:** `hub_nation` no existe, pero p. ej. `hub_customer` solo ve los clientes de `customer.csv`. Una clave que apareciera únicamente como referencia en otra entidad (un `o_custkey` sin cliente) no entraría en el hub. Con estos datos no ocurre (se comprobó que no hay claves huérfanas); con otros, el hub debería cargarse desde todas las fuentes que traen la clave.
