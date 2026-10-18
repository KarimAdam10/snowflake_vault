# Decisión técnica: `nation` y `region` como reference tables (no como hubs)

- **Estado:** aceptada e **implementada** (2026-10-17). Hasta entonces estaban modeladas como hubs.
- **Fecha:** 2026-10-17
- **Ámbito:** `metadata/vault/vault.yml`, `dbt/scripts/generate_models.py`, `dbt/models/{staging,raw_vault}/`, mart

## 1. Contexto

Las dos entidades vienen de `nation.csv` y `region.csv`:

| Entidad | Columnas | Filas 202512 → 202601 | Cambios de atributos entre lotes |
|---|---|---|---|
| `nation` | `n_nationkey`, `n_name`, `n_regionkey`, `n_comment` | 10 → 11 | 0 (solo una fila nueva) |
| `region` | `r_regionkey`, `r_name`, `r_comment` | 4 → 4 | 0 |

Son tablas de códigos y descripciones. Otras entidades las referencian por su código: `customer.c_nationkey`, `supplier.s_nationkey` y `nation.n_regionkey`.

Hoy el Raw Vault las modela como hubs, con 7 objetos para ellas:

| Tipo | Objetos |
|---|---|
| Hubs (2) | `hub_nation`, `hub_region` |
| Links (3) | `lnk_nation_region`, `lnk_customer_nation`, `lnk_supplier_nation` |
| Satélites (2) | `sat_nation`, `sat_region` |

## 2. Decisión

`nation` y `region` pasan a ser **reference tables**: `ref_nation` y `ref_region`. Se eliminan los 7 objetos anteriores y el Raw Vault pasa de **20 a 15 objetos**.

Los códigos que las referencian (`c_nationkey`, `s_nationkey`, `n_regionkey`) dejan de ser claves de links y pasan a ser **atributos normales**: los dos primeros, en los satélites `sat_customer` y `sat_supplier`; el tercero, en `ref_nation`.

## 3. Por qué reference tables

1. **Son datos de catálogo, no entidades de negocio.** Una reference table guarda códigos y descripciones que otras entidades consultan por su código (países, regiones, estados, tipos). Es el criterio habitual de la metodología para este tipo de datos: sin ciclo de vida de negocio propio, sin transacciones propias y compartidos por todo el modelo. `nation` y `region` encajan; `customer`, `supplier`, `part` y `order` sí son entidades de negocio y siguen siendo hubs.
2. **Los datos lo confirman.** `region` no cambia y `nation` solo recibe una fila nueva; ningún atributo se modifica entre lotes. La capacidad de versionar atributos de los satélites no se usa en estas dos entidades.
3. **Mucha estructura para tablas diminutas.** Un hub, un satélite, y hash keys, hashdiff y `LOAD_DATETIME` para 4 y 11 filas, más tres links para expresar "tiene un código". Con reference tables son dos tablas.
4. **Un modelo más fiel a la realidad del dato.** "El cliente 8 es de la nación 3" es un atributo del cliente que cambia con él (y se versiona en `sat_customer`); no es una relación entre dos entidades de negocio independientes. Como atributo, un cambio de nación del cliente queda en su historial sin necesitar un link.
5. **Un mart más simple.** `dim_customer` y `dim_supplier` obtienen nación y región con un join directo por código, en vez de atravesar links y satélites.
6. **El paquete lo soporta.** AutomateDV 0.11.5 incluye la macro `automate_dv.ref_table` para Snowflake (comprobado en `dbt/dbt_packages/automate_dv/macros/tables/snowflake/ref_table.sql`).

## 4. Qué alternativa se descarta (y qué se pierde)

**Mantener los hubs** habría dado:

- **Historia de los atributos** de nation y region (`sat_nation`/`sat_region` versionan por hashdiff).
- **Un solo patrón** para todas las entidades, más fácil de generar y de explicar.
- Relaciones explícitas como links.

Se renuncia a eso porque, con estos datos, la historia de nation/region no existe, y las relaciones se conservan como atributos con el mismo contenido de información. El patrón único era la ventaja más real, y se acepta un segundo patrón (el de reference tables) en metadatos y generador, que es pequeño.

## 5. Cómo funciona `ref_table` y qué consecuencia tiene

Según el código del paquete, la macro:

1. recibe una clave (`src_pk`), columnas de atributos (`src_extra_columns`), `src_ldts` y `src_source`;
2. hace `SELECT DISTINCT` del modelo origen con `src_pk` no nulo;
3. en cada ejecución incremental **inserta solo las filas cuya clave todavía no existe** en la tabla destino.

La clave puede ser la natural (p. ej. `N_NATIONKEY`); no necesita hash key.

**Consecuencia importante:** si un código ya existe y cambia un atributo en landing (por ejemplo, el nombre de una nación), la tabla **conserva la primera versión** y no registra el cambio. Hoy no ocurre, pero es un riesgo de desfase silencioso.

### Mitigación

- Un test dbt que compare `ref_nation` y `ref_region` con su staging y falle si algún atributo difiere para el mismo código. Se ejecuta en la etapa `dbt_raw_vault` de cada lote, así que un cambio de código en landing se vería como un fallo del DAG.
- Si en el futuro el negocio necesita historia de estos catálogos, el paso siguiente es un modelo propio que inserte una fila cuando cambie el hashdiff de los atributos (equivalente a un satélite sin hub). No se hace ahora porque no hay necesidad.

## 6. Implementación prevista

**Metadatos** (`metadata/vault/vault.yml`): nueva sección y retirada de los objetos antiguos.

```yaml
reference_tables:
  ref_nation: {source: nation, key: [n_nationkey]}
  ref_region: {source: region, key: [r_regionkey]}
```

Se eliminan de `hubs`, `links` y `satellites`: `hub_nation`, `hub_region`, `lnk_nation_region`, `lnk_customer_nation`, `lnk_supplier_nation`, `sat_nation`, `sat_region`.

**Generador** (`dbt/scripts/generate_models.py`):

- Genera un modelo por tabla de referencia con `automate_dv.ref_table`:
  ```sql
  {{ automate_dv.ref_table(src_pk='N_NATIONKEY',
                           src_extra_columns=['N_NAME', 'N_REGIONKEY', 'N_COMMENT'],
                           src_ldts='LOAD_DATETIME', src_source='RECORD_SOURCE',
                           source_model='stg_nation') }}
  ```
- El staging de `nation` y `region` ya no calcula hashes de hub/link/satélite; solo `LOAD_DATETIME` y `RECORD_SOURCE`.
- El staging de `customer` y `supplier` deja de calcular `NATION_HK`; `c_nationkey` y `s_nationkey` pasan al payload de sus satélites. Esto ocurre solo: el generador ya excluye del payload únicamente las columnas que son clave de un hub o de un link.
- Tests: `unique` y `not_null` en la clave de cada tabla de referencia, más el test de comparación del §5.

**Efecto sobre el mart:** `dim_customer` y `dim_supplier` obtienen nación y región con un join directo por código a `ref_nation` y `ref_region` (en la macro `scd2_dimension`), en lugar de atravesar links y satélites.

**Migración**: regenerar los modelos, reconstruir con `dbt build --full-refresh` y recargar los dos lotes. El hashdiff de `sat_customer` y `sat_supplier` cambia porque su payload incorpora la clave de nación, así que no se puede reutilizar la historia ya cargada: se vuelve a construir desde los dos lotes (landing → `dbt build --full-refresh` con 202512, después 202601).

## 7. Verificación (resultado)

Tras eliminar los 7 objetos antiguos de Snowflake, regenerar, recargar landing con 202512 y 202601 y ejecutar `dbt build` en cada lote:

- Raw Vault con **15** tablas: 4 hubs, 3 links, 6 satélites y 2 reference tables.
- `ref_nation` = 11 filas y `ref_region` = 4, con atributos idénticos a los CSV (comprobado fila a fila).
- `sat_customer` = 25 y `sat_supplier` = 18, igual que antes del cambio: ningún cliente ni proveedor cambió de nación entre lotes, así que incorporar el código de nación al payload no creó versiones nuevas.
- Los recuentos de los 15 objetos coinciden con un cálculo independiente desde los CSV.
- Los dos lotes pasan **68 de 68** modelos y tests, incluidos los dos tests de comparación `ref_*` contra staging.
- Relanzar `dbt run` sin recargar landing no inserta ninguna fila.
