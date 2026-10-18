# Decisión técnica: capa de STAGING (y la alternativa sin ella)

- **Estado:** aceptada e implementada
- **Fecha:** 2026-10-17
- **Ámbito:** `dbt/models/staging/`, `dbt/scripts/generate_models.py`, `metadata/vault/vault.yml`

## 1. Contexto

El flujo es `CSV → LANDING → (STAGING) → RAW_VAULT → BUSINESS_VAULT → MART`.

LANDING se carga con un cargador genérico (`landing/loader.py`) que solo conoce ficheros, columnas y tipos (contratos de `metadata/ingestion/contracts/`). Guarda el **último lote**: cada carga sustituye a la anterior; el histórico vive en el vault.

Para cargar hubs, links y satélites hace falta calcular, a partir de cada fila de landing:

| Dato técnico | Para qué sirve |
|---|---|
| Hash key (`CUSTOMER_HK`) | Identifica la clave de negocio; une hub, satélite y links |
| Hash key de cada link (`CUSTOMER_NATION_HK`) | Identifica la relación |
| Hashdiff (`CUSTOMER_HASHDIFF`) | Detecta si los atributos han cambiado (nueva versión en el satélite) |
| `LOAD_DATETIME` | Fecha del lote (último día del mes de `_period`) |
| `RECORD_SOURCE` | Fichero de origen (`_source_file`) |

En la metodología Data Vault 2.0 estas son las *hard rules* que se aplican antes de cargar el Raw Vault. La pregunta es **dónde** se calculan.

## 2. Decisión

Se añade una capa **STAGING** compuesta por **una vista dbt por entidad** (`STAGING.STG_*`, 8 en total) que lee de LANDING y calcula esos datos con la macro `automate_dv.stage`. Los hubs, links y satélites leen de esas vistas.

Características:

- **No almacena datos**: son vistas; el único almacén es LANDING (último lote) y el vault (histórico).
- **Se genera, no se escribe**: `dbt/scripts/generate_models.py` produce los 8 modelos a partir de `metadata/vault/vault.yml` y los contratos. No hay código de staging mantenido a mano.
- **LANDING sigue siendo genérica**: no conoce claves de negocio ni el modelo del vault.

## 3. Por qué es necesaria la capa de staging (con AutomateDV)

1. **Es cómo funciona AutomateDV.** Las macros `hub`, `link` y `sat` no calculan nada: seleccionan columnas que ya existen en su `source_model` (la hash key, el hashdiff, `LOAD_DATETIME`, `RECORD_SOURCE`) y comparan por ellas. Lo comprobamos en el código del paquete (`macros/tables/snowflake/sat.sql`). Quien calcula esas columnas es la macro `stage`, y necesita un modelo propio donde ejecutarse.
2. **Una sola definición de las reglas técnicas.** Los hashes se definen una vez por entidad y los consumen hub, link y satélite. Es el sitio único donde está la normalización de claves (`UPPER(TRIM(...))`, nulos como `'^^'`, separador `'||'`). Si hub, link y satélite calcularan su propio hash, habría varias copias de la expresión que mantener alineadas.
3. **Desacopla la ingesta del modelo.** Las hash keys dependen de qué es clave de negocio y qué es atributo: decisiones del Data Vault, no de la ingesta. Con staging, cambiar una clave es regenerar y recompilar vistas; sin él, habría que **recargar los ficheros** en LANDING.
4. **Mantiene LANDING reutilizable.** La ingesta genérica y dirigida por metadatos es un punto explícito del enunciado; meter en ella los hashes del vault la acoplaría al modelo.
5. **Coste nulo en almacenamiento y en mantenimiento.** Son vistas generadas.

### Qué no aporta (limitaciones honestas)

- **No es una capa "física"**: no hay datos persistidos en STAGING. Su valor es lógico (centralizar el cálculo), no de almacenamiento.
- **Los metadatos ya definen las claves.** Lo que la capa hace es *ejecutar* esa definición en SQL; la información sobre qué es clave ya está en `vault.yml`. La capa existe porque AutomateDV exige un origen con las columnas calculadas, no porque aporte información nueva.
- Si no se usara AutomateDV, la capa dejaría de ser imprescindible (ver §5).

## 4. Alternativas descartadas

### 4.1 Calcular los hashes al cargar LANDING (landing = staging)

Viable y habitual en DV 2.0. Se descarta porque:

- Obliga a que `landing/loader.py` lea `metadata/vault/vault.yml`: **la ingesta pasa a depender del modelo del vault**, justo lo que se separó al dividir contratos y metadatos.
- Cambiar una clave exige **recargar** los ficheros.
- Hay que **reproducir a mano** en el cargador la normalización exacta de AutomateDV; si difiere en un detalle, los hashes almacenados no coinciden con los que el paquete espera.

### 4.2 Sin capa de staging y con AutomateDV

**No es posible**, por lo explicado en §3.1: las macros de carga necesitan las columnas ya calculadas en el origen. Se puede *ocultar* el staging con `materialized='ephemeral'` (dbt lo inyecta como subconsulta y no crea objeto en Snowflake), pero sigue existiendo como código. No se ha probado en este proyecto: la documentación del paquete recomienda `view`.

## 5. Alternativa sin staging y sin AutomateDV (macros propias)

Si se renunciara a AutomateDV, la capa de staging desaparece: cada modelo del vault calcula el hash directamente desde LANDING mediante una macro propia. Es una arquitectura distinta, equivalente en resultado.

**Esquema** (boceto de diseño, **no probado** en este proyecto):

```
LANDING ──► hub_customer   (calcula CUSTOMER_HK)
        ──► sat_customer   (calcula CUSTOMER_HK y CUSTOMER_HASHDIFF)
        ──► lnk_*          (calcula las HK de sus hubs y la del link)
```

Macro de hash, definida una vez:

```sql
{% macro dv_hash(cols) -%}
MD5_BINARY(CONCAT_WS('||',
  {%- for c in cols %}
  IFNULL(NULLIF(UPPER(TRIM(CAST({{ c }} AS VARCHAR))), ''), '^^'){{ ',' if not loop.last }}
  {%- endfor %}))
{%- endmacro %}
```

Hub (inserta solo claves nuevas):

```sql
{{ config(materialized='incremental') }}
SELECT {{ dv_hash(['C_CUSTKEY']) }} AS CUSTOMER_HK,
       C_CUSTKEY,
       LAST_DAY(TO_DATE(_PERIOD || '01', 'YYYYMMDD')) AS LOAD_DATETIME,
       _SOURCE_FILE AS RECORD_SOURCE
FROM {{ source('landing', 'LND_CUSTOMER') }}
{% if is_incremental() %}
WHERE {{ dv_hash(['C_CUSTKEY']) }} NOT IN (SELECT CUSTOMER_HK FROM {{ this }})
{% endif %}
```

Satélite (inserta si la clave es nueva o cambia el hashdiff respecto a la última versión):

```sql
{{ config(materialized='incremental') }}
WITH src AS (
  SELECT {{ dv_hash(['C_CUSTKEY']) }} AS CUSTOMER_HK,
         {{ dv_hash(['C_NAME','C_ADDRESS','C_PHONE','C_ACCTBAL','C_MKTSEGMENT','C_COMMENT']) }} AS CUSTOMER_HASHDIFF,
         C_NAME, C_ADDRESS, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT,
         LAST_DAY(TO_DATE(_PERIOD || '01', 'YYYYMMDD')) AS LOAD_DATETIME,
         _SOURCE_FILE AS RECORD_SOURCE
  FROM {{ source('landing', 'LND_CUSTOMER') }}
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN (
  SELECT CUSTOMER_HK, CUSTOMER_HASHDIFF FROM {{ this }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATETIME DESC) = 1
) cur ON cur.CUSTOMER_HK = s.CUSTOMER_HK
WHERE cur.CUSTOMER_HK IS NULL OR cur.CUSTOMER_HASHDIFF <> s.CUSTOMER_HASHDIFF
{% endif %}
```

Nota: AutomateDV expone una macro suelta de hash (`automate_dv.hash`, en `macros/supporting/hash.sql` del paquete instalado) que podría sustituir a `dv_hash` y dar **hashes idénticos** a los actuales, pero entonces los cargadores seguirían siendo propios.

### Comparación

| | Con staging + AutomateDV (actual) | Sin staging + macros propias |
|---|---|---|
| Capas / objetos extra | 8 vistas `STG_*` (generadas) | Ninguno |
| Dónde se calcula el hash | Una vez por entidad, en el stage | En cada modelo que lo necesita (hub, link y satélite): se evalúa varias veces |
| Lógica de carga incremental (hub, link, sat) | La aporta el paquete, probada y mantenida | La escribimos y mantenemos nosotros (hub, link, satélite, y los casos límite) |
| Hashes | Los define la macro `stage` del paquete | Los define `dv_hash` (hay que mantenerla coherente) |
| Riesgo | Dependencia de un paquete (compatibilidad con dbt; en este proyecto ya verificada con dbt 1.11) | Más código propio y más superficie de error |
| Control y transparencia | Cada línea es del paquete (menos explicable) | Total: cada línea se puede explicar |
| Cambiar una clave | Regenerar vistas | Regenerar los modelos que la usan |

## 6. Consecuencias

- Hay **un modelo `stg_<entidad>` por entidad**, generado; añadir una entidad es añadir su contrato y sus metadatos y volver a ejecutar el generador.
- LANDING permanece genérica: se puede reutilizar la ingesta con cualquier modelo.
- Los modelos de staging se regeneran en cada ejecución del DAG (etapa `generate_models`) y el generador cubre solo staging y Raw Vault: el Business Vault y el mart son SQL escrito a mano.
- La dependencia de AutomateDV queda localizada en los modelos de `staging/` y `raw_vault/`. Cambiar a macros propias (§5) sería una sustitución de esos modelos, no de la ingesta ni de los metadatos.
- Los hashes son `BINARY(16)` (MD5), con el tratamiento de nulos y la normalización del paquete.
