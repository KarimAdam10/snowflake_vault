# Orquestación del pipeline end-to-end

Los DAGs de Airflow **se generan desde metadatos**; no hay código por tabla, por fichero ni por pipeline.

| Pieza | Fichero | Qué define |
|---|---|---|
| Pipelines y etapas | [`metadata/orchestration/pipeline.yml`](../metadata/orchestration/pipeline.yml) | etapas, dependencias, qué DAGs existen y reintentos y timeout de la carga de cada entidad |
| Entidades de ingesta | [`metadata/ingestion/contracts/<entidad>.yml`](../metadata/ingestion/contracts) | fichero, columnas y tipos, tipo de carga |
| Origen de los lotes | [`metadata/ingestion/source.yml`](../metadata/ingestion/source.yml) | dónde está el zip de cada periodo |
| Modelo del Raw Vault | [`metadata/vault/vault.yml`](../metadata/vault/vault.yml) | qué hubs, links, satélites y tablas de referencia existen (la etapa `generate_models` regenera de ahí staging y Raw Vault) |
| Factoría de DAGs | [`airflow/dags/dv_pipelines.py`](../airflow/dags/dv_pipelines.py) | lee lo anterior y construye los DAGs |
| Tareas | [`airflow/dags/dv_lib/`](../airflow/dags/dv_lib) | qué ejecuta cada tipo de etapa y cómo se registra |

## Flujo de `dv_end_to_end`

```
extract ──► landing (8 tareas, una por contrato, en paralelo) ──┐
                                                                ├─► dbt_staging ─► dbt_raw_vault ─► dbt_business_vault ─► dbt_mart ─► cleanup
generate_models ────────────────────────────────────────────────┘
```

| Etapa | Qué hace |
|---|---|
| `extract` | Localiza el zip del periodo (`{period}_sample*.zip` en `data/`), lo descomprime en un directorio temporal, comprueba que están los 8 ficheros de los contratos |
| `landing` | Una tarea por contrato: `CSV → LANDING.LND_*`, según `load.type`: `snapshot` sustituye el lote anterior; `incremental` conserva los anteriores y solo sustituye el del mismo periodo |
| `generate_models` | Regenera los modelos dbt de staging y Raw Vault desde `metadata/vault/vault.yml`: un cambio de metadatos se aplica en la siguiente ejecución sin tocar código (los modelos generados no se versionan en git) |
| `dbt_*` | `dbt build` de cada capa (modelos y sus tests). Si una capa falla, las siguientes no se ejecutan. Cada etapa excluye las capas posteriores (deducidas de `depends_on`), para que no ejecute tests que dependen de capas aún sin reconstruir |
| `cleanup` | Borra los ficheros temporales aunque alguna etapa haya fallado |

## Parametrización

Cada ejecución recibe dos parámetros al lanzar el DAG (*Trigger DAG* en la UI de Airflow o `airflow dags trigger --conf`):

| Parámetro | Valores | Efecto |
|---|---|---|
| `period` | Uno por cada zip encontrado (hoy `202512` y `202601`) | Qué zip se procesa; las dos cargas son el mismo DAG con distinto parámetro |
| `full_refresh` | `false` (por defecto) / `true` | `true` pasa `--full-refresh` a dbt y reconstruye el vault **desde cero**, borrando el histórico acumulado. Solo para el primer lote |

`max_active_runs=1`: un lote cada vez, porque landing se sustituye en cada carga y el vault lo consume en orden.

## Cómo cambiar el comportamiento solo con metadatos

| Quiero… | Edito… |
|---|---|
| Añadir o quitar una etapa de un pipeline, o crear otro pipeline | `metadata/orchestration/pipeline.yml` (`stages`, `pipelines`) |
| Cambiar el orden o las dependencias | `depends_on` en `metadata/orchestration/pipeline.yml` |
| Cambiar reintentos o timeout de la carga de una entidad | `entity_defaults` (para todas) y `entities` (excepciones) en `metadata/orchestration/pipeline.yml` |
| Cambiar de dónde salen los lotes o el nombre del zip | `metadata/ingestion/source.yml` |
| Añadir una entidad | su contrato (y su entrada en `metadata/vault/vault.yml`); aparece sola una tarea de carga |
| Cambiar el Raw Vault | `metadata/vault/vault.yml` |
| Cambiar las reglas de negocio, las dimensiones o los hechos | Los modelos SQL de `dbt/models/business_vault/` y `dbt/models/mart/` (no tienen metadatos) |
| Cambiar la regla de qué es un periodo válido | los zips presentes en `data/` definen los valores permitidos de `period` |

## Trazabilidad operativa

Cada ejecución deja constancia en **`CONTROL.PIPELINE_LOG`** (Snowflake), una fila por objeto procesado:

| Columna | Contenido |
|---|---|
| `DAG_ID`, `RUN_ID` | Qué ejecución de Airflow lo produjo |
| `PERIOD` | Periodo procesado |
| `STAGE`, `OBJECT_NAME` | Etapa y objeto: el zip, cada entidad cargada, cada modelo y test de dbt |
| `STATUS` | `SUCCESS`, `FAILED`, `PASS`, `ERROR`... según el origen |
| `ROWS_AFFECTED`, `DURATION_SECONDS`, `MESSAGE` | Filas, duración y mensaje (por ejemplo, el nombre del zip) |

Ejemplo de consulta: filas cargadas por entidad en cada ejecución.

```sql
SELECT run_id, period, object_name, rows_affected
FROM CONTROL.PIPELINE_LOG
WHERE stage = 'landing' ORDER BY log_ts DESC;
```

Para ver qué falló en una ejecución:

```sql
SELECT stage, object_name, status, message
FROM CONTROL.PIPELINE_LOG
WHERE run_id = '<run_id>' AND status IN ('FAILED', 'FAIL', 'ERROR');
```

Además, el vault ya conserva `RECORD_SOURCE` (fichero de origen) y `LOAD_DATETIME` (fecha del lote) en cada fila.

## Verificación realizada

Se ejecutó el flujo real contra Snowflake con los dos zips (`full_refresh=true` con `202512`; después `202601` sin él):

| Comprobación | Resultado |
|---|---|
| Lote 202512 | 15 tareas en `success`; 809 filas cargadas en landing (igual que los CSV) |
| Lote 202601 | 15 tareas en `success` |
| Raw Vault (15 objetos) frente a un cálculo independiente desde los CSV | Todos los recuentos coinciden; contenido de `ref_nation` y `ref_region` idéntico al de los CSV |
| Mart frente a los CSV | 25/18/46 versiones en las dimensiones, 1050 líneas en el hecho; sumas de precio, cantidad, precio neto y cargo, días en tránsito y de retraso, iguales; versiones antiguas correctamente referenciadas (75/60/32 líneas) |
| `CONTROL.PIPELINE_LOG` | Una fila por zip, entidad, modelo y test de cada ejecución, con filas y duración |
| Un fallo no queda oculto | Con un DAG de prueba (desechado), cuando una tarea falla la limpieza se ejecuta y el run termina en `failed` |

**Un defecto encontrado y corregido durante la verificación.** En la primera ejecución el registro de `PIPELINE_LOG` mostró que el test del hecho (`equal_rowcount` contra `lnk_lineitem`) se ejecutaba en la etapa `dbt_raw_vault`: dbt incluye por defecto los tests si *cualquiera* de sus padres está seleccionado, y comparó el hecho, todavía sin reconstruir, con el vault ya reconstruido. Se corrigió excluyendo en cada etapa las capas posteriores. Esa misma ejecución terminó en verde en Airflow aunque había fallado una etapa, porque la limpieza (siempre se ejecuta) era la última tarea; se corrigió declarándola como *teardown* de Airflow.

## Cómo ejecutarlo

1. Deja los zips en `data/` (`<periodo>_sample*.zip`; el patrón admite sufijos como `202512_sample (1).zip`).
2. En la UI de Airflow (http://localhost:8080), activa `dv_end_to_end` (los DAGs nacen pausados) y lánzalo con `period=202512` y `full_refresh=true`.
3. Cuando termine, lánzalo de nuevo con `period=202601` y `full_refresh=false`.

Desde la terminal, dentro del contenedor (`docker compose exec airflow-scheduler bash`):

```
airflow dags trigger dv_end_to_end --conf '{"period": "202512", "full_refresh": true}'
airflow dags trigger dv_end_to_end --conf '{"period": "202601", "full_refresh": false}'
```

## Límites conocidos

- **Orden de los lotes:** el vault asume que los lotes llegan en orden cronológico. Procesar `202512` después de `202601` sin `full_refresh` rompe el historial (por eso los tests de unicidad de los satélites fallarían).
- **Ficheros temporales en el disco del contenedor** (`/tmp/dv_pipeline`): vale con un único ejecutor local. En una arquitectura distribuida, el zip y los CSV extraídos irían a un almacenamiento compartido (bucket) o directamente a un stage de Snowflake.
- **`load.type`** (`snapshot` | `incremental`) decide cómo se carga LANDING: `snapshot` sustituye todo el contenido (y falla si llega con cero filas); `incremental` conserva los lotes anteriores y solo sustituye el del mismo periodo (con `full_refresh` se parte de cero). No cambia el staging ni el vault: la detección de borrados que aprovecharía la diferencia está en [mejoras_futuras.md](mejoras_futuras.md).
