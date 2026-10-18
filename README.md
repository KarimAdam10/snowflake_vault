# DV Pipeline · Snowflake + dbt + Airflow

Pipeline de datos end-to-end sobre **Snowflake** con arquitectura **Data Vault 2.0**: carga un lote de ficheros (un zip por periodo), lo transforma por capas con **dbt** y deja un **modelo dimensional** listo para analítica, todo orquestado con **Airflow**. El comportamiento del pipeline (ingesta, vault, orquestación) se gobierna con ficheros de metadatos, no con código por tabla.

El proyecto corre en local con Docker (Airflow, dbt y el código de ingesta); Snowflake es la base de datos en la nube donde viven todos los datos y se ejecuta todo el SQL.

## Resumen

```
zip del periodo
   │  extract + landing (Python, una tarea por entidad)
   ▼
LANDING ──► STAGING ──► RAW_VAULT ──► BUSINESS_VAULT ──► MART
 último       vistas      histórico       reglas de          dimensiones
 lote         con hashes  (insert-only)   negocio (vista)    y hechos
                                                              
Orquestado por Airflow · transformado por dbt (+ AutomateDV) · trazado en CONTROL.PIPELINE_LOG
```

| Capa (esquema Snowflake) | Qué contiene | Cómo se construye |
|---|---|---|
| `LANDING` | Un CSV por entidad, tal cual, más columnas técnicas (`_load_id`, `_period`, `_source_file`...). **Solo el último lote**: cada carga sustituye a la anterior | `landing/` (Python), dirigido por los contratos de `metadata/ingestion/` |
| `STAGING` | 8 vistas (`stg_*`): calculan hash keys, hashdiff, `LOAD_DATETIME` y `RECORD_SOURCE` | dbt + `automate_dv.stage`, **generadas** desde `vault.yml` |
| `RAW_VAULT` | 15 objetos: 4 hubs, 3 links, 6 satélites, 2 tablas de referencia. Aquí vive el **histórico** | dbt + AutomateDV, **generados** desde `vault.yml` |
| `BUSINESS_VAULT` | 1 vista: `bsat_lineitem` (reglas de negocio de la línea de pedido) | modelo SQL escrito a mano |
| `MART` | 7 tablas: `dim_customer`, `dim_supplier`, `dim_part`, `dim_order` (SCD2), `dim_date`, `fact_lineitem`, `fact_orders` | modelos SQL escritos a mano + macros |
| `CONTROL` | `PIPELINE_LOG`: una fila por zip, entidad, modelo y test de cada ejecución | Airflow (`dv_lib/audit.py`) |

## Estructura del repositorio

```
airflow/dags/
  dv_pipelines.py      Factoría: genera los DAGs de Airflow desde metadata/orchestration/pipeline.yml
  dv_lib/              Qué ejecuta cada etapa (tasks.py) y cómo se registra en CONTROL (audit.py)
landing/               Ingesta genérica zip -> LANDING (loader.py, main.py para uso por CLI, snowflake_conn.py)
metadata/              Todos los metadatos, una carpeta por tema
  ingestion/             contracts/<entidad>.yml (fichero, columnas, tipos, tipo de carga) y source.yml (dónde está el zip)
  vault/                 vault.yml (hubs, links, satélites y tablas de referencia del Raw Vault)
  orchestration/         pipeline.yml (etapas, dependencias, DAGs, reintentos y timeout por entidad)
dbt/
  models/staging|raw_vault   Generados por scripts/generate_models.py en cada ejecución (no se versionan)
  models/business_vault      bsat_lineitem.sql (+ tests)
  models/mart                dim_*.sql, fact_*.sql, dim_date.sql (+ tests)
  macros/                    scd2_dimension, join_scd2, generate_schema_name
  scripts/generate_models.py Genera staging y Raw Vault desde metadata/
  tests/                     Tests singulares generados (ref_*_matches_staging)
data/                  Un zip por lote: <periodo>_sample*.zip (202512 y 202601)
.github/               Workflow de CI y rulesets de las ramas `dev` y `prod`
snowflake/             Reservada para scripts SQL de objetos de Snowflake (vacía)
keys/                  Clave privada de Snowflake (no se versiona)
docs/                  La documentación (menos este README)
Dockerfile · docker-compose.yml · requirements-*.txt    Entorno local (Airflow + dbt en un virtualenv aparte)
```

## Qué se gobierna por metadatos, y qué no

| Tema | Fichero | Qué cambia sin tocar código |
|---|---|---|
| Entidades de ingesta | `metadata/ingestion/contracts/*.yml` | Añadir o cambiar una entidad: fichero, columnas, tipos. Aparece sola una tarea de carga |
| Origen de los lotes | `metadata/ingestion/source.yml` | Dónde está el zip y cómo se llama; los periodos disponibles son los zips que haya |
| Raw Vault | `metadata/vault/vault.yml` | Hubs, links, satélites, tablas de referencia; de aquí salen staging y Raw Vault |
| Orquestación | `metadata/orchestration/pipeline.yml` | Etapas, dependencias, qué DAGs existen, reintentos y timeout por entidad |

El **Business Vault y el mart no tienen metadatos**: son modelos SQL a propósito. Cada hecho es una decisión de diseño distinta (grano, medidas, dimensiones), y expresarla en YAML equivalía a reescribir SQL en otro lenguaje. Lo que se repite con la misma forma (ingesta, hubs/links/satélites, dimensiones SCD2) se resuelve con generadores o macros; lo que es diseño, con SQL.

## Los datos de ejemplo

Dos lotes con las mismas 8 entidades, que simulan los mismos orígenes en dos momentos: `202512` y `202601`.

| Entidad | 202512 → 202601 | Comportamiento |
|---|---|---|
| customer, supplier, part, partsupp | 20→22, 15→16, 40→43, 120→132 | Foto completa; algunos registros cambian (3 clientes, 2 proveedores, 3 piezas) y es lo que prueba el historial |
| nation, region | 10→11, 4→4 | Catálogos casi estáticos: tablas de referencia |
| orders, lineitem | 100→110, 500→550 | Solo filas nuevas |

Resultado esperado tras cargar los dos lotes: hubs de customer, supplier, part y order con 22, 16, 43 y 210 filas; `lnk_lineitem` 1050; `sat_customer` 25, `sat_supplier` 18, `sat_part` 46; `ref_nation` 11 y `ref_region` 4. En el mart: `dim_customer` 25 filas (22 vigentes), `dim_supplier` 18 (16), `dim_part` 46 (43), `dim_order` 210, `fact_lineitem` 1050, `fact_orders` 210.

## Puesta en marcha (PowerShell, en la raíz del repo)

Hay dos entornos, dev y prod: dos clones del repo, cada uno con su `.env` y su base de datos (ver [docs/cicd.md](docs/cicd.md)). Lo que sigue vale para cualquiera de los dos.

1. **Configurar `.env`.** No hay fichero de ejemplo en el repositorio; crea `.env` con estas variables (no se versiona):

   | Variable | Para qué |
   |---|---|
   | `SNOWFLAKE_ACCOUNT` (formato `ORG-CUENTA`), `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` | Conexión; la base de datos y el warehouse deben existir y el rol tener permiso de crear esquemas. En dev, `DV_PIPELINE_DEV`; en prod, `DV_PIPELINE_PROD` |
   | `SNOWFLAKE_PASSWORD` **o** `SNOWFLAKE_PRIVATE_KEY_PATH` (+ `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` si la clave la tiene) | Autenticación. Con par de claves, la ruta es la del contenedor: `/opt/keys/rsa_key.p8` |
   | `SNOWFLAKE_SCHEMA` (opcional) | Esquema por defecto de la conexión (dbt usa `PUBLIC` si falta) |
   | `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET` | Secretos locales de Airflow |
   | `AIRFLOW_UID`, `AIRFLOW_VERSION`, `AIRFLOW_PORT` (opcionales) | Por defecto `50000`, `3.3.2` y `8080`. Pon `AIRFLOW_PORT=8081` en prod si los dos stacks corren a la vez |

2. **Autenticación por par de claves (recomendada).**
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
   openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
   ```
   En Snowsight: `ALTER USER <TU_USUARIO> SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';` (el contenido de `rsa_key.pub` sin las líneas BEGIN/END). Con MFA activo, la contraseña no sirve.

3. **Construir y arrancar.**
   ```powershell
   docker compose build
   docker compose up airflow-init
   docker compose up -d
   ```
   UI de Airflow: http://localhost:8080 (en local no pide login).

4. **Comprobar la conexión de dbt.**
   ```powershell
   docker compose exec airflow-scheduler bash -c "cd /opt/dbt && /opt/dbt-venv/bin/dbt debug"
   ```

## Ejecutar el pipeline

Los DAGs nacen pausados. Deja los zips en `data/` y, en la UI de Airflow, activa `dv_end_to_end` y lánzalo:

1. `period = 202512` y `full_refresh = true` (reconstruye el vault desde cero).
2. Cuando termine, `period = 202601` y `full_refresh = false`.

Solo ingesta (sin transformar): `dv_landing_only`. Detalle de etapas, parámetros y trazabilidad en [orquestacion.md](docs/orquestacion.md).

Staging y Raw Vault se regeneran en cada ejecución desde `vault.yml` (etapa `generate_models`) y **no se versionan en git** (están en `.gitignore`): lo que se versiona son los metadatos. A mano, dentro del contenedor:

```powershell
docker compose exec airflow-scheduler bash -c "python /opt/dbt/scripts/generate_models.py"
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && /opt/dbt-venv/bin/dbt build"
```

## Qué crea en Snowflake

Todo lo crea el propio pipeline; la base de datos y el warehouse deben existir antes.

| Objeto | Quién lo crea |
|---|---|
| Esquema `LANDING`, stage `LANDING.STG_INGEST`, tablas `LND_*` | El cargador (`landing/loader.py`), si no existen |
| Esquema `CONTROL` y tabla `PIPELINE_LOG` | El registro de trazabilidad (`dv_lib/audit.py`), si no existen |
| Esquemas `STAGING`, `RAW_VAULT`, `BUSINESS_VAULT`, `MART` y sus objetos | dbt (la macro `generate_schema_name` usa el nombre del esquema tal cual, sin prefijo) |

`dbt deps` (AutomateDV y `dbt_utils`) se ejecuta solo, dentro del DAG, si falta el paquete `automate_dv` en `dbt/dbt_packages`.

## Verificación realizada

Se ejecutó el flujo real con los dos zips y se comprobó que **todo coincide con un cálculo independiente desde los CSV**: recuentos de los 15 objetos del Raw Vault, contenido de las tablas de referencia, versiones de las dimensiones, importes del hecho, días de tránsito y de retraso, y que las líneas del primer lote apuntan a la versión antigua de las dimensiones que cambiaron. `dbt` tiene **31 modelos y 100 tests**, y los dos lotes ejecutan las 15 tareas del DAG en verde. Ese cálculo independiente se hizo con scripts de la sesión de trabajo que **no están en el repositorio** (ver acciones pendientes).

## Documentación

| Documento | Contenido |
|---|---|
| [orquestacion.md](docs/orquestacion.md) | DAGs generados desde metadatos, etapas, parámetros, trazabilidad y límites |
| [raw_vault_model.md](docs/raw_vault_model.md) | Diagrama y convenciones del Raw Vault |
| [business_vault_y_mart.md](docs/business_vault_y_mart.md) | Business Vault, estrella del mart, decisiones de modelado y verificación |
| [decision_staging.md](docs/decision_staging.md) | Por qué existe la capa de staging y la alternativa sin ella |
| [decision_reference_tables.md](docs/decision_reference_tables.md) | Por qué `nation` y `region` son tablas de referencia y no hubs |
| [cicd.md](docs/cicd.md) | Flujo de trabajo dev → prod: ramas, pull requests, CI, configuración de GitHub y alta de los entornos |

## Límites conocidos

- **Orden de los lotes:** el vault asume lotes en orden cronológico. Procesar un periodo anterior sin `full_refresh` rompe el historial. `dv_landing_only` sustituye la landing sin actualizar el vault: úsalo solo para ingerir.
- **`ref_table` conserva la primera versión** de cada código; un test compara cada tabla de referencia con su staging y falla si divergen.
- **`load.type`** (`snapshot`/`incremental`) casi no tiene efecto: solo hace fallar una entidad `snapshot` con cero filas.
- **Cada hub se carga desde una sola entidad.** Con estos datos no hay claves huérfanas (se comprobó); con otros, habría que cargar el hub desde todas las fuentes que traen la clave.
- **Las claves de negocio de las dimensiones** están escritas en su SQL y también en `vault.yml`: si cambia una, hay que cambiar las dos.
- **`dim_date`** genera hasta 3660 días desde la fecha mínima; los tests del hecho avisarían si los datos abarcaran más.
- **Ficheros temporales** (`/tmp/dv_pipeline`) en el disco del contenedor: vale con un único ejecutor local.
- **Entorno:** el DAG antiguo `01_load_landing` sigue registrado en la base de datos de Airflow aunque su fichero ya no existe. Se puede retirar con `airflow dags delete 01_load_landing`.
- **Datos en git:** los zips de `data/` se versionan solo porque es una demo con datos de ejemplo y repo público; en un entorno real los datos nunca irían al repositorio.

## Problemas frecuentes

| Síntoma | Causa probable |
|---|---|
| `docker compose build` falla al hacer `pip install` | Proxy corporativo: configura el proxy en Docker Desktop (Settings > Resources > Proxies) |
| El contenedor se reinicia en bucle | Docker con poca memoria: asigna al menos 4 GB (ideal 8 GB) |
| `dbt debug` falla en la conexión | Revisa `SNOWFLAKE_ACCOUNT`, que `SNOWFLAKE_DATABASE` exista y, con par de claves, que la clave pública esté asignada al usuario |
| `390100 Incorrect username or password` | Credenciales distintas dentro del contenedor: tras cambiar `.env`, recrea con `docker compose up -d --force-recreate` |
| `Creating schema on shared database 'SNOWFLAKE_SAMPLE_DATA' is not allowed` | `SNOWFLAKE_DATABASE` apunta a la base de datos de ejemplo: usa una propia (p. ej. `DV_PIPELINE_DEV`) |
| Error de MFA con contraseña | Usa autenticación por par de claves |
| Tras un `git pull` los DAGs o dbt fallan por una librería | Cambió `Dockerfile` o `requirements-*.txt`: `docker compose up -d --build` |
| El DAG no aparece | Espera ~10 s (el dag-processor refresca) o revisa `docker compose logs airflow-dag-processor` |
| El run sale en rojo en una capa dbt | Mira la tarea en Airflow y `CONTROL.PIPELINE_LOG` (qué modelo o test falló y con qué mensaje) |

> `SIMPLE_AUTH_MANAGER_ALL_ADMINS` y los secretos de `.env` son solo para uso local. No se levanta `triggerer` porque no hay tareas deferrables.

## Acciones pendientes

Lo que queda por hacer, y solo esto, para dejar el proyecto desplegable: el CI/CD y la gestión de permisos y objetos de Snowflake.

### CI/CD

Implementado el flujo de ramas con CI (ver [docs/cicd.md](docs/cicd.md)). Falta:

- [ ] **Aplicar la configuración de GitHub** (renombrar `main` a `dev`, crear `prod`, importar los rulesets) y dar de alta el clon de prod: está descrito, pero son pasos manuales.
- [ ] **Integración contra Snowflake**: base de datos efímera por ejecución, cargar los dos zips, `dbt build` y borrarla. Requiere un rol de CI acotado y secretos en GitHub.
- [ ] **Convertir en tests automáticos la verificación independiente** contra los CSV (recuentos, importes, versiones SCD2). Hoy son scripts fuera del repositorio.
- [ ] **Publicar la imagen** (p. ej. en GitHub Container Registry) en lugar de construirla en cada clon.
- [ ] **Datos fuera del repo** en un entorno real (ver límites conocidos).

### Permisos, roles y objetos de Snowflake

Hoy todo se ejecuta con el rol y el warehouse del `.env` (en la cuenta de pruebas, `ACCOUNTADMIN` y `COMPUTE_WH`, y los objetos pertenecen a `ACCOUNTADMIN`).

- [ ] **Jerarquía de roles de mínimo privilegio** en lugar de `ACCOUNTADMIN`: un rol de ingesta (escribe en `LANDING` y `CONTROL`), uno de transformación para dbt (crea y modifica `STAGING`, `RAW_VAULT`, `BUSINESS_VAULT` y `MART`, y lee `LANDING`), uno de lectura analítica (solo `MART`) y uno administrador que los agrupe.
- [ ] **Usuarios de servicio** para Airflow y para CI con autenticación por par de claves, sin contraseñas ni MFA interactivo, y rotación de claves.
- [ ] **Permisos futuros** (`FUTURE TABLES/VIEWS IN SCHEMA`) para que los objetos nuevos que cree dbt hereden los permisos.
- [ ] **Warehouses por carga de trabajo** (ingesta, transformación, consulta), de tamaño mínimo, con suspensión automática y un *resource monitor*.
- [ ] **Separar los permisos de dev y prod.** `DV_PIPELINE_DEV` y `DV_PIPELINE_PROD` ya existen. Cada stack solo llega a la base de su `.env` si el usuario de dev no tiene permisos sobre `DV_PIPELINE_PROD` y prod usa su propio usuario de servicio con su par de claves. Verificarlo, y añadir la base efímera de CI.
- [ ] **Objetos como código:** llenar `snowflake/` con scripts versionados (base de datos, esquemas, stage, roles, permisos) y quitar del cargador, del registro de trazabilidad y de dbt los `CREATE SCHEMA/STAGE/TABLE IF NOT EXISTS` en tiempo de ejecución, que hoy obligan a que el rol de ejecución tenga permisos de creación.
- [ ] **Seguridad de acceso:** política de red para el acceso al servicio y gestión de secretos fuera de `.env`.
