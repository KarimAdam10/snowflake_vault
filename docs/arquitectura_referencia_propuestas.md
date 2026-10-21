# Arquitectura de referencia: propuestas

El enunciado pide una **propuesta de arquitectura de referencia para industrializar la solución en una cloud habitual** (AWS, Azure o Google Cloud) que contemple al menos:

| # | Requisito |
|---|---|
| R1 | Servicios principales de **almacenamiento, procesamiento, base de datos o lakehouse y orquestación** |
| R2 | **Aterrizaje del diseño de Snowflake**: objetos, roles y permisos, warehouses y justificación |
| R3 | **Ingesta y orquestación**: papel de Airflow y cómo se parametrizan y generan los pipelines desde metadatos |
| R4 | **Ubicación y responsabilidad de cada capa** de datos |
| R5 | **Papel de dbt** y su integración con el resto |
| R6 | **Ciclos de CI/CD** |

Se proponen **tres alternativas**, una por cloud, con la misma arquitectura lógica y distintos servicios. Cada una está escrita **como una traducción de la demo**: para cada pieza que ya conoces del prototipo (carpeta `data/`, contenedor de Airflow, `.env`, `PUT`...) se indica qué servicio de la cloud la sustituye y qué cambia. Quien conozca la demo puede así deducir la arquitectura sin conocer la cloud.

---

## 1. Lo que no cambia en ninguna alternativa

La cloud solo cambia **dónde se ejecuta y se guarda** cada cosa. Estas piezas se trasladan tal cual:

| Pieza | Por qué no cambia |
|---|---|
| **Diseño de Snowflake** ([snowflake.md](snowflake.md)): una base de datos por entorno, los mismos seis esquemas, roles `DV_ETL_*` y `DV_READER_*`, usuarios de servicio con clave, warehouses X-Small con tope de créditos | Es de Snowflake, no de la cloud en la que corra el resto |
| **Metadatos** (`metadata/`): contratos, `vault.yml`, `pipeline.yml` | Son ficheros de texto versionados en el repositorio |
| **Factoría de DAGs** y tareas (`dv_pipelines.py`, `dv_lib/`) | Es código de Airflow; funciona en cualquier Airflow |
| **Proyecto dbt**, generador de modelos y macros | dbt solo envía SQL a Snowflake |
| **Capas de datos** y su responsabilidad | Ver abajo |
| **Trazabilidad** en `CONTROL.PIPELINE_LOG` | Está en Snowflake |

**Capas y responsabilidad (R4)**

| Capa | Dónde vive | Quién la construye |
|---|---|---|
| Zips de entrada (`raw`) | Almacenamiento de objetos: inmutables, es lo único que no se puede regenerar de otro sitio | Quien entrega los datos |
| `LANDING` | Snowflake, tablas transitorias: último lote (`snapshot`) o lotes acumulados (`incremental`) de cada entidad | Tareas de carga de Airflow (`COPY INTO`) |
| `STAGING` | Snowflake, vistas: hash keys y hashdiff | dbt (generado desde `vault.yml`) |
| `RAW_VAULT` | Snowflake, tablas permanentes: histórico completo | dbt con AutomateDV |
| `BUSINESS_VAULT` | Snowflake, vistas: reglas de negocio | dbt |
| `MART` | Snowflake, tablas: estrella para analítica | dbt |
| `CONTROL` | Snowflake: `PIPELINE_LOG` | Airflow |

**Papel de Airflow (R3).** Orquesta; no procesa datos. La factoría genera los DAGs leyendo `pipeline.yml` y los contratos: añadir una entidad añade su tarea de carga sin tocar código. Se parametriza con `period` (qué lote) y `full_refresh` (reconstruir el vault, solo para el primer lote).

**Papel de dbt (R5).** Recibe el proyecto y se conecta a Snowflake con el usuario de servicio del entorno; genera SQL que ejecuta el warehouse. Airflow lo invoca por capas (`dbt build --select <capa>`).

**Ciclos de CI/CD (R6), iguales en todas.** Tres ciclos:

| Ciclo | Qué despliega | Cómo |
|---|---|---|
| 1. Código del pipeline (DAGs, cargador, dbt, metadatos) | Pull request → CI (`dbt parse`, construcción de la imagen, importación de los DAGs) → rama `dev` → PR con aprobación → rama `prod` | Como ya funciona en la demo ([cicd.md](cicd.md)) |
| 2. Imagen de ejecución (Airflow y dbt con sus dependencias) | Se reconstruye cuando cambian `Dockerfile` o `requirements` y se promueve la misma imagen de dev a prod | Registro de imágenes de la cloud |
| 3. Snowflake (roles, warehouses, esquemas) | Los scripts de `snowflake/`, por PR y ejecutados por un administrador | Pipeline manual con aprobación |

Lo que cambia entre alternativas es **qué servicio ejecuta cada paso**, no el flujo.

### Dos adaptaciones que valen para las tres

1. **Los zips en un bucket y carga desde un *stage externo*.** En la demo, el cargador extrae el zip a `/tmp`, hace `PUT` de cada CSV a un stage interno de Snowflake y ejecuta `COPY INTO`. En la nube no hay disco local: la tarea extrae el zip a otro prefijo del bucket (`landing/<periodo>/`) y Snowflake lee directamente de ahí con un **stage externo**; `COPY INTO` se queda igual y desaparece el `PUT`. Snowflake accede al bucket mediante una ***storage integration***: un rol de la cloud que Snowflake asume, con permiso de solo lectura sobre ese prefijo, de modo que no se guardan claves.
2. **Los modelos de dbt se generan dentro de la tarea.** Staging y Raw Vault no están en git: los genera `generate_models.py` desde los metadatos. En un contenedor efímero, la tarea de dbt debe ejecutar los dos pasos juntos (`generate_models.py` y después `dbt build --select <capa>`). Los resultados de dbt (`run_results.json`) se dejan en el bucket y una tarea ligera de Airflow los lee para escribir en `CONTROL.PIPELINE_LOG`, que es lo que hoy hace `run_dbt`.

---

## 2. Alternativa A: AWS (recomendada)

**En una frase.** El mismo Airflow, pero gestionado por AWS (**MWAA**); el contenedor donde corre dbt pasa a ser una tarea de **ECS Fargate**; las carpetas se vuelven buckets de **S3**.

La cuenta de Snowflake de este proyecto está en AWS (región `AWS_EU_WEST_3`, París), así que S3 y Snowflake quedan en la misma región, sin tráfico entre clouds.

**Qué es cada servicio, por analogía con la demo**
- **S3**: almacenamiento de objetos; equivale a la carpeta `data/`, pero compartida y duradera.
- **MWAA (Managed Workflows for Apache Airflow)**: Airflow que AWS ejecuta por ti (planificador, workers y base de datos de metadatos incluidos). Equivale a los contenedores `scheduler`, `dag-processor`, `apiserver` y `postgres` del `docker-compose`. Lee los DAGs de una carpeta de S3.
- **ECS con Fargate**: ejecuta un contenedor bajo demanda sin gestionar servidores; equivale a `docker run` de la imagen con dbt.
- **ECR**: registro de imágenes de contenedor; equivale a la imagen que hoy se construye en local.
- **Secrets Manager**: almacén de secretos; sustituye al `.env` y a la carpeta `keys/`.
- **IAM**: permisos; define quién puede leer qué bucket y lanzar qué tarea.
- **CloudWatch**: logs y alarmas.

**Traducción de la demo**

| En la demo | En AWS | Qué cambia |
|---|---|---|
| Carpeta `data/` con los zips | Bucket de S3, prefijo `raw/` | Mismos ficheros; el periodo sigue saliendo del nombre del zip (`source.yml` apunta al prefijo) |
| `/tmp/dv_pipeline` (CSV extraídos) | Prefijo `landing/<periodo>/` del bucket | Sin disco local |
| `PUT` + `COPY INTO` desde stage interno | Stage externo sobre S3 + `COPY INTO` | Una *storage integration* con un rol de IAM de solo lectura |
| Contenedores `scheduler`, `dag-processor`, `apiserver`, `postgres` | **Entorno de MWAA** | Se crea el entorno y se sube el código; se pierde el control del sistema operativo |
| DAGs y `metadata/` montados por volumen | Carpeta `dags/` del bucket de MWAA (DAGs, factoría y metadatos juntos) | El despliegue es sincronizar esa carpeta |
| Imagen con el entorno virtual de dbt, en el mismo contenedor | Imagen de dbt en ECR, ejecutada por una **tarea de Fargate** lanzada desde Airflow | dbt sale del proceso de Airflow; la separación del entorno virtual se mantiene |
| `.env` (cuenta, usuario, rol, warehouse) | Secrets Manager; MWAA lo lee como *secrets backend* | Ningún secreto en ficheros |
| `keys/svc_dv_*.p8` | Secreto en Secrets Manager | Se entrega a la tarea en tiempo de ejecución |
| UI en `localhost:8080` / `8081` | Interfaz web de MWAA, con acceso por IAM | Mismos DAGs y mismo botón «Trigger DAG» |
| Stack de dev y stack de prod (dos carpetas, dos `docker compose`) | Dos entornos de MWAA, dos buckets, idealmente en dos cuentas de AWS | Aislamiento por cuenta además de por rol de Snowflake |
| `git pull` en la carpeta de prod | Pipeline de despliegue | Ver CI/CD |

**Flujo de un lote** (nombres de las tareas de la demo)
1. `extract`: una tarea de MWAA localiza el zip del periodo en `raw/`, lo extrae a `landing/<periodo>/` y comprueba que están los 8 ficheros.
2. `landing` (8 tareas en paralelo): cada tarea ejecuta `COPY INTO` de su entidad desde el stage externo.
3. `generate_models` + `dbt_staging`, `dbt_raw_vault`, `dbt_business_vault`, `dbt_mart`: cada capa es una tarea de Fargate que genera los modelos y ejecuta `dbt build --select <capa>`, y deja `run_results.json` en el bucket.
4. Una tarea ligera de Airflow lee los resultados y los registra en `CONTROL.PIPELINE_LOG`.
5. `cleanup`: borra el prefijo `landing/<periodo>/`.

**Parametrización.** `period` y `full_refresh` se pasan al lanzar el DAG, desde la interfaz de MWAA o desde su API; el periodo disponible se deduce de los zips presentes en `raw/`. Como evolución, una notificación de S3 al llegar un zip podría lanzar el DAG sola.

**CI/CD.** GitHub Actions con identidad federada (OIDC) hacia AWS, sin claves guardadas. En cada PR ejecuta `dbt parse`, construye la imagen y comprueba que los DAGs se importan. Al fusionar en `dev` o `prod`: publica la imagen en ECR (la misma de dev a prod) y sincroniza `dags/` y `metadata/` con el bucket del entorno. Los scripts de Snowflake se ejecutan aparte.

**Seguridad.** Una cuenta de AWS por entorno; roles de IAM mínimos (la tarea de dbt solo lee su secreto y escribe en su prefijo); bucket sin acceso público.

**Qué hay que adaptar en el código.** Poco: el cargador usa el stage externo en lugar de `PUT`; `source.yml` pasa de una carpeta a un prefijo de S3; y `run_dbt` deja de ser un subproceso para lanzar una tarea de Fargate y leer su resultado.

**Ventajas.** Casi sin operación; mismo región que Snowflake; permisos sin claves.
**Inconvenientes.** MWAA tiene un coste fijo aunque no haya ejecuciones; hay que comprobar que la versión de Airflow que ofrece **coincide con la del proyecto (Airflow 3)**; queda atada a AWS.

---

## 3. Alternativa B: Azure

**En una frase.** La misma arquitectura sobre Azure. Azure no ofrece un Airflow gestionado de propósito general, así que el Airflow corre en **Kubernetes (AKS)**, que es la parte que más cambia respecto a la demo.

En un caso real, la cuenta de Snowflake se crearía en una región de Azure para evitar tráfico entre clouds (la actual está en AWS).

**Qué es cada servicio**
- **ADLS Gen2 (Azure Data Lake Storage)**: almacenamiento de objetos; equivale a `data/`.
- **AKS (Azure Kubernetes Service)**: Kubernetes gestionado. Airflow se instala con su chart oficial: el planificador, el procesador de DAGs y la API son tres *pods* (equivalen a los tres contenedores del `docker-compose`).
- **Azure Database for PostgreSQL**: base de datos de metadatos de Airflow (equivale al contenedor `postgres`).
- **Azure Container Apps Jobs**: ejecuta un contenedor bajo demanda; equivale a `docker run` de la imagen de dbt.
- **ACR (Azure Container Registry)**: registro de imágenes.
- **Key Vault**: secretos; sustituye a `.env` y `keys/`.
- **Managed identities / Entra ID**: identidades sin claves para que los servicios accedan entre sí.
- **Azure Monitor**: logs y alarmas.

**Traducción de la demo**

| En la demo | En Azure | Qué cambia |
|---|---|---|
| Carpeta `data/` | Contenedor de ADLS, `raw/` | Mismos zips |
| `/tmp/dv_pipeline` | Contenedor ADLS, `landing/<periodo>/` | Sin disco local |
| `PUT` + `COPY INTO` | Stage externo `azure://` + `COPY INTO` | *Storage integration* con una aplicación de Entra ID autorizada sobre la cuenta de almacenamiento |
| Contenedores de Airflow y `postgres` | Pods de Airflow en AKS + PostgreSQL gestionado | El equipo opera Kubernetes |
| DAGs y metadatos montados por volumen | Sincronización automática desde la rama del repositorio (*git-sync*) | Es el equivalente del `git pull` de prod, pero automático |
| Imagen con dbt en el mismo contenedor | Imagen en ACR ejecutada como job de Container Apps (o pod con el operador de Kubernetes) | |
| `.env` y `keys/` | Key Vault, entregado a los pods con identidad gestionada | Sin secretos en ficheros |
| UI local | Interfaz de Airflow detrás de un acceso con autenticación | |
| Dev y prod | Dos clústeres o dos espacios de nombres, dos cuentas de almacenamiento, idealmente dos suscripciones | |

**Flujo de un lote.** Idéntico al de A (`extract` → `landing` ×8 → `generate_models` + dbt por capas → registro en `CONTROL` → `cleanup`), con los zips en ADLS y dbt como job de contenedor.

**Parametrización.** `period` y `full_refresh` al lanzar el DAG desde la interfaz o la API de Airflow.

**CI/CD.** GitHub Actions (o Azure DevOps) con federación de identidad hacia Azure: construye la imagen y la publica en ACR; los DAGs y metadatos llegan a Airflow por la sincronización con la rama; los scripts de Snowflake, aparte.

**Qué hay que adaptar en el código.** Lo mismo que en A (stage externo, `source.yml` hacia ADLS, `run_dbt` lanzando un job) y además el despliegue de Airflow en Kubernetes.

**Ventajas.** Encaja si la organización ya es Azure; buena integración de identidad y secretos.
**Inconvenientes.** Más operación que A (Kubernetes); la cuenta de Snowflake actual no es de Azure.

---

## 4. Alternativa C: Google Cloud

**En una frase.** La misma arquitectura sobre Google Cloud, con Airflow gestionado (**Cloud Composer**) y dbt en **Cloud Run Jobs**. Es muy parecida a A.

**Qué es cada servicio**
- **Cloud Storage (GCS)**: almacenamiento de objetos; equivale a `data/`.
- **Cloud Composer**: Airflow gestionado por Google (se ejecuta sobre Kubernetes sin que lo operes); lee los DAGs de un bucket. Equivale al conjunto de contenedores de Airflow del `docker-compose`.
- **Cloud Run Jobs**: ejecuta un contenedor hasta que termina; equivale a `docker run` de la imagen de dbt.
- **Artifact Registry**: registro de imágenes.
- **Secret Manager**: secretos; sustituye a `.env` y `keys/`.
- **Workload Identity Federation / cuentas de servicio**: identidades para que los servicios (y GitHub) accedan sin claves.
- **Cloud Logging y Monitoring**.

**Traducción de la demo**

| En la demo | En Google Cloud | Qué cambia |
|---|---|---|
| Carpeta `data/` | Bucket de GCS, `raw/` | Mismos zips |
| `/tmp/dv_pipeline` | Prefijo `landing/<periodo>/` | Sin disco local |
| `PUT` + `COPY INTO` | Stage externo `gcs://` + `COPY INTO` | *Storage integration*: Snowflake crea una cuenta de servicio a la que se da lectura sobre el bucket |
| Contenedores de Airflow y `postgres` | Entorno de Cloud Composer | Se pierde el control del sistema operativo |
| DAGs y metadatos montados por volumen | Carpeta de DAGs del bucket de Composer | El despliegue es sincronizar esa carpeta |
| Imagen con dbt en el mismo contenedor | Imagen en Artifact Registry ejecutada como job de Cloud Run | |
| `.env` y `keys/` | Secret Manager (Composer lo usa como *secrets backend*) | |
| Dev y prod | Dos entornos de Composer, dos buckets, idealmente dos proyectos de Google Cloud | |

**Flujo de un lote.** Idéntico al de A, con los zips en GCS y dbt como job de Cloud Run.

**Parametrización.** `period` y `full_refresh` al lanzar el DAG desde la interfaz o la API de Composer.

**CI/CD.** GitHub Actions con *Workload Identity Federation* (o Cloud Build): imagen a Artifact Registry y sincronización de DAGs y metadatos con el bucket; scripts de Snowflake aparte.

**Qué hay que adaptar en el código.** Lo mismo que en A.

**Ventajas.** Orquestación gestionada con poca operación y modelo de ejecución muy simple.
**Inconvenientes.** Mismo aviso que A sobre la versión de Airflow soportada; coste fijo del entorno de Composer; la cuenta de Snowflake actual no es de GCP.

---

## 5. Comparación y recomendación

| Criterio | A: AWS | B: Azure | C: Google Cloud |
|---|---|---|---|
| Airflow | Gestionado (MWAA) | **Propio sobre Kubernetes** | Gestionado (Composer) |
| Ejecución de dbt | Tarea de Fargate | Job de Container Apps o pod | Job de Cloud Run |
| Operación necesaria | Baja | **Media-alta** | Baja |
| Encaje con la cuenta actual de Snowflake (AWS) | **Alto** | Bajo | Bajo |
| Cambios en el código del prototipo | Pocos | Pocos, más despliegue de Kubernetes | Pocos |
| Riesgo principal | Versión de Airflow soportada | Operar Kubernetes | Versión de Airflow soportada |

**Recomendación: la alternativa A.** Tres motivos: la cuenta de Snowflake ya está en AWS; cubre todos los requisitos con la menor operación; y mantiene la separación entre orquestación (Airflow) y ejecución (dbt en contenedor), que ya es la del prototipo. B y C se presentan como la misma arquitectura en otra cloud, con la diferencia de Kubernetes en B.

**Antes de fijar una alternativa gestionada, comprobar la versión de Airflow que soporta el servicio**: el proyecto usa Airflow 3.

## 6. Cómo se cubre el enunciado

| Requisito | Dónde se responde |
|---|---|
| R1 Servicios principales | Las tablas «Qué es cada servicio» y «Traducción de la demo» de la alternativa elegida |
| R2 Diseño de Snowflake | §1 y [snowflake.md](snowflake.md); *storage integration* y stage externo en cada alternativa |
| R3 Ingesta y orquestación | «Papel de Airflow» (§1), «Flujo de un lote» y «Parametrización» de cada alternativa |
| R4 Capas | Tabla de capas (§1) |
| R5 Papel de dbt | «Papel de dbt» (§1) y [dbt.md](dbt.md) |
| R6 CI/CD | Ciclos (§1) y «CI/CD» de cada alternativa; [cicd.md](cicd.md) |
