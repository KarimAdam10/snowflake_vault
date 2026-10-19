# Diseño de Snowflake

Qué objetos existen, quién puede hacer qué, con qué cómputo y por qué. Todo se crea con los scripts de [`snowflake/`](../snowflake), que se pueden ejecutar varias veces sin destruir nada.

## Resumen

```
                         ┌──────────── DEV ────────────┐   ┌──────────── PROD ───────────┐
usuario de servicio      │ SVC_DV_DEV                  │   │ SVC_DV_PROD                 │
rol de ejecución         │ DV_ETL_DEV                  │   │ DV_ETL_PROD                 │
rol de lectura           │ DV_READER_DEV               │   │ DV_READER_PROD              │
warehouse (cómputo)      │ WH_DV_DEV                   │   │ WH_DV_PROD · WH_DV_BI_PROD  │
base de datos            │ DV_PIPELINE_DEV             │   │ DV_PIPELINE_PROD            │
                         │  LANDING · STAGING          │   │  (los mismos seis esquemas) │
                         │  RAW_VAULT · BUSINESS_VAULT │   │                             │
                         │  MART · CONTROL             │   │                             │
                         └─────────────────────────────┘   └─────────────────────────────┘
persona que desarrolla:  DV_DEVELOPER = escribe en dev (DV_ETL_DEV) + solo lee el mart de prod (DV_READER_PROD)
```

Un entorno es un conjunto cerrado: **su usuario de servicio solo conoce su rol, su warehouse y su base de datos**. No existe forma de que dev escriba en prod porque ni siquiera ve la base de datos.

## Objetos

| Esquema | Contenido | Tipo | Por qué |
|---|---|---|---|
| `LANDING` | Último lote tal cual llega del zip | transitorio | Se regenera desde el zip; no necesita Fail-safe (ahorra almacenamiento) |
| `STAGING` | Vistas con hash keys y hashdiff | (vistas) | No almacenan datos |
| `RAW_VAULT` | Histórico del Data Vault | **permanente** | Es la única copia del histórico: no se puede regenerar del zip una vez sustituido el lote |
| `BUSINESS_VAULT` | Reglas de negocio calculadas | (vistas) | No almacenan datos |
| `MART` | Modelo dimensional | transitorio | Se recalcula completo desde el vault |
| `CONTROL` | `PIPELINE_LOG`, trazabilidad | permanente | Es auditoría |

**Detalle importante:** dbt crea las tablas como *transitorias* por defecto. Sin intervención, el Raw Vault habría quedado sin Fail-safe y con un día de Time Travel como máximo para su cuenta. Por eso `dbt_project.yml` declara `+transient: false` en `raw_vault` y `+transient: true` en `mart`, y los esquemas `LANDING` y `MART` se crean como `TRANSIENT SCHEMA` (las tablas que contienen son transitorias por defecto, también las que crea el cargador).

**Time Travel:** la cuenta es de edición Standard, cuyo máximo es **1 día** (se comprobó: Snowflake rechaza 2). Con la edición Enterprise se ampliaría a 7 en `RAW_VAULT` (la línea está en `02_database.sql`, comentada).

## Roles y permisos

| Rol | Qué puede | Quién lo usa |
|---|---|---|
| `DV_ETL_<ENV>` | Propietario de los seis esquemas de su base de datos (crea y reemplaza lo que contienen) y uso de su warehouse. **Nada fuera de su base de datos** | `SVC_DV_<ENV>`: la carga de Python y dbt |
| `DV_READER_<ENV>` | `SELECT` en `MART` y `BUSINESS_VAULT` (también lo que dbt cree después) y uso del warehouse de lectura | Analistas, herramientas de BI |
| `DV_DEVELOPER` | Hereda `DV_ETL_DEV` y `DV_READER_PROD`: escribe en dev, solo lee el mart de prod | Quien desarrolla |
| `SYSADMIN` | Crea bases de datos y warehouses; ve lo que crean los roles anteriores (cuelgan de él) | Administración |
| `ACCOUNTADMIN` | Solo monitores de recursos y administración de la cuenta | Administración puntual |

**Cómo se reparten los privilegios de sistema en los scripts:** `USERADMIN` crea roles y usuarios, `SYSADMIN` crea bases de datos y warehouses, `SECURITYADMIN` reparte permisos, `ACCOUNTADMIN` solo crea los monitores. Cada bloque usa únicamente el rol que necesita.

**Permisos futuros.** dbt reemplaza las tablas del mart en cada ejecución y un permiso directo se perdería con cada reemplazo. Por eso los lectores reciben `GRANT SELECT ON FUTURE TABLES/VIEWS`.

**Usuarios de servicio.** `SVC_DV_DEV` y `SVC_DV_PROD` son de tipo `SERVICE`: sin contraseña, sin MFA interactivo, solo con par de claves RSA. La clave privada vive en `keys/` del clon de cada entorno (no se versiona); la pública se asigna al usuario con un `ALTER USER` que no está en el repositorio. Las claves se rotan asignando una segunda clave pública (`RSA_PUBLIC_KEY_2`) antes de retirar la primera.

## Warehouses

| Warehouse | Para qué | Parámetros |
|---|---|---|
| `WH_DV_DEV` | Carga y transformación de dev. Los lectores de dev lo comparten | X-Small, `AUTO_SUSPEND = 60`, `AUTO_RESUME`, creado suspendido, tiempo máximo de consulta 30 min |
| `WH_DV_PROD` | Carga y transformación de prod | igual |
| `WH_DV_BI_PROD` | Consultas de los lectores en prod | igual, con tiempo máximo de consulta de 10 min |

Cada uno tiene un **monitor de recursos** (10 créditos al mes: aviso al 75 % y 90 %, suspensión al 100 %). Las cuotas son una cifra inicial que se ajusta a la cuenta.

**Por qué X-Small y 60 s.** Medido en la cuenta: en 30 horas el pipeline lanzó 4.823 consultas que sumaron unos 346 s de ejecución. Son consultas diminutas y la talla no las acelera; el coste lo marca el tiempo que el warehouse permanece encendido. Antes de este diseño `COMPUTE_WH` se apagaba a los 300 s. Bajarlo a 60 s elimina casi todo el tiempo ocioso.

**Por qué un warehouse por entorno y uno aparte para lectura en prod.** Para que la carga de dev no compita con la de prod, para atribuir el coste de cada entorno, y para que una consulta pesada de un analista no frene la carga de prod. Con este volumen no hace falta más. La ruta de crecimiento es subir de talla si `lineitem` crece mucho, y añadir clústeres (edición Enterprise) si muchos analistas consultan a la vez.

## Decisiones y alternativas descartadas

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| Una base de datos por entorno en la misma cuenta | Una cuenta de Snowflake por entorno | Una cuenta distinta aislaría más, pero cuesta más y complica la administración; aquí el aislamiento por roles y warehouses basta |
| Un rol de ejecución por entorno | Separar cargador y transformador | Hoy la carga y dbt usan la misma conexión. Separarlos exige dos usuarios y que dbt lea su propio rol. Es una mejora posible, no necesaria para el aislamiento entre entornos |
| El rol ETL es propietario de los esquemas | Dárselos a `SYSADMIN` y conceder permisos sueltos | Con la propiedad, dbt crea y reemplaza objetos sin que haya que repartir permisos objeto a objeto |
| Los esquemas los crean los scripts, no el código | Que el cargador y el registro ejecuten `CREATE SCHEMA IF NOT EXISTS` | Snowflake exige el privilegio `CREATE SCHEMA` aunque el esquema ya exista. Quitar esas sentencias del código evita dar ese privilegio al rol de ejecución |
| Scripts SQL con variables de sesión | Terraform o `schemachange` | Menos piezas y se pueden ejecutar en Snowsight sin instalar nada. Con más entornos u organizaciones compensaría una herramienta de infraestructura |
| Usuarios `SERVICE` con clave | Reutilizar el usuario personal con contraseña | El pipeline no debe depender de una persona ni de MFA interactivo, y las claves se rotan sin parar nada |

## Cómo recrear un entorno

1. Con un usuario que pueda usar `USERADMIN`, `SYSADMIN`, `SECURITYADMIN` y `ACCOUNTADMIN`, en Snowsight (o con el conector): `SET env = 'DEV';` (o `'PROD'`) y ejecutar `01_roles_warehouses.sql`, `02_database.sql` y `03_users.sql` en ese orden.
2. Asignar la clave pública al usuario de servicio: `ALTER USER SVC_DV_DEV SET RSA_PUBLIC_KEY = '...'`.
3. Una sola vez, con ambos entornos creados: `SET dev_user = '<tu usuario>';` y `04_developer.sql`.
4. En el `.env` del entorno: `SNOWFLAKE_USER=SVC_DV_<ENV>`, `SNOWFLAKE_ROLE=DV_ETL_<ENV>`, `SNOWFLAKE_WAREHOUSE=WH_DV_<ENV>`, `SNOWFLAKE_DATABASE=DV_PIPELINE_<ENV>` y `SNOWFLAKE_PRIVATE_KEY_PATH=/opt/keys/svc_dv_<env>.p8`.
5. Lanzar `dv_end_to_end` con `202512` y `full_refresh=true`, y después con `202601`.

## Cómo comprobar el aislamiento

Con `USE SECONDARY ROLES NONE` (si no, los roles secundarios de tu usuario, entre ellos `ACCOUNTADMIN`, falsean la prueba):

| Prueba | Resultado esperado |
|---|---|
| Como `SVC_DV_DEV`: `CREATE TABLE` o `SELECT` en `DV_PIPELINE_PROD` | Rechazado: la base de datos ni siquiera existe para ese usuario |
| Como `SVC_DV_DEV`: `USE WAREHOUSE WH_DV_PROD` o `USE ROLE DV_ETL_PROD` | Rechazado |
| Como `DV_READER_PROD`: leer `MART` | Permitido |
| Como `DV_READER_PROD`: leer `RAW_VAULT` o crear una tabla en `MART` | Rechazado |
| Como `DV_DEVELOPER`: crear una tabla en dev | Permitido |
| Como `DV_DEVELOPER`: crear una tabla en prod | Rechazado |
| `SHOW GRANTS TO ROLE DV_ETL_DEV` | Nada menciona `PROD` |

## Pendiente

- **Red y secretos:** política de red para el acceso al servicio, y gestionar las claves fuera de ficheros locales.
- **Rol de CI** acotado y base de datos efímera para la integración continua.
- **Separar cargador y transformador** si se quiere el mínimo privilegio estricto.
- **Gobierno de datos** (enmascarado, etiquetas): no aplica con datos de ejemplo.
