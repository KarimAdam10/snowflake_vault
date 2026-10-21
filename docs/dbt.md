# dbt aplicado a este proyecto

Guía teórica de dbt orientada a lo que hay implementado: qué es, cómo está organizado el proyecto, cómo se ejecuta y cómo funcionan las pruebas.

## 1. Qué es dbt y qué no

dbt es la **T** de ELT (extraer, cargar, transformar). Escribes `SELECT` en ficheros y dbt los convierte en objetos de la base de datos (vistas o tablas), resolviendo el orden entre ellos, y ejecuta pruebas sobre el resultado.

**No mueve datos ni calcula nada él mismo: genera SQL y se lo manda a Snowflake, que hace el trabajo y cobra el cómputo.** Esto es lo más importante para entenderlo:

```
 Contenedor de Airflow                                   Snowflake
┌────────────────────────────┐   SQL por la conexión   ┌───────────────────────────┐
│ dbt (entorno virtual)      │ ──────────────────────► │ warehouse: ejecuta el SQL │
│  1. lee los modelos .sql   │                         │ bases de datos: guardan   │
│  2. resuelve el orden      │ ◄────────────────────── │   el resultado            │
│  3. compila el Jinja a SQL │   estado y nº de filas  └───────────────────────────┘
└────────────────────────────┘
```

Un ejemplo concreto. El modelo `stg_customer.sql` contiene un `SELECT`. dbt lo compila y lo envuelve en una sentencia de definición (`CREATE OR REPLACE VIEW ... AS SELECT ...`), la manda a Snowflake y recibe solo el resultado (correcto o error). Los datos de los clientes nunca pasan por el contenedor: se leen y escriben dentro de Snowflake, y su cómputo se factura al warehouse. Por eso un proyecto dbt es ligero: es un conjunto de ficheros de texto y un proceso que envía consultas.

**Qué aporta dbt sobre escribir los SQL a mano:**
- **Orden automático.** Cada modelo dice de cuáles depende (`ref()`), y dbt deduce en qué orden construirlos y qué se puede hacer en paralelo.
- **Reutilización.** Las macros (funciones que generan SQL) evitan repetir el mismo patrón en 15 objetos del vault y cuatro dimensiones.
- **Pruebas.** Comprobaciones de calidad integradas, que se ejecutan junto a los modelos.
- **Documentación y linaje.** Genera una web con el grafo de dependencias.

**Qué variante usamos.** dbt tiene tres formas de uso: *dbt Core* (el programa de código abierto, por línea de comandos), *dbt Cloud* (servicio gestionado) y *dbt Projects on Snowflake* (ejecutarlo dentro de Snowflake). Aquí usamos **dbt Core**, con el adaptador **`dbt-snowflake`** (el componente que sabe hablar el dialecto de Snowflake y conectarse a él).

**Entorno virtual aparte.** dbt está instalado en `/opt/dbt-venv`, separado del Python de Airflow, porque cada uno exige versiones de librerías que podrían chocar entre sí (el conector de Snowflake, por ejemplo). Airflow no importa dbt: lo **invoca como un programa externo** (`/opt/dbt-venv/bin/dbt build ...`) desde sus tareas y lee después el resultado (`run_results.json`) para dejar la traza en `CONTROL.PIPELINE_LOG`. Por eso, para ejecutar dbt a mano hay que usar esa ruta completa dentro del contenedor.

## 2. Piezas del proyecto (carpeta `dbt/`)

| Pieza | Qué es | En el proyecto |
|---|---|---|
| `dbt_project.yml` | Configuración global: rutas, materialización y esquema por carpeta | Staging vistas; Raw Vault incremental permanente; Business Vault vistas; mart tablas |
| `profiles.yml` | Cómo conectarse (cuenta, usuario, rol, warehouse, base de datos) | Todo sale de variables de entorno, sin secretos en el fichero |
| `packages.yml` | Paquetes externos | AutomateDV (carga del Data Vault) y `dbt_utils` |
| `models/` | Los `SELECT` que se convierten en objetos | Staging y Raw Vault **generados**; Business Vault y mart **a mano** |
| `macros/` | Funciones Jinja reutilizables | `scd2_dimension`, `join_scd2`, `generate_schema_name` |
| `tests/` | Pruebas escritas como SQL | Las dos `ref_*_matches_staging` (generadas) |
| `target/` | Lo que dbt produce (no se versiona) | SQL compilado, resultados, manifiesto |

## 3. Modelos y materializaciones

Un **modelo** es un fichero `.sql` con un `SELECT`. Su **materialización** decide qué objeto crea dbt en la base de datos a partir de ese `SELECT`, es decir, *cómo se "materializa" la consulta*: como una vista que se recalcula cada vez que se consulta, como una tabla con los datos guardados, o como una tabla que solo recibe las filas nuevas.

| Tipo | Qué crea | Dónde se usa aquí |
|---|---|---|
| `view` | Una vista, sin datos almacenados | Staging, Business Vault |
| `table` | Tabla reconstruida entera en cada ejecución | Mart |
| `incremental` | Tabla que en cada ejecución solo añade lo nuevo | Raw Vault (hubs, links, satélites) |

Un modelo incremental es el mecanismo de la carga histórica: la primera vez crea la tabla y las siguientes ejecuciones insertan solo filas nuevas. `--full-refresh` lo reconstruye desde cero, por eso es el parámetro del primer lote. dbt crea las tablas como *transitorias* por defecto, y por eso fijamos `+transient: false` en el Raw Vault (ver [snowflake.md](snowflake.md)).

## 4. Jinja, `ref()` y macros

**Jinja** es un lenguaje de plantillas (el mismo que usan Flask o Ansible): permite meter en un texto fragmentos de lógica, como variables, condiciones y bucles, que se resuelven antes de usar el texto. En dbt sirve para que el SQL deje de ser fijo: lo que escribes entre `{{ ... }}` o `{% ... %}` se evalúa al *compilar* y el resultado es SQL puro.

Lo esencial:
- **`ref('hub_customer')`** referencia otro modelo. Dos efectos: se sustituye por el nombre real del objeto, y **dbt construye el grafo de dependencias** (qué se ejecuta antes de qué). Es lo que ordena el pipeline.
- **`source()`** referencia una tabla que dbt no crea (aquí, las de landing).
- **`config(...)`** fija opciones por modelo.
- **`is_incremental()`** es verdadero cuando el modelo ya existe y se ejecuta incrementalmente. Es lo que usan por dentro las macros de AutomateDV.
- **Macros:** funciones que generan SQL. `automate_dv.hub(...)`, `.sat(...)`, `.link(...)` y `.stage(...)` producen el SQL completo de cada objeto del vault, que es por lo que los modelos del Raw Vault son una llamada de pocas líneas. `scd2_dimension` hace lo mismo con las dimensiones del mart.

Dos fases: *compilar* (convertir Jinja en SQL puro, visible en `target/compiled/`) y *ejecutar* (mandarlo a Snowflake, visible en `target/run/`). Es la mejor forma de entender qué hace una macro: abre el SQL compilado.

## 5. Ejecución y selección

- `dbt run` crea los modelos, `dbt test` ejecuta las pruebas, `dbt build` hace ambas cosas en orden de dependencia: **cada modelo seguido de sus pruebas, y si una falla, no se ejecutan los modelos que dependen de él**. Así un error de datos no se propaga.
- **Quién ejecuta en paralelo.** Los modelos que no dependen entre sí se ejecutan a la vez, hasta el número de hilos que fija `threads: 4` en `profiles.yml`. Quien lo gestiona es **el propio proceso dbt** dentro del contenedor de Airflow: abre hasta 4 conexiones simultáneas y envía 4 sentencias a la vez. **El trabajo pesado lo ejecuta el warehouse de Snowflake**, no dbt ni "una nube de dbt" (eso solo ocurriría con dbt Cloud). Si llegan más consultas de las que el warehouse admite a la vez, esperan en cola.
- `--select staging` ejecuta una carpeta; `--exclude` quita partes. En Airflow cada etapa llama a `dbt build --select <capa>` y excluye las capas posteriores. Esto último existe por un detalle que ya nos dio un fallo: por defecto dbt incluye una prueba si *cualquiera* de sus modelos padre está seleccionado (*indirect selection*), así que una prueba del mart se colaba en la etapa del Raw Vault.
- `dbt parse` (lo que hace el CI) solo compila y valida sin conectar; `dbt ls` lista objetos.

## 6. Pruebas

Una prueba en dbt es **una consulta que debe devolver cero filas**: si devuelve alguna, esas filas son los fallos. Hay dos tipos:

**Genéricas (se declaran en YAML, junto al modelo)**
- `unique`, `not_null`, `accepted_values` y `relationships` (integridad referencial) vienen con dbt.
- `dbt_utils` añade, entre otras, `unique_combination_of_columns`, `mutually_exclusive_ranges` (rangos sin solape) y `equal_rowcount`.

**Singulares (un fichero `.sql` en `tests/`)**: una consulta propia que devuelve las filas incorrectas.

Cómo se ve por dentro la prueba `unique` de una clave:
```sql
select CUSTOMER_HK, count(*) as n
from hub_customer
where CUSTOMER_HK is not null
group by CUSTOMER_HK
having count(*) > 1        -- si hay filas, la prueba falla
```

**Qué tienes: 90 pruebas**, de las que 88 son genéricas y 2 singulares.

| Prueba | Aprox. | Qué protege |
|---|---|---|
| `not_null` | 34 | Que no falten claves ni medidas |
| `relationships` | 22 | Que cada clave del link o del hecho exista en su hub o dimensión |
| `unique` | 15 | Una fila por clave: sin duplicados en hubs, links y dimensiones |
| `unique_combination_of_columns` | 11 | Un satélite no repite versión (clave + fecha de carga) |
| `mutually_exclusive_ranges` | 4 | Las versiones SCD2 de una clave no se solapan |
| `equal_rowcount` | 2 | El hecho tiene tantas filas como su origen (no se pierde nada al resolver dimensiones) |
| Singulares `ref_*_matches_staging` | 2 | Que las tablas de referencia no se desfasen del dato real |

Los de `RAW_VAULT` los genera el generador desde `vault.yml`; los del mart y el Business Vault están en sus `.yml`.

**Cómo decidir qué recortar.** Cada prueba es una consulta que gasta cómputo y tiempo; con este volumen cuesta céntimos, con millones de filas no. El criterio útil es *¿qué se rompería en silencio si no existiera?*:
- **Imprescindibles según el diseño:** `unique` y `not_null` de las hash keys (si fallan, el Data Vault está mal construido) y `relationships` de los links. Son la justificación de que el vault es correcto.
- **Protegen decisiones de diseño:** los rangos sin solape y el `equal_rowcount` del hecho (sin ellos, la historia SCD2 puede producir duplicados o perder líneas sin que nadie lo vea) y las dos singulares (la limitación de `ref_table`).
- **Más prescindibles:** muchas de las `not_null` y `relationships` de las claves de fecha del hecho. Fallarían solo si `dim_date` no cubre el rango, y esa limitación ya está documentada.

## 7. Otras capacidades de dbt (no usadas, útiles para la defensa)

- **`dbt docs generate` y `dbt docs serve`**: genera una web con el **grafo de linaje** de los 30 modelos. Es un buen apoyo visual para la demo.
- **Tests unitarios** (dbt 1.8 o posterior): prueban la lógica de un modelo con datos simulados, sin tocar Snowflake.
- **Contratos de modelo:** se declaran en el YAML las columnas y tipos que un modelo *debe* devolver (`contract: {enforced: true}`). Antes de crear el objeto, dbt comprueba que el `SELECT` devuelve exactamente eso y falla si no coincide. Sirve para dar una "interfaz estable" a quien consume el modelo (por ejemplo, el mart hacia BI), de modo que un cambio interno no rompa a los consumidores sin avisar. Aquí no se usan.
- **Frescura de fuentes y exposiciones**: gobierno de datos, para entornos más grandes.

## 8. Aviso de deprecación (resuelto)

Las pruebas del Raw Vault que genera el generador escribían los argumentos en el nivel superior, y las versiones futuras de dbt exigen que vayan bajo `arguments:` (el mart ya lo hacía). Producía 19 avisos `MissingArgumentsPropertyInGenericTestDeprecation` en cada compilación. El generador (`dbt/scripts/generate_models.py`) ya escribe `arguments:` y `dbt parse` no emite avisos.

## Cómo practicarlo (sin romper nada)

dbt está dentro del contenedor, en su entorno virtual y **no está en el PATH**, así que `dbt ...` a secas da `command not found`. Hay que entrar al contenedor, ir a `/opt/dbt` y usar la ruta completa. Desde PowerShell, en la carpeta del repo:

```powershell
docker compose exec airflow-scheduler bash -c "cd /opt/dbt && /opt/dbt-venv/bin/dbt ls --quiet --resource-type test"
```

(`--quiet` evita las líneas de registro mezcladas con el listado.) Ejercicios:

1. **Ver las pruebas:** el comando anterior lista las 90.
2. **Ver qué hace una macro:** `dbt compile --select hub_customer` y abrir el SQL resultante en `target/compiled/dv_pipeline/models/raw_vault/`.
3. **Ver la consulta de una prueba:** `dbt test --select hub_customer` y mirar su SQL en `target/compiled`.
4. **Ver el orden de dependencia:** `dbt build --select mart`.
5. **Generar el grafo de linaje:** `dbt docs generate`.

Todos van dentro de `docker compose exec airflow-scheduler bash -c "cd /opt/dbt && /opt/dbt-venv/bin/dbt <comando>"`.
