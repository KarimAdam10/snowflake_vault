# Flujo de trabajo: de dev a prod

Dos entornos con el mismo código y bases de datos de Snowflake distintas. **Cada entorno es un clon del repositorio con su propio `.env`; el código solo llega a prod mediante un pull request.**

```
rama feature ──PR──► dev ──PR──► prod
                      │            │
              clon DEV (Docker)  clon PROD (Docker)
              .env → SVC_DV_DEV   .env → SVC_DV_PROD
                     DV_PIPELINE_DEV     DV_PIPELINE_PROD
```

## Cómo funciona, paso a paso

| # | Qué haces | Cómo se garantiza |
|---|---|---|
| 1 | **Levantas el Docker de dev** (`docker compose up -d` en el clon de dev). | El `.env` de ese clon usa el usuario, rol, warehouse y base de datos de dev. |
| 2 | **Trabajas en una rama** (`git switch -c mi-cambio`) y la subes (`git push`). | GitHub no deja escribir directamente en `dev` ni en `prod`: lo único que se puede subir son ramas de trabajo. |
| 3 | **Abres un pull request hacia `dev`.** Si se acepta, tus cambios entran en `dev`. | El PR exige que pase el CI. Hasta que no esté en verde no se puede fusionar. |
| 4 | **Para llevar los cambios a producción abres un pull request de `dev` hacia `prod`.** | Mismo CI y mismas reglas: sin PR no entra nada en `prod`. Usa *Create a merge commit* (no *Squash*, que rompe el historial entre las dos ramas). |
| 5 | **En el clon de prod haces `git pull`** y levantas su Docker (`docker compose up -d`; con `--build` si cambió el `Dockerfile` o algún `requirements-*.txt`). | El clon de prod sigue la rama `prod`, que solo contiene lo que pasó por PR. |
| 6 | **Cada Docker solo ve su base de datos.** | Cada `.env` trae el usuario de servicio de su entorno, y ese usuario solo tiene rol, warehouse y permisos sobre su base de datos: el de dev ni siquiera ve `DV_PIPELINE_PROD`. Ver [snowflake.md](snowflake.md). |

El despliegue es solo actualizar código; los datos los sigue cargando Airflow cuando lanzas un DAG. **No hay despliegue automático:** el `git pull` de prod es manual a propósito.

## Qué comprueba el CI

`.github/workflows/ci.yml` se ejecuta **cuando se abre o se actualiza un pull request** hacia `dev` o `prod` (no cuando se aprueba ni después de fusionar). **No se conecta a Snowflake.**

| Job | Qué comprueba |
|---|---|
| `dbt-parse` | Genera staging y Raw Vault desde los metadatos y comprueba que dbt compila todos los modelos y tests |
| `docker` | Que la imagen se construye y que los DAGs de Airflow se importan sin errores |

Son la barrera mínima sin credenciales: cazan metadatos incoherentes, `ref()` rotos, `requirements` o `Dockerfile` rotos y DAGs que no importan. **No** comprueban que el SQL funcione en Snowflake ni que los datos salgan bien.

**Los modelos generados no están en git.** `dbt/models/staging`, `dbt/models/raw_vault` y `dbt/tests/*_matches_staging.sql` salen de `metadata/` y están en `.gitignore`: cada entorno los genera en la etapa `generate_models` del DAG, antes de ejecutar dbt. Lo que se versiona y se revisa en los PR son los metadatos.

## Reglas de GitHub

Se configuran en GitHub (Settings → Rules → Rulesets), no en el repositorio. Un mismo ruleset cubre las dos ramas (*Target branches*: `dev` y `prod`).

| Regla | Efecto |
|---|---|
| Restringir actualizaciones: solo por pull request | Nadie puede hacer push directo a `dev` ni a `prod` |
| Checks obligatorios: `dbt-parse` y `docker` | El botón de merge no se habilita hasta que ambos estén en verde |
| Sin force-push ni borrado de la rama | El historial de `dev` y `prod` no se reescribe |

La rama por defecto es `dev`: los PR nuevos apuntan a ella, y llevar algo a `prod` exige abrir el PR hacia `prod` a propósito.

**Aprobación.** Con una sola persona, GitHub no permite aprobar tu propio PR, así que el ruleset no puede exigir una aprobación aparte sin dejarte bloqueado: el control es PR obligatorio más CI en verde. Cuando haya otra persona con acceso, sube las aprobaciones requeridas de `prod` a 1.

## Cómo recrear un entorno

1. **Ramas:** `dev` (por defecto) y `prod` (`git push origin dev:prod`), con el ruleset de arriba.
2. **Snowflake:** ejecutar los scripts de [`snowflake/`](../snowflake) con `SET env = 'DEV'` o `'PROD'` y asignar la clave pública al usuario de servicio (ver [snowflake.md](snowflake.md#cómo-recrear-un-entorno)).
3. **Dev:** en el clon de trabajo, `.env` con `SNOWFLAKE_USER=SVC_DV_DEV`, `SNOWFLAKE_ROLE=DV_ETL_DEV`, `SNOWFLAKE_WAREHOUSE=WH_DV_DEV`, `SNOWFLAKE_DATABASE=DV_PIPELINE_DEV` y la ruta de su clave; `docker compose build`, `docker compose up airflow-init`, `docker compose up -d`.
4. **Prod:** en otra carpeta, `git clone -b prod <url> snowflake_vault_prod`. Su `.env` con lo mismo pero de `PROD`, `AIRFLOW_FERNET_KEY` y `AIRFLOW_JWT_SECRET` propios y `AIRFLOW_PORT=8081` (si no, choca con dev en el 8080). La clave privada va en su carpeta `keys/` (no se versiona). Mismos tres comandos de arranque.
5. **Primera carga de cada entorno:** `dv_end_to_end` con `202512` y `full_refresh=true`, y después con `202601` y `full_refresh=false`.

## Estado

| Comprobación | Estado |
|---|---|
| Flujo de ramas con PR: rama de trabajo → `dev` (PR #1) → `prod` (PR #2) | Verificado: ambos PR fusionados |
| Dev: carga de los dos lotes con `SVC_DV_DEV` y 16 tablas con los recuentos esperados | Verificado (120 de 120 modelos y tests por lote) |
| Dev no puede escribir ni ver `DV_PIPELINE_PROD` | Verificado con el usuario de servicio (ver [snowflake.md](snowflake.md#cómo-comprobar-el-aislamiento)) |
| Prod: carga con el usuario de servicio `SVC_DV_PROD` | Pendiente: prod se cargó antes con el usuario personal; falta cambiar su `.env` y recargar |

## Límites y pendientes

- **Prod protegido en GitHub, no en tu disco.** Nada puede entrar en la rama `prod` sin PR, pero el clon de prod es una carpeta en tu máquina y se puede editar. Si solo haces `git pull` en ella, es exactamente la rama `prod`; `git status` debe salir limpio.
- **Sin pruebas contra Snowflake en el CI.** Pendiente: una base efímera que cargue los dos zips y compruebe los recuentos (necesita un rol de CI acotado).
- **Despliegue manual:** `git pull` y, si cambian `Dockerfile` o `requirements-*.txt`, `up -d --build`.
- **Datos en el repo:** los zips de `data/` se versionan solo porque es una demo con datos de ejemplo y el repo es público. En un entorno real los datos nunca irían al repositorio (bucket o stage de Snowflake con permisos).
