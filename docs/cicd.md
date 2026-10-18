# Flujo de trabajo: de dev a prod

Dos entornos con el mismo código y bases de datos de Snowflake distintas. **Cada entorno es un clon del repositorio con su propio `.env`; el código solo llega a prod mediante un pull request aprobado.**

```
rama feature ──PR──► dev ──PR (aprobado)──► prod
                      │                      │
              clon DEV (Docker)       clon PROD (Docker)
              .env → DV_PIPELINE_DEV  .env → DV_PIPELINE_PROD
```

## Cómo funciona, paso a paso

| # | Qué haces | Cómo se garantiza |
|---|---|---|
| 1 | **Levantas el Docker de dev** (`docker compose up -d` en el clon de dev). | El `.env` de ese clon apunta a `DV_PIPELINE_DEV`. |
| 2 | **Trabajas en una rama** (`git switch -c mi-cambio`) y la subes (`git push`). | Nadie puede escribir directamente en `dev` ni en `prod` (ver "Reglas de GitHub"): lo único que se puede subir son ramas de trabajo. |
| 3 | **Abres un pull request hacia `dev`.** Si se acepta, tus cambios entran en `dev`. | El PR exige que pase el CI (abajo). Hasta que no esté en verde no se puede fusionar. |
| 4 | **Para llevar los cambios a producción abres un pull request de `dev` hacia `prod`.** | `prod` exige el CI en verde y **una aprobación**. Sin ella GitHub no deja fusionar, y tampoco permite subir nada a `prod` por otra vía. |
| 5 | **En el clon de prod haces `git pull`** y levantas su Docker (`docker compose up -d`; con `--build` si cambió el `Dockerfile` o algún `requirements-*.txt`). | El clon de prod sigue la rama `prod`, que solo contiene lo aprobado. |
| 6 | **Cada Docker solo ve su base de datos.** | Las credenciales vienen del `.env` de su clon: el de dev no tiene la clave de prod y viceversa. El aislamiento real lo da Snowflake: el usuario de dev no debe tener permisos sobre `DV_PIPELINE_PROD` (pendiente, ver el README). |

El despliegue es solo actualizar código; los datos los sigue cargando Airflow cuando lanzas un DAG.

## Qué comprueba el CI

`.github/workflows/ci.yml` se ejecuta en cada pull request hacia `dev` o `prod`. **No se conecta a Snowflake.**

| Job | Qué comprueba |
|---|---|
| `dbt-parse` | Genera staging y Raw Vault desde los metadatos y comprueba que dbt compila todos los modelos y tests |
| `docker` | Que la imagen se construye y que los DAGs de Airflow se importan sin errores |

**Los modelos generados no están en git.** `dbt/models/staging`, `dbt/models/raw_vault` y `dbt/tests/*_matches_staging.sql` salen de `metadata/` y están en `.gitignore`: cada entorno los genera en la etapa `generate_models` del DAG, antes de ejecutar dbt. Lo que se versiona y se revisa en los PR son los metadatos.

## Reglas de GitHub

Se definen en dos ficheros que se importan tal cual (Settings → Rules → Rulesets → New ruleset → *Import a ruleset*): [`.github/rulesets/dev.json`](../.github/rulesets/dev.json) y [`.github/rulesets/prod.json`](../.github/rulesets/prod.json).

| Rama | Reglas |
|---|---|
| `dev` | Solo por pull request, con los dos checks del CI en verde. Sin force-push ni borrado. |
| `prod` | Solo por pull request, con los dos checks en verde (actualizados con `dev`) y **1 aprobación**. Sin force-push ni borrado. |

**Aprobar siendo una sola persona.** GitHub no permite aprobar tu propio PR. Por eso `prod.json` deja al rol *Admin* una única vía: fusionar el PR pulsando el botón de *bypass* (el "Merge without waiting for requirements"), que es un acto explícito y queda registrado en el PR. En cuanto haya otra persona con acceso, que sea ella quien apruebe y quita el bypass del ruleset: a partir de ahí sin aprobación no hay forma de entrar en `prod`.

## Puesta en marcha (una vez)

1. **Sube el trabajo actual** a `main` (todo el código nuevo está sin commitear): `git add -A`, `git commit`, `git push`. Hazlo antes de activar los rulesets.
2. **Renombra `main` a `dev`**: en GitHub, Settings → Branches → icono del lápiz junto a `main`. En local:
   ```powershell
   git branch -m main dev
   git fetch origin
   git branch -u origin/dev dev
   git remote set-head origin -a
   ```
3. **Crea `prod`:** `git push origin dev:prod`.
4. **Importa los dos rulesets** (arriba).
5. **Clon de prod:** en otra carpeta, `git clone -b prod <url> snowflake_vault_prod`. Crea ahí su `.env` (ver el README) con `SNOWFLAKE_DATABASE=DV_PIPELINE_PROD`, el usuario y la clave de prod, secretos de Airflow propios y `AIRFLOW_PORT=8081`. Levanta el stack (`docker compose up -d --build`) y haz la primera carga: `dv_end_to_end` con `202512` y `full_refresh=true`, y después `202601`.
6. **En dev**, el `.env` debe usar `SNOWFLAKE_DATABASE=DV_PIPELINE_DEV`.

## Límites

- **Prod protegido en GitHub, no en tu disco.** Nada puede entrar en la rama `prod` sin PR aprobado, pero el clon de prod es una carpeta en tu máquina y se puede editar. Lo que se ejecuta es lo que haya en esa carpeta: si no tocas el clon de prod salvo para `git pull`, es exactamente la rama `prod`. `git status` en esa carpeta debe salir limpio.
- **Aislamiento real de datos:** lo da Snowflake (permisos del usuario de cada `.env`), no el repositorio.
- **Datos en el repo:** los zips de `data/` se versionan solo porque es una demo con datos de ejemplo y el repo es público. En un entorno real los datos nunca irían al repositorio (bucket o stage de Snowflake con permisos).
- **Sin pruebas contra Snowflake en el CI:** pendiente (ver el README).
