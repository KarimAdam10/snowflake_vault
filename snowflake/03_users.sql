-- Usuario de servicio de UN entorno (el que usa Airflow/dbt). Se puede ejecutar varias veces.
-- Requiere haber ejecutado antes 01 y 02 con el mismo entorno.
--
-- Antes de ejecutar, elige el entorno:    SET env = 'DEV';     -- o 'PROD'
--
-- El usuario NO tiene contraseña: se autentica con un par de claves. La clave pública no se guarda en el
-- repositorio; se asigna aparte, con el administrador, una vez por usuario (y al rotarla):
--     ALTER USER SVC_DV_DEV SET RSA_PUBLIC_KEY = '<contenido de la clave pública sin las líneas BEGIN/END>';
-- La clave privada vive en keys/ del clon de cada entorno (no se versiona) y se indica en el .env.

SET svc_user  = 'SVC_DV_'    || $env;
SET etl_role  = 'DV_ETL_'    || $env;
SET wh_etl    = 'WH_DV_'     || $env;

USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS IDENTIFIER($svc_user) TYPE = SERVICE COMMENT = 'Usuario de servicio del pipeline DV';
ALTER USER IDENTIFIER($svc_user) SET DEFAULT_ROLE = $etl_role DEFAULT_WAREHOUSE = $wh_etl;

USE ROLE SECURITYADMIN;
GRANT ROLE IDENTIFIER($etl_role) TO USER IDENTIFIER($svc_user);
