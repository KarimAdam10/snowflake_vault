-- Base de datos, esquemas y permisos de UN entorno. Se puede ejecutar varias veces (no destruye nada).
-- Requiere haber ejecutado antes 01_roles_warehouses.sql con el mismo entorno.
--
-- Antes de ejecutar, elige el entorno:    SET env = 'DEV';     -- o 'PROD'

SET db          = 'DV_PIPELINE_' || $env;
SET etl_role    = 'DV_ETL_'      || $env;
SET reader_role = 'DV_READER_'   || $env;

-- 1) Base de datos y esquemas (SYSADMIN)
--    LANDING y MART son TRANSIENT: se reconstruyen desde los CSV de origen y desde el vault, y no necesitan Fail-safe.
--    RAW_VAULT es permanente: es el histórico, la única copia que no se puede regenerar de otro sitio.
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS IDENTIFIER($db);
USE DATABASE IDENTIFIER($db);

CREATE TRANSIENT SCHEMA IF NOT EXISTS LANDING        COMMENT = 'Último lote tal cual llega de los CSV';
CREATE SCHEMA           IF NOT EXISTS STAGING        COMMENT = 'Vistas con hash keys y hashdiff';
CREATE SCHEMA           IF NOT EXISTS RAW_VAULT      COMMENT = 'Histórico Data Vault (hubs, links, satélites, referencias)';
CREATE SCHEMA           IF NOT EXISTS BUSINESS_VAULT COMMENT = 'Reglas de negocio calculadas';
CREATE TRANSIENT SCHEMA IF NOT EXISTS MART           COMMENT = 'Modelo dimensional para analítica';
CREATE SCHEMA           IF NOT EXISTS CONTROL        COMMENT = 'Trazabilidad operativa del pipeline';

-- Time Travel: en la edición Standard el máximo es 1 día (el valor por defecto). Con la edición Enterprise se
-- ampliaría en el histórico con:
--     ALTER SCHEMA RAW_VAULT SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- 2) El rol ETL es propietario de los esquemas: crea y reemplaza lo que contienen
GRANT OWNERSHIP ON SCHEMA LANDING        TO ROLE IDENTIFIER($etl_role) COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA STAGING        TO ROLE IDENTIFIER($etl_role) COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA RAW_VAULT      TO ROLE IDENTIFIER($etl_role) COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA BUSINESS_VAULT TO ROLE IDENTIFIER($etl_role) COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA MART           TO ROLE IDENTIFIER($etl_role) COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA CONTROL        TO ROLE IDENTIFIER($etl_role) COPY CURRENT GRANTS;

-- 3) Permisos (SECURITYADMIN)
USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE IDENTIFIER($db) TO ROLE IDENTIFIER($etl_role);
GRANT USAGE ON DATABASE IDENTIFIER($db) TO ROLE IDENTIFIER($reader_role);

-- Los lectores ven el mart y el Business Vault; también lo que dbt cree después (permisos futuros),
-- porque dbt reemplaza las tablas en cada ejecución y un permiso directo se perdería.
USE DATABASE IDENTIFIER($db);
GRANT USAGE  ON SCHEMA MART           TO ROLE IDENTIFIER($reader_role);
GRANT USAGE  ON SCHEMA BUSINESS_VAULT TO ROLE IDENTIFIER($reader_role);
GRANT SELECT ON ALL    TABLES IN SCHEMA MART           TO ROLE IDENTIFIER($reader_role);
GRANT SELECT ON FUTURE TABLES IN SCHEMA MART           TO ROLE IDENTIFIER($reader_role);
GRANT SELECT ON ALL    VIEWS  IN SCHEMA MART           TO ROLE IDENTIFIER($reader_role);
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA MART           TO ROLE IDENTIFIER($reader_role);
GRANT SELECT ON ALL    VIEWS  IN SCHEMA BUSINESS_VAULT TO ROLE IDENTIFIER($reader_role);
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA BUSINESS_VAULT TO ROLE IDENTIFIER($reader_role);
