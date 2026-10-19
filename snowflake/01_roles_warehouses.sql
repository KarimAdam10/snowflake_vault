-- Roles, warehouses y topes de coste de UN entorno. Se puede ejecutar varias veces (no destruye nada).
--
-- Antes de ejecutar, elige el entorno:    SET env = 'DEV';     -- o 'PROD'
-- Hay que ejecutarlo con un usuario que pueda usar USERADMIN, SYSADMIN, SECURITYADMIN y ACCOUNTADMIN
-- (cada bloque usa solo el rol de sistema que necesita: separación de funciones).

SET etl_role    = 'DV_ETL_'    || $env;                   -- ejecuta el pipeline (carga + dbt)
SET reader_role = 'DV_READER_' || $env;                   -- solo lee el mart
SET wh_etl      = 'WH_DV_'     || $env;                   -- cómputo del pipeline
SET wh_reader   = IFF($env = 'PROD', 'WH_DV_BI_PROD', 'WH_DV_DEV');   -- cómputo de los lectores (en dev comparte el del pipeline)
SET rm_etl      = 'RM_' || $wh_etl;
SET rm_reader   = 'RM_' || $wh_reader;

-- 1) Roles (USERADMIN crea roles y usuarios)
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS IDENTIFIER($etl_role)    COMMENT = 'Pipeline DV: carga y transforma en su base de datos';
CREATE ROLE IF NOT EXISTS IDENTIFIER($reader_role) COMMENT = 'Pipeline DV: solo lectura del mart';

-- 2) Jerarquía: los roles propios cuelgan de SYSADMIN para que el administrador vea lo que crean
USE ROLE SECURITYADMIN;
GRANT ROLE IDENTIFIER($etl_role)    TO ROLE SYSADMIN;
GRANT ROLE IDENTIFIER($reader_role) TO ROLE SYSADMIN;

-- 3) Warehouses (SYSADMIN): X-Small, se apagan a los 60 s, se encienden solos y nacen apagados
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($wh_etl) WITH
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 1800
    COMMENT = 'Pipeline DV: carga y transformación';
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($wh_reader) WITH
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 600
    COMMENT = 'Pipeline DV: consultas de los lectores';

-- 4) Tope de coste (solo ACCOUNTADMIN crea monitores): aviso al 75 % y 90 %, suspensión al 100 %
USE ROLE ACCOUNTADMIN;
CREATE RESOURCE MONITOR IF NOT EXISTS IDENTIFIER($rm_etl) WITH
    CREDIT_QUOTA = 10 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY ON 90 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
CREATE RESOURCE MONITOR IF NOT EXISTS IDENTIFIER($rm_reader) WITH
    CREDIT_QUOTA = 10 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY ON 90 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
-- (ALTER WAREHOUSE no admite IDENTIFIER() en este valor: se construye la sentencia y se ejecuta)
SET sql_rm_etl    = 'ALTER WAREHOUSE ' || $wh_etl    || ' SET RESOURCE_MONITOR = ' || $rm_etl;
SET sql_rm_reader = 'ALTER WAREHOUSE ' || $wh_reader || ' SET RESOURCE_MONITOR = ' || $rm_reader;
EXECUTE IMMEDIATE $sql_rm_etl;
EXECUTE IMMEDIATE $sql_rm_reader;

-- 5) Quién puede usar cada warehouse (SECURITYADMIN reparte permisos)
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE IDENTIFIER($wh_etl)    TO ROLE IDENTIFIER($etl_role);
GRANT USAGE ON WAREHOUSE IDENTIFIER($wh_reader) TO ROLE IDENTIFIER($reader_role);
