-- Demostración de roles para las PERSONAS (la del pipeline está en demo_roles.py).
-- Se pega en una hoja de Snowsight con tu usuario y se ejecuta sentencia a sentencia (Ctrl+Enter).
-- Requiere que los dos entornos estén creados y cargados.
--
-- IMPORTANTE: tu usuario tiene más roles (entre ellos ACCOUNTADMIN). Con "USE SECONDARY ROLES NONE" Snowflake
-- solo aplica el rol que has elegido; sin ello las pruebas no demuestran nada.
-- Cada línea indica el resultado esperado.

-- =====================================================================================================
-- 1. DV_DEVELOPER: escribe en dev y solo lee el mart de prod
-- =====================================================================================================
USE ROLE DV_DEVELOPER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE WH_DV_DEV;
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE();                                  -- DV_DEVELOPER · WH_DV_DEV

CREATE TABLE DV_PIPELINE_DEV.LANDING.T_DEMO (a INT);                         -- OK: escribe en dev
DROP TABLE DV_PIPELINE_DEV.LANDING.T_DEMO;                                   -- OK

USE WAREHOUSE WH_DV_BI_PROD;                                                 -- OK: warehouse de lectura de prod
SELECT COUNT(*) FROM DV_PIPELINE_PROD.MART.FACT_LINEITEM;                    -- OK: lee el mart de prod
SELECT COUNT(*) FROM DV_PIPELINE_PROD.RAW_VAULT.HUB_CUSTOMER;                -- ERROR: no ve el Raw Vault de prod
CREATE TABLE DV_PIPELINE_PROD.MART.T_DEMO (a INT);                           -- ERROR: Insufficient privileges (prod es solo lectura)
USE WAREHOUSE WH_DV_PROD;                                                    -- ERROR: no puede usar el warehouse de carga de prod

-- =====================================================================================================
-- 2. DV_ETL_DEV: el rol con el que corre el pipeline de dev (aquí, como lo vería un usuario)
-- =====================================================================================================
USE ROLE DV_ETL_DEV;
USE SECONDARY ROLES NONE;
USE WAREHOUSE WH_DV_DEV;

SELECT COUNT(*) FROM DV_PIPELINE_DEV.INFORMATION_SCHEMA.TABLES;              -- OK
SELECT COUNT(*) FROM DV_PIPELINE_PROD.INFORMATION_SCHEMA.TABLES;             -- ERROR: Database 'DV_PIPELINE_PROD' does not exist or not authorized
SELECT COUNT(*) FROM DV_PIPELINE_PROD.MART.FACT_LINEITEM;                    -- ERROR: ni siquiera ve la base de datos
USE ROLE DV_ETL_PROD;                                                        -- ERROR: Requested role is not assigned to the executing user

-- =====================================================================================================
-- 3. DV_READER_PROD: solo consulta el mart (y el Business Vault) de prod
-- =====================================================================================================
USE ROLE DV_READER_PROD;
USE SECONDARY ROLES NONE;
USE WAREHOUSE WH_DV_BI_PROD;

SELECT COUNT(*) FROM DV_PIPELINE_PROD.MART.FACT_LINEITEM;                    -- OK
SELECT * FROM DV_PIPELINE_PROD.MART.DIM_CUSTOMER LIMIT 5;                    -- OK
SELECT * FROM DV_PIPELINE_PROD.BUSINESS_VAULT.BSAT_LINEITEM LIMIT 5;         -- OK
SELECT COUNT(*) FROM DV_PIPELINE_PROD.RAW_VAULT.HUB_CUSTOMER;                -- ERROR: no ve el Raw Vault
SELECT COUNT(*) FROM DV_PIPELINE_PROD.LANDING.LND_CUSTOMER;                  -- ERROR: no ve landing
CREATE TABLE DV_PIPELINE_PROD.MART.T_DEMO (a INT);                           -- ERROR: Insufficient privileges
SELECT COUNT(*) FROM DV_PIPELINE_DEV.INFORMATION_SCHEMA.TABLES;              -- ERROR: no ve dev

-- =====================================================================================================
-- 4. Qué tiene concedido cada rol y qué cuesta cada warehouse (se ejecuta como administrador)
-- =====================================================================================================
USE ROLE ACCOUNTADMIN;
SHOW GRANTS TO ROLE DV_ETL_DEV;                                              -- propiedad de sus 6 esquemas + USAGE de su base de datos y su warehouse; NADA de prod
SHOW GRANTS TO ROLE DV_READER_PROD;                                          -- USAGE de la base de datos, de MART y BUSINESS_VAULT, SELECT, USAGE de WH_DV_BI_PROD
SHOW GRANTS TO ROLE DV_DEVELOPER;                                            -- hereda DV_ETL_DEV y DV_READER_PROD
SHOW WAREHOUSES LIKE 'WH_DV%';                                               -- X-Small, auto_suspend 60, resource_monitor asignado
SHOW RESOURCE MONITORS;                                                      -- cuota mensual con avisos al 75 % y 90 % y suspensión al 100 %
