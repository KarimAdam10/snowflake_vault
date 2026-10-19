-- Rol de la persona que desarrolla: trabaja en dev y solo LEE el mart de prod. Se ejecuta una vez,
-- después de haber ejecutado 01 y 02 para DEV y para PROD.
--
-- Antes de ejecutar, indica tu usuario:    SET dev_user = 'KAREM';

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS DV_DEVELOPER COMMENT = 'Desarrollo: escribe en dev, solo lee prod';

USE ROLE SECURITYADMIN;
GRANT ROLE DV_ETL_DEV      TO ROLE DV_DEVELOPER;
GRANT ROLE DV_READER_PROD  TO ROLE DV_DEVELOPER;
GRANT ROLE DV_DEVELOPER    TO ROLE SYSADMIN;
GRANT ROLE DV_DEVELOPER    TO USER IDENTIFIER($dev_user);
