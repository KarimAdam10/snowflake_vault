"""Conexión a Snowflake con las variables del .env (autenticación por par de claves)."""
import os

import snowflake.connector


def get_connection():
    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
        "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    }
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    if passphrase:
        params["private_key_file_pwd"] = passphrase

    # Los parámetros vacíos no se envían
    params = {k: v for k, v in params.items() if v}
    return snowflake.connector.connect(**params)
