"""Conexión única a Snowflake para la ingesta y las tareas de Airflow.

Lee la misma configuración (.env) que usa dbt, de modo que hay una sola fuente
de verdad para las credenciales.
"""
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
    }
    params["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    if passphrase:
        params["private_key_file_pwd"] = passphrase

    params = {k: v for k, v in params.items() if v}
    return snowflake.connector.connect(**params)
