"""Punto de entrada CLI de la ingesta (la orquestación completa vive en Airflow).

Ejemplos (dentro del contenedor):
  python -m landing.main customer 202512
  python -m landing.main all 202601
"""
import argparse
import sys
import tempfile

from snowflake.connector import DictCursor

from landing.loader import check_contents, extract_archive, list_entities, run
from landing.snowflake_conn import get_connection


def main():
    parser = argparse.ArgumentParser(description="Carga el zip de un periodo a la landing de Snowflake")
    parser.add_argument("entity", help="Entidad (metadata/ingestion/contracts/<entity>.yml) o 'all'")
    parser.add_argument("period", help="Periodo YYYYMM, p. ej. 202512")
    args = parser.parse_args()

    entities = list_entities() if args.entity == "all" else [args.entity]
    failed = 0
    with tempfile.TemporaryDirectory() as workdir:
        extract_archive(args.period, workdir)
        check_contents(workdir)
        with get_connection() as conn, conn.cursor(DictCursor) as cur:
            for entity in entities:
                try:
                    rows = run(conn, cur, entity, args.period, workdir)
                    print(f"OK     {entity:<10} {args.period}  {rows} filas")
                except Exception as exc:
                    failed += 1
                    print(f"ERROR  {entity:<10} {args.period}  {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
