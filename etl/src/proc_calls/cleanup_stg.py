"""
cleanup_stg.py
──────────────
Truncate a *single* staging table just before you reload it.

Examples
========
$ python cleanup_stg.py --table stg_fixtures              # default schema = public
$ python cleanup_stg.py --table stg_standings --schema staging

You can call the same script from Airflow’s BashOperator *or*
import `cleanup_table()` into a PythonOperator – see bottom notes.
"""

# ────────────────────────────────────────────────────────────────────
# 1. Imports & config
# ────────────────────────────────────────────────────────────────────
import argparse
import logging
import os
import sys
import time

# ▸  Your existing DB helper lives in   test_scripts/get_db_conn.py
#    Keep the exact import style you had in the old script so we
#    don’t duplicate connection logic / credentials handling.
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../test_scripts")
    )
)
from get_db_conn import get_db_connection        # ← opens & returns psycopg2.conn

# Configure root logger once (Airflow will inherit it)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ────────────────────────────────────────────────────────────────────
# 2. CLI helper
# ────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    """
    --table  <required>  Which staging table to truncate.
    --schema <optional>  Defaults to 'public'.
    """
    p = argparse.ArgumentParser(description="Truncate one staging table.")
    p.add_argument(
        "--table",
        required=True,
        help="table to TRUNCATE, e.g. stg_fixtures",
    )
    p.add_argument(
        "--schema",
        default="public",
        help="schema name (default: public)",
    )
    return p.parse_args()


# ────────────────────────────────────────────────────────────────────
# 3. Core logic – callable so a PythonOperator can import & reuse it
# ────────────────────────────────────────────────────────────────────
def cleanup_table(schema: str, table: str) -> None:
    """
    Connect → TRUNCATE \"schema\".\"table\" CASCADE → COMMIT.
    Raises any psycopg2 errors to caller (Airflow will mark task failed).
    """
    fqtn = f'"{schema}"."{table}"'  # fully-qualified table name, quoted

    started = time.time()
    logging.info("Connecting to DB …")
    conn = get_db_connection()  # ← comes from your helper
    try:
        with conn, conn.cursor() as cur:  # `with conn` -> commit / rollback mgmt
            logging.info("Running TRUNCATE %s CASCADE;", fqtn)
            cur.execute(f"TRUNCATE TABLE {fqtn} CASCADE;")


        logging.info("Success – %s truncated in %.2f s", fqtn, time.time() - started)
    finally:
        conn.close()


# ────────────────────────────────────────────────────────────────────
# 4. Entrypoint for shell usage  (Airflow BashOperator hits this path)
# ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = _parse_args()
    try:
        cleanup_table(args.schema, args.table)
    except Exception:
        # full traceback in logs, exit 1 so Airflow / shell sees a failure
        logging.exception("Failed while truncating %s.%s", args.schema, args.table)
        sys.exit(1)
