#!/usr/bin/env python3
"""
update_match_statistics.py
──────────────────────────────────────────────────────────────────────────────
Calls the stored procedure `update_match_statistics()` and prints any
RAISE NOTICE messages it emits (chunk offsets, row counts, final summary).
"""

import os
import sys

# repo-local helper to get a psycopg2 connection
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection  # noqa: E402


def call_update_match_statistics() -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # ensure NOTICE messages are returned to the client
        cur.execute("SET client_min_messages = NOTICE;")

        # clear any stale notices from this connection
        if hasattr(conn, "notices"):
            conn.notices.clear()

        # run the upsert
        cur.execute("CALL update_match_statistics();")
        conn.commit()

        # print any NOTICE lines from the procedure
        if getattr(conn, "notices", None):
            print("\n---- PostgreSQL NOTICE messages ----")
            for msg in conn.notices:
                print(msg.strip())
            print("---- end of NOTICE list ----\n")
            conn.notices.clear()

        print("Stored procedure `update_match_statistics` executed successfully.")
    except Exception as exc:
        conn.rollback()
        print(f"ERROR while running update_match_statistics → {exc}")
        raise  # let Airflow mark the task as failed
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    call_update_match_statistics()
