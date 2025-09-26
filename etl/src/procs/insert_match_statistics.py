#!/usr/bin/env python
"""
insert_match_statistics.py
──────────────────────────────────────────────────────────────────────────────
Executes the stored procedure `insert_match_statistics()` and prints:

• every RAISE NOTICE emitted by the procedure (chunk offsets, row counts)
• a final OK / error line that Airflow can grep for.
"""

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../test_scripts")))
from get_db_conn import get_db_connection


def call_insert_match_statistics() -> None:
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        # Let Postgres send NOTICES back to the client
        cur.execute("SET client_min_messages = 'NOTICE';")
        cur.execute("CALL insert_match_statistics();")
        conn.commit()

        # --- NEW: print every server-side NOTICE -------------------------
        if conn.notices:
            print("\n---- PostgreSQL NOTICE messages ----")
            for msg in conn.notices:
                print(msg.strip())          # strip trailing newline
            print("---- end of NOTICE list ----\n")
            # Clear list so a retry run doesn’t print them twice
            conn.notices.clear()

        print("Upsert stored procedure insert_match_statistics executed successfully.")
    except Exception as exc:
        conn.rollback()
        print(f"ERROR while running insert_match_statistics → {exc}")
        raise                                           # let Airflow mark task failed
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    call_insert_match_statistics()
