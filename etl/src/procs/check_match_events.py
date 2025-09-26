#!/usr/bin/env python3
"""
validate_match_events.py
────────────────────────
Run the PL/pgSQL procedure that flags invalid rows in stg_match_events
and writes them to data_load_errors.
"""
import os, sys, psycopg2
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def main():
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        # Ensure NOTICE messages from the procedure are displayed
        cur.execute("SET client_min_messages = 'NOTICE';")

        print("Calling check_stg_match_events() …")
        cur.execute("CALL check_stg_match_events();")
        conn.commit()

        for n in conn.notices:
            print("DB:", n.strip())

        print("Validation finished.")
    except Exception as exc:
        conn.rollback()
        print("Validation error:", exc)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
