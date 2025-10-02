#!/usr/bin/env python3
"""
Invokes the Postgres procedure insert_match_events()
and streams any server‑side NOTICE messages.
"""
import os, sys, psycopg2
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection


def main():
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SET client_min_messages = 'NOTICE';")
        print("Calling update_match_events() …")
        cur.execute("CALL update_match_events();")
        conn.commit()

        # Display all RAISE NOTICE lines
        for notice in conn.notices:
            print("DB:", notice.strip())

        print("Upsert to match_events finished.\n")
    except Exception as exc:
        conn.rollback()
        print("ERROR during update_match_events():", exc)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
