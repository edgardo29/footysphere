#!/usr/bin/env python3
import os
import sys
import re

# Add your DB connection script path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

PROC_NAME = "update_news"  # stored PROCEDURE name

def run_update_news():
    """
    Calls stored procedure update_news(), captures RAISE NOTICE messages,
    and prints a short summary of inserts/updates.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Ensure we get NOTICE-level messages
        cur.execute("SET client_min_messages = 'NOTICE';")

        # Clear any leftover notices
        conn.notices.clear()

        print(f"Calling procedure `{PROC_NAME}`...")
        cur.execute(f"CALL {PROC_NAME}();")
        conn.commit()
        print(f"Procedure `{PROC_NAME}` executed successfully.")

        inserts = None
        updates = None

        # Print DB NOTICEs and extract counts if present
        if conn.notices:
            for notice in conn.notices:
                msg = notice.strip()
                print("NOTICE:", msg)
                m = re.search(r'update_news:\s*inserted=(\d+),\s*updated=(\d+)', msg, re.IGNORECASE)
                if m:
                    inserts = int(m.group(1))
                    updates = int(m.group(2))
        else:
            print("No NOTICE messages from the DB.")

        if inserts is not None and updates is not None:
            print(f"Summary — inserts: {inserts}, updates: {updates}")
        else:
            print("Summary — counts not found in NOTICE output.")

    except Exception as e:
        conn.rollback()
        print(f"Error calling `{PROC_NAME}`: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_update_news()
