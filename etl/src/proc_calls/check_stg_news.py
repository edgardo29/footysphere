#!/usr/bin/env python3
import os
import sys

# Add your DB connection script path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

PROC_NAME = "check_stg_news"  # stored PROCEDURE name

def check_stg_news():
    """
    Calls stored procedure check_stg_news(),
    captures RAISE NOTICE messages from conn.notices, and prints them.
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

        # Print DB NOTICEs
        if conn.notices:
            for notice in conn.notices:
                print("NOTICE:", notice.strip())
        else:
            print("No NOTICE messages from the DB.")

        print("Validation of stg_news complete. Check `data_load_errors` for any issues.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling `{PROC_NAME}`: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    check_stg_news()
