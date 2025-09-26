import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def call_check_players():
    """
    Calls the stored procedure check_players(), prints any NOTICE logs,
    and commits or rolls back on error.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Ensures we capture NOTICE lines
        cur.execute("SET client_min_messages = 'NOTICE';")
        print("Calling procedure check_players()...")

        cur.execute("CALL check_players();")
        conn.commit()

        for notice in conn.notices:
            print("NOTICE from DB:", notice.strip())

        print("Validation done. Check stg_players.is_valid and data_load_errors table for details.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling check_players: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_check_players()
