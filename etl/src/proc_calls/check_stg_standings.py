import os
import sys

# Add your DB connection script path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def check_stg_standings():
    """
    Calls stored procedure check_stg_standings(), captures RAISE NOTICE messages from conn.notices,
    and prints them to the console for debugging/logging.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Ensure we get NOTICE-level messages
        cur.execute("SET client_min_messages = 'NOTICE';")

        # Clear any leftover notices from a previous session
        conn.notices.clear()

        print("Calling procedure `check_stg_standings`...")
        cur.execute("CALL check_stg_standings();")
        conn.commit()

        print("Procedure `check_stg_standings` executed successfully.")
        
        # Now let's print the notices from the connection object
        if conn.notices:
            for notice in conn.notices:
                # Each notice often ends with a newline
                print("NOTICE from DB:", notice.strip())
        else:
            print("No NOTICE messages from the DB.")

        print("Validation of stg_standings done. Check `data_load_errors` if needed.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling `check_stg_standings`: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    check_stg_standings()
