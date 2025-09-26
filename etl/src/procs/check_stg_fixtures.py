import os
import sys

# Add your DB connection script path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def call_check_stg_fixtures():
    """
    Connects to Postgres, calls the stored procedure `check_stg_fixtures()` to
    validate the stg_fixtures table. Prints out statuses for success/failure.
    """
def call_check_stg_fixtures():
    conn = get_db_connection()          # ← your helper
    cur  = conn.cursor()

    try:
        # make sure NOTICE messages are sent by the server
        cur.execute("SET client_min_messages TO NOTICE;")

        print("Calling procedure `check_stg_fixtures`...")
        cur.execute("CALL check_stg_fixtures();")
        conn.commit()

        # psycopg2 stores every NOTICE in conn.notices
        print("\n── PostgreSQL notices ──")
        for msg in conn.notices:
            print(msg.strip())          # strip trailing newline
        conn.notices.clear()            # optional: reset the buffer

        print("\nProcedure completed. Check stg_fixtures & data_load_errors.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling `check_stg_fixtures`: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    call_check_stg_fixtures()
