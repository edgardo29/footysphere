import os
import sys
import psycopg2

# Add your test_scripts or config directories
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def call_insert_standings():
    """
    Calls the stored procedure insert_standings() in Postgres.
    Captures RAISE NOTICE messages, prints them for logging.
    """

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Let the server send NOTICE messages back
        cur.execute("SET client_min_messages = 'NOTICE';")

        print("Calling procedure `insert_standings`...")
        cur.execute("CALL insert_standings();")
        conn.commit()

        print("Procedure `insert_standings` executed successfully.")

        # Fetch any notice messages from connection.notices
        notices = conn.notices
        if notices:
            for notice in notices:
                print("DB NOTICE:", notice.strip())

        print("Chunked upsert from stg_standings into `standings` is complete.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling `insert_standings`: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_insert_standings()
