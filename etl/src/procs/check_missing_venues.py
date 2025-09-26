import os
import sys

# Add your get_db_conn import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def call_insert_missing_venues():
    """
    Calls the stored procedure `insert_missing_venues()`,
    printing a success/failure message.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        print("Calling procedure `insert_missing_venues`...")
        # Execute the procedure.
        cur.execute("CALL insert_missing_venues();")
        conn.commit()

        print("Procedure `insert_missing_venues` executed successfully.")
        print("Any RAISE NOTICE messages appear in the Postgres server logs by default.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling `insert_missing_venues`: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_insert_missing_venues()
