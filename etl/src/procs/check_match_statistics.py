import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def call_validate_fixture_stats():
    """
    Calls check_match_statistics(), minimal logging. 
    If you want to see RAISE NOTICE lines, we can do 'SET client_min_messages = NOTICE;'
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("CALL check_match_statistics();")
        conn.commit()
        print("Validation stored procedure `check_match_statistics` executed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error validating fixture stats: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_validate_fixture_stats()
