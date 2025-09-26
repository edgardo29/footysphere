import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def call_check_player_season_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET client_min_messages = 'NOTICE';")
        print("Calling procedure check_player_season_stats()...")

        cur.execute("CALL check_player_season_stats();")
        conn.commit()

        # Print any notice messages
        for notice in conn.notices:
            print("DB NOTICE:", notice.strip())

        print("Validation complete. Check check_player_season_stats.is_valid and data_load_errors for details.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling check_player_season_stats: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_check_player_season_stats()
