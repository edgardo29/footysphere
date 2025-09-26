import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def call_insert_player_season_stats():
    """
    Calls insert_player_season_stats() in Postgres,
    capturing RAISE NOTICE lines and printing them.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET client_min_messages = 'NOTICE';")
        print("Calling procedure insert_player_season_stats()...")

        cur.execute("CALL insert_player_season_stats();")
        conn.commit()

        for notice in conn.notices:
            print("DB NOTICE:", notice.strip())

        print("Upsert to player_season_stats complete.")
    except Exception as e:
        conn.rollback()
        print(f"Error calling insert_player_season_stats: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_insert_player_season_stats()