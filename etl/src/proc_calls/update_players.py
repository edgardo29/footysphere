import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def call_update_players():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # capture NOTICE lines
        cur.execute("SET client_min_messages = 'NOTICE';")
        print("Calling procedure update_players()...")

        cur.execute("CALL update_players();")
        conn.commit()

        for notice in conn.notices:
            print("DB NOTICE:", notice.strip())
    except Exception as e:
        conn.rollback()
        print(f"Error calling update_players: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_update_players()
