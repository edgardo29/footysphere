import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def call_check_stg_match_statistic():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1) make sure NOTICEs are sent to the client
        cur.execute("SET client_min_messages = NOTICE;")

        # 2) clear any old notices
        if hasattr(conn, "notices"):
            conn.notices.clear()

        # 3) call the proc
        cur.execute("CALL check_stg_match_statistics();")
        conn.commit()

        # 4) print the NOTICE lines from the proc
        if getattr(conn, "notices", None):
            for n in conn.notices:
                print(n.strip())
        else:
            print("(No NOTICE messages returned)")

    except Exception as e:
        conn.rollback()
        print(f"Error validating fixture stats: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    call_check_stg_match_statistic()
