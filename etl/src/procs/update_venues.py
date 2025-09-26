#!/usr/bin/env python
"""
run_merge_venues.py
────────────────────────────────────────────────────────────────────────────
Calls the stored procedure  sp_merge_venues() , commits the transaction,
and prints any NOTICE lines the server produced.

Place this file in:
    footysphere/src/procs/run_merge_venues.py

Typical usage in your DAG or shell sequence:
    python src/procs/run_merge_venues.py
"""

import os
import sys
from datetime import datetime

# helper path for get_db_conn
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../test_scripts")
    )
)

from get_db_conn import get_db_connection


def main() -> None:
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # call the merge proc
        cur.execute("CALL update_venues();")
        conn.commit()

        # print any server-side NOTICE messages
        if conn.notices:
            print("\n".join(n.strip() for n in conn.notices))
        else:
            print("update_venues executed — no NOTICE messages returned.")

    except Exception as err:
        conn.rollback()
        print(f"[{datetime.utcnow()}] update_venues ERROR → {err}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
