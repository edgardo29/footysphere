#!/usr/bin/env python3
"""
etl/update_fixtures.py
──────────────────────
Call update_fixtures() and dump its PostgreSQL NOTICE messages.

• Works on psycopg2 < 2.8 (no set_notice_receiver)
• Exit‑code 0 = success, 1 = failure
"""

import sys
import os
import collections
import psycopg2
from psycopg2.extensions import connection

# Adjust to your repo layout
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../test_scripts")))
from get_db_conn import get_db_connection   # must return a psycopg2 connection


def main() -> None:
    conn: connection = get_db_connection()

    # Bigger ring‑buffer so we don’t lose notices (default is 50)
    conn.notices = collections.deque(maxlen=500)

    try:
        with conn, conn.cursor() as cur:     # BEGIN / COMMIT automatically
            cur.execute("SET client_min_messages = NOTICE;")
            cur.execute("CALL update_fixtures();")

        # Dump all NOTICE messages after commit
        for msg in conn.notices:
            # Each notice already ends with '\n'
            sys.stdout.write(msg.strip() + "\n")

    except Exception as exc:
        # Transaction rolled back by the context manager
        sys.stderr.write(f"update_fixtures() failed: {exc}\n")
        sys.exit(1)

    finally:
        conn.close()

    sys.exit(0)


if __name__ == "__main__":
    main()
