#!/usr/bin/env python
"""
debug_db_session.py
────────────────────────────────────────────────────────────────────────────
Quick one-shot diagnostic:

• prints the role, database, server IP, and search_path the Python session
  actually uses
• shows which schema(s) contain an object named update_teams
"""

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../test_scripts")))
from get_db_conn import get_db_connection


def main() -> None:
    conn = get_db_connection()
    cur  = conn.cursor()

    # ── Session identity & search_path
    cur.execute("""
        SELECT current_user            AS role,
               current_database()      AS database,
               inet_server_addr()      AS server_ip,
               current_setting('search_path') AS search_path
    """)
    print("\nSESSION INFO\n------------")
    role, db, ip, sp = cur.fetchone()
    print(f"role         : {role}")
    print(f"database     : {db}")
    print(f"server_ip    : {ip}")
    print(f"search_path  : {sp}")

    # ── Where does update_teams live (if anywhere)?
    cur.execute("""
        SELECT n.nspname
        FROM   pg_proc p
        JOIN   pg_namespace n ON n.oid = p.pronamespace
        WHERE  p.proname = 'update_teams';
    """)
    schemas = [row[0] for row in cur.fetchall()]
    print("\nupdate_teams found in schema(s):", schemas or "NOT FOUND")

    cur.execute("CALL update_teams();")
    print("CALL ok →", cur.fetchone())

    cur.close(); conn.close()


if __name__ == "__main__":
    main()
