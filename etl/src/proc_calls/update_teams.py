#!/usr/bin/env python3
"""
run_update_teams.py
────────────────────────────────────────────────────────────────────────────
Executes the update_teams() stored procedure (zero-param version), prints
all server NOTICEs, and shows the inserted / updated counts extracted from
the “[update_teams] …” NOTICE line.

Example console output
────────────────────────────────────────────────────────────────────────────
NOTICE:  [update_teams] 12 inserts, 4 updates

update_teams summary
--------------------
inserted_rows       : 12
updated_rows        : 4
"""

import os
import re
import sys
from typing import Optional, Tuple

# Helper that returns a psycopg connection
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../test_scripts")))
from get_db_conn import get_db_connection                         # noqa: E402


NOTICE_RE = re.compile(r'\[update_teams\]\s+(\d+)\s+inserts?,\s+(\d+)\s+updates?', re.I)


def call_update_teams() -> Tuple[Optional[int], Optional[int]]:
    """Run the procedure and return (inserted_rows, updated_rows)."""
    conn = get_db_connection()
    cur = conn.cursor()

    inserted = updated = None
    try:
        cur.execute("CALL update_teams();")
        conn.commit()

        # Print all server NOTICEs and parse the counts
        for note in conn.notices:
            msg = note.strip()
            print(msg)
            m = NOTICE_RE.search(msg)
            if m:
                inserted, updated = map(int, m.groups())
        conn.notices.clear()

    except Exception as err:
        conn.rollback()
        print(f"update_teams ERROR → {err}")

    finally:
        cur.close()
        conn.close()

    return inserted, updated


def main() -> None:
    inserted, updated = call_update_teams()

    print("\nupdate_teams summary")
    print("--------------------")
    print(f"{'inserted_rows':<18} : {inserted if inserted is not None else '—'}")
    print(f"{'updated_rows':<18} : {updated  if updated  is not None else '—'}")


if __name__ == "__main__":
    main()
