#!/usr/bin/env python
"""Call check_stg_venues() and print its summary block."""

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../test_scripts")))
from get_db_conn import get_db_connection

COL_ORDER = [
    "total_rows", "valid_rows", "invalid_rows",
    "null_id_name", "dup_ids", "bad_capacity",
]

def main():
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM check_stg_venues();")
    row  = cur.fetchone()
    cols = [c.name for c in cur.description]
    summary = dict(zip(cols, row))

    print("\ncheck_stg_venues summary")
    print("------------------------")
    for k in COL_ORDER:
        print(f"{k:<18} : {summary[k]}")
    conn.commit()
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
