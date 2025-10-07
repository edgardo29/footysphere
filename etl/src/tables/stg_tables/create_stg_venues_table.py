#!/usr/bin/env python
"""
create_stg_venues_table.py
────────────────────────────────────────────────────────────────────────────
Run once to create the scratch-pad table `stg_venues` and its indexes.

• The table is TRUNCATEd at the start (or end) of every loader run.
• Only staging/QA logic writes here; production data lives in `venues`.

"""

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../test_scripts")))
from get_db_conn import get_db_connection


def create_stg_venues_table() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS stg_venues (
        venue_id        INT PRIMARY KEY,
        venue_name      VARCHAR(255),
        city            VARCHAR(120),
        country         VARCHAR(120),
        capacity        INT,
        is_valid        BOOLEAN      DEFAULT TRUE,
        error_reason    TEXT,
        load_timestamp  TIMESTAMP    DEFAULT NOW()
    );

    /* helpful index when you QA by country or city */
    CREATE INDEX IF NOT EXISTS idx_stg_venues_country
        ON stg_venues (country);
    """

    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close(); conn.close()
    print("`stg_venues` table and indexes created.")


if __name__ == "__main__":
    create_stg_venues_table()
