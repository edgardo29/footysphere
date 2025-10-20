#!/usr/bin/env python
"""
Creates `league_directory`
• Raw nightly dump of every league the API exposes
• Minimal set of columns that remain useful long-term
"""

import os
import sys

# Add test_scripts dir to import get_db_connection
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts"))
)
from get_db_conn import get_db_connection


def create_league_directory_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS league_directory (
        league_id   INT  PRIMARY KEY,
        league_name TEXT NOT NULL,
        country     TEXT,
        logo_url    TEXT,
        seasons     JSONB,                     -- e.g. [1996,1997,…,2024]
        load_date   TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date    TIMESTAMP(0) NOT NULL DEFAULT now()
    );

    -- Fast case-insensitive search by name
    CREATE INDEX IF NOT EXISTS idx_league_directory_name
        ON league_directory (lower(league_name));
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        print("`league_directory` table created.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `league_directory`: {e}")
        raise


if __name__ == "__main__":
    create_league_directory_table()
