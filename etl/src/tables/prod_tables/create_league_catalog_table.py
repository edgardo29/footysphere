#!/usr/bin/env python
"""
Creates `league_catalog`
• Curated control list that the DAG reads
"""

import os
import sys

# Add test_scripts dir to import get_db_connection
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts"))
)
from get_db_conn import get_db_connection


def create_league_catalog_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.league_catalog (
        league_id     INT  PRIMARY KEY
                    REFERENCES public.league_directory (league_id),
        folder_alias  TEXT UNIQUE NOT NULL,      -- safe for blob paths
        league_name   TEXT NOT NULL,
        first_season  INT  NOT NULL,
        last_season   INT,
        is_enabled    BOOLEAN NOT NULL DEFAULT TRUE,
        players_target_windows TEXT[] NOT NULL DEFAULT '{}'::text[],
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now(),
        CONSTRAINT chk_players_target_windows_allowed
        CHECK (players_target_windows <@ ARRAY['summer','winter']::text[])
    );

    -- Quick filter for active leagues
    CREATE INDEX IF NOT EXISTS idx_league_catalog_enabled
        ON league_catalog (is_enabled);
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        print("`league_catalog` table created.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `league_catalog`: {e}")
        raise


if __name__ == "__main__":
    create_league_catalog_table()
