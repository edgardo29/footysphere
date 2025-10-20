#!/usr/bin/env python3
import os
import sys

# Add your get_db_conn path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection  # expects a psycopg2-style connection

DDL_STATEMENTS = [
    # 1) Table
    """
    CREATE TABLE IF NOT EXISTS standings (
        -- Natural key
        league_id       INT NOT NULL,
        season_year     INT NOT NULL,
        team_id         INT NOT NULL,
        group_label     TEXT NOT NULL DEFAULT '',

        -- Authoritative numbers (no NULLs)
        rank            INT NOT NULL,
        points          INT NOT NULL,
        played          INT NOT NULL,
        win             INT NOT NULL,
        draw            INT NOT NULL,
        lose            INT NOT NULL,
        goals_for       INT NOT NULL,
        goals_against   INT NOT NULL,
        goals_diff      INT NOT NULL,

        -- Persisted recent form (future‑proofed)
        form_window     SMALLINT NOT NULL DEFAULT 5,  -- how many matches form_compact summarizes
        form_compact    TEXT     NOT NULL DEFAULT '', -- e.g., 'WWDWL'; only W/D/L allowed

        -- Audit
        load_date       TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date        TIMESTAMP(0) NOT NULL DEFAULT now(),

        -- Constraints
        PRIMARY KEY (league_id, season_year, team_id, group_label),
        CONSTRAINT fk_standings_league FOREIGN KEY (league_id) REFERENCES leagues(league_id),
        CONSTRAINT fk_standings_team   FOREIGN KEY (team_id)   REFERENCES teams(team_id),
        CONSTRAINT ck_form_chars   CHECK (form_compact ~ '^[WDL]*$'),
        CONSTRAINT ck_form_window  CHECK (form_window BETWEEN 1 AND 20)
    );
    """,

    # 2) Indexes for common reads
    """
    CREATE INDEX IF NOT EXISTS ix_standings_rank
        ON standings (league_id, season_year, group_label, rank);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_standings_team_lookup
        ON standings (league_id, season_year, group_label, team_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_standings_league_season
        ON standings (league_id, season_year);
    """,

    # 3) Trigger to keep upd_date fresh on UPDATE
    """
    CREATE OR REPLACE FUNCTION set_timestamp() RETURNS trigger AS $$
    BEGIN
      NEW.upd_date := now();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """,
    "DROP TRIGGER IF EXISTS trg_standings_set_upd_date ON standings;",
    """
    CREATE TRIGGER trg_standings_set_upd_date
    BEFORE UPDATE ON standings
    FOR EACH ROW EXECUTE FUNCTION set_timestamp();
    """,
]

def create_standings_table():
    """
    Recreates the `standings` table with strict integrity and performance indexes.
    """
    conn = None
    try:
        print("[standings] Connecting to DB…")
        conn = get_db_connection()
        conn.autocommit = False
        cur = conn.cursor()

        for i, stmt in enumerate(DDL_STATEMENTS, start=1):
            print(f"[standings] Executing DDL {i}/{len(DDL_STATEMENTS)}…")
            cur.execute(stmt)

        conn.commit()
        cur.close()
        print("[standings] Table, indexes, and trigger created successfully.")
        print("[standings] Reminder: update your ETL upsert to populate form_compact (e.g., 'WWDLW').")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[standings] ERROR during creation: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_standings_table()
