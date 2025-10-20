import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

def create_stg_standings_table():
    """
    Creates `stg_standings` for raw data from the API, with is_valid, error_reason, load_timestamp.
    Includes indexes on (league_id, season_year, team_id) plus a quick index on (league_id, season_year).
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_standings (
        league_id       INT,
        season_year     INT,
        team_id         INT,

        rank            INT,
        points          INT,
        played          INT,
        win             INT,
        draw            INT,
        lose            INT,
        goals_for       INT,
        goals_against   INT,
        goals_diff      INT,
        group_label     TEXT NOT NULL DEFAULT '', 

        is_valid        BOOLEAN DEFAULT TRUE,
        error_reason    TEXT,
        load_timestamp  TIMESTAMP DEFAULT NOW()
    );

    -- Index for merges/upserts
    CREATE INDEX IF NOT EXISTS idx_stg_standings_league_season_team
        ON stg_standings (league_id, season_year, team_id);

    -- Possibly also index on (league_id, season_year) if you want partial updates
    CREATE INDEX IF NOT EXISTS idx_stg_standings_league_season
        ON stg_standings (league_id, season_year);
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()

        print("`stg_standings` table + indexes created successfully.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `stg_standings` table: {e}")
        raise

if __name__ == "__main__":
    create_stg_standings_table()
