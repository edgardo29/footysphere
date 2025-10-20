import os
import sys

# Add the `test_scripts` directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection


def create_player_stats_load_state_table():
    """
    Creates `player_stats_load_state`, which tracks ETL status for each league/season.

    Columns
    -------
    league_id          INT  PK, FK → leagues.league_id
    season_year        INT  PK     (e.g. 2024 for the 2024-25 season)
    is_active_load     BOOLEAN     -- flip to false to pause ETL for this target
    last_load_ts       TIMESTAMP   -- NULL until the first successful load
    last_mode          TEXT        -- 'initial' or 'incremental' (optional audit)

    The composite PK (league_id, season_year) guarantees one row per target season.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS player_stats_load_state (
        league_id      INT     NOT NULL,
        season_year    INT     NOT NULL,
        is_active_load BOOLEAN NOT NULL DEFAULT TRUE,
        last_load_ts   TIMESTAMP,
        last_mode      TEXT,

        PRIMARY KEY (league_id, season_year),
        CONSTRAINT fk_league
            FOREIGN KEY (league_id)
            REFERENCES leagues (league_id)
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(create_table_query)
        conn.commit()

        print("Table `player_stats_load_state` created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print("Error creating the `player_stats_load_state` table:", e)


if __name__ == "__main__":
    print("Creating the `player_stats_load_state` table...")
    create_player_stats_load_state_table()
