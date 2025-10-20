import os
import sys

# Add the `test_scripts` dir for get_db_conn
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

def create_stg_fixtures_table():
    """
    Creates `stg_fixtures` with indexes on (fixture_id) and (league_id, fixture_date).
    This table stores raw fixture data with minimal constraints.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_fixtures (
        fixture_id      INT,
        league_id       INT,
        season_year     INT,
        home_team_id    INT,
        away_team_id    INT,
        fixture_date    TIMESTAMPTZ,
        status          VARCHAR(20),
        round           VARCHAR(50),
        home_score      INT,
        away_score      INT,
        venue_id        INT,

        is_valid        BOOLEAN DEFAULT TRUE,
        error_reason    TEXT,
        load_timestamp  TIMESTAMP DEFAULT NOW()
    );

    -- Index on fixture_id (for upserts/merges)
    CREATE INDEX IF NOT EXISTS idx_stg_fixtures_fixture_id
        ON stg_fixtures (fixture_id);

    -- Additional index on (league_id, fixture_date) for potential queries
    CREATE INDEX IF NOT EXISTS idx_stg_fixtures_league_date
        ON stg_fixtures (league_id, fixture_date);
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()

        print("`stg_fixtures` table + indexes created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `stg_fixtures` table: {e}")
        raise

if __name__ == "__main__":
    create_stg_fixtures_table()
