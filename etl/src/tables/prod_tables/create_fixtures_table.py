import os
import sys

# Add your test_scripts dir for get_db_conn
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

def create_fixtures_table():
    """
    Creates the final `fixtures` table with:
    - PRIMARY KEY (fixture_id) => implicit index
    - Additional index on (league_id, fixture_date)
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS fixtures (
        fixture_id    INT PRIMARY KEY,
        league_id     INT NOT NULL,
        season_year   INT NOT NULL,
        home_team_id  INT NOT NULL,
        away_team_id  INT NOT NULL,
        fixture_date  TIMESTAMPTZ,
        status        VARCHAR(20),
        round         VARCHAR(50),
        home_score    INT,
        away_score    INT,
        venue_id      INT,
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()

        FOREIGN KEY (league_id)     REFERENCES leagues(league_id),
        FOREIGN KEY (home_team_id)  REFERENCES teams(team_id),
        FOREIGN KEY (away_team_id)  REFERENCES teams(team_id),
        FOREIGN KEY (venue_id)      REFERENCES venues(venue_id)
    );

    -- Additional index on (league_id, fixture_date)
    CREATE INDEX IF NOT EXISTS idx_fixtures_league_date
        ON fixtures (league_id, fixture_date);
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()

        print("`fixtures` table + index on (league_id, fixture_date) created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `fixtures` table: {e}")
        raise

if __name__ == "__main__":
    create_fixtures_table()
