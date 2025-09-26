# create_stg_teams_table.py
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def create_stg_teams_table():
    sql = """
    CREATE TABLE IF NOT EXISTS stg_teams (
        team_id        INT,
        team_name      VARCHAR(255),
        team_country   VARCHAR(100),
        team_logo_url  TEXT,
        venue_id       INT,
        league_id      INT,
        season_year    INT,

        is_valid       BOOLEAN DEFAULT TRUE,
        error_reason   TEXT,
        load_timestamp TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_stg_teams_team_id
        ON stg_teams (team_id);

    CREATE INDEX IF NOT EXISTS idx_stg_teams_league_season
        ON stg_teams (league_id, season_year);
    """
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close(); conn.close()
    print("`stg_teams` table + indexes created.")

if __name__ == "__main__":
    create_stg_teams_table()
