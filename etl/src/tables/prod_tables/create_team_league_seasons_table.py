# create_team_league_seasons_table.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def create_team_league_seasons_table():
    """
    Many-to-many link: which team is in which league+season.
    Columns:
      team_id, league_id, season_year
      PRIMARY KEY (team_id, league_id, season_year)
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS team_league_seasons (
        team_id INT NOT NULL,
        league_id INT NOT NULL,
        season_year INT NOT NULL,
        load_date    TIMESTAMP(0)  default now(),
        upd_date     TIMESTAMP(0)  default now()  

        PRIMARY KEY (team_id, league_id, season_year),

        FOREIGN KEY (team_id) REFERENCES teams(team_id),
        FOREIGN KEY (league_id) REFERENCES leagues(league_id)
        -- Optionally: FOREIGN KEY (league_id, season_year)
        --   REFERENCES league_seasons (league_id, season_year)
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("`team_league_seasons` table created successfully.")
    except Exception as e:
        print(f"Error creating `team_league_seasons` table: {e}")
        raise

if __name__ == "__main__":
    create_team_league_seasons_table()
