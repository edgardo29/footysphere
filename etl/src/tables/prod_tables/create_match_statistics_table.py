import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection

def create_match_statistics_table():
    """
    Creates the final `match_statistics` table with:
      fixture_id, team_id, league_id, season_year
      plus columns for each stat:
        shots_on_goal, shots_off_goal, shots_inside_box, shots_outside_box,
        blocked_shots, goalkeeper_saves, total_shots,
        corners, offsides, fouls, possession, yellow_cards, red_cards,
        total_passes, passes_accurate, pass_accuracy, expected_goals, goals_prevented, etc.

    The PK is (fixture_id, team_id).
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS match_statistics (
        fixture_id          INT NOT NULL,
        team_id             INT NOT NULL,
        league_id           INT NOT NULL,
        season_year         INT NOT NULL,

        shots_on_goal       INT,
        shots_off_goal      INT,
        shots_inside_box    INT,
        shots_outside_box   INT,
        blocked_shots       INT,
        goalkeeper_saves    INT,
        total_shots         INT,

        corners             INT,
        offsides            INT,
        fouls               INT,
        possession          INT,   -- store 55 for "55%"
        yellow_cards        INT,
        red_cards           INT,

        total_passes        INT,
        passes_accurate     INT,
        pass_accuracy       INT,   -- store 82 for "82%"
        expected_goals      NUMERIC(5,2),  -- e.g. "2.43"
        goals_prevented     NUMERIC(5,2),  -- or int, if you prefer
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()

        PRIMARY KEY (fixture_id, team_id),

        FOREIGN KEY (fixture_id) REFERENCES fixtures(fixture_id),
        FOREIGN KEY (team_id)    REFERENCES teams(team_id)
    );

    CREATE INDEX IF NOT EXISTS idx_match_stats_fixture_season
        ON match_statistics (fixture_id, league_id, season_year);
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("`match_statistics` table + index created successfully.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `match_statistics` table: {e}")
        raise

if __name__ == "__main__":
    create_match_statistics_table()
