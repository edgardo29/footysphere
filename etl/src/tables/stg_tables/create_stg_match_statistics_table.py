import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection

def create_stg_match_statistics_table():
    """
    Staging table with same columns + is_valid, error_reason, load_timestamp.
    Possibly omit league_id, season_year if we plan to join with `fixtures` in the stored procedure.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_match_statistics (
        fixture_id          INT,
        team_id             INT,

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
        possession          INT,
        yellow_cards        INT,
        red_cards           INT,

        total_passes        INT,
        passes_accurate     INT,
        pass_accuracy       INT,
        expected_goals      NUMERIC(5,2),
        goals_prevented     NUMERIC(5,2),

        is_valid            BOOLEAN DEFAULT TRUE,
        error_reason        TEXT,
        load_timestamp      TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_stg_match_stats_fixture_team
        ON stg_match_statistics (fixture_id, team_id);
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("`stg_match_statistics` table + index created successfully.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `stg_match_statistics` table: {e}")
        raise

if __name__ == "__main__":
    create_stg_match_statistics_table()
