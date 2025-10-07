import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def create_player_season_stats_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS player_season_stats (
        player_id      INT NOT NULL,
        team_id        INT NOT NULL,
        league_id      INT NOT NULL,
        season         INT NOT NULL,
        position       VARCHAR(50),
        number         INT,
        appearances    INT,
        lineups        INT,
        minutes_played INT,
        goals          INT,
        assists        INT,
        saves          INT,
        dribbles_attempts INT,
        dribbles_success INT,
        fouls_committed INT,
        tackles        INT,
        passes_total   INT,
        passes_key     INT,
        pass_accuracy  NUMERIC(5,2),
        yellow_cards   INT,
        red_cards      INT,
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()

        PRIMARY KEY (player_id, team_id, league_id, season)
    );

    -- Additional indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_player_season_stats_team
        ON player_season_stats (team_id, season);
    CREATE INDEX IF NOT EXISTS idx_player_season_stats_league
        ON player_season_stats (league_id, season);
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("`player_season_stats` table + indexes created.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `player_season_stats` table: {e}")
        raise

if __name__ == "__main__":
    create_player_season_stats_table()
