import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def create_stg_player_season_stats_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_player_season_stats (
        stg_stats_id     BIGSERIAL PRIMARY KEY,
        player_id        INT,
        team_id          INT,
        league_id        INT,
        season           INT,
        position         VARCHAR(50),
        number           INT,
        appearances      INT,
        lineups          INT,
        minutes_played   INT,
        goals            INT,
        assists          INT,
        saves            INT,
        dribbles_attempts INT,
        dribbles_success INT,
        fouls_committed  INT,
        tackles          INT,
        passes_total     INT,
        passes_key       INT,
        pass_accuracy    NUMERIC(5,2),
        yellow_cards     INT,
        red_cards        INT,
        
        is_valid         BOOLEAN DEFAULT TRUE,
        error_reason     TEXT,
        load_timestamp   TIMESTAMP DEFAULT NOW()
    );

    -- Composite index for upserts/joins
    CREATE INDEX IF NOT EXISTS idx_stg_player_season_stats_comp
        ON stg_player_season_stats (player_id, team_id, league_id, season);

    -- Index on team or league if needed for partial lookups
    CREATE INDEX IF NOT EXISTS idx_stg_player_season_stats_team
        ON stg_player_season_stats (team_id);
    CREATE INDEX IF NOT EXISTS idx_stg_player_season_stats_league
        ON stg_player_season_stats (league_id);

    CREATE UNIQUE INDEX IF NOT EXISTS stg_pss_pk
        ON stg_player_season_stats
      (player_id, team_id, league_id, season, stg_stats_id DESC);
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("`stg_player_season_stats` table + indexes created.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `stg_player_season_stats` table: {e}")
        raise

if __name__ == "__main__":
    create_stg_player_season_stats_table()
