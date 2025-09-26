import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def create_stg_players_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_players (
        stg_player_id  BIGSERIAL PRIMARY KEY,
        player_id      INT,
        firstname      VARCHAR(100),
        lastname       VARCHAR(100),
        full_name      VARCHAR(200),
        birth_date     DATE,
        birth_country  VARCHAR(100),
        nationality    VARCHAR(100),
        height         VARCHAR(10),
        weight         VARCHAR(10),
        photo_url      TEXT,
        is_valid       BOOLEAN DEFAULT TRUE,
        error_reason   TEXT,
        load_timestamp TIMESTAMP DEFAULT NOW()
    );

    -- For efficient domain lookups when upserting
    CREATE INDEX IF NOT EXISTS idx_stg_players_player_id
        ON stg_players (player_id);

    -- Composite index if name-based checks needed
    CREATE INDEX IF NOT EXISTS idx_stg_players_name
        ON stg_players (lastname, firstname);
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("`stg_players` table + indexes created.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `stg_players` table: {e}")
        raise

if __name__ == "__main__":
    create_stg_players_table()
