import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def create_players_table():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS players (
        player_id      INT PRIMARY KEY,
        firstname      VARCHAR(100),
        lastname       VARCHAR(100),
        full_name      VARCHAR(200),
        birth_date     DATE,
        birth_country  VARCHAR(100),
        nationality    VARCHAR(100),
        height         VARCHAR(10),
        weight         VARCHAR(10),
        photo_url      TEXT
    );

    -- Composite index on lastname and firstname (if queries typically use both)
    CREATE INDEX IF NOT EXISTS idx_players_name ON players(lastname, firstname);
    -- Alternatively, if most queries use the full name, index that column:
    CREATE INDEX IF NOT EXISTS idx_players_full_name ON players(full_name);
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("Players table and indexes created successfully.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating players table: {e}")
        raise

if __name__ == "__main__":
    create_players_table()
