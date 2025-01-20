import os
import sys
from psycopg2 import sql

# Add `config` and `test_scripts` directories to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection  # Reuse modular connection logic


def create_match_stats_table():
    """
    Creates the `match_stats` table in the database if it doesn't already exist.
    """
    try:
        # Connect to the database
        conn = get_db_connection()
        cur = conn.cursor()

        # SQL query to create the `match_stats` table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS match_stats (
            stat_id BIGSERIAL PRIMARY KEY,
            match_id BIGINT NOT NULL REFERENCES fixtures(fixture_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
            ball_possession DECIMAL(5, 2),
            yellow_cards INTEGER,
            red_cards INTEGER,
            fouls INTEGER,
            total_shots INTEGER,
            UNIQUE (match_id, team_id) -- Avoid duplicate stats for the same match and team
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        print("`match_stats` table created successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating the `match_stats` table: {e}")
        raise


if __name__ == "__main__":
    # Run the table creation script
    create_match_stats_table()
