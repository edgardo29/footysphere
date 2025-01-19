import os
import sys
from psycopg2 import sql

# Add `config` and `test_scripts` directories to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection  # Reuse modular connection logic


def create_fixtures_table():
    """
    Creates the `fixtures` table in the database if it doesn't already exist.
    """
    try:
        # Connect to the database
        conn = get_db_connection()
        cur = conn.cursor()

        # SQL query to create the `fixtures` table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS fixtures (
            fixture_id BIGINT PRIMARY KEY,
            league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
            season INTEGER NOT NULL,
            home_team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
            away_team_id INTEGER NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            time TIME NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        print("`fixtures` table created successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating the `fixtures` table: {e}")
        raise


if __name__ == "__main__":
    # Run the table creation script
    create_fixtures_table()
