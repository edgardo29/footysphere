import os
import sys
from psycopg2 import sql

# Add the `test_scripts` and `config` directories to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection  # Reuse the modular connection logic

def create_teams_table():
    """
    Creates the `teams` table in the database if it doesn't already exist.
    """
    try:
        # Connect to the database
        conn = get_db_connection()
        cur = conn.cursor()

        # SQL query to create the `teams` table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            league_id INTEGER NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            logo_url TEXT
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        print("`teams` table created successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating the `teams` table: {e}")
        raise


if __name__ == "__main__":
    # Run the table creation script
    create_teams_table()
