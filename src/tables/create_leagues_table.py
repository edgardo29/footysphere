import os
import sys

# Add the `test_scripts` directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def create_leagues_table():
    """
    Creates the `leagues` table in the database if it doesn't already exist.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        logo_url TEXT,
        country VARCHAR(100)
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Execute the table creation query
        cur.execute(create_table_query)
        conn.commit()

        print("Table `leagues` created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print("Error creating the `leagues` table:", e)

if __name__ == "__main__":
    print("Creating the `leagues` table...")
    create_leagues_table()
