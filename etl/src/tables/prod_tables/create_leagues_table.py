import os
import sys

# Add the `test_scripts` directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def create_leagues_table():
    """
    Creates the `leagues` table in the database if it doesn't already exist,
    matching the schema from your ERD:
    
    Leagues {
        league_id int pk
        league_name varchar
        league_logo_url text
        league_country varchar
    }
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id INT PRIMARY KEY,
        league_name VARCHAR(255) NOT NULL,
        league_logo_url TEXT,
        league_country VARCHAR(100)
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
