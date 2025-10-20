import os
import sys

# Add the `test_scripts` directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

def create_league_seasons_table():
    """
    Creates the `league_seasons` table in the database if it doesn't already exist.
    LeagueSeasons {
        league_id int pk > leagues.league_id
        season_year int pk
        start_date  DATE,
        end_date    DATE,
        is_current boolean
    }
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS league_seasons (
        league_id INT NOT NULL,
        season_year INT NOT NULL,
        start_date  DATE,
        end_date    DATE,
        is_current BOOLEAN DEFAULT FALSE,
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()

        PRIMARY KEY (league_id, season_year),
        CONSTRAINT fk_league
            FOREIGN KEY (league_id)
            REFERENCES leagues (league_id)
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Execute the table creation query
        cur.execute(create_table_query)
        conn.commit()

        print("Table `league_seasons` created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print("Error creating the `league_seasons` table:", e)

if __name__ == "__main__":
    print("Creating the `league_seasons` table...")
    create_league_seasons_table()
