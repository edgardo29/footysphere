# create_teams_table.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def create_teams_table():
    """
    Creates the `teams` table with a FK to `venues.venue_id`.
    Columns:
      team_id      (INT PK)
      team_name    (VARCHAR)
      team_country (VARCHAR)
      team_logo_url (TEXT)
      venue_id     (INT) -> references `venues`
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS teams (
        team_id INT PRIMARY KEY,
        team_name VARCHAR(255) NOT NULL,
        team_country VARCHAR(100),
        team_logo_url TEXT,
        venue_id INT,
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()

        FOREIGN KEY (venue_id) REFERENCES venues(venue_id)
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("`teams` table created successfully.")
    except Exception as e:
        print(f"Error creating `teams` table: {e}")
        raise

if __name__ == "__main__":
    create_teams_table()
