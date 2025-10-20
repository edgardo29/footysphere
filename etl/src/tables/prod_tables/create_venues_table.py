# create_venues_table.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

def create_venues_table():
    """
    Creates the `venues` table for storing stadium info.
    
    We'll keep columns:
      venue_id (INT PK),
      venue_name (VARCHAR),
      city (VARCHAR),
      country (VARCHAR)
    Add more if your API returns them (capacity, address, etc.).
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS venues (
        venue_id INT PRIMARY KEY,
        venue_name VARCHAR(255),
        city VARCHAR(100),
        country VARCHAR(100),
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("`venues` table created successfully.")
    except Exception as e:
        print(f"Error creating `venues` table: {e}")
        raise

if __name__ == "__main__":
    create_venues_table()
