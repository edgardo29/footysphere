import os
import sys

# Add your test_scripts dir for get_db_conn
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))

from get_db_conn import get_db_connection

def create_data_load_errors_table():
    """
    Creates a `data_load_errors` table with an index on (record_key).
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS data_load_errors (
        error_id       SERIAL PRIMARY KEY,
        staging_table  VARCHAR(50) NOT NULL,
        record_key     VARCHAR(50),
        error_reason   TEXT,
        created_at     TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_data_load_errors_record_key
        ON data_load_errors (record_key);
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()

        print("`data_load_errors` table + index created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `data_load_errors` table: {e}")
        raise

if __name__ == "__main__":
    create_data_load_errors_table()
