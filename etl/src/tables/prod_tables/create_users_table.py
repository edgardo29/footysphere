import os
import sys

# Add your test_scripts dir for get_db_conn
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection


def create_users_table():
    """
    Creates the `users` table for authentication.

    Columns:
    - user_id: primary key (auto-generated)
    - email: unique identifier for login
    - password_hash: hashed password (never store plaintext)
    - load_date: row creation timestamp (your convention)
    - upd_date: row update timestamp (your convention)

    Constraints:
    - UNIQUE(email) prevents duplicate accounts
    """

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        user_id       BIGSERIAL PRIMARY KEY,
        email         TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()

        print("`users` table created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating `users` table: {e}")
        raise


if __name__ == "__main__":
    create_users_table()