import os
import sys
import psycopg2

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../config")))

from credentials import DB_CREDENTIALS

def get_db_connection():
    """
    Creates and returns a connection to the PostgreSQL database.
    Reads credentials from the DB_CREDENTIALS dictionary in credentials.py.

    Returns:
        psycopg2.extensions.connection: A live connection object to the database.

    Raises:
        Exception: If the connection fails, an error message is printed and re-raised.
    """
    try:
        conn = psycopg2.connect(
            host=DB_CREDENTIALS["host"],
            port=DB_CREDENTIALS["port"],
            database=DB_CREDENTIALS["dbname"],
            user=DB_CREDENTIALS["user"],
            password=DB_CREDENTIALS["password"]
        )
        return conn
    except Exception as e:
        print("Error connecting to the database:", e)
        raise

def test_db_connection():
    """
    Tests the database connection using the reusable connection logic.
    Prints a success message if the connection works, or an error message if it fails.
    """
    try:
        conn = get_db_connection()
        print("Modular test: Connection successful!")
        conn.close()
    except Exception as e:
        print("Modular test: Connection failed:", e)

if __name__ == "__main__":
    print("Starting modular database connection test...")
    test_db_connection()
