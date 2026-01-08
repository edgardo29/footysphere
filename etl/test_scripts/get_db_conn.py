import os
import sys
import psycopg2

# Ensure we can import from the config directory (where credentials.py lives)
# Adjust the relative path if your structure is slightly different.
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../config")
    )
)

from credentials import DB_CREDENTIALS, DB_TEST_CREDENTIALS


def get_db_connection(target_db: str = "main"):
    """
    Create and return a connection to the PostgreSQL database.

    This is the ONE helper your other scripts should import and use.

    Args:
        target_db (str, optional):
            Which database to connect to:
            - "main" (default): uses DB_CREDENTIALS (your main app DB)
            - "test": uses DB_TEST_CREDENTIALS (your test DB, e.g. footysphere_test_db)

    Returns:
        psycopg2.extensions.connection:
            A live connection object to the selected database.

    Raises:
        Exception:
            If the connection fails, prints an error and re-raises.
    """
    # Normalize the input (handle None / weird casing)
    target_db = (target_db or "main").lower()

    if target_db == "main":
        creds = DB_CREDENTIALS
    elif target_db == "test":
        creds = DB_TEST_CREDENTIALS
    else:
        raise ValueError(
            f"Unknown target_db value: {target_db!r}. Use 'main' or 'test'."
        )

    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            database=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the {target_db.upper()} database:", e)
        raise


def smoke_test_connection(target_db: str = "main", list_tables: bool = True):
    """
    Manual smoke test for the DB connection.

    You can run this file directly to check MAIN or TEST DB.

    Args:
        target_db (str, optional):
            "main" or "test" (defaults to "main").
        list_tables (bool, optional):
            If True, will also list tables from the 'public' schema.
    """
    target_db = (target_db or "main").lower()

    try:
        conn = get_db_connection(target_db)
        cur = conn.cursor()

        # Basic 'SELECT 1' sanity check
        cur.execute("SELECT 1;")
        cur.fetchone()

        print(f"\n{target_db.upper()} DB connection successful!")

        if list_tables:
            print("Listing tables in 'public' schema:")
            cur.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
            )
            rows = cur.fetchall()
            if not rows:
                print("  (no tables found)")
            else:
                for (table_name,) in rows:
                    print(f"  - {table_name}")

        cur.close()
        conn.close()
        print(f"\n{target_db.upper()} DB smoke test completed.\n")

    except Exception as e:
        print(f"\n{target_db.upper()} DB smoke test FAILED")
        print("Error:", e)


if __name__ == "__main__":
    """
    Run this file directly to manually test DB connections.

    Examples (from your repo root, assuming this file is in test_scripts/):

        python test_scripts/get_db_conn.py
            -> tests MAIN DB by default

        python test_scripts/get_db_conn.py main
            -> explicitly tests MAIN DB

        python test_scripts/get_db_conn.py test
            -> tests TEST DB (using DB_TEST_CREDENTIALS)
    """
    # Choose which DB to test from CLI arg: "main" or "test"
    arg = sys.argv[1].lower() if len(sys.argv) > 1 else "main"
    print(f"Running smoke test for {arg.upper()} DB...")
    smoke_test_connection(arg, list_tables=True)
