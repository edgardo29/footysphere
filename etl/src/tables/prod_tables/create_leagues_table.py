import os
import sys

# Add the `test_scripts` directory to sys.path for imports
# (Adjust the relative path if your structure is slightly different.)
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../test_scripts")
    )
)

from get_db_conn import get_db_connection


def create_leagues_table(target_db: str = "main"):
    """
    Creates the `leagues` table in the selected database if it doesn't already exist,
    matching the schema from your ERD:

        Leagues {
            league_id       int pk
            league_name     varchar
            league_logo_url text
            league_country  varchar
        }

    Args:
        target_db (str, optional):
            Which DB to create the table in:
            - "main" (default): your main app DB
            - "test": your test DB (e.g. footysphere_test_db)
    """
    # Normalize the DB selector
    target_db = (target_db or "main").lower()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS leagues (
        league_id INT PRIMARY KEY,
        league_name VARCHAR(255) NOT NULL,
        league_logo_url TEXT,
        league_country VARCHAR(100)
    );
    """

    try:
        # 🔹 This now uses the same helper you just updated:
        #    get_db_connection("main") or get_db_connection("test")
        conn = get_db_connection(target_db)
        cur = conn.cursor()

        # Execute the table creation query
        cur.execute(create_table_query)
        conn.commit()

        print(f"Table `leagues` created successfully in {target_db.upper()} DB.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating the `leagues` table in {target_db.upper()} DB:", e)


if __name__ == "__main__":
    """
    Run this script to create the `leagues` table in MAIN or TEST DB.

    Examples (from your repo root):

        python path/to/create_leagues_table.py
            -> creates table in MAIN DB (default)

        python path/to/create_leagues_table.py main
            -> explicitly creates table in MAIN DB

        python path/to/create_leagues_table.py test
            -> creates table in TEST DB
    """
    # Decide which DB to target from CLI arg; default to "main"
    arg = sys.argv[1].lower() if len(sys.argv) > 1 else "main"
    print(f"Creating `leagues` table in {arg.upper()} DB...")
    create_leagues_table(arg)
