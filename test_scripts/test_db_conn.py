import os
import sys

# Add the parent directory of `config` to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../config")))

import psycopg2
from credentials import DB_CREDENTIALS  # Now this should work!

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=DB_CREDENTIALS["host"],
        port=DB_CREDENTIALS["port"],
        database=DB_CREDENTIALS["dbname"],
        user=DB_CREDENTIALS["user"],
        password=DB_CREDENTIALS["password"]
    )

    cur = conn.cursor()

    # Test query: List all tables in the public schema
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    tables = cur.fetchall()
    print("Tables in the database:", tables)

    cur.close()
    conn.close()
    print("Connection successful!")

except Exception as e:
    print("Error connecting to the database:", e)
