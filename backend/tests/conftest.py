import sys
from pathlib import Path

import pytest

# Make etl/ imports available (same as your tests do)
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR / "etl" / "test_scripts"))
sys.path.append(str(BASE_DIR / "etl" / "config"))

from get_db_conn import get_db_connection


@pytest.fixture(scope="session", autouse=True)
def ensure_test_schema():
    """
    Always reset + recreate test schema once per pytest run.
    This removes the need to manually run schema_test.sql locally.
    """
    schema_path = Path(__file__).resolve().parent / "schema_test.sql"
    sql = schema_path.read_text()

    conn = get_db_connection("test")
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
