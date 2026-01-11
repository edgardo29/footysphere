import sys
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# ─────────────────────────────────────
# Compute repo root and adjust sys.path
# ─────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]

sys.path.append(str(BASE_DIR / "etl" / "test_scripts"))
sys.path.append(str(BASE_DIR / "etl" / "config"))
sys.path.append(str(BASE_DIR / "backend"))

from get_db_conn import get_db_connection, DB_TEST_CREDENTIALS
from app.routers.homepage import popular_leagues as popular_leagues_endpoint


ASYNC_TEST_DB_URL = (
    f"postgresql+asyncpg://"
    f"{DB_TEST_CREDENTIALS['user']}:"
    f"{DB_TEST_CREDENTIALS['password']}@"
    f"{DB_TEST_CREDENTIALS['host']}:"
    f"{DB_TEST_CREDENTIALS['port']}/"
    f"{DB_TEST_CREDENTIALS['dbname']}"
)


# ─────────────────────────────────────
# Test DB seeding helpers (sync, psycopg2)
# ─────────────────────────────────────
def clear_leagues_in_test_db():
    conn = get_db_connection("test")
    cur = conn.cursor()

    # FK-safe clear order
    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")

    conn.commit()
    cur.close()
    conn.close()


def seed_popular_leagues_in_test_db():
    """
    Seed leagues with a mix of popular/non-popular and out-of-order display_order
    so we can verify filtering + ordering.
    """
    conn = get_db_connection("test")
    cur = conn.cursor()

    # FK-safe clear order
    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")

    insert_sql = """
        INSERT INTO leagues (
            league_id, league_name, league_logo_url, league_country, is_popular, display_order
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    rows = [
        (101, "La Liga", "https://example.com/laliga.png", "Spain", True, 2),
        (102, "Premier League", "https://example.com/epl.png", "England", True, 1),
        (103, "Serie A", "https://example.com/seriea.png", "Italy", True, 3),

        (201, "Championship", "https://example.com/championship.png", "England", False, 0),
        (202, "Bundesliga 2", "https://example.com/b2.png", "Germany", False, 0),
    ]

    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def seed_only_non_popular_leagues_in_test_db():
    conn = get_db_connection("test")
    cur = conn.cursor()

    # FK-safe clear order
    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")

    insert_sql = """
        INSERT INTO leagues (
            league_id, league_name, league_logo_url, league_country, is_popular, display_order
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    rows = [
        (301, "Non Popular A", "https://example.com/a.png", "X", False, 1),
        (302, "Non Popular B", "https://example.com/b.png", "Y", False, 2),
    ]

    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_popular_leagues_returns_only_popular_and_orders_by_display_order():
    seed_popular_leagues_in_test_db()

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        result = await popular_leagues_endpoint(db=session)
        data = [dict(row) for row in result]

        assert [row["name"] for row in data] == ["Premier League", "La Liga", "Serie A"]

        assert all("id" in row for row in data)
        assert all("name" in row for row in data)
        assert all("country" in row for row in data)
        assert all("league_logo_url" in row for row in data)

    await async_engine.dispose()


@pytest.mark.anyio
async def test_popular_leagues_empty_when_none_popular():
    seed_only_non_popular_leagues_in_test_db()

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        result = await popular_leagues_endpoint(db=session)
        assert [dict(row) for row in result] == []

    await async_engine.dispose()
