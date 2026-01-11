import sys
from pathlib import Path

import pytest
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# ─────────────────────────────────────
# Compute repo root and adjust sys.path
# ─────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR / "etl" / "test_scripts"))
sys.path.append(str(BASE_DIR / "etl" / "config"))
sys.path.append(str(BASE_DIR / "backend"))

from get_db_conn import get_db_connection, DB_TEST_CREDENTIALS

from app.main import app
from app.db import get_session


ASYNC_TEST_DB_URL = (
    f"postgresql+asyncpg://"
    f"{DB_TEST_CREDENTIALS['user']}:"
    f"{DB_TEST_CREDENTIALS['password']}@"
    f"{DB_TEST_CREDENTIALS['host']}:"
    f"{DB_TEST_CREDENTIALS['port']}/"
    f"{DB_TEST_CREDENTIALS['dbname']}"
)

# ─────────────────────────────────────
# Seeding helpers (sync)
# ─────────────────────────────────────
def seed_popular_leagues_in_test_db():
    conn = get_db_connection("test")
    cur = conn.cursor()

    # FK-safe clear order
    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")

    cur.executemany(
        """
        INSERT INTO leagues (
            league_id, league_name, league_logo_url, league_country, is_popular, display_order
        )
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        [
            (101, "La Liga", "https://example.com/laliga.png", "Spain", True, 2),
            (102, "Premier League", "https://example.com/epl.png", "England", True, 1),
            (103, "Serie A", "https://example.com/seriea.png", "Italy", True, 3),
            (201, "Championship", "https://example.com/championship.png", "England", False, 0),
        ],
    )

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

    cur.executemany(
        """
        INSERT INTO leagues (
            league_id, league_name, league_logo_url, league_country, is_popular, display_order
        )
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        [
            (301, "Non Popular A", "https://example.com/a.png", "X", False, 1),
            (302, "Non Popular B", "https://example.com/b.png", "Y", False, 2),
        ],
    )

    conn.commit()
    cur.close()
    conn.close()


# ─────────────────────────────────────
# Fixtures
# ─────────────────────────────────────
@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    """
    HTTP client that hits the real FastAPI app (ASGI),
    while overriding get_session to use the TEST database.
    """
    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async def override_get_session():
        async with AsyncSessionLocal() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
    await async_engine.dispose()


# ─────────────────────────────────────
# Tests
# ─────────────────────────────────────
@pytest.mark.anyio
async def test_http_popular_leagues_filters_and_orders(client):
    seed_popular_leagues_in_test_db()

    r = await client.get("/leagues/popular")
    assert r.status_code == 200

    data = r.json()
    assert isinstance(data, list)

    # Ordered by display_order: 1,2,3
    assert [x["name"] for x in data] == ["Premier League", "La Liga", "Serie A"]

    # HTTP layer returns alias key: league_logo_url
    assert all("id" in x for x in data)
    assert all("name" in x for x in data)
    assert all("country" in x for x in data)
    assert all("league_logo_url" in x for x in data)


@pytest.mark.anyio
async def test_http_popular_leagues_empty_when_none_popular(client):
    seed_only_non_popular_leagues_in_test_db()

    r = await client.get("/leagues/popular")
    assert r.status_code == 200
    assert r.json() == []
