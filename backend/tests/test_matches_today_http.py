import sys
from pathlib import Path
from datetime import datetime, timezone

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

import app.routers.homepage as homepage_module  # for monkeypatching datetime


# Build async SQLAlchemy URL for the TEST DB
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
def clear_homepage_tables_in_test_db():
    conn = get_db_connection("test")
    cur = conn.cursor()
    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")
    conn.commit()
    cur.close()
    conn.close()


def seed_leagues_teams_fixtures_for_matches_today(fixtures_rows):
    """
    fixtures_rows entries:
      (fixture_id, league_id, home_team_id, away_team_id, fixture_date_iso, status)
    """
    conn = get_db_connection("test")
    cur = conn.cursor()

    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")

    cur.executemany(
        """
        INSERT INTO leagues (league_id, league_name, league_logo_url, league_country, is_popular, display_order)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        [
            (1, "Premier League", "x", "England", False, 0),
            (2, "La Liga", "x", "Spain", False, 0),
        ],
    )

    cur.executemany(
        """
        INSERT INTO teams (team_id, team_name, team_logo_url)
        VALUES (%s, %s, %s);
        """,
        [
            (10, "Arsenal", "x"),
            (11, "Chelsea", "x"),
            (20, "Real Madrid", "x"),
            (21, "Barcelona", "x"),
        ],
    )

    cur.executemany(
        """
        INSERT INTO fixtures (fixture_id, league_id, home_team_id, away_team_id, fixture_date, status)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        fixtures_rows,
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
def freeze_homepage_now(monkeypatch):
    """
    Freeze homepage datetime.now(timezone.utc) so /matches/today is deterministic.
    """
    fixed_now = datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(homepage_module, "datetime", FixedDateTime)
    return fixed_now


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
async def test_http_matches_today_rejects_invalid_timezone(client):
    clear_homepage_tables_in_test_db()

    r = await client.get("/matches/today", params={"tz": "Not/A_Real_TZ"})
    assert r.status_code == 400
    assert "Invalid timezone" in r.text


@pytest.mark.anyio
async def test_http_matches_today_filters_utc_window_and_sets_headers(client, freeze_homepage_now):
    """
    With tz=UTC and fixed now=2026-01-10 12:00Z:
    window is [2026-01-10T00:00Z, 2026-01-11T00:00Z)
    """
    seed_leagues_teams_fixtures_for_matches_today(
        fixtures_rows=[
            (1001, 1, 10, 11, "2026-01-10T00:00:00+00:00", "NS"),  # include
            (1002, 2, 20, 21, "2026-01-10T23:59:59+00:00", "NS"),  # include
            (1003, 1, 10, 11, "2026-01-11T00:00:00+00:00", "NS"),  # exclude
            (1004, 2, 20, 21, "2026-01-09T23:59:59+00:00", "NS"),  # exclude
        ]
    )

    r = await client.get("/matches/today", params={"tz": "UTC"})
    assert r.status_code == 200
    data = r.json()

    assert {x["id"] for x in data} == {1001, 1002}

    # headers are lowercased by httpx
    assert r.headers.get("cache-control") == "no-store"
    assert r.headers.get("vary") == "tz"


@pytest.mark.anyio
async def test_http_matches_today_orders_by_league_then_kickoff(client, freeze_homepage_now):
    seed_leagues_teams_fixtures_for_matches_today(
        fixtures_rows=[
            (2002, 1, 10, 11, "2026-01-10T20:00:00+00:00", "NS"),  # EPL later
            (2001, 1, 10, 11, "2026-01-10T10:00:00+00:00", "NS"),  # EPL earlier
            (2004, 2, 20, 21, "2026-01-10T09:00:00+00:00", "NS"),  # La Liga earlier
            (2003, 2, 20, 21, "2026-01-10T18:00:00+00:00", "NS"),  # La Liga later
        ]
    )

    r = await client.get("/matches/today", params={"tz": "UTC"})
    assert r.status_code == 200
    data = r.json()

    assert [x["league_name"] for x in data] == ["La Liga", "La Liga", "Premier League", "Premier League"]

    la_liga_times = [x["kickoff_utc"] for x in data if x["league_name"] == "La Liga"]
    epl_times = [x["kickoff_utc"] for x in data if x["league_name"] == "Premier League"]
    assert la_liga_times == sorted(la_liga_times)
    assert epl_times == sorted(epl_times)


@pytest.mark.anyio
async def test_http_matches_today_america_chicago_window(client, freeze_homepage_now):
    """
    Freeze now at 2026-01-10 12:00Z.
    America/Chicago local day 2026-01-10 => UTC window [2026-01-10T06:00Z, 2026-01-11T06:00Z)
    """
    seed_leagues_teams_fixtures_for_matches_today(
        fixtures_rows=[
            (3001, 1, 10, 11, "2026-01-10T05:59:59+00:00", "NS"),  # exclude
            (3002, 1, 10, 11, "2026-01-10T06:00:00+00:00", "NS"),  # include
            (3003, 2, 20, 21, "2026-01-11T05:59:59+00:00", "NS"),  # include
            (3004, 2, 20, 21, "2026-01-11T06:00:00+00:00", "NS"),  # exclude
        ]
    )

    r = await client.get("/matches/today", params={"tz": "America/Chicago"})
    assert r.status_code == 200
    data = r.json()

    assert {x["id"] for x in data} == {3002, 3003}

    assert r.headers.get("cache-control") == "no-store"
    assert r.headers.get("vary") == "tz"
