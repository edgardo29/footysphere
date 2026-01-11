import sys
from pathlib import Path
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException, Response
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

# Import the actual endpoint function + module (for monkeypatching datetime)
import app.routers.homepage as homepage_module
from app.routers.homepage import matches_today as matches_today_endpoint


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
# Test DB seeding helpers (sync)
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

    fixture_date_iso should be a UTC ISO string, e.g. "2026-01-10T06:00:00+00:00"
    """
    conn = get_db_connection("test")
    cur = conn.cursor()

    # clear in FK-safe order
    cur.execute("DELETE FROM fixtures;")
    cur.execute("DELETE FROM teams;")
    cur.execute("DELETE FROM leagues;")

    # minimal leagues needed for joins + ordering
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

    # minimal teams needed for joins
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

    # fixtures
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
# AnyIO backend fixture
# ─────────────────────────────────────
@pytest.fixture
def anyio_backend():
    return "asyncio"


# ─────────────────────────────────────
# Time control fixture (patch homepage_module.datetime.now)
# ─────────────────────────────────────
@pytest.fixture
def freeze_homepage_now(monkeypatch):
    """
    Monkeypatch homepage.py's datetime.now(timezone.utc) to a fixed UTC instant.
    """
    fixed_now = datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(homepage_module, "datetime", FixedDateTime)
    return fixed_now


# ─────────────────────────────────────
# Tests
# ─────────────────────────────────────
@pytest.mark.anyio
async def test_matches_today_rejects_invalid_timezone():
    clear_homepage_tables_in_test_db()

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        resp = Response()
        with pytest.raises(HTTPException) as exc:
            await matches_today_endpoint(response=resp, tz="Not/A_Real_TZ", db=session)

        assert exc.value.status_code == 400

    await async_engine.dispose()


@pytest.mark.anyio
async def test_matches_today_filters_by_local_day_utc_bounds_and_sets_headers(freeze_homepage_now):
    """
    With tz=UTC and fixed now=2026-01-10 12:00Z, "today" local is 2026-01-10.
    Bounds: [2026-01-10T00:00Z, 2026-01-11T00:00Z)
    """
    utc_start = "2026-01-10T00:00:00+00:00"
    just_before_end = "2026-01-10T23:59:59+00:00"
    utc_end = "2026-01-11T00:00:00+00:00"
    before_start = "2026-01-09T23:59:59+00:00"

    seed_leagues_teams_fixtures_for_matches_today(
        fixtures_rows=[
            (1001, 1, 10, 11, utc_start, "NS"),           # include
            (1002, 2, 20, 21, just_before_end, "NS"),     # include
            (1003, 1, 10, 11, utc_end, "NS"),             # exclude (end exclusive)
            (1004, 2, 20, 21, before_start, "NS"),        # exclude
        ]
    )

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        resp = Response()
        result = await matches_today_endpoint(response=resp, tz="UTC", db=session)
        data = [dict(row) for row in result]

        assert {row["id"] for row in data} == {1001, 1002}

        assert resp.headers["Cache-Control"] == "no-store"
        assert resp.headers["Vary"] == "tz"

    await async_engine.dispose()


@pytest.mark.anyio
async def test_matches_today_orders_by_league_name_then_kickoff(freeze_homepage_now):
    """
    Endpoint SQL orders by league_name, fixture_date.
    We seed fixtures out of time order and across leagues to verify ordering.
    """
    seed_leagues_teams_fixtures_for_matches_today(
        fixtures_rows=[
            # Intentionally unordered
            (2002, 1, 10, 11, "2026-01-10T20:00:00+00:00", "NS"),  # EPL later
            (2001, 1, 10, 11, "2026-01-10T10:00:00+00:00", "NS"),  # EPL earlier
            (2004, 2, 20, 21, "2026-01-10T09:00:00+00:00", "NS"),  # La Liga earlier
            (2003, 2, 20, 21, "2026-01-10T18:00:00+00:00", "NS"),  # La Liga later
        ]
    )

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        resp = Response()
        result = await matches_today_endpoint(response=resp, tz="UTC", db=session)
        data = [dict(row) for row in result]

        assert [row["league_name"] for row in data] == [
            "La Liga",
            "La Liga",
            "Premier League",
            "Premier League",
        ]

        la_liga_times = [row["kickoff_utc"] for row in data if row["league_name"] == "La Liga"]
        epl_times = [row["kickoff_utc"] for row in data if row["league_name"] == "Premier League"]
        assert la_liga_times == sorted(la_liga_times)
        assert epl_times == sorted(epl_times)

    await async_engine.dispose()


@pytest.mark.anyio
async def test_matches_today_timezone_america_chicago_changes_window(freeze_homepage_now):
    """
    Freeze now at 2026-01-10 12:00Z.
    In America/Chicago (CST, UTC-6), local "today" is 2026-01-10.
    UTC window is:
      [2026-01-10T06:00Z, 2026-01-11T06:00Z)
    """
    seed_leagues_teams_fixtures_for_matches_today(
        fixtures_rows=[
            (3001, 1, 10, 11, "2026-01-10T05:59:59+00:00", "NS"),  # 23:59:59 1/9 CST -> exclude
            (3002, 1, 10, 11, "2026-01-10T06:00:00+00:00", "NS"),  # 00:00:00 1/10 CST -> include
            (3003, 2, 20, 21, "2026-01-11T05:59:59+00:00", "NS"),  # 23:59:59 1/10 CST -> include
            (3004, 2, 20, 21, "2026-01-11T06:00:00+00:00", "NS"),  # 00:00:00 1/11 CST -> exclude
        ]
    )

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        resp = Response()
        result = await matches_today_endpoint(response=resp, tz="America/Chicago", db=session)
        data = [dict(row) for row in result]

        assert {row["id"] for row in data} == {3002, 3003}

        assert resp.headers["Cache-Control"] == "no-store"
        assert resp.headers["Vary"] == "tz"

    await async_engine.dispose()
