import os
import sys
from typing import List
from pathlib import Path

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# ─────────────────────────────────────
# Compute repo root and adjust sys.path
# ─────────────────────────────────────

# Current file:  <repo_root>/backend/tests/test_country_league_endpoints.py
# parents[0] = tests
# parents[1] = backend
# parents[2] = repo root
BASE_DIR = Path(__file__).resolve().parents[2]  # repo root

# Add <repo_root>/etl/test_scripts so we can import get_db_conn.py
sys.path.append(str(BASE_DIR / "etl" / "test_scripts"))

# Add <repo_root>/etl/config so we can import credentials.py
sys.path.append(str(BASE_DIR / "etl" / "config"))

# Add <repo_root>/backend so we can import app code
sys.path.append(str(BASE_DIR / "backend"))

# ─────────────────────────────────────
# Project imports
# ─────────────────────────────────────

from get_db_conn import get_db_connection  # psycopg2 helper (main/test DB)
from credentials import DB_TEST_CREDENTIALS

# Import the actual route functions we want to test
from app.routers.leagues_by_country import (
    list_countries as list_countries_endpoint,
    leagues_by_country as leagues_by_country_endpoint,
)

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
# Test DB seeding helpers (sync, psycopg2)
# ─────────────────────────────────────

def seed_leagues_in_test_db(include_null_country: bool = False):
    """
    Seed the TEST database's `leagues` table with a small, known data set.

    NOTE:
    - This function assumes the `leagues` table already exists.
    - In CI, schema is created via backend/tests/schema_test.sql (run before pytest).
    """
    conn = get_db_connection("test")
    cur = conn.cursor()

    cur.execute("DELETE FROM leagues;")

    insert_sql = """
        INSERT INTO leagues (league_id, league_name, league_logo_url, league_country)
        VALUES (%s, %s, %s, %s);
    """

    rows = [
        (1, "Premier League", "https://example.com/epl.png", "England"),
        (2, "Championship", "https://example.com/championship.png", "England"),
        (3, "La Liga", "https://example.com/laliga.png", "Spain"),
        (4, "Serie A", "https://example.com/seriea.png", "Italy"),
    ]

    if include_null_country:
        rows.append((5, "Null Country League", "https://example.com/null.png", None))

    cur.executemany(insert_sql, rows)
    conn.commit()

    cur.close()
    conn.close()


def clear_leagues_in_test_db():
    """
    Clear the TEST database's `leagues` table.

    NOTE:
    - This function assumes the `leagues` table already exists.
    - In CI, schema is created via backend/tests/schema_test.sql (run before pytest).
    """
    conn = get_db_connection("test")
    cur = conn.cursor()
    cur.execute("DELETE FROM leagues;")
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
# Tests
# ─────────────────────────────────────

@pytest.mark.anyio
async def test_list_countries_returns_distinct_sorted():
    """
    /countries:
      - returns distinct countries
      - sorted alphabetically
      - ignores NULL countries
    """
    seed_leagues_in_test_db(include_null_country=True)

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with AsyncSessionLocal() as session:
        result = await list_countries_endpoint(db=session)
        data = [dict(row) for row in result]

        # Your SQL orders by league_country, so expect alphabetical order:
        # England, Italy, Spain
        assert data == [
            {"country": "England"},
            {"country": "Italy"},
            {"country": "Spain"},
        ]

    await async_engine.dispose()


@pytest.mark.anyio
async def test_leagues_by_country_happy_path():
    """
    /leagues_by_country:
      - filters by exact country
      - orders by league_name ascending
    """
    seed_leagues_in_test_db()

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with AsyncSessionLocal() as session:
        england_result = await leagues_by_country_endpoint(country="England", db=session)
        england_data = [dict(row) for row in england_result]

        assert len(england_data) == 2
        assert [row["name"] for row in england_data] == ["Championship", "Premier League"]
        assert {row["country"] for row in england_data} == {"England"}

        italy_result = await leagues_by_country_endpoint(country="Italy", db=session)
        italy_data = [dict(row) for row in italy_result]
        assert len(italy_data) == 1
        assert italy_data[0]["name"] == "Serie A"
        assert italy_data[0]["country"] == "Italy"

        spain_result = await leagues_by_country_endpoint(country="Spain", db=session)
        spain_data = [dict(row) for row in spain_result]
        assert len(spain_data) == 1
        assert spain_data[0]["name"] == "La Liga"
        assert spain_data[0]["country"] == "Spain"

        # whitespace normalization check (your endpoint does .strip())
        spain_ws_result = await leagues_by_country_endpoint(country="  Spain  ", db=session)
        spain_ws_data = [dict(row) for row in spain_ws_result]
        assert len(spain_ws_data) == 1
        assert spain_ws_data[0]["name"] == "La Liga"

    await async_engine.dispose()


@pytest.mark.anyio
async def test_leagues_by_country_empty_table_and_unknown_country():
    """
    /leagues_by_country edge cases:
      - empty table -> []
      - unknown country -> []
    """
    clear_leagues_in_test_db()

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with AsyncSessionLocal() as session:
        result_empty = await leagues_by_country_endpoint(country="England", db=session)
        assert [dict(row) for row in result_empty] == []

        result_unknown = await leagues_by_country_endpoint(country="NowhereLand", db=session)
        assert [dict(row) for row in result_unknown] == []

    await async_engine.dispose()


@pytest.mark.anyio
async def test_leagues_by_country_rejects_blank_country():
    """
    Your endpoint should 400 on blank/whitespace country values.
    """
    seed_leagues_in_test_db()

    async_engine = create_async_engine(ASYNC_TEST_DB_URL, echo=False, future=True)
    AsyncSessionLocal = sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with AsyncSessionLocal() as session:
        with pytest.raises(HTTPException) as exc:
            await leagues_by_country_endpoint(country="   ", db=session)

        assert exc.value.status_code == 400

    await async_engine.dispose()
