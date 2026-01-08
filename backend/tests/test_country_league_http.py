import pytest
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Reuse the seeding + test DB URL you already maintain in one place.
# This also avoids duplicating that same rows[] list.
from test_country_league_endpoints import seed_leagues_in_test_db, ASYNC_TEST_DB_URL

from app.main import app
from app.db import get_session


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
    AsyncSessionLocal = sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async def override_get_session():
        async with AsyncSessionLocal() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
    await async_engine.dispose()


@pytest.mark.anyio
async def test_http_countries_and_leagues_by_country(client):
    seed_leagues_in_test_db()

    # /countries
    r = await client.get("/countries")
    assert r.status_code == 200
    countries = r.json()
    assert isinstance(countries, list)
    assert all("country" in x for x in countries)

    country_names = [x["country"] for x in countries]
    assert "England" in country_names
    assert "Spain" in country_names
    assert "Italy" in country_names

    # /leagues_by_country?country=England
    r2 = await client.get("/leagues_by_country", params={"country": "England"})
    assert r2.status_code == 200
    leagues = r2.json()
    assert isinstance(leagues, list)
    assert len(leagues) == 2

    # ordering + shape
    assert [x["name"] for x in leagues] == ["Championship", "Premier League"]
    assert {x["country"] for x in leagues} == {"England"}
    assert all("logo_url" in x for x in leagues)
