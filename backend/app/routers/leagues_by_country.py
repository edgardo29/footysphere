"""
Routes for browsing leagues by country
──────────────────────────────────────
Endpoints
• GET /countries                  – list of distinct countries that have leagues
• GET /leagues_by_country         – leagues for a given country name
"""

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

logger = logging.getLogger(__name__)

# This router will be mounted in main.py, e.g.:
#   app.include_router(router)
router = APIRouter(tags=["leagues_by_country"])


# ────────────────────────────────────────────────
# Pydantic models ⇢ shape of outgoing JSON
# ────────────────────────────────────────────────
class CountryOut(BaseModel):
    """Single country that has at least one league in the `leagues` table."""
    country: str


class LeagueByCountryOut(BaseModel):
    """
    A league that belongs to a specific country.

    `id` is used internally by the frontend (navigation, React keys, etc.),
    but does not need to be *displayed* to the user.
    """
    id: int
    name: str
    country: str
    logo_url: str


# ────────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────────
@router.get("/countries", response_model=List[CountryOut])
async def list_countries(db: AsyncSession = Depends(get_session)):
    """
    Return the list of distinct countries that have at least one league.

    Source of truth is `leagues.league_country`.
    Example response:
        [
          { "country": "Brazil" },
          { "country": "England" },
          { "country": "Mexico" },
          { "country": "Spain" }
        ]
    """
    sql = text(
        """
        SELECT DISTINCT
               league_country AS country
        FROM   leagues
        WHERE  league_country IS NOT NULL
        ORDER  BY league_country;
        """
    )

    try:
        rows = (await db.execute(sql)).mappings()
        # rows is an iterable of dict-like objects; Pydantic will handle them.
        return list(rows)
    except SQLAlchemyError as err:
        logger.exception("DB error while fetching countries")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error",
        ) from err


@router.get("/leagues_by_country", response_model=List[LeagueByCountryOut])
async def leagues_by_country(
    country: str = Query(..., description="Exact league_country value, e.g. 'Spain'"),
    db: AsyncSession = Depends(get_session),
):
    """
    Return all leagues for a given country.

    • `country` maps directly to `leagues.league_country`
      (e.g. 'Spain', 'Mexico', 'England').

    Behavior:
    • If the country exists but has no leagues → empty list, 200 OK.
    • If caller passes a typo / unknown country → also an empty list.
      (This keeps the API simple for the frontend.)

    Example response:
        [
          {
            "id": 143,
            "name": "Copa del Rey",
            "country": "Spain",
            "logo_url": "https://media.api-sports.io/football/leagues/143.png"
          },
          {
            "id": 140,
            "name": "La Liga",
            "country": "Spain",
            "logo_url": "https://media.api-sports.io/football/leagues/140.png"
          }
        ]
    """
    # Normalize basic whitespace so " Spain " still works.
    country = country.strip()

    if not country:
        # Guard against empty query param like ?country= or just spaces
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query parameter 'country' must be a non-empty string.",
        )

    sql = text(
        """
        SELECT  league_id       AS id,
                league_name     AS name,
                league_country  AS country,
                league_logo_url AS logo_url
        FROM    leagues
        WHERE   league_country = :country
        ORDER BY league_name;
        """
    )

    try:
        rows = (await db.execute(sql, {"country": country})).mappings()
        # If no leagues match, this will just be an empty list → 200 OK.
        return list(rows)
    except SQLAlchemyError as err:
        logger.exception("DB error while fetching leagues for country=%s", country)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error",
        ) from err
