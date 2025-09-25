"""
Routes that power the *Home* page
─────────────────────────────────
Endpoints
• GET /leagues/popular  – 5 league cards
• GET /matches/today    – today's fixtures
"""
import logging
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

# create logger for this module
logger = logging.getLogger(__name__)

router = APIRouter(tags=["homepage"])   # will be mounted in main.py

# ────────────────────────────────────────────────
# Pydantic models ⇢ shape of outgoing JSON
# ────────────────────────────────────────────────
class LeagueOut(BaseModel):
    id: int
    name: str
    country: str
    logo_url: str = Field(..., alias="league_logo_url")

class MatchOut(BaseModel):
    id: int
    league_name: str
    home_name: str
    home_logo: str
    away_name: str
    away_logo: str
    kickoff_utc: datetime
    status: str

# ────────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────────
@router.get("/leagues/popular", response_model=List[LeagueOut])
async def popular_leagues(db: AsyncSession = Depends(get_session)):
    """
    Five “Popular Leagues” – flagged in the *leagues* table with
    `is_popular = TRUE` and ordered by `display_order`.
    """
    sql = text(
        """
        SELECT  league_id      AS id,
                league_name    AS name,
                league_country AS country,
                league_logo_url
        FROM    leagues
        WHERE   is_popular = TRUE
        ORDER BY display_order;
        """
    )

    try:
        rows = (await db.execute(sql)).mappings()
        return list(rows)
    except SQLAlchemyError as err:
        logger.exception("DB error while fetching popular leagues")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error",
        ) from err


@router.get("/matches/today", response_model=List[MatchOut])
async def matches_today(db: AsyncSession = Depends(get_session)):
    """
    All fixtures with fixture_date = today (UTC).  The front-end will group
    them by `league_name`.
    """
    sql = text(
        """
        SELECT  f.fixture_id      AS id,
                l.league_name     AS league_name,
                ht.team_name      AS home_name,
                ht.team_logo_url  AS home_logo,
                at.team_name      AS away_name,
                at.team_logo_url  AS away_logo,
                f.fixture_date    AS kickoff_utc,
                f.status          AS status
        FROM    fixtures      f
        JOIN    leagues       l  ON l.league_id = f.league_id
        JOIN    teams         ht ON ht.team_id  = f.home_team_id
        JOIN    teams         at ON at.team_id  = f.away_team_id
        WHERE   f.fixture_date::date = CURRENT_DATE
        ORDER BY l.league_name, f.fixture_date;
        """
    )

    try:
        rows = (await db.execute(sql)).mappings()
        return list(rows)
    except SQLAlchemyError as err:
        logger.exception("DB error while fetching today's fixtures")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error",
        ) from err
