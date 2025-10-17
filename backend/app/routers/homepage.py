"""
Routes that power the *Home* page
─────────────────────────────────
Endpoints
• GET /leagues/popular  – popular league cards
• GET /matches/today    – today's fixtures (tz-aware)
"""
import logging
from datetime import datetime, date, time, timedelta, timezone
from typing import List
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, status, Response
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

# create logger for this module
logger = logging.getLogger(__name__)

router = APIRouter(tags=["homepage"])  # will be mounted in main.py

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
    Popular leagues – flagged in *leagues* (`is_popular = TRUE`) and ordered by `display_order`.
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
async def matches_today(
    response: Response,
    tz: str = "UTC",
    db: AsyncSession = Depends(get_session),
):
    """
    Return fixtures whose LOCAL day (in `tz`) is 'today'.
    Validates tz, computes [utc_start, utc_end) in Python, and handles DB errors.
    Also sets cache headers to avoid stale/cached results.
    """
    # 1) Validate timezone early → user error = 400
    try:
        tzinfo = ZoneInfo(tz)
    except ZoneInfoNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid timezone: {tz!r}. Use a valid IANA name like 'America/Chicago'.",
        )

    # 2) Compute local-day bounds and convert to UTC
    now_utc = datetime.now(timezone.utc)
    today_local: date = now_utc.astimezone(tzinfo).date()
    start_local = datetime.combine(today_local, time(0, 0), tzinfo)
    end_local = start_local + timedelta(days=1)
    utc_start = start_local.astimezone(timezone.utc)
    utc_end = end_local.astimezone(timezone.utc)

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
        FROM    fixtures f
        JOIN    leagues  l  ON l.league_id = f.league_id
        JOIN    teams    ht ON ht.team_id  = f.home_team_id
        JOIN    teams    at ON at.team_id  = f.away_team_id
        WHERE   f.fixture_date >= :utc_start
            AND f.fixture_date <  :utc_end
        ORDER BY l.league_name, f.fixture_date;
        """
    )

    try:
        rows = (await db.execute(sql, {"utc_start": utc_start, "utc_end": utc_end})).mappings()

        # 3) Prevent caching issues (browsers/CDNs) and note tz variance
        response.headers["Cache-Control"] = "no-store"
        response.headers["Vary"] = "tz"

        return list(rows)
    except SQLAlchemyError as err:
        logger.exception("DB error in /matches/today (tz=%s, %s–%s)", tz, utc_start, utc_end)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error",
        ) from err
