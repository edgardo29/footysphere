# routers/leaguesPage.py
from datetime import datetime
from typing import List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/leaguesPage", tags=["leaguesPage"])

# ────────────────────────────────────────────────────────────────
# Pydantic models
# ────────────────────────────────────────────────────────────────
class LeagueDetailOut(BaseModel):
    id: int
    name: str
    country: str
    league_logo_url: str
    current_season: Optional[int] = None
    last_updated_utc: Optional[datetime] = None

class StandingRowOut(BaseModel):
    position: int = Field(..., description="Table rank")
    team_id: int
    team_name: str
    team_logo_url: Optional[str] = None
    played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goal_diff: int
    points: int
    form: List[str] = Field(default_factory=list)
    group_label: str

# ────────────────────────────────────────────────────────────────s
# SQL (use league_catalog.last_season, alias to current_season)
# ────────────────────────────────────────────────────────────────
SQL_LEAGUE_DETAIL = text(
    """
    WITH season_src AS (
        SELECT
            COALESCE(lc.last_season, MAX(s.season_year)) AS current_season
        FROM leagues l
        LEFT JOIN league_catalog lc
               ON lc.league_id = l.league_id
        LEFT JOIN standings s
               ON s.league_id = l.league_id
        WHERE l.league_id = :league_id
        GROUP BY lc.last_season
    )
    SELECT
        l.league_id      AS id,
        l.league_name    AS name,
        l.league_country AS country,
        l.league_logo_url,
        ss.current_season,
        (
            SELECT MAX(upd_date)
            FROM standings
            WHERE league_id = l.league_id
              AND (ss.current_season IS NOT NULL AND season_year = ss.current_season)
        ) AS last_updated_utc
    FROM leagues l
    LEFT JOIN season_src ss ON TRUE
    WHERE l.league_id = :league_id;
    """
)

SQL_CURRENT_SEASON_FOR_LEAGUE = text(
    """
    SELECT COALESCE(lc.last_season, MAX(s.season_year)) AS current_season
    FROM leagues l
    LEFT JOIN league_catalog lc
           ON lc.league_id = l.league_id
    LEFT JOIN standings s
           ON s.league_id = l.league_id
    WHERE l.league_id = :league_id
    GROUP BY lc.last_season;
    """
)

SQL_STANDINGS = text(
    """
    SELECT
        s.rank               AS position,
        t.team_id            AS team_id,
        t.team_name          AS team_name,
        t.team_logo_url      AS team_logo_url,
        s.played             AS played,
        s.win                AS wins,
        s.draw               AS draws,
        s.lose               AS losses,
        s.goals_for          AS goals_for,
        s.goals_against      AS goals_against,
        s.goals_diff         AS goal_diff,
        s.points             AS points,
        s.form_compact       AS form_compact,
        s.group_label        AS group_label
    FROM standings s
    JOIN teams t ON t.team_id = s.team_id
    WHERE s.league_id = :league_id
      AND s.season_year = :season_year
    ORDER BY s.group_label, s.rank NULLS LAST, t.team_name;
    """
)

# ────────────────────────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────────────────────────
@router.get("/{league_id}", response_model=LeagueDetailOut)
async def get_league_detail(
    league_id: int,
    db: AsyncSession = Depends(get_session),
):
    try:
        res = await db.execute(SQL_LEAGUE_DETAIL.bindparams(league_id=league_id))
        row = res.mappings().first()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="League not found")

        return LeagueDetailOut(
            id=row["id"],
            name=row["name"],
            country=row["country"],
            league_logo_url=row["league_logo_url"],
            current_season=row["current_season"],
            last_updated_utc=row["last_updated_utc"],
        )
    except SQLAlchemyError:
        logger.exception("DB error in get_league_detail league_id=%s", league_id)
        raise HTTPException(status_code=500, detail="Database error")


@router.get("/{league_id}/standings", response_model=List[StandingRowOut])
async def get_league_standings(
    league_id: int,
    season: Optional[int] = Query(None, description="Season year; defaults to current season for the league"),
    db: AsyncSession = Depends(get_session),
):
    try:
        resolved_season = season
        if resolved_season is None:
            res = await db.execute(SQL_CURRENT_SEASON_FOR_LEAGUE.bindparams(league_id=league_id))
            row = res.mappings().first()
            resolved_season = row["current_season"] if row else None

        if resolved_season is None:
            return []

        res = await db.execute(
            SQL_STANDINGS.bindparams(league_id=league_id, season_year=resolved_season)
        )
        rows = res.mappings().all()

        out: List[StandingRowOut] = []
        for r in rows:
            form_compact = r["form_compact"] or ""
            out.append(
                StandingRowOut(
                    position=r["position"],
                    team_id=r["team_id"],
                    team_name=r["team_name"],
                    team_logo_url=r["team_logo_url"],
                    played=r["played"],
                    wins=r["wins"],
                    draws=r["draws"],
                    losses=r["losses"],
                    goals_for=r["goals_for"],
                    goals_against=r["goals_against"],
                    goal_diff=r["goal_diff"],
                    points=r["points"],
                    form=list(form_compact),
                    group_label=r["group_label"] or "",
                )
            )
        return out
    except SQLAlchemyError:
        logger.exception("DB error in get_league_standings league_id=%s season=%s", league_id, season)
        raise HTTPException(status_code=500, detail="Database error")






# ────────────────────────────────────────────────────────────────
# Weekly matches for a league
#   GET /leaguesPage/{league_id}/weekly-matches?start=YYYY-MM-DD&days=7
# ────────────────────────────────────────────────────────────────
from datetime import date as _date, datetime as _dt, timedelta as _td

class WeeklyMatchOut(BaseModel):
    id: int
    kickoff_utc: datetime
    status: str
    round: Optional[str] = None
    home_id: int
    home_name: str
    home_logo: Optional[str] = None
    away_id: int
    away_name: str
    away_logo: Optional[str] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None

SQL_WEEKLY_MATCHES = text(
    """
    SELECT
        f.fixture_id     AS id,
        f.fixture_date   AS kickoff_utc,
        f.status         AS status,
        f.round          AS round,
        ht.team_id       AS home_id,
        ht.team_name     AS home_name,
        ht.team_logo_url AS home_logo,
        at.team_id       AS away_id,
        at.team_name     AS away_name,
        at.team_logo_url AS away_logo,
        f.home_score     AS home_score,
        f.away_score     AS away_score
    FROM fixtures f
    JOIN teams ht ON ht.team_id = f.home_team_id
    JOIN teams at ON at.team_id = f.away_team_id
    WHERE f.league_id   = :league_id
      AND f.fixture_date >= :start_ts
      AND f.fixture_date <  :end_ts
    ORDER BY f.fixture_date ASC;
    """
)

@router.get("/{league_id}/weekly-matches", response_model=List[WeeklyMatchOut])
async def get_weekly_matches(
    league_id: int,
    start: Optional[_date] = Query(
        None, description="ISO date (YYYY-MM-DD) for week start; defaults to Monday this week (UTC)"
    ),
    days: int = Query(7, ge=1, le=14),
    db: AsyncSession = Depends(get_session),
):
    try:
        # Default to Monday of the current week (UTC-ish; timestamp is naive UTC in your schema)
        if start is None:
            today = _date.today()
            start = today - _td(days=today.weekday())  # Monday
        start_ts = _dt.combine(start, _dt.min.time())
        end_ts   = start_ts + _td(days=days)

        res = await db.execute(
            SQL_WEEKLY_MATCHES.bindparams(
                league_id=league_id,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )
        return list(res.mappings())
    except SQLAlchemyError:
        # Log and return a 500 with a generic message
        import logging
        logging.getLogger(__name__).exception(
            "DB error in get_weekly_matches league_id=%s start=%s days=%s",
            league_id, start, days
        )
        raise HTTPException(status_code=500, detail="Database error")
