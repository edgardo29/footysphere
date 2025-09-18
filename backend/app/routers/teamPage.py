# routers/teamPage.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional, Literal

import logging
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/teamPage", tags=["teamPage"])

# ────────────────────────────────────────────────────────────────
# Pydantic models (shape matches your React mocks)
# ────────────────────────────────────────────────────────────────

class SummaryLeague(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    logo_url: Optional[str] = None
    country: Optional[str] = None

class RecordOut(BaseModel):
    wins: int
    draws: int
    losses: int
    goal_diff: int

class TeamSummaryOut(BaseModel):
    team_id: int
    team_name: str
    team_logo_url: Optional[str] = None
    league: Optional[SummaryLeague] = None
    position: Optional[int] = None
    points: Optional[int] = None
    record: Optional[RecordOut] = None

class FixtureTeam(BaseModel):
    id: int
    name: str
    logo: Optional[str] = None

class FixtureScore(BaseModel):
    home: Optional[int] = None
    away: Optional[int] = None

class FixtureCompetition(BaseModel):
    id: int
    name: str

class FixtureOut(BaseModel):
    match_id: int
    date_utc: Optional[str] = None
    status: str
    round: Optional[str] = None
    home: FixtureTeam
    away: FixtureTeam
    score: FixtureScore = Field(default_factory=FixtureScore)
    competition: FixtureCompetition
    venue: Optional[str] = None  # ← string to match React

class FixturesResponse(BaseModel):
    meta: dict
    fixtures: List[FixtureOut]

# ────────────────────────────────────────────────────────────────
# SQL (read-only)
# ────────────────────────────────────────────────────────────────

# League selection: membership → standings → fixtures majority (deterministic tie-break)
SQL_TEAM_SUMMARY = text(
    """
    WITH membership AS (
      SELECT tls.league_id, 1 AS pri
      FROM team_league_seasons tls
      WHERE tls.team_id = :team_id AND tls.season_year = :season_year
    ),
    standings_pick AS (
      SELECT DISTINCT s.league_id, 2 AS pri
      FROM standings s
      WHERE s.team_id = :team_id AND s.season_year = :season_year
    ),
    fixtures_majority AS (
      SELECT league_id, 3 AS pri
      FROM (
        SELECT f.league_id, COUNT(*) AS games,
               ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, f.league_id) AS rn
        FROM fixtures f
        WHERE f.season_year = :season_year
          AND (:team_id IN (f.home_team_id, f.away_team_id))
        GROUP BY f.league_id
      ) t
      WHERE rn = 1
    ),
    league_pick AS (
      SELECT * FROM membership
      UNION ALL
      SELECT * FROM standings_pick
      UNION ALL
      SELECT * FROM fixtures_majority
    ),
    chosen AS (
      SELECT league_id
      FROM league_pick
      ORDER BY pri
      LIMIT 1
    )
    SELECT
      t.team_id,
      t.team_name,
      t.team_logo_url,
      l.league_id,
      l.league_name,
      l.league_logo_url,
      l.league_country,
      s.rank         AS position,
      s.points       AS points,
      s.win          AS wins,
      s.draw         AS draws,
      s.lose         AS losses,
      s.goals_diff   AS goal_diff
    FROM teams t
    LEFT JOIN chosen c ON TRUE
    LEFT JOIN leagues l ON l.league_id = c.league_id
    -- Prefer the "overall" group if present; else best rank (deterministic single row)
    LEFT JOIN LATERAL (
      SELECT s.rank, s.points, s.win, s.draw, s.lose, s.goals_diff
      FROM standings s
      WHERE s.league_id = c.league_id
        AND s.season_year = :season_year
        AND s.team_id = :team_id
      ORDER BY (s.group_label = '') DESC, s.rank NULLS LAST
      LIMIT 1
    ) s ON TRUE
    WHERE t.team_id = :team_id;
    """
)

SQL_TEAM_FIXTURES_BASE = text(
    """
    SELECT
      f.fixture_id                 AS match_id,
      f.fixture_date               AS date_utc,
      f.status                     AS raw_status,
      f.round                      AS round,
      ht.team_id                   AS home_id,
      ht.team_name                 AS home_name,
      ht.team_logo_url             AS home_logo,
      at.team_id                   AS away_id,
      at.team_name                 AS away_name,
      at.team_logo_url             AS away_logo,
      f.home_score                 AS home_score,
      f.away_score                 AS away_score,
      l.league_id                  AS competition_id,
      l.league_name                AS competition_name,
      v.venue_name                 AS venue_name
    FROM fixtures f
    JOIN teams ht ON ht.team_id = f.home_team_id
    JOIN teams at ON at.team_id = f.away_team_id
    JOIN leagues l ON l.league_id = f.league_id
    LEFT JOIN venues v ON v.venue_id = f.venue_id
    WHERE f.season_year = :season_year
      AND (:team_id = f.home_team_id OR :team_id = f.away_team_id)
      AND (
           :status = 'all'
        OR (:status = 'upcoming' AND f.status IN ('NS','TBD','PST','SUSP','INT','POST'))
        OR (:status = 'played'   AND f.status IN ('FT','AET','PEN'))
      )
      AND (
           :q = ''
        OR ht.team_name ILIKE :q_like
        OR at.team_name ILIKE :q_like
        OR f.round ILIKE :q_like
      )
    ORDER BY
      CASE WHEN :status = 'played' THEN f.fixture_date END DESC,
      CASE WHEN :status <> 'played' THEN f.fixture_date END ASC
    LIMIT :limit OFFSET :offset;
    """
)

SQL_TEAM_FIXTURES_COUNTS = text(
    """
    SELECT
      COUNT(*)  AS count_total,
      COUNT(*) FILTER (WHERE status IN ('NS','TBD','PST','SUSP','INT','POST')) AS count_upcoming,
      COUNT(*) FILTER (WHERE status IN ('FT','AET','PEN'))                      AS count_played
    FROM fixtures
    WHERE season_year = :season_year
      AND (:team_id = home_team_id OR :team_id = away_team_id);
    """
)

# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

_FINISHED = {"FT", "AET", "PEN"}
_NOT_STARTED = {"NS", "TBD", "PST", "SUSP", "INT", "POST"}

def _normalize_status(raw: Optional[str]) -> str:
    if not raw:
        return "—"
    code = raw.upper()
    if code in _FINISHED:
        return "Full-Time"
    if code in _NOT_STARTED:
        return "Scheduled"
    return raw  # pass through LIVE/1H/HT/2H/ET/etc.

def _iso_utc(ts: Optional[datetime]) -> Optional[str]:
    if ts is None:
        return None
    # Treat DB timestamp as UTC
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")

# ────────────────────────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────────────────────────

@router.get("/{team_id}/summary", response_model=TeamSummaryOut)
async def get_team_summary(
    team_id: int,
    season: int = Query(..., description="Season year, required"),
    db: AsyncSession = Depends(get_session),
):
    try:
        res = await db.execute(
            SQL_TEAM_SUMMARY.bindparams(team_id=team_id, season_year=season)
        )
        row = res.mappings().first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found"
            )

        league = None
        if row["league_id"] is not None:
            league = SummaryLeague(
                id=row["league_id"],
                name=row["league_name"],
                logo_url=row["league_logo_url"],
                country=row["league_country"],
            )

        # Build record only when all four fields are present (UI shows dashes otherwise)
        record = None
        if (
            row["wins"] is not None and
            row["draws"] is not None and
            row["losses"] is not None and
            row["goal_diff"] is not None
        ):
            record = RecordOut(
                wins=row["wins"],
                draws=row["draws"],
                losses=row["losses"],
                goal_diff=row["goal_diff"],
            )

        return TeamSummaryOut(
            team_id=row["team_id"],
            team_name=row["team_name"],
            team_logo_url=row["team_logo_url"],
            league=league,
            position=row["position"],
            points=row["points"],
            record=record,
        )
    except SQLAlchemyError:
        logger.exception("DB error in get_team_summary team_id=%s season=%s", team_id, season)
        raise HTTPException(status_code=500, detail="Database error")


@router.get("/{team_id}/fixtures", response_model=FixturesResponse)
async def get_team_fixtures(
    team_id: int,
    season: int = Query(..., description="Season year, required"),
    status_param: Literal["all", "upcoming", "played"] = Query("all", alias="status"),
    q: str = Query("", description="Search opponent name or round (ILIKE)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_session),
):
    try:
        # Counts (for badges/tabs)
        counts_res = await db.execute(
            SQL_TEAM_FIXTURES_COUNTS.bindparams(team_id=team_id, season_year=season)
        )
        counts = counts_res.mappings().first() or {}
        meta = {
            "count_total": int(counts.get("count_total", 0)),
            "count_upcoming": int(counts.get("count_upcoming", 0)),
            "count_played": int(counts.get("count_played", 0)),
        }

        q_like = f"%{q.strip()}%" if q else "%"

        rows_res = await db.execute(
            SQL_TEAM_FIXTURES_BASE.bindparams(
                team_id=team_id,
                season_year=season,
                status=status_param,
                q=q,
                q_like=q_like,
                limit=limit,
                offset=offset,
            )
        )
        rows = rows_res.mappings().all()

        out_list: List[FixtureOut] = []
        for r in rows:
            raw_status = r["raw_status"]
            norm_status = _normalize_status(raw_status)

            # Only expose scores as numbers for finished matches
            home_score = r["home_score"] if raw_status and raw_status.upper() in _FINISHED else None
            away_score = r["away_score"] if raw_status and raw_status.upper() in _FINISHED else None

            out_list.append(
                FixtureOut(
                    match_id=r["match_id"],
                    date_utc=_iso_utc(r["date_utc"]),
                    status=norm_status,
                    round=r["round"],
                    home=FixtureTeam(id=r["home_id"], name=r["home_name"], logo=r["home_logo"]),
                    away=FixtureTeam(id=r["away_id"], name=r["away_name"], logo=r["away_logo"]),
                    score=FixtureScore(home=home_score, away=away_score),
                    competition=FixtureCompetition(id=r["competition_id"], name=r["competition_name"]),
                    venue=r["venue_name"] or None,  # string or None for React
                )
            )

        return FixturesResponse(meta=meta, fixtures=out_list)

    except SQLAlchemyError:
        logger.exception(
            "DB error in get_team_fixtures team_id=%s season=%s status=%s q=%s",
            team_id, season, status_param, q
        )
        raise HTTPException(status_code=500, detail="Database error")
