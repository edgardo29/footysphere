# backend/routers/matchDetailsPage.py
from __future__ import annotations

import html
import logging
from datetime import datetime, timezone
from typing import Optional, List, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from ..db import get_session

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/matchDetailsPage", tags=["matchDetailsPage"])

# --- small helpers ------------------------------------------------------------

_FINISHED     = {"FT", "AET", "PEN"}
_SCHEDULED    = {"NS", "TBD"}
_POSTPONED    = {"PST", "POST"}
_INTERRUPTED  = {"INT", "SUSP"}

def _normalize_status(raw: Optional[str]) -> str:
    # Map DB status codes to UI labels; keep live codes (1H/HT/2H/ET/P, etc.) as-is.
    if not raw:
        return "—"
    code = raw.strip().upper()
    if code in _FINISHED:
        return "Full-Time"
    if code in _SCHEDULED:
        return "Scheduled"
    if code in _POSTPONED:
        return "Postponed"
    if code in __INTERRUPTED:
        return "Interrupted"
    return raw  # pass-through for live codes

def _is_finished(raw: Optional[str]) -> bool:
    return bool(raw and raw.strip().upper() in _FINISHED)

def _iso_utc(ts: Optional[datetime]) -> Optional[str]:
    # Ensure ISO-8601 with 'Z'. Treat naive timestamps as UTC (DB default).
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")

def _short_name(full: Optional[str]) -> Optional[str]:
    # "John Smith" → "J. Smith"; also unescape HTML entities once.
    if not full:
        return None
    full = html.unescape(full).strip()
    parts = full.split()
    if len(parts) == 1:
        return parts[0]
    first, last = parts[0], parts[-1]
    return f"{first[0]}." + (f" {last}" if last else "")

def _clean_str(s: Optional[str]) -> Optional[str]:
    # HTML-unescape + trim; return None if blank after cleaning.
    if s is None:
        return None
    val = html.unescape(s).strip()
    return val or None

# --- pydantic models (shape == frontend props) --------------------------------

class Competition(BaseModel):
    id: int
    name: str

class Venue(BaseModel):
    name: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None

class TeamMini(BaseModel):
    id: int
    name: str
    logo: Optional[str] = None

class FulltimeScore(BaseModel):
    home: Optional[int] = None   # null until finished
    away: Optional[int] = None

class Score(BaseModel):
    fulltime: FulltimeScore = Field(default_factory=FulltimeScore)

class EventOut(BaseModel):
    # UI-visible types (kept stable for frontend)
    minute: int
    minute_extra: int = 0
    team_id: int
    type: Literal["goal", "yellow", "red", "substitution", "var"]
    player: Optional[str] = None
    assist: Optional[str] = None
    detail: Optional[str] = None

class StatsSide(BaseModel):
    # Default zeros so the UI never renders "null" for numeric stats.
    possession_pct: int = 0
    shots_total: int = 0
    shots_on_target: int = 0
    shots_inside_box: int = 0
    shots_outside_box: int = 0
    corners: int = 0
    offsides: int = 0
    passes_total: int = 0
    pass_accuracy_pct: int = 0
    fouls: int = 0
    yellow: int = 0
    red: int = 0

class StatsOut(BaseModel):
    home: StatsSide = Field(default_factory=StatsSide)
    away: StatsSide = Field(default_factory=StatsSide)

class MatchDetailsOut(BaseModel):
    # Mirrors your mock JSON (no halftime).
    match_id: int
    season: int
    competition: Competition
    round: Optional[str] = None
    date_utc: Optional[str] = None
    status: str
    venue: Venue = Field(default_factory=Venue)
    home: TeamMini
    away: TeamMini
    score: Score = Field(default_factory=Score)
    events: List[EventOut] = Field(default_factory=list)
    stats: StatsOut = Field(default_factory=StatsOut)

# --- SQL (COALESCE numeric → 0; normalize event types) ------------------------

SQL_MATCH_HEADER = text("""
/* Basic match header: teams, league, venue, raw status, scores */
SELECT
  f.fixture_id  AS match_id,
  f.season_year AS season,
  f.round,
  f.fixture_date AS date_utc,
  f.status       AS raw_status,
  l.league_id   AS competition_id,
  l.league_name AS competition_name,
  ht.team_id    AS home_id,  ht.team_name AS home_name,  ht.team_logo_url AS home_logo,
  at.team_id    AS away_id,  at.team_name AS away_name,  at.team_logo_url AS away_logo,
  v.venue_name  AS venue_name,
  v.city        AS venue_city,
  v.country     AS venue_country,
  f.home_score,
  f.away_score
FROM fixtures f
JOIN leagues l ON l.league_id = f.league_id
JOIN teams ht  ON ht.team_id  = f.home_team_id
JOIN teams at  ON at.team_id  = f.away_team_id
LEFT JOIN venues v ON v.venue_id = f.venue_id
WHERE f.fixture_id = :fixture_id
""")

SQL_MATCH_STATS = text("""
/* Two rows (home/away). COALESCE all numeric stats to 0.
   pass_accuracy: prefer stored value; if NULL compute; then cast to int. */
WITH sides AS (
  SELECT fixture_id, home_team_id, away_team_id
  FROM fixtures
  WHERE fixture_id = :fixture_id
)
SELECT
  ms.fixture_id,
  ms.team_id,
  CASE
    WHEN ms.team_id = s.home_team_id THEN 'home'
    WHEN ms.team_id = s.away_team_id THEN 'away'
    ELSE 'other'
  END AS side,
  COALESCE(ms.total_shots,0)        AS total_shots,
  COALESCE(ms.shots_on_goal,0)      AS shots_on_goal,
  COALESCE(ms.shots_inside_box,0)   AS shots_inside_box,
  COALESCE(ms.shots_outside_box,0)  AS shots_outside_box,
  COALESCE(ms.corners,0)            AS corners,
  COALESCE(ms.offsides,0)           AS offsides,
  COALESCE(ms.fouls,0)              AS fouls,
  COALESCE(ms.total_passes,0)       AS total_passes,
  COALESCE(ms.passes_accurate,0)    AS passes_accurate,
  COALESCE(
    ms.pass_accuracy,
    CASE
      WHEN ms.total_passes > 0 AND ms.passes_accurate IS NOT NULL
      THEN ROUND(100.0 * ms.passes_accurate / ms.total_passes)
    END,
    0
  )::int                            AS pass_accuracy,
  COALESCE(ms.possession,0)         AS possession,
  COALESCE(ms.yellow_cards,0)       AS yellow_cards,
  COALESCE(ms.red_cards,0)          AS red_cards
FROM match_statistics ms
JOIN sides s ON s.fixture_id = ms.fixture_id
WHERE ms.fixture_id = :fixture_id
""")

SQL_MATCH_TIMELINE = text("""
/* Full timeline. Normalize types + details into 5 UI types.
   - Treat 'penalty' and 'own goal' tokens as goals (detail conveys specificity).
   - Accept both 'subst' and 'substitution' as substitutions.
   - Unrecognized tokens (e.g., 'missed penalty') fall back to 'var' (misc). */
WITH e AS (
  SELECT
    me.event_id,
    me.minute,
    COALESCE(me.minute_extra,0)               AS minute_extra,
    me.team_id,
    LOWER(TRIM(me.event_type))                AS et,   -- 'goal','card','var','subst','penalty','own goal','missed penalty',...
    LOWER(TRIM(COALESCE(me.event_detail,''))) AS ed,   -- 'yellow card','red card','normal goal','penalty','own goal','missed penalty',...
    me.player_id,
    me.assist_player_id
  FROM match_events me
  WHERE me.fixture_id = :fixture_id
)
SELECT
  e.event_id,
  e.minute,
  e.minute_extra,
  e.team_id,
  CASE
    WHEN e.et IN ('goal','penalty','own goal') THEN 'goal'
    WHEN e.et = 'card'  AND e.ed = 'yellow card' THEN 'yellow'
    WHEN e.et = 'card'  AND e.ed = 'red card'    THEN 'red'
    WHEN e.et IN ('subst','substitution')        THEN 'substitution'
    WHEN e.et = 'var'                             THEN 'var'
    ELSE 'var'
  END AS type,
  p.full_name  AS actor,
  pa.full_name AS assistant,
  NULLIF(e.ed,'') AS detail
FROM e
LEFT JOIN players p  ON p.player_id  = e.player_id
LEFT JOIN players pa ON pa.player_id = e.assist_player_id
ORDER BY e.minute, e.minute_extra, e.event_id
""")

# --- route --------------------------------------------------------------------

@router.get(
    "/{fixture_id}/details",
    response_model=MatchDetailsOut,
    response_model_exclude_none=True,
)
async def get_match_details(
    fixture_id: int,
    db: AsyncSession = Depends(get_session),
):
    try:
        # 1) Header (single row; also tells us finished/not)
        hdr = (await db.execute(SQL_MATCH_HEADER.bindparams(fixture_id=fixture_id))).mappings().first()
        if not hdr:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fixture not found")

        finished = _is_finished(hdr["raw_status"])
        status_label = _normalize_status(hdr["raw_status"])

        # 2) Stats (two rows → map to home/away; defaults already 0)
        stats_home = StatsSide()
        stats_away = StatsSide()
        for r in (await db.execute(SQL_MATCH_STATS.bindparams(fixture_id=fixture_id))).mappings().all():
            target = stats_home if r["side"] == "home" else stats_away if r["side"] == "away" else None
            if not target:
                continue  # ignore stray rows
            # Map DB column names → API property names
            target.shots_total       = r["total_shots"]
            target.shots_on_target   = r["shots_on_goal"]
            target.shots_inside_box  = r["shots_inside_box"]
            target.shots_outside_box = r["shots_outside_box"]
            target.corners           = r["corners"]
            target.offsides          = r["offsides"]
            target.fouls             = r["fouls"]
            target.passes_total      = r["total_passes"]
            target.pass_accuracy_pct = r["pass_accuracy"]
            target.possession_pct    = r["possession"]
            target.yellow            = r["yellow_cards"]
            target.red               = r["red_cards"]

        # 3) Timeline (normalize types; shorten player/assist names; unescape detail)
        events: List[EventOut] = []
        for e in (await db.execute(SQL_MATCH_TIMELINE.bindparams(fixture_id=fixture_id))).mappings().all():
            etype = e["type"]
            # type is already normalized by SQL CASE above
            events.append(
                EventOut(
                    minute=e["minute"],
                    minute_extra=e["minute_extra"] or 0,
                    team_id=e["team_id"],
                    type=etype,  # one of: goal, yellow, red, substitution, var
                    player=_short_name(e["actor"]),
                    assist=_short_name(e["assistant"]) if e["assistant"] else None,
                    detail=_clean_str(e["detail"]),
                )
            )

        # 4) Build response object
        payload = MatchDetailsOut(
            match_id=hdr["match_id"],
            season=hdr["season"],
            competition=Competition(
                id=hdr["competition_id"],
                name=_clean_str(hdr["competition_name"]) or "",
            ),
            round=_clean_str(hdr["round"]),
            date_utc=_iso_utc(hdr["date_utc"]),
            status=status_label,
            venue=Venue(
                name=_clean_str(hdr["venue_name"]),
                city=_clean_str(hdr["venue_city"]),
                country=_clean_str(hdr["venue_country"]),
            ),
            home=TeamMini(
                id=hdr["home_id"],
                name=_clean_str(hdr["home_name"]) or "",
                logo=_clean_str(hdr["home_logo"]),
            ),
            away=TeamMini(
                id=hdr["away_id"],
                name=_clean_str(hdr["away_name"]) or "",
                logo=_clean_str(hdr["away_logo"]),
            ),
            score=Score(
                fulltime=FulltimeScore(
                    home=hdr["home_score"] if finished else None,
                    away=hdr["away_score"] if finished else None,
                )
            ),
            events=events,
            stats=StatsOut(home=stats_home, away=stats_away),
        )

        logger.info(
            "matchDetails fixture_id=%s status=%s events=%d stats_home=%s stats_away=%s",
            fixture_id, payload.status, len(payload.events),
            bool(stats_home), bool(stats_away)
        )
        return payload

    except SQLAlchemyError:
        logger.exception("DB error in get_match_details fixture_id=%s", fixture_id)
        raise HTTPException(status_code=500, detail="Database error")
