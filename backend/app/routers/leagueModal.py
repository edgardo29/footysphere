# backend/routers/leagueModal.py
from __future__ import annotations

import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import text, bindparam
from sqlalchemy.types import String, Integer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from ..db import get_session

logger = logging.getLogger(__name__)

# Match other routers: no '/api' here
router = APIRouter(prefix="/leagueModal", tags=["leagueModal"])

# ---------- Models ----------
class LeagueItem(BaseModel):
    id: int
    name: str
    league_logo_url: Optional[str] = None
    country: Optional[str] = None

class ListOut(BaseModel):
    items: List[LeagueItem]
    next_cursor: Optional[str] = None  # stringified next page number if more pages exist

# ---------- SQL (typed bind params to avoid asyncpg type ambiguity) ----------
SQL_POPULAR = (
    text("""
        SELECT
          l.league_id      AS id,
          l.league_name    AS name,
          l.league_logo_url,
          l.league_country AS country
        FROM leagues l
        ORDER BY l.league_name
        LIMIT :limit
    """)
    .bindparams(bindparam("limit", type_=Integer))
)

SQL_LIST = (
    text("""
        SELECT
          l.league_id      AS id,
          l.league_name    AS name,
          l.league_logo_url,
          l.league_country AS country
        FROM leagues l
        WHERE (:country IS NULL OR l.league_country ILIKE CAST(:country AS TEXT))
        ORDER BY l.league_name
        OFFSET :offset
        LIMIT :limit
    """)
    .bindparams(
        bindparam("country", type_=String),
        bindparam("offset", type_=Integer),
        bindparam("limit", type_=Integer),
    )
)

SQL_SEARCH = (
    text("""
        SELECT
          l.league_id      AS id,
          l.league_name    AS name,
          l.league_logo_url,
          l.league_country AS country
        FROM leagues l
        WHERE
          (:country IS NULL OR l.league_country ILIKE CAST(:country AS TEXT))
          AND (
            l.league_name    ILIKE :pattern
            OR l.league_country ILIKE :pattern
          )
        ORDER BY
          CASE WHEN LOWER(l.league_name) = LOWER(:q) THEN 0 ELSE 1 END,
          l.league_name ASC
        LIMIT :limit
    """)
    .bindparams(
        bindparam("q", type_=String),
        bindparam("pattern", type_=String),
        bindparam("country", type_=String),
        bindparam("limit", type_=Integer),
    )
)

# ---------- Routes ----------
@router.get("/popular", response_model=List[LeagueItem])
async def get_popular_leagues(
    limit: int = Query(5, ge=1, le=50),
    db: AsyncSession = Depends(get_session),
):
    try:
        rows = (await db.execute(SQL_POPULAR, {"limit": limit})).mappings().all()
        return [LeagueItem(**r) for r in rows]
    except SQLAlchemyError:
        logger.exception("DB error in GET /leagueModal/popular")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")

@router.get("/list", response_model=ListOut, response_model_exclude_none=True)
async def list_leagues(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    country: Optional[str] = Query(None, description="Optional country filter (ILIKE)"),
    db: AsyncSession = Depends(get_session),
):
    try:
        offset = (page - 1) * page_size
        country_filter = f"%{country}%" if country else None

        rows = (
            await db.execute(
                SQL_LIST,
                {"offset": offset, "limit": page_size, "country": country_filter},
            )
        ).mappings().all()

        next_cursor = str(page + 1) if len(rows) == page_size else None
        return ListOut(items=[LeagueItem(**r) for r in rows], next_cursor=next_cursor)
    except SQLAlchemyError:
        logger.exception("DB error in GET /leagueModal/list")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")

@router.get("/search", response_model=ListOut, response_model_exclude_none=True)
async def search_leagues(
    q: str = Query(..., min_length=2, description="Search term (min 2 chars)"),
    limit: int = Query(20, ge=1, le=50),
    country: Optional[str] = Query(None, description="Optional country filter (ILIKE)"),
    db: AsyncSession = Depends(get_session),
):
    try:
        pattern = f"%{q}%"
        country_filter = f"%{country}%" if country else None

        rows = (
            await db.execute(
                SQL_SEARCH,
                {"q": q, "pattern": pattern, "country": country_filter, "limit": limit},
            )
        ).mappings().all()

        return ListOut(items=[LeagueItem(**r) for r in rows], next_cursor=None)
    except SQLAlchemyError:
        logger.exception("DB error in GET /leagueModal/search q=%s", q)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
