"""
News feed routes
────────────────
GET /news        → newest first, optional tab filter, keyset pagination
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from base64 import urlsafe_b64encode, urlsafe_b64decode
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

logger = logging.getLogger(__name__)
router = APIRouter(tags=["news"])

ALLOWED_TABS = {"transfers", "injuries", "matchreports"}

# ───────────────────────────
# Pydantic response models
# ───────────────────────────
class NewsItemOut(BaseModel):
    article_id: str
    tab: str
    title: str
    summary: Optional[str] = None
    image_url: Optional[str] = None
    source: str
    url: str
    published_at_utc: datetime

class NewsPageOut(BaseModel):
    items: List[NewsItemOut]
    next_cursor: Optional[str] = None

# ───────────────────────────
# Cursor helpers (opaque)
# ───────────────────────────
def _encode_cursor(ts: datetime, article_id: str) -> str:
    payload = {"ts": ts.astimezone(timezone.utc).isoformat(), "id": article_id}
    return urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")

def _decode_cursor(cursor: str) -> Tuple[datetime, str]:
    try:
        data = json.loads(urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8"))
        ts = datetime.fromisoformat(data["ts"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc), data["id"]
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid cursor"
        )

# ───────────────────────────
# Routes
# ───────────────────────────
@router.get("/news", response_model=NewsPageOut)
async def list_news(
    response: Response,
    tab: Optional[str] = Query(None, description="transfers|injuries|matchreports"),
    limit: int = Query(20, ge=1, le=50),
    cursor: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_session),
):
    """
    Returns newest articles. If `tab` is omitted, returns across all tabs.
    Keyset pagination via `cursor` (opaque).
    """
    if tab is not None and tab not in ALLOWED_TABS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid tab: {tab!r}. Allowed: {sorted(ALLOWED_TABS)}",
        )

    params = {"limit": limit}
    where_clauses = ["1=1"]
    if tab:
        where_clauses.append("tab = :tab")
        params["tab"] = tab

    if cursor:
        cur_ts, cur_id = _decode_cursor(cursor)
        where_clauses.append(
            "(published_at_utc < :cur_ts OR (published_at_utc = :cur_ts AND article_id < :cur_id))"
        )
        params["cur_ts"] = cur_ts
        params["cur_id"] = cur_id

    sql = text(f"""
        SELECT
            article_id,
            tab,
            title,
            summary,
            image_url,
            source,
            url,
            published_at_utc
        FROM news
        WHERE {' AND '.join(where_clauses)}
        ORDER BY published_at_utc DESC, article_id DESC
        LIMIT :limit
    """)

    try:
        result = await db.execute(sql, params)
        rows = list(result.mappings())
    except SQLAlchemyError as err:
        logger.exception("DB error in GET /news (tab=%s, limit=%s)", tab, limit)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error",
        ) from err

    items = [NewsItemOut(**row) for row in rows]

    next_cursor = None
    if len(items) == limit:
        last = items[-1]
        next_cursor = _encode_cursor(last.published_at_utc, last.article_id)

    # Light caching; safe to add now
    response.headers["Cache-Control"] = "public, max-age=60"

    return NewsPageOut(items=items, next_cursor=next_cursor)
