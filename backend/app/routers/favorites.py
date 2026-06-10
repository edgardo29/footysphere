from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session
from .auth import get_current_user  # reuse existing dependency


router = APIRouter(tags=["favorites"])


@router.post(
    "/users/me/favorites/teams/{team_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def add_favorite_team(
    team_id: int,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    # Validate team exists
    team_exists_sql = "SELECT 1 FROM teams WHERE team_id = :team_id"
    res = await db.execute(text(team_exists_sql), {"team_id": team_id})
    if res.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Team not found")

    insert_sql = """
        INSERT INTO user_favorite_teams (user_id, team_id)
        VALUES (:user_id, :team_id)
        ON CONFLICT (user_id, team_id) DO NOTHING
    """

    try:
        await db.execute(
            text(insert_sql),
            {"user_id": current_user["id"], "team_id": team_id},
        )
        await db.commit()
        return
    except Exception:
        await db.rollback()
        raise


@router.delete(
    "/users/me/favorites/teams/{team_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_favorite_team(
    team_id: int,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    # Validate team exists
    team_exists_sql = "SELECT 1 FROM teams WHERE team_id = :team_id"
    res = await db.execute(text(team_exists_sql), {"team_id": team_id})
    if res.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Team not found")

    delete_sql = """
        DELETE FROM user_favorite_teams
        WHERE user_id = :user_id AND team_id = :team_id
    """

    try:
        await db.execute(
            text(delete_sql),
            {"user_id": current_user["id"], "team_id": team_id},
        )
        await db.commit()
        return
    except Exception:
        await db.rollback()
        raise


@router.get("/users/me/favorites/teams")
async def list_favorite_teams(
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    sql = """
        SELECT
            t.team_id,
            t.team_name,
            t.team_country,
            t.team_logo_url,
            t.venue_id
        FROM user_favorite_teams uft
        JOIN teams t ON t.team_id = uft.team_id
        WHERE uft.user_id = :user_id
        ORDER BY t.team_name
    """

    res = await db.execute(text(sql), {"user_id": current_user["id"]})
    return res.mappings().all()