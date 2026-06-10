"""
Auth routes (register/login/current-user)
─────────────────────────────────────────
Implements a basic JWT auth flow:

• POST /api/v1/auth/register  – create a user account
• POST /api/v1/auth/login     – verify credentials, return access token
• GET  /api/v1/users/me       – return the current user (requires Bearer token)

DB expectations (users table):
• users.user_id (PK)
• users.email (UNIQUE, NOT NULL)
• users.password_hash (NOT NULL)
• users.load_date (DEFAULT now())
• users.upd_date (DEFAULT now())

Notes
• Uses async SQLAlchemy session + raw SQL via sqlalchemy.text(), consistent with your existing routers.
• Passwords are never stored; only password_hash.
• JWT config is read from OS env vars, and (optionally) from repo-root `env` file.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import get_session

logger = logging.getLogger(__name__)
router = APIRouter(tags=["auth"])

# ────────────────────────────────────────────────
# Optional: load JWT config from repo-root `env`
# (matches your existing pattern in main.py, but scoped to auth)
# ────────────────────────────────────────────────
def _load_env_file_if_present() -> None:
    """
    Loads key=value lines from repo-root `env` file into os.environ
    only if the key is not already set in the process environment.
    """
    # auth.py lives at: backend/app/routers/auth.py
    # repo root is 3 levels up from backend/app/routers: parents[3]
    try:
        repo_root = Path(__file__).resolve().parents[3]
    except Exception:
        return

    env_file = repo_root / "env"
    if not env_file.exists():
        return

    try:
        for line in env_file.read_text().splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            key, _, val = s.partition("=")
            key = key.strip()
            val = val.strip()
            if not key:
                continue
            if key not in os.environ and val:
                os.environ[key] = val
    except Exception as e:
        logger.warning("Failed reading env file %s: %s", env_file, e)


_load_env_file_if_present()

# ────────────────────────────────────────────────
# JWT + password hashing config
# ────────────────────────────────────────────────
JWT_SECRET = os.environ.get("JWT_SECRET")
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", "60"))

if not JWT_SECRET:
    raise RuntimeError(
        "JWT_SECRET is not set. Add JWT_SECRET=<long_random_string> to your repo-root env file or OS env var."
    )

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=True)


# ────────────────────────────────────────────────
# Pydantic models (request/response shapes)
# ────────────────────────────────────────────────
class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=70)


class RegisterOut(BaseModel):
    id: int
    email: EmailStr


class LoginIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=70)


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MeOut(BaseModel):
    id: int
    email: EmailStr


# ────────────────────────────────────────────────
# Internal helpers
# ────────────────────────────────────────────────
def _hash_password(password: str) -> str:
    return pwd_context.hash(password)


def _verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


def _create_access_token(user_id: int, email: str) -> str:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=JWT_EXPIRE_MINUTES)

    payload = {
        "sub": str(user_id),
        "email": email,
        "iat": int(now.timestamp()),
        "exp": exp,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


async def _get_user_by_id(db: AsyncSession, user_id: int):
    sql = text(
        """
        SELECT user_id AS id,
               email
        FROM   users
        WHERE  user_id = :user_id
        """
    )
    return (await db.execute(sql, {"user_id": user_id})).mappings().first()


async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_session),
):
    """
    Dependency for protected routes.
    Validates Bearer token, extracts user_id, then loads the user from DB.
    """
    token = creds.credentials

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        sub = payload.get("sub")
        if not sub:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token (missing subject).")
        user_id = int(sub)
    except (JWTError, ValueError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token.")

    try:
        user = await _get_user_by_id(db, user_id)
    except SQLAlchemyError as err:
        logger.exception("DB error while fetching current user")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error") from err

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found for token.")

    return user  # mapping with keys: id, email


# ────────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────────
@router.post("/auth/register", response_model=RegisterOut, status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterIn, db: AsyncSession = Depends(get_session)):
    """
    Create a new user account.

    Behavior:
    • Email normalized to lowercase.
    • Relies on DB UNIQUE(email) to prevent duplicates.
    • If email already exists → 409 Conflict.
    """
    email = payload.email.strip().lower()
    password_hash = _hash_password(payload.password)

    insert_sql = text(
        """
        INSERT INTO users (email, password_hash)
        VALUES (:email, :password_hash)
        RETURNING user_id AS id, email
        """
    )

    try:
        row = (await db.execute(insert_sql, {"email": email, "password_hash": password_hash})).mappings().first()
        await db.commit()
        return row

    except IntegrityError:
        # Most likely UNIQUE(email) violation
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered.")
    except SQLAlchemyError as err:
        await db.rollback()
        logger.exception("DB error during register")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error") from err


@router.post("/auth/login", response_model=TokenOut)
async def login(payload: LoginIn, db: AsyncSession = Depends(get_session)):
    """
    Verify credentials and return a Bearer access token.

    Behavior:
    • Email normalized to lowercase.
    • If invalid credentials → 401 Unauthorized (do not reveal which part failed).
    """
    email = payload.email.strip().lower()

    sql = text(
        """
        SELECT user_id AS id,
               email,
               password_hash
        FROM   users
        WHERE  email = :email
        """
    )

    try:
        user = (await db.execute(sql, {"email": email})).mappings().first()
    except SQLAlchemyError as err:
        logger.exception("DB error during login")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error") from err

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    if not _verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    token = _create_access_token(user_id=user["id"], email=user["email"])
    return {"access_token": token, "token_type": "bearer"}


@router.get("/users/me", response_model=MeOut)
async def me(current_user=Depends(get_current_user)):
    """
    Return the currently authenticated user.
    """
    return current_user