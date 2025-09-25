"""
Async engine & session factory
──────────────────────────────
• Centralises DB connection details
• `get_session()` is imported by every route that needs the DB
"""
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

POSTGRES_URL = (
    "postgresql+asyncpg://"
    f"{os.getenv('DB_USER',     'football_pgadmin')}:"
    f"{os.getenv('DB_PASSWORD', 'Post123gres')}@"
    f"{os.getenv('DB_HOST',     'football-db.postgres.database.azure.com')}:"
    f"{os.getenv('DB_PORT',     '5432')}/"
    f"{os.getenv('DB_NAME',     'footysphere_db')}"
)

engine = create_async_engine(
    POSTGRES_URL,
    pool_pre_ping=True,   # ping before handing out connection → fewer stale-conn errors
    echo=False,           # flip to True for raw SQL debugging
)

SessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_session() -> AsyncSession:
    """FastAPI dependency → yields a session per request, then closes it."""
    async with SessionLocal() as session:
        yield session
