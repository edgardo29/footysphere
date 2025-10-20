"""
App entry-point
───────────────
• Sets up logging
• Loads CORS config from repo-root `env` file (no hardcoding)
• Mounts CORS
• Includes routers
"""
import logging
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import homepage
from .routers import leaguesPage
from .routers import teamPage
from .routers import matchDetailsPage
from .routers import leagueModal
from .routers import news



# ─────────── logging config ───────────
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "footysphere.log"),
        logging.StreamHandler(),  # also print to console
    ],
)
logger = logging.getLogger("footysphere.main")

# ─────────── load config (CORS) ───────────
# Repo root is two levels up from this file: backend/app/main.py -> backend -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = REPO_ROOT / "env"

# Defaults for local development; we’ll extend these from config
ALLOWED_ORIGINS = {
    "http://localhost:5173",
    "http://localhost:3000",
}

# 1) Load from OS env var if present (lets you override in process manager)
os_env_origins = os.environ.get("ALLOWED_ORIGINS")
if os_env_origins:
    ALLOWED_ORIGINS.update({o.strip() for o in os_env_origins.split(",") if o.strip()})

# 2) Load from repo-root env file (footysphere-repo/env)
if ENV_FILE.exists():
    try:
        for line in ENV_FILE.read_text().splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            key, _, val = s.partition("=")
            if key.strip() == "ALLOWED_ORIGINS":
                ALLOWED_ORIGINS.update({o.strip() for o in val.split(",") if o.strip()})
    except Exception as e:
        logger.warning("Failed reading %s: %s", ENV_FILE, e)

# Normalize to a sorted list for deterministic logging / FastAPI
ALLOWED_ORIGINS = sorted(ALLOWED_ORIGINS)
logger.info("CORS allow_origins: %s", ALLOWED_ORIGINS)

# ─────────── FastAPI instance ─────────
app = FastAPI(
    title="Footysphere API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ─────────── CORS (strict; expand later if needed) ─────────
# For HomePage reads we only need GET and basic headers.
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,                # set True only if you actually use cookies
    allow_methods=["GET"],                  # expand if you later add POST/PUT/DELETE from the browser
    allow_headers=["Accept", "Content-Type"],
    max_age=86400,                          # cache preflight for a day
)

# ─────────── health route (tiny, no DB) ─────────
@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}

# ─────────── routers ─────────
app.include_router(homepage.router)
app.include_router(leaguesPage.router)
app.include_router(teamPage.router)
app.include_router(matchDetailsPage.router)
app.include_router(leagueModal.router)
app.include_router(news.router)

