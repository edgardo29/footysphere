"""
App entry-point
───────────────
• Sets up logging
• Mounts CORS
• Includes routers
"""
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import homepage
from .routers import leaguesPage
from .routers import teamPage
from .routers import matchDetailsPage  
from .routers import leagueModal 



# ─────────── logging config ───────────
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "footysphere.log"),
        logging.StreamHandler(),          # also print to console
    ],
)

# ─────────── FastAPI instance ─────────
app = FastAPI(
    title="Footysphere API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS – allow React dev servers
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev
        "http://localhost:3000",  # CRA/dev if you still use it
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health route (tiny, no DB)
@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}

# Plug in routers
app.include_router(homepage.router)
app.include_router(leaguesPage.router)
app.include_router(teamPage.router)
app.include_router(matchDetailsPage.router)
app.include_router(leagueModal.router)


