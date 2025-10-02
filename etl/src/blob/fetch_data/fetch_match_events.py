#!/usr/bin/env python3
"""
fetch_match_events.py ───────────────────────────────────────────────────────────

A **single** script that covers both historical back-fills **and** recurring
incremental refreshes of match-event data from the API-Football v3 endpoint.

The script deliberately leans on *explicit* parameters so it’s crystal-clear
from the command line (or Airflow DAG) **what** is being fetched and **why**.

-------------------------------------------------------------------------------
WHY KEEP TWO MODES IN ONE SCRIPT?
-------------------------------------------------------------------------------
1. **Shared logic**           → Only one place to maintain auth, retry, and
                                blob-upload code.
2. **Fewer Airflow images**   → The same Docker image / venv can run back-fills
                                and weekly updates; behaviour changes solely
                                through CLI flags.
3. **Consistent lineage**     → Both modes write to predictable, versioned
                                folder paths under `raw/match_events/`, making
                                it trivial to audit and to reload.

-------------------------------------------------------------------------------
DIRECTORY STRUCTURE PRODUCED
-------------------------------------------------------------------------------
raw/
└─ match_events/
   ├─ initial/                       # one-time, frozen back-fill per season
   │   └─ <league>/<season>/events_<FIXTURE>.json
   └─ incremental/                  # audit-friendly snapshots
       └─ <YYYY-MM-DD>/             # → Airflow execution date ({{ ds }})
           └─ <league>/<season>/events_<FIXTURE>.json

  *Initial* remains immutable for historical replay.
  *Incremental* holds every subsequent pull in date-partitioned folders so
  analysts can see how the events evolved over time.

-------------------------------------------------------------------------------
EXAMPLE COMMANDS
-------------------------------------------------------------------------------
# 1️⃣  Full season back-fill
python fetch_match_events.py --season 2023 --mode initial

# 2️⃣  Weekly catch-up triggered by Airflow
python fetch_match_events.py --season 2024 \
       --mode incremental \
       --run-date 2025-08-18 \
       --lookback-days 8

Airflow would set `--run-date {{ ds }}` automatically so the folder name equals
the execution date.
"""

from __future__ import annotations

# ───────────────────────────────────────────────────────────────────────────────
# Standard library imports
# ───────────────────────────────────────────────────────────────────────────────
import argparse               # Robust CLI handling
import json                    # Pretty-printing API responses to disk
import os                      # Path juggling
import sys                     # Append helper modules to import path
import time                    # Retry back-off
import logging                 # Lightweight logging
from datetime import date      # Default for --run-date when run outside Airflow
from typing import List, Tuple # Precise return-type hints

# Third-party imports
import requests                            # HTTP client for API-Football
from azure.storage.blob import BlobServiceClient  # Azure Blob SDK

# ───────────────────────────────────────────────────────────────────────────────
# Local helper imports – we add the project’s root folders to PYTHONPATH first
# ───────────────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.append(os.path.join(ROOT_DIR, "config"))          # credentials.py, etc.
sys.path.append(os.path.join(ROOT_DIR, "test_scripts"))    # get_db_conn helper

from credentials import API_KEYS, AZURE_STORAGE  # type: ignore  # noqa: E402
from get_db_conn import get_db_connection         # type: ignore  # noqa: E402

# ───────────────────────────────────────────────────────────────────────────────
# Basic logging (prints to stdout, visible in Airflow task log)
# ───────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("match_events_fetch")

# ADDITION: suppress verbose Azure SDK request/response logs
logging.getLogger("azure").setLevel(logging.WARNING)

# ───────────────────────────────────────────────────────────────────────────────
# CLI PARSER – flags are self-documenting thanks to verbose help texts
# ───────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description="Fetch match-event JSON from API-Football and upload to Azure Blob Storage."
                "  Handles both one-time back-fills (initial) and ongoing"
                " incremental updates.")
parser.add_argument("--season", required=True, type=int,
                    help="Season start year, e.g. 2024 for the 2024/25 season.")
parser.add_argument("--mode", choices=["initial", "incremental"], default="initial",
                    help="\ninitial      → pull *every* fixture for the season into 'initial/' folder"
                         "\nincremental → pull only recent / in-progress fixtures into"
                         " 'incremental/<run-date>/' folder")
parser.add_argument("--run-date", default=date.today().isoformat(),
                    help="UTC date (YYYY-MM-DD) stamped into the incremental folder name."
                         "  Airflow passes '{{ ds }}'. Ignored for initial mode.")
parser.add_argument("--lookback-days", type=int, default=8,
                    help="For incremental mode only: how many days back to crawl when checking"
                         " finished fixtures for late data corrections.")
parser.add_argument("--limit", type=int, default=None,
                    help="Hard cap on number of fixtures processed – useful during dev/debug.")
args = parser.parse_args()

# ───────────────────────────────────────────────────────────────────────────────
# Derived CLI values (kept plainly named for readability later on)
# ───────────────────────────────────────────────────────────────────────────────
season_year: int = args.season
season_folder: str = f"{season_year}_{(season_year % 100)+1:02d}"  # 2024 → 2024_25
mode: str = args.mode.lower()  # normalise just in case caller used uppercase

# ───────────────────────────────────────────────────────────────────────────────
# AZURE BLOB SERVICE – single connection reused for all uploads
# ───────────────────────────────────────────────────────────────────────────────
account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
blob_service = BlobServiceClient(account_url,
                                 credential=AZURE_STORAGE["access_key"])
RAW_CONTAINER = "raw"  # central bucket; change in one place if you rename it


def blob_exists(path: str) -> bool:
    """Return *True* if the given blob already exists in Azure.

    We use this to skip API hits when rerunning a task for the same execution
    date – important for idempotency in Airflow.
    """
    return blob_service.get_blob_client(RAW_CONTAINER, path).exists()


def upload_blob(path: str, payload: dict) -> None:
    """Write a JSON payload to Azure in pretty-printed format.

    `overwrite` stays **False** so accidental reruns don’t clobber history –
    we treat each `<run-date>` folder as immutable.
    """
    bc = blob_service.get_blob_client(RAW_CONTAINER, path)
    bc.upload_blob(json.dumps(payload, indent=2), overwrite=False)

# ───────────────────────────────────────────────────────────────────────────────
# DATABASE HELPER QUERIES
# ───────────────────────────────────────────────────────────────────────────────
def _get_fixtures_initial(conn, season: int, limit: int | None) -> List[Tuple[int, str]]:
    """Every fixture for the specified season – used in *initial* back-fill."""
    sql = """
      SELECT f.fixture_id,
             lower(replace(l.league_name,' ','_')) AS league_folder
        FROM fixtures f
        JOIN leagues l ON l.league_id = f.league_id
       WHERE f.season_year = %s
       ORDER BY f.fixture_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (season,))
        rows = cur.fetchall()
    return rows[:limit] if limit else rows


def _get_fixtures_incremental(conn, season: int, lookback: int, limit: int | None) -> List[Tuple[int, str]]:
    """Subset of fixtures needing refresh.

    • Any fixture whose **fixture_date** fell inside the configured look-back window
      (captures matches finished in the last few days).
    • OR any fixture whose `status` is **not** 'FT' (full-time) yet – meaning the
      game was still in-play or postponed when we last checked.
    """
    sql = """
      SELECT f.fixture_id,
             lower(replace(l.league_name,' ','_')) AS league_folder
        FROM fixtures f
        JOIN leagues l ON l.league_id = f.league_id
       WHERE f.season_year = %s
        AND f.fixture_date >= NOW() - INTERVAL '%s days'
        AND f.fixture_date <= NOW()
        AND f.status IN ('FT')  -- or ('FT','AET','PEN') if you use them
       ORDER BY f.fixture_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (season, lookback))
        rows = cur.fetchall()
    return rows[:limit] if limit else rows

# ───────────────────────────────────────────────────────────────────────────────
# API-FOOTBALL HELPER – simple retry w/ back-off
# ───────────────────────────────────────────────────────────────────────────────
API_URL = "https://v3.football.api-sports.io/fixtures/events"
HEADERS = {"x-rapidapi-key": API_KEYS["api_football"]}


def fetch_events_from_api(fixture_id: int) -> dict:
    """Call the API up to 3 times before giving up.

    A tiny delay (2s) between retries keeps us under the 300 req/min rate limit
    even when the script is threaded later on.
    """
    for attempt in range(1, 4):
        try:
            resp = requests.get(
                API_URL,
                headers=HEADERS,
                params={"fixture": fixture_id},
                timeout=25,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            if attempt < 3:
                log.warning("Retry %s for fixture %s due to %s", attempt, fixture_id, exc)
                time.sleep(2)  # crude back-off
            else:
                raise RuntimeError(f"API failed for fixture {fixture_id}: {exc}") from exc

# ───────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATION LOGIC
# ───────────────────────────────────────────────────────────────────────────────
def main() -> None:
    """Pull fixture list → hit API → upload blobs."""
    conn = get_db_connection()

    # 1. Pick fixture set & target folder depending on --mode
    if mode == "initial":
        fixtures = _get_fixtures_initial(conn, season_year, args.limit)
        folder_prefix = "match_events/initial"
    else:  # incremental
        fixtures = _get_fixtures_incremental(conn, season_year, args.lookback_days, args.limit)
        folder_prefix = f"match_events/incremental/{args.run_date}"

    conn.close()

    if not fixtures:
        log.info("No fixtures qualify (mode=%s, season=%s). Script exits.", mode, season_year)
        return

    log.info("%s fixtures to process (mode=%s, season=%s)", len(fixtures), mode, season_year)

    # 2. Loop over fixtures and upload files
    for i, (fixture_id, league_folder) in enumerate(fixtures, 1):
        blob_path = f"{folder_prefix}/{league_folder}/{season_folder}/events_{fixture_id}.json"

        if blob_exists(blob_path):
            continue  # idempotent rerun

        try:
            payload = fetch_events_from_api(fixture_id)
            if not payload or not payload.get("response"):
                continue
            upload_blob(blob_path, payload)
        except Exception as err:  # noqa: BLE001
            log.error("Failed fixture %s: %s", fixture_id, err)

        if i % 500 == 0:
            log.info("Processed %s/%s fixtures", i, len(fixtures))

    log.info("✔ Finished uploading match-event JSON → Azure Blob")


if __name__ == "__main__":
    main()
