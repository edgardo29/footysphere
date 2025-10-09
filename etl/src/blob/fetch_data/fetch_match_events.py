#!/usr/bin/env python3
"""
fetch_match_events.py

A single script that covers both historical back-fills and recurring incremental
refreshes of match-event data from the API-Football v3 endpoint.

Directory structure produced
----------------------------
raw/
└─ match_events/
   └─ <league_folder>/<season_folder>/<mode>/<mode>_<YYYYMMDD_HHMMSS>Z/
      └─ events_<FIXTURE>.json

Where:
- <league_folder>   → from DB: league_catalog.folder_alias (already includes the ID)
- <season_folder>   → YYYY_YY  (e.g., 2025_26)
- <mode>            → initial | incremental
- <YYYYMMDD_HHMMSS> → UTC timestamp per run (fresh snapshot every run)

Usage examples
--------------
# 1) Full season back-fill (auto season per league from DB) for specific leagues
python fetch_match_events.py --mode initial --league-ids 94 203

# 2) Incremental (windowed by finished fixtures in last N days), specific leagues
python fetch_match_events.py --mode incremental --run-date 2025-10-09 --lookback-days 15 --league-ids 94 203

# 3) Force one explicit season for all targeted leagues
python fetch_match_events.py --season 2025 --mode initial --league-ids 94 203
"""

from __future__ import annotations

# ───────────────────────────────────────────────────────────────────────────────
# Stdlib
# ───────────────────────────────────────────────────────────────────────────────
import argparse
import json
import os
import sys
import time
import logging
from datetime import date
from typing import List, Tuple

# ───────────────────────────────────────────────────────────────────────────────
# Third-party
# ───────────────────────────────────────────────────────────────────────────────
import requests
from azure.storage.blob import BlobServiceClient  # type: ignore

# ───────────────────────────────────────────────────────────────────────────────
# Local imports / paths
# ───────────────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.append(os.path.join(ROOT_DIR, "config"))
sys.path.append(os.path.join(ROOT_DIR, "test_scripts"))

from credentials import API_KEYS, AZURE_STORAGE  # type: ignore  # noqa: E402
from get_db_conn import get_db_connection         # type: ignore  # noqa: E402

# ───────────────────────────────────────────────────────────────────────────────
# Logging — minimal: start + per-league saved + total
# ───────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("match_events_fetch")
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ───────────────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description="Fetch match-event JSON from API-Football and upload to Azure Blob Storage. "
                "Season is auto-resolved per league from DB unless --season is provided."
)
parser.add_argument(
    "--season",
    type=int,
    help="Optional season start year (e.g., 2025 for 2025/26). If omitted, season is chosen per-league from DB.",
)
parser.add_argument(
    "--mode",
    choices=["initial", "incremental"],
    default="initial",
    help="initial → full season; incremental → finished fixtures in the recent lookback window."
)
parser.add_argument(
    "--run-date",
    default=date.today().isoformat(),
    help="For logging/partitioning; not used in queries. Format YYYY-MM-DD. Defaults to today."
)
parser.add_argument(
    "--lookback-days",
    type=int,
    default=8,
    help="Incremental window length in days (finished fixtures)."
)
parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Max fixtures to process (for testing)."
)
parser.add_argument(
    "--league-ids",
    nargs="+",
    type=int,
    metavar="LEAGUE_ID",
    help="Optional space-separated league IDs, e.g. --league-ids 94 203. "
         "If omitted, all enabled leagues (league_catalog.is_enabled) are used."
)
args = parser.parse_args()
mode: str = args.mode.lower()

# ───────────────────────────────────────────────────────────────────────────────
# Azure Blob helpers
# ───────────────────────────────────────────────────────────────────────────────
account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
blob_service = BlobServiceClient(account_url, credential=AZURE_STORAGE["access_key"])
RAW_CONTAINER = "raw"

def blob_exists(path: str) -> bool:
    return blob_service.get_blob_client(RAW_CONTAINER, path).exists()

def upload_blob(path: str, payload: dict) -> None:
    blob_service.get_blob_client(RAW_CONTAINER, path)\
        .upload_blob(json.dumps(payload, indent=2), overwrite=False)

# ───────────────────────────────────────────────────────────────────────────────
# API-Football
# ───────────────────────────────────────────────────────────────────────────────
API_URL = "https://v3.football.api-sports.io/fixtures/events"
HEADERS = {"x-rapidapi-key": API_KEYS["api_football"]}

def fetch_events_from_api(fixture_id: int) -> dict:
    for attempt in range(1, 4):
        try:
            r = requests.get(API_URL, headers=HEADERS, params={"fixture": fixture_id}, timeout=25)
            r.raise_for_status()
            return r.json()
        except Exception as exc:  # noqa: BLE001
            if attempt < 3:
                time.sleep(2)
            else:
                raise RuntimeError(f"API failed for fixture {fixture_id}: {exc}") from exc

# ───────────────────────────────────────────────────────────────────────────────
# DB helpers (named params only; uses league_catalog + league_seasons)
# Returns tuples: (fixture_id, league_folder, season_folder)
# ───────────────────────────────────────────────────────────────────────────────
def _fixtures_initial(conn, forced_season: int | None, limit: int | None,
                      league_ids: list[int] | None) -> List[Tuple[int, str, str]]:
    """
    Full-season back-fill candidates. season_folder is YYYY_YY (e.g., 2025_26).
    """
    use_filter = bool(league_ids)

    scope_cte = (
        """
        WITH scope AS (
          SELECT lc.league_id,
                 lc.folder_alias AS league_folder,
                 lc.last_season
            FROM league_catalog lc
           WHERE lc.is_enabled = TRUE
        )
        """
        if not use_filter else
        """
        WITH scope AS (
          SELECT lc.league_id,
                 lc.folder_alias AS league_folder,
                 lc.last_season
            FROM league_catalog lc
            JOIN (SELECT unnest(%(ids)s::int[]) AS league_id) i
              ON i.league_id = lc.league_id
        )
        """
    )

    season_year_expr = (
        "COALESCE(%(forced_season)s,"
        "  COALESCE( "
        "    (SELECT ls1.season_year FROM league_seasons ls1 "
        "      WHERE ls1.league_id = s.league_id AND ls1.is_current = TRUE "
        "      ORDER BY ls1.season_year DESC LIMIT 1), "
        "    (SELECT ls2.season_year FROM league_seasons ls2 "
        "      WHERE ls2.league_id = s.league_id "
        "        AND CURRENT_DATE BETWEEN ls2.start_date AND ls2.end_date "
        "      ORDER BY ls2.season_year DESC LIMIT 1), "
        "    (SELECT MAX(ls3.season_year) FROM league_seasons ls3 "
        "      WHERE ls3.league_id = s.league_id), "
        "    s.last_season "
        "  ) "
        ")"
    )

    sql = f"""
    {scope_cte},
    chosen AS (
      SELECT s.league_id,
             s.league_folder,
             {season_year_expr} AS season_year
        FROM scope s
    )
    SELECT f.fixture_id,
           c.league_folder,
           to_char(c.season_year, 'FM9999') || '_' || to_char(mod(c.season_year, 100) + 1, 'FM00') AS season_folder
      FROM fixtures f
      JOIN chosen c
        ON c.league_id = f.league_id
       AND c.season_year = f.season_year
     WHERE c.season_year IS NOT NULL
     ORDER BY f.fixture_id;
    """

    params: dict = {"forced_season": forced_season}
    if use_filter:
        params["ids"] = league_ids

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return rows[:limit] if limit else rows


def _fixtures_incremental(conn, forced_season: int | None, lookback_days: int, limit: int | None,
                          league_ids: list[int] | None) -> List[Tuple[int, str, str]]:
    """
    Finished-fixture window (lookback) candidates. season_folder is YYYY_YY (e.g., 2025_26).
    """
    use_filter = bool(league_ids)

    scope_cte = (
        """
        WITH scope AS (
          SELECT lc.league_id,
                 lc.folder_alias AS league_folder,
                 lc.last_season
            FROM league_catalog lc
           WHERE lc.is_enabled = TRUE
        )
        """
        if not use_filter else
        """
        WITH scope AS (
          SELECT lc.league_id,
                 lc.folder_alias AS league_folder,
                 lc.last_season
            FROM league_catalog lc
            JOIN (SELECT unnest(%(ids)s::int[]) AS league_id) i
              ON i.league_id = lc.league_id
        )
        """
    )

    season_year_expr = (
        "COALESCE(%(forced_season)s,"
        "  COALESCE( "
        "    (SELECT ls1.season_year FROM league_seasons ls1 "
        "      WHERE ls1.league_id = s.league_id AND ls1.is_current = TRUE "
        "      ORDER BY ls1.season_year DESC LIMIT 1), "
        "    (SELECT ls2.season_year FROM league_seasons ls2 "
        "      WHERE ls2.league_id = s.league_id "
        "        AND CURRENT_DATE BETWEEN ls2.start_date AND ls2.end_date "
        "      ORDER BY ls2.season_year DESC LIMIT 1), "
        "    (SELECT MAX(ls3.season_year) FROM league_seasons ls3 "
        "      WHERE ls3.league_id = s.league_id), "
        "    s.last_season "
        "  ) "
        ")"
    )

    sql = f"""
    {scope_cte},
    chosen AS (
      SELECT s.league_id,
             s.league_folder,
             {season_year_expr} AS season_year
        FROM scope s
    )
    SELECT f.fixture_id,
           c.league_folder,
           to_char(c.season_year, 'FM9999') || '_' || to_char(mod(c.season_year, 100) + 1, 'FM00') AS season_folder
      FROM fixtures f
      JOIN chosen c
        ON c.league_id = f.league_id
       AND c.season_year = f.season_year
     WHERE c.season_year IS NOT NULL
       AND f.fixture_date >= NOW() - make_interval(days => %(lookback)s)
       AND f.fixture_date <= NOW()
       AND f.status IN ('FT')
     ORDER BY f.fixture_id;
    """

    params: dict = {"forced_season": forced_season, "lookback": int(lookback_days)}
    if use_filter:
        params["ids"] = league_ids

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return rows[:limit] if limit else rows

# ───────────────────────────────────────────────────────────────────────────────
# Main — unified, clean pathing & minimal logs
# ───────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # fresh UTC snapshot every run
    snapshot_ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    run_segment = f"{mode}/{mode}_{snapshot_ts}Z"  # e.g., incremental/incremental_20251009_163109Z

    season_scope = (f"{args.season}" if args.season is not None else "per-league (DB)")
    leagues_scope = args.league_ids if args.league_ids else "ALL_ENABLED"

    if args.league_ids:
        log.info("Start %s | leagues=%s | season=%s | dest=raw/match_events/<league>/<season>/%s",
                 mode, args.league_ids, season_scope, run_segment)
    else:
        log.info("Start %s | leagues=ALL_ENABLED | season=%s | dest=raw/match_events/<league>/<season>/%s",
                 mode, season_scope, run_segment)

    conn = get_db_connection()
    if mode == "initial":
        fixtures = _fixtures_initial(
            conn=conn,
            forced_season=args.season,
            limit=args.limit,
            league_ids=args.league_ids,
        )
    else:
        fixtures = _fixtures_incremental(
            conn=conn,
            forced_season=args.season,
            lookback_days=args.lookback_days,
            limit=args.limit,
            league_ids=args.league_ids,
        )
    conn.close()

    # Only report leagues where we actually saved new files.
    saved_by: dict[tuple[str, str], int] = {}  # {(league_folder, season_folder): count}
    saved_total = 0

    for fixture_id, league_folder, season_folder in fixtures:
        if not season_folder:
            continue

        blob_path = (
            f"match_events/{league_folder}/{season_folder}/"
            f"{run_segment}/events_{fixture_id}.json"
        )

        if blob_exists(blob_path):
            continue  # idempotent

        try:
            payload = fetch_events_from_api(fixture_id)
            if not payload or not payload.get("response"):
                continue
            upload_blob(blob_path, payload)
            key = (league_folder, season_folder)
            saved_by[key] = saved_by.get(key, 0) + 1
            saved_total += 1
        except Exception:
            # keep logs focused on what was saved
            pass

    if saved_total == 0:
        if args.league_ids:
            log.info("Finished — no new event files saved for leagues=%s.", args.league_ids)
        else:
            log.info("Finished — no new event files saved.")
        return

    if mode == "initial":
        log.info("Finished — backfill results:")
    else:
        log.info("Finished — incremental results:")

    for (lfolder, sfolder), cnt in sorted(saved_by.items(), key=lambda x: (x[0][0], x[0][1])):
        log.info("- %s (%s): %s event files saved", lfolder, sfolder, cnt)

    log.info("Total new event files saved: %s", saved_total)


if __name__ == "__main__":
    main()
