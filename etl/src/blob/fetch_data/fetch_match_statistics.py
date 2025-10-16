#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_match_statistics.py — minimal-params, DB-resolved leagues & seasons
──────────────────────────────────────────────────────────────────────────────
Pulls match statistics from API-Football and uploads JSONs to Azure Blob
Storage. Season and leagues are resolved from the DB; CLI takes only:

  • --mode {initial, incremental}
  • [optional] --league-id <int> ... <int>   # now supports 1+ IDs in one run

Blob layout (LEAGUE → SEASON → MODE → TIMESTAMP):
raw/
  └─ match_statistics/<folder_alias>/<YYYY_YY>/
       ├─ initial/<YYYY-MM-DD_HHMMSS>/statistics_<fixture>.json
       └─ incremental/<YYYY-MM-DD_HHMMSS>/statistics_<fixture>.json
"""

from __future__ import annotations

import os
import sys
import json
import time
import argparse
import datetime as dt
import logging
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import requests
from azure.storage.blob import BlobServiceClient

# ───────────────────────── repo-local imports (robust path resolution) ─────────────────────────
HERE = Path(__file__).resolve().parent

# Try both typical layouts you’ve used before:
#  - <repo>/etl/src/blob/fetch_data/... → ../../../config (within etl/)
#  - <repo>/etl/src/blob/fetch_data/... → ../../../test_scripts
#  - <repo>/etl/src/blob/fetch_data/... → ../../../../config (repo-root/config)
#  - <repo>/etl/src/blob/fetch_data/... → ../../../../test_scripts
CANDIDATE_PATHS = [
    HERE / "../../../config",
    HERE / "../../../test_scripts",
    HERE / "../../../../config",
    HERE / "../../../../test_scripts",
]

# Also honor FOOTY_ROOT if it’s set (env var or Airflow Variable exported)
FOOTY_ROOT = os.environ.get("FOOTY_ROOT", "")
if FOOTY_ROOT:
    CANDIDATE_PATHS += [
        Path(FOOTY_ROOT) / "etl" / "config",
        Path(FOOTY_ROOT) / "etl" / "test_scripts",
        Path(FOOTY_ROOT) / "config",
        Path(FOOTY_ROOT) / "test_scripts",
    ]

for p in CANDIDATE_PATHS:
    try:
        rp = p.resolve()
    except Exception:
        continue
    if rp.exists():
        sp = str(rp)
        if sp not in sys.path:
            sys.path.insert(0, sp)

try:
    # Must exist in one of the searched folders
    from credentials import API_KEYS, AZURE_STORAGE
    from get_db_conn import get_db_connection
except ModuleNotFoundError as e:
    tried = "\n".join(f" - {str(Path(x).resolve())}" for x in CANDIDATE_PATHS)
    raise SystemExit(
        "Could not import credentials/get_db_conn. Looked in:\n"
        f"{tried}\n\n"
        "Make sure `credentials.py` and `get_db_conn.py` are in one of those folders."
    ) from e

# ───────────────────────── logging ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fetch_match_statistics")
for noisy in ("azure.storage", "azure.core.pipeline.policies.http_logging_policy", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# ───────────────────────── constants ─────────────────────────
FINISHED_STATUSES = ("FT", "AET", "PEN")
DEFAULT_LOOKBACK_DAYS = 14  # rolling window for incremental; internal default

# ───────────────────────── Azure helpers ─────────────────────────
def get_blob_service() -> BlobServiceClient:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])

def upload_json(svc: BlobServiceClient, *, container: str, path: str, data: dict) -> None:
    bc = svc.get_blob_client(container=container, blob=path)
    # Guard: if retried quickly, avoid duplicate within the same path.
    if bc.exists():
        return
    bc.upload_blob(json.dumps(data, indent=4), overwrite=False)

# ───────────────────────── tiny HTTP retry ─────────────────────────
def http_get_with_retry(url: str, headers: dict, params: dict,
                        retries: int = 3, base_sleep: float = 1.0) -> requests.Response:
    """Minimal backoff for 429 and 5xx. Honors Retry-After if present."""
    for attempt in range(1, retries + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if attempt == retries:
                return resp
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    sleep_s = base_sleep * attempt
            else:
                sleep_s = base_sleep * attempt
            time.sleep(sleep_s)
            continue
        return resp
    return resp  # fallback

# ───────────────────────── DB helpers ─────────────────────────
def get_enabled_leagues(conn, league_ids: Optional[List[int]]) -> List[Tuple[int, str, str]]:
    """
    Returns list of (league_id, folder_alias, league_name).
      - If league_ids is None/empty → all enabled leagues.
      - Else → only those league_ids (even if disabled).
    """
    with conn.cursor() as cur:
        if not league_ids:
            cur.execute(
                """
                SELECT lc.league_id, lc.folder_alias, lc.league_name
                  FROM league_catalog lc
                 WHERE lc.is_enabled = TRUE
                 ORDER BY lc.league_id
                """
            )
        else:
            cur.execute(
                """
                SELECT lc.league_id, lc.folder_alias, lc.league_name
                  FROM league_catalog lc
                 WHERE lc.league_id = ANY(%s)
                 ORDER BY lc.league_id
                """,
                (league_ids,),
            )
        rows = cur.fetchall()
        return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]

def resolve_season_year(conn, league_id: int) -> Optional[int]:
    """
    Resolve the current season for a league using:
      1) is_current = TRUE (max season)
      2) else active-by-date (max season)
      3) else absolute max(season_year)
    """
    today = dt.date.today()
    with conn.cursor() as cur:
        # 1) Explicit current
        cur.execute(
            """
            SELECT MAX(season_year)
              FROM league_seasons
             WHERE league_id = %s AND is_current = TRUE
            """,
            (league_id,),
        )
        s = cur.fetchone()[0]
        if s:
            return int(s)

        # 2) Active by dates
        cur.execute(
            """
            SELECT MAX(season_year)
              FROM league_seasons
             WHERE league_id = %s
               AND (start_date IS NULL OR start_date <= %s)
               AND (end_date   IS NULL OR end_date   >= %s)
            """,
            (league_id, today, today),
        )
        s = cur.fetchone()[0]
        if s:
            return int(s)

        # 3) Fallback max
        cur.execute(
            "SELECT MAX(season_year) FROM league_seasons WHERE league_id = %s",
            (league_id,),
        )
        s = cur.fetchone()[0]
        return int(s) if s else None

def season_str_from_year(year: int) -> str:
    return f"{year}_{(year % 100) + 1}"

def select_finished_matches_for_initial(conn, league_id: int, season_year: int) -> List[Tuple[str, int, int]]:
    """
    Returns [(fixture_id, league_id, season_year)] for finished matches for the league+season.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.fixture_id::text, f.league_id, f.season_year
              FROM fixtures f
             WHERE f.league_id   = %s
               AND f.season_year = %s
               AND f.status = ANY(%s)
            """,
            (league_id, season_year, list(FINISHED_STATUSES)),
        )
        return [(r[0], int(r[1]), int(r[2])) for r in cur.fetchall()]

def select_finished_matches_for_incremental(conn, league_ids: List[int],
                                            start_date: dt.date, end_exclusive: dt.date) -> List[Tuple[str, int, int]]:
    """
    Returns [(fixture_id, league_id, season_year)] for finished matches in the date window
    across the provided league_ids.
    """
    if not league_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.fixture_id::text, f.league_id, f.season_year
              FROM fixtures f
             WHERE f.league_id = ANY(%s)
               AND f.fixture_date >= %s
               AND f.fixture_date <  %s
               AND f.status = ANY(%s)
            """,
            (league_ids, start_date, end_exclusive, list(FINISHED_STATUSES)),
        )
        return [(r[0], int(r[1]), int(r[2])) for r in cur.fetchall()]

# ───────────────────────── API helper ─────────────────────────
def fetch_stats_api(fixture_id: str) -> dict:
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"fixture": fixture_id}
    resp = http_get_with_retry(url, headers, params, retries=3, base_sleep=1.0)
    resp.raise_for_status()
    return resp.json()

# ───────────────────────── main ─────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch API-Football match statistics and upload to Azure.")
    ap.add_argument("--mode", choices=["initial", "incremental"], required=True,
                    help="initial = full-season backfill; incremental = rolling window")
    # CHANGED: accept 1+ integers for league IDs
    ap.add_argument("--league-id", type=int, nargs="+", default=None,
                    help="Optional one or more leagues to target (default: all enabled).")
    args = ap.parse_args()

    now_utc = dt.datetime.now(dt.timezone.utc)
    run_stamp = now_utc.strftime("%Y-%m-%d_%H%M%S")
    container = "raw"

    uploaded = skipped = failed = 0
    svc = get_blob_service()

    with get_db_connection() as conn:
        # Resolve leagues (enabled by default or an explicit list)
        leagues = get_enabled_leagues(conn, args.league_id)
        if not leagues:
            log.warning("No leagues found (enabled or matching IDs) → exit 0")
            return

        alias_by_league: Dict[int, str] = {lid: alias for (lid, alias, _name) in leagues}

        if args.mode == "initial":
            for lid, alias, lname in leagues:
                season_year = resolve_season_year(conn, lid)
                if season_year is None:
                    log.warning("League %s (%s) has no seasons in DB; skipping.", lid, lname)
                    continue

                season_str = season_str_from_year(season_year)
                rows = select_finished_matches_for_initial(conn, lid, season_year)
                log.info("INITIAL • league=%s (%s) • season=%s • fixtures=%d • stamp=%s",
                         lid, lname, season_str, len(rows), run_stamp)

                run_prefix = f"match_statistics/{alias}/{season_str}/initial/{run_stamp}"

                for (fixture_id, _league_id, _season_year) in rows:
                    blob_path = f"{run_prefix}/statistics_{fixture_id}.json"
                    try:
                        data = fetch_stats_api(fixture_id)
                        if not data or "response" not in data:
                            skipped += 1
                            continue
                        upload_json(svc, container=container, path=blob_path, data=data)
                        uploaded += 1
                        if uploaded % 100 == 0:
                            log.info("Uploaded %d …", uploaded)
                    except Exception as exc:
                        failed += 1
                        log.error("Fixture %s → %s", fixture_id, exc)

        else:  # incremental
            today_utc = dt.datetime.now(dt.timezone.utc).date()
            start_date = today_utc - dt.timedelta(days=DEFAULT_LOOKBACK_DAYS)
            end_exclusive = today_utc + dt.timedelta(days=1)

            lid_list = [lid for (lid, _alias, _name) in leagues]
            rows = select_finished_matches_for_incremental(conn, lid_list, start_date, end_exclusive)
            log.info("INCREMENTAL • leagues=%d • window=[%s,%s) • fixtures=%d • stamp=%s",
                     len(lid_list), start_date, end_exclusive, len(rows), run_stamp)

            for (fixture_id, lid, season_year) in rows:
                alias = alias_by_league.get(lid)
                if not alias:
                    # Should not happen for enabled leagues; safe-guard.
                    skipped += 1
                    continue
                season_str = season_str_from_year(season_year)
                run_prefix = f"match_statistics/{alias}/{season_str}/incremental/{run_stamp}"
                blob_path = f"{run_prefix}/statistics_{fixture_id}.json"
                try:
                    data = fetch_stats_api(fixture_id)
                    if not data or "response" not in data:
                        skipped += 1
                        continue
                    upload_json(svc, container=container, path=blob_path, data=data)
                    uploaded += 1
                    if uploaded % 100 == 0:
                        log.info("Uploaded %d …", uploaded)
                except Exception as exc:
                    failed += 1
                    log.error("Fixture %s → %s", fixture_id, exc)

    total = uploaded + skipped + failed
    league_desc = "ALL ENABLED" if not args.league_id else ", ".join(map(str, args.league_id))
    print(
        "\n======== FETCH MATCH STATISTICS — SUMMARY ========\n"
        f"Mode             : {args.mode}\n"
        f"League filter    : {league_desc}\n"
        f"Total matches    : {total}\n"
        f"Uploaded         : {uploaded}\n"
        f"Skipped          : {skipped}\n"
        f"Failed           : {failed}\n"
        "Run folder base  : match_statistics/<alias>/<YYYY_YY>/{mode}/" + run_stamp + "\n"
        "==================================================\n"
    )

if __name__ == "__main__":
    main()
