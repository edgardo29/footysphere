#!/usr/bin/env python
"""
fetch_match_statistics.py  (league-first layout, UTC-safe)
──────────────────────────────────────────────────────────────────────────────
Pulls per-fixture statistics from API-Football and stores each response as JSON
in Azure Blob Storage.

Modes
  • initial      → full-season backfill of finished matches
  • incremental  → date-window fetch of finished matches

Blob layout (LEAGUE → SEASON → MODE → TIMESTAMP):
raw/
  └─ match_statistics/<folder_alias>/<YYYY_YY>/
       ├─ initial/<YYYY-MM-DD_HHMMSS>/statistics_<fixture>.json
       └─ incremental/<YYYY-MM-DD_HHMMSS>/statistics_<fixture>.json
"""

import os
import sys
import json
import time
import argparse
import datetime as dt
import logging
from typing import List, Tuple, Optional, Dict, Set

import requests
from azure.storage.blob import BlobServiceClient

# ───────────── repo-local imports ─────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from credentials import API_KEYS, AZURE_STORAGE  # noqa: E402
from get_db_conn import get_db_connection        # noqa: E402

# ───────────── logging ─────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
for noisy in ("azure.storage", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

FINISHED_STATUSES = ("FT", "AET", "PEN")


# ───────────── Azure helpers ─────────────
def get_blob_service() -> BlobServiceClient:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])


def upload_json(svc: BlobServiceClient, *, container: str, path: str, data: dict) -> None:
    bc = svc.get_blob_client(container=container, blob=path)
    # Path is timestamped per run; exists() is only a safety net for same-second reruns.
    if bc.exists():
        return
    bc.upload_blob(json.dumps(data, indent=4), overwrite=False)


# ───────────── tiny HTTP retry ─────────────
def http_get_with_retry(url: str, headers: dict, params: dict,
                        retries: int = 3, base_sleep: float = 1.0) -> requests.Response:
    """
    Minimal backoff for 429 and 5xx. Honors Retry-After if present.
    """
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


# ───────────── DB helpers ─────────────
def _fetch_rows(sql: str, params: Tuple) -> List[Tuple[str, int, str]]:
    """
    Returns rows as (fixture_id, league_id, folder_alias) strings.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        out: List[Tuple[str, int, str]] = []
        for r in cur.fetchall():
            fid = str(r[0])
            lg = int(r[1])
            alias = r[2] or f"league_{lg}"
            out.append((fid, lg, alias))
        return out


def fixture_rows_for_season(season: int, league_id: Optional[int]) -> List[Tuple[str, int, str]]:
    sql = """
        SELECT f.fixture_id, f.league_id, lc.folder_alias
          FROM fixtures f
          LEFT JOIN league_catalog lc ON lc.league_id = f.league_id
         WHERE f.season_year = %s
           AND f.status = ANY(%s)
    """
    params: Tuple = (season, list(FINISHED_STATUSES))
    if league_id is not None:
        sql += " AND f.league_id = %s"
        params = (season, list(FINISHED_STATUSES), league_id)
    return _fetch_rows(sql, params)


def fixture_rows_by_window(start_date: dt.date, end_exclusive: dt.date,
                           league_id: Optional[int]) -> List[Tuple[str, int, str]]:
    sql = """
        SELECT f.fixture_id, f.league_id, lc.folder_alias
          FROM fixtures f
          LEFT JOIN league_catalog lc ON lc.league_id = f.league_id
         WHERE f.fixture_date >= %s
           AND f.fixture_date <  %s
           AND f.status = ANY(%s)
    """
    params: Tuple = (start_date, end_exclusive, list(FINISHED_STATUSES))
    if league_id is not None:
        sql += " AND f.league_id = %s"
        params = (start_date, end_exclusive, list(FINISHED_STATUSES), league_id)
    return _fetch_rows(sql, params)


# ───────────── API helper ─────────────
def fetch_stats_api(fixture_id: str) -> dict:
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"fixture": fixture_id}
    resp = http_get_with_retry(url, headers, params, retries=3, base_sleep=1.0)
    resp.raise_for_status()
    return resp.json()


# ───────────── initial-scan helpers ─────────────
def existing_initial_fixtures_for_alias(
    svc: BlobServiceClient, container: str, season_str: str, alias: str
) -> Set[str]:
    """
    Scan once under initial/ for this league+season and return fixture_ids present.
    """
    prefix = f"match_statistics/{alias}/{season_str}/initial/"
    ids: Set[str] = set()
    container_client = svc.get_container_client(container)
    for blob in container_client.list_blobs(name_starts_with=prefix):
        name = blob.name.rsplit("/", 1)[-1]  # statistics_<id>.json
        if name.startswith("statistics_") and name.endswith(".json"):
            fid = name[len("statistics_"):-len(".json")]
            ids.add(fid)
    return ids


# ───────────── main ─────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch API-Football match statistics and upload to Azure.")
    ap.add_argument("--mode", choices=["initial", "incremental"], default="incremental")
    ap.add_argument("--season", type=int, required=True, help="Season start-year, e.g., 2025 → 2025_26")
    ap.add_argument("--league-id", type=int, help="Optional league filter (e.g., 39 for EPL)")
    ap.add_argument("--lookback-days", type=int, default=8,
                    help="Incremental only, used if --start-date is not provided")
    ap.add_argument("--start-date", help="YYYY-MM-DD (incremental window start). If omitted, uses lookback-days.")
    args = ap.parse_args()

    season_str = f"{args.season}_{(args.season % 100) + 1}"
    now_utc = dt.datetime.now(dt.timezone.utc)
    run_stamp = now_utc.strftime("%Y-%m-%d_%H%M%S")  # unique per run (UTC)

    # Build fixture rows (fixture_id, league_id, alias)
    if args.mode == "initial":
        rows = fixture_rows_for_season(args.season, args.league_id)
        mode_dir = "initial"
        log.info("INITIAL • season=%s • league_id=%s • fixtures=%d • stamp=%s",
                 season_str, args.league_id or "ALL", len(rows), run_stamp)
    else:
        today_utc = dt.datetime.now(dt.timezone.utc).date()
        if args.start_date:
            start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        else:
            start_date = today_utc - dt.timedelta(days=args.lookback_days)
        end_exclusive = today_utc + dt.timedelta(days=1)  # include today (UTC)

        if start_date >= end_exclusive:
            raise SystemExit(f"Invalid window: start_date ({start_date}) >= end_exclusive ({end_exclusive})")

        rows = fixture_rows_by_window(start_date, end_exclusive, args.league_id)
        mode_dir = "incremental"
        log.info("INCREMENTAL • season=%s • league_id=%s • window=[%s,%s) • fixtures=%d • stamp=%s",
                 season_str, args.league_id or "ALL", start_date, end_exclusive, len(rows), run_stamp)

    if not rows:
        log.warning("No fixtures found → nothing to do (exit 0)")
        return

    # Azure client
    svc = get_blob_service()
    container = "raw"

    # For incremental, build a per-alias skip map from INITIAL area (avoid pointless refetch)
    initial_seen_by_alias: Dict[str, Set[str]] = {}
    if args.mode == "incremental":
        aliases = sorted({alias for _, _, alias in rows})
        for alias in aliases:
            initial_seen_by_alias[alias] = existing_initial_fixtures_for_alias(svc, container, season_str, alias)

    # Upload loop
    uploaded = skipped = failed = 0
    for (fixture_id, league_id, alias) in rows:
        # Per-run league-first prefix
        run_prefix = f"match_statistics/{alias}/{season_str}/{mode_dir}/{run_stamp}"
        blob_path = f"{run_prefix}/statistics_{fixture_id}.json"

        # Skip if present in INITIAL (for this league+season), only for incremental mode
        if args.mode == "incremental":
            if fixture_id in initial_seen_by_alias.get(alias, set()):
                skipped += 1
                continue

        try:
            data = fetch_stats_api(fixture_id)
            if not data or "response" not in data:
                log.warning("Fixture %s → empty/odd payload; skipping", fixture_id)
                skipped += 1
                continue

            upload_json(svc, container=container, path=blob_path, data=data)
            uploaded += 1
            if uploaded % 100 == 0:
                log.info("Uploaded %d/%d …", uploaded, len(rows))
        except Exception as exc:
            failed += 1
            log.error("Fixture %s → %s", fixture_id, exc)

    print(
        "\n======== FETCH MATCH STATISTICS — SUMMARY ========\n"
        f"Mode             : {args.mode}\n"
        f"Season           : {season_str}\n"
        f"League filter    : {args.league_id or 'ALL'}\n"
        f"Total fixtures   : {len(rows)}\n"
        f"Uploaded         : {uploaded}\n"
        f"Skipped          : {skipped}\n"
        f"Failed           : {failed}\n"
        "Run folder base  : match_statistics/<alias>/" + season_str + f"/{mode_dir}/{run_stamp}\n"
        "==================================================\n"
    )


if __name__ == "__main__":
    main()
