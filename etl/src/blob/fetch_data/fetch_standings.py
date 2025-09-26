#!/usr/bin/env python3
"""
fetch_standings.py
───────────────────────────────────────────────────────────────────────────────
DB‑driven fetch of standings snapshots → Azure Blob.

Behavior
- Selects league‑seasons that are currently IN‑SEASON:
    league_catalog.is_enabled = TRUE
    league_seasons.is_current = TRUE
    CURRENT_DATE BETWEEN start_date AND end_date + grace_days
- Optional targeting: --league-ids 244,253,71
- Always writes a FULL snapshot (standings are snapshot-based).

Blob path
  raw/standings/{folder_alias}/{season_str}/full_YYYY-MM-DD_HH-MM.json

Examples
  python src/blob/fetch_data/fetch_standings.py --mode full
  python src/blob/fetch_data/fetch_standings.py --mode full --league-ids 244,253,71
  python src/blob/fetch_data/fetch_standings.py --print-scope
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Iterable, List, Optional, Sequence, Tuple

import requests
from azure.storage.blob import BlobServiceClient

# ── repo-local imports (adjust paths only if your tree differs) ──────────────
THIS_DIR = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(THIS_DIR, "../../../test_scripts")))
sys.path.append(os.path.abspath(os.path.join(THIS_DIR, "../../../config")))

from get_db_conn import get_db_connection                         # noqa: E402
from credentials import API_KEYS, AZURE_STORAGE                   # noqa: E402


API_URL = "https://v3.football.api-sports.io/standings"
HTTP_TIMEOUT = 30
DEFAULT_GRACE_DAYS = 7


# ─────────────────────────────────────────────────────────────────────────────
# SQL scope selector (no preseason)
# ─────────────────────────────────────────────────────────────────────────────
BASE_SELECTOR_SQL = r"""
SELECT
    l.league_id,
    ls.season_year,
    ls.season_str,
    ls.start_date,
    ls.end_date,
    COALESCE(lc.folder_alias,
             l.folder_alias,
             LOWER(REGEXP_REPLACE(l.league_name, '\s+', '_', 'g')) || '_' || l.league_id::text
    ) AS folder_alias
FROM leagues l
JOIN league_seasons ls USING (league_id)
JOIN league_catalog lc   USING (league_id)
WHERE lc.is_enabled = TRUE
  AND ls.is_current = TRUE
  AND CURRENT_DATE BETWEEN ls.start_date AND (ls.end_date + (%s || ' days')::interval)
"""

def _append_league_filter(sql: str, num_ids: int) -> str:
    if num_ids <= 0:
        return sql
    placeholders = ", ".join(["%s"] * num_ids)
    return sql + f"\n  AND l.league_id IN ({placeholders})\nORDER BY l.league_id, ls.season_year;"

def select_scope(cur, grace_days: int, league_ids: Optional[Sequence[int]]) -> List[Tuple]:
    """
    Returns list of tuples:
      (league_id, season_year, season_str, start_date, end_date, folder_alias)
    """
    params: List = [grace_days]
    sql = BASE_SELECTOR_SQL
    if league_ids:
        sql = _append_league_filter(sql, len(league_ids))
        params.extend(list(league_ids))
    else:
        sql = sql + "\nORDER BY l.league_id, ls.season_year;"

    cur.execute(sql, params)
    return list(cur.fetchall())


# ─────────────────────────────────────────────────────────────────────────────
# Fetch + upload
# ─────────────────────────────────────────────────────────────────────────────
def fetch_standings(league_id: int, season: int) -> dict:
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params  = {"league": league_id, "season": season}
    # Simple retry for transient 429/5xx
    for attempt in range(4):
        try:
            r = requests.get(API_URL, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            # Raise for non-2xx
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            if attempt == 3:
                raise
            # Backoff 1s, 2s, 4s
            sleep_s = 2 ** attempt
            print(f"[warn] fetch {league_id}/{season} failed ({exc}); retrying in {sleep_s}s...")
            time.sleep(sleep_s)

def stamp_now() -> str:
    # e.g., 2025-07-27_23-05 (local, tz-aware then formatted)
    now = dt.datetime.now().astimezone()
    return now.strftime("%Y-%m-%d_%H-%M")

def season_folder(season_year: int) -> str:
    # Keep using DB's season_str; this is only a fallback if ever needed
    return f"{season_year}_{(season_year % 100) + 1:02d}"

def build_blob_path(folder_alias: str, season_str: str, stamp: str) -> str:
    fname = f"full_{stamp}.json"
    return f"standings/{folder_alias}/{season_str}/{fname}"

def upload_json_to_blob(payload: dict, blob_path: str) -> None:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    svc  = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
    blob = svc.get_blob_client(container="raw", blob=blob_path)
    blob.upload_blob(json.dumps(payload, indent=4), overwrite=False)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch current standings snapshots (DB-driven).")
    p.add_argument("--mode", choices=["full"], required=False, default="full",
                   help="Compatibility flag; standings are always snapshot-based.")
    p.add_argument("--league-ids", default="", help="Comma-separated league IDs to target (optional).")
    p.add_argument("--grace-days", type=int, default=DEFAULT_GRACE_DAYS,
                   help="Days after end_date to keep fetching (default: 7).")
    p.add_argument("--print-scope", action="store_true",
                   help="Print the selected league-seasons and exit without fetching.")
    return p.parse_args()

def parse_league_ids(csv: str) -> List[int]:
    if not csv:
        return []
    vals: List[int] = []
    for tok in csv.split(","):
        tok = tok.strip()
        if tok:
            try:
                vals.append(int(tok))
            except ValueError:
                raise SystemExit(f"--league-ids contains a non-integer: {tok}")
    return vals

def main() -> None:
    args = parse_args()
    target_ids = parse_league_ids(args.league_ids)

    # Connect to Postgres
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            scope = select_scope(cur, grace_days=args.grace_days, league_ids=target_ids)
    finally:
        conn.close()

    if args.print_scope:
        print("── Standings fetch scope (in-season only) ──")
        if not scope:
            print("(none)")
        else:
            for row in scope:
                league_id, season_year, season_str, start_date, end_date, folder_alias = row
                print(f"league_id={league_id:>4}  season={season_year}  "
                      f"folder={folder_alias}  season_str={season_str}  "
                      f"window={start_date}..{end_date}+{args.grace_days}d")
        return

    if not scope:
        print("No in-season league-seasons selected; exiting 0.")
        return

    print(f"Selected {len(scope)} league‑season(s). Starting fetch…")
    failures = 0
    stamp = stamp_now()

    for row in scope:
        league_id, season_year, season_str, start_date, end_date, folder_alias = row
        try:
            print(f"[{league_id}/{season_year}] Fetching standings…", end="", flush=True)
            payload = fetch_standings(league_id, season_year)

            blob_rel = build_blob_path(folder_alias, season_str, stamp)
            upload_json_to_blob(payload, blob_rel)
            print(f" saved → raw/{blob_rel}")
        except Exception as exc:
            failures += 1
            print(f"\n[error] {league_id}/{season_year} → {exc}")

    if failures:
        raise SystemExit(f"Completed with {failures} failure(s).")
    print("All snapshots saved successfully.")


if __name__ == "__main__":
    main()
