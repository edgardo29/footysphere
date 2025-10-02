#!/usr/bin/env python3
"""
fetch_fixtures.py
───────────────────────────────────────────────────────────────────────────────
Purpose
-------
Fetch fixture JSON from API-Football and upload it to Azure Blob Storage.

Run in **two modes**:

1. --mode full
   • One-time season baseline for every league-season where
     backfill_done = TRUE   AND
     full_done     = FALSE
   • After a successful upload, sets full_done → TRUE
     so the same league-season is never fetched again
     until the next season rollover.

2. --mode inc
   • Ad-hoc, on-demand delta pull.
   • You pass a **look-back window in days** (default 30 via --days D).
     The script converts D into a **date window** and calls:
       /fixtures?league=…&season=…&from=YYYY-MM-DD&to=YYYY-MM-DD&status=FT
     Flags in league_catalog are **not** modified.

Folder layout (fixed 23-Jul-2025)
---------------------------------
raw/fixtures/<slug>_<id>/<yyyy>_<yy>/
    full/full_YYYYMMDD.json                 ← season baseline
    inc/inc_YYYYMMDDTHHMMZ.json             ← incremental deltas
"""

import os
import sys
import json
import argparse
import datetime as dt
import requests
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# ── dynamic import paths -----------------------------------------------------
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.extend([
    os.path.join(BASE, "config"),        # credentials.py
    os.path.join(BASE, "test_scripts"),  # get_db_conn.py
])

from credentials import API_KEYS, AZURE_STORAGE              # noqa: E402
from get_db_conn import get_db_connection                    # noqa: E402

# ── constants ---------------------------------------------------------------
RAW_CONTAINER = "raw"
API_URL       = "https://v3.football.api-sports.io/fixtures"
HEADERS       = {"x-rapidapi-key": API_KEYS["api_football"]}


# ═════════════════════════════════════════════════════════════════════
# Helper functions
# ═════════════════════════════════════════════════════════════════════
def query_leagues(mode: str):
    """
    Return (league_id, season, folder_alias) tuples for the leagues
    that this run should process.

    • mode == "full" → backfill_done = TRUE AND full_done = FALSE
    • mode == "inc"  → backfill_done = TRUE AND full_done = TRUE
    """
    conn = get_db_connection()
    cur  = conn.cursor()

    if mode == "full":
        cur.execute("""
            SELECT league_id,
                   last_season  AS season,
                   folder_alias
              FROM league_catalog
             WHERE is_enabled    = TRUE
               AND backfill_done = TRUE
               AND full_done     = FALSE
             ORDER BY league_id;
        """)
    else:  # mode == "inc"
        cur.execute("""
            SELECT league_id,
                   last_season  AS season,
                   folder_alias
              FROM league_catalog
             WHERE is_enabled    = TRUE
               AND backfill_done = TRUE
               AND full_done     = TRUE
             ORDER BY league_id;
        """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def call_api(league_id: int, season: int, mode: str, days: int = 0) -> dict:
    """
    Call API-Football /fixtures with league & season.
    • For incremental mode, use a date window: from=<YYYY-MM-DD>, to=<YYYY-MM-DD>, status=FT.
    • Single request (no page param).
    """
    params = {"league": league_id, "season": season}

    if mode == "inc" and days:
        today_utc = dt.datetime.now(dt.timezone.utc).date()
        from_date = (today_utc - dt.timedelta(days=days)).isoformat()
        to_date   = today_utc.isoformat()
        params.update({"from": from_date, "to": to_date, "status": "FT"})
        # If you prefer to include ET/PK finals too and your plan supports it:
        # params["status"] = "FT-AET-PEN"

    resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def make_blob_path(alias: str, season: int, mode: str) -> str:
    """
    Build deterministic blob path:
      fixtures/<alias>/<yyyy>_<yy>/(full|inc)/<file>.json
    """
    season_folder = f"{season}_{(season % 100) + 1:02d}"
    now = dt.datetime.now(dt.timezone.utc)

    if mode == "full":
        subfolder = "full"
        fname     = f"full_{now:%Y%m%d}.json"
    else:
        subfolder = "inc"
        fname     = f"inc_{now:%Y%m%dT%H%MZ}.json"

    return f"fixtures/{alias}/{season_folder}/{subfolder}/{fname}"


def upload_blob(path: str, payload: dict):
    """
    Upload JSON to Azure Blob Storage.
    The file is immutable; skip if it already exists.
    """
    svc = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )
    client = svc.get_blob_client(container=RAW_CONTAINER, blob=path)

    try:
        client.upload_blob(json.dumps(payload, indent=4), overwrite=False)
        print(f"  Uploaded → {RAW_CONTAINER}/{path}")
    except ResourceExistsError:
        print(f"  Skipped (exists): {RAW_CONTAINER}/{path}")


def mark_full_done(league_id: int):
    """Flip full_done → TRUE after a successful baseline upload."""
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute(
        "UPDATE league_catalog SET full_done = TRUE WHERE league_id = %s;",
        (league_id,),
    )
    conn.commit()
    cur.close()
    conn.close()


# ═════════════════════════════════════════════════════════════════════
# Main routine
# ═════════════════════════════════════════════════════════════════════
def main(args):
    leagues = query_leagues(args.mode)
    if not leagues:
        print(f"No leagues qualify for mode='{args.mode}'. Nothing to do.")
        sys.exit(0)

    # Pass days straight through; call_api builds from/to
    window_days = args.days if args.mode == "inc" else 0

    success = 0
    for league_id, season, alias in leagues:
        print(f"Fetching {alias} season {season} [{args.mode}]")

        # ── API call ──────────────────────────────────────────────────
        try:
            data = call_api(league_id, season, args.mode, window_days)
        except Exception as e:
            print(f"  ERROR fetching data: {e}")
            continue

        # ── Upload to Blob ───────────────────────────────────────────
        try:
            upload_blob(make_blob_path(alias, season, args.mode), data)
            success += 1
        except Exception as e:
            print(f"  ERROR uploading blob: {e}")
            continue  # do not update flag if upload failed

        # ── Mark baseline complete ───────────────────────────────────
        if args.mode == "full":
            try:
                mark_full_done(league_id)
            except Exception as e:
                print(f"  ERROR updating full_done flag: {e}")

    # Airflow interprets non-zero exit → task failure
    sys.exit(0 if success else 1)


# ═════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Fetch fixture snapshots: "
            "'full' = one-time season baseline; "
            "'inc'  = ad-hoc delta pull covering the last <days>."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["full", "inc"],
        required=True,
        help="Select 'full' for baseline or 'inc' for incremental pull.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help=(
            "Look-back window in *days* for incremental mode. "
            "Ignored when --mode full (default 30)."
        ),
    )
    main(parser.parse_args())
