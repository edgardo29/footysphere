#!/usr/bin/env python
"""
fetch_leagues.py
────────────────
Purpose
-------
1. Look up every enabled league (and the season range you want) in
   `league_catalog`.
2. For each (league, season) pair that is **not yet present** in Blob,
   call the API and save one JSON file:

       raw/leagues/<folder_alias>/<YYYY_YY>/league.json

   Example: raw/leagues/major_league_soccer/2024_25/league.json

This script only touches Blob. Loading those blobs into the database
belongs in a separate loader (e.g. load_leagues.py).

Run examples
------------
Fetch only blobs that are missing:
    python fetch_leagues.py

Force-refresh everything, overwriting existing blobs:
    python fetch_leagues.py --overwrite
"""

import argparse
import json
import os
import re
import sys
from typing import List, Tuple

import requests
from azure.storage.blob import BlobServiceClient

# ──────────────────────────────────────────────────────────────
# Project-level imports (update paths if your structure differs)
# ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(os.path.join(BASE_DIR, "config"))
sys.path.append(os.path.join(BASE_DIR, "test_scripts"))

from credentials import API_KEYS, AZURE_STORAGE          # API key + blob creds
from get_db_conn import get_db_connection                # psycopg conn helper


# ──────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────
def season_suffix(year: int) -> str:
    """Return folder suffix '2024_25' for season 2024, etc."""
    return f"{year}_{str(year + 1)[-2:]}"


def build_work_list(cur):
    """
    Return tuples: (league_id, folder_alias, first_season, last_season)
    """
    cur.execute(
        """
        SELECT lc.league_id,
               lc.folder_alias,
               lc.first_season,
               lc.last_season,
               ld.seasons              -- jsonb → Python list via psycopg2
        FROM   league_catalog  lc
        JOIN   league_directory ld USING (league_id)
        WHERE  lc.is_enabled
        """
    )

    work = []
    for league_id, folder_alias, first_season, last_season, seasons_json in cur.fetchall():
        # seasons_json is already a Python list when fetched via psycopg2
        seasons_list = seasons_json if isinstance(seasons_json, list) else json.loads(seasons_json)

        if last_season is None:
            last_season = max(seasons_list)     # pick the most recent year

        work.append((league_id, folder_alias, first_season, last_season))

    return work



def blob_exists(blob_svc: BlobServiceClient, path: str) -> bool:
    """Check whether the given blob already exists."""
    return blob_svc.get_blob_client(container="raw", blob=path).exists()


def write_blob(blob_svc: BlobServiceClient, path: str, payload: dict) -> None:
    """Upload (or overwrite) a JSON payload to Blob Storage."""
    blob = blob_svc.get_blob_client(container="raw", blob=path)
    blob.upload_blob(json.dumps(payload, indent=2), overwrite=True)
    print(f"stored    raw/{path}")


def fetch_api(league_id: int, season: int) -> dict:
    """Call API-Football for a specific league + season."""
    url = "https://v3.football.api-sports.io/leagues"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    resp = requests.get(url, params={"id": league_id, "season": season},
                        headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ──────────────────────────────────────────────────────────────
# Main routine
# ──────────────────────────────────────────────────────────────
def main() -> None:
    # ------------------------------------------------------------------
    # Parse CLI flags
    # ------------------------------------------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the blob even if it already exists",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Set up DB cursor
    # ------------------------------------------------------------------
    conn = get_db_connection()
    cur = conn.cursor()

    # Build the work list (enabled leagues + season range)
    work_items = build_work_list(cur)
    if not work_items:
        print("league_catalog has no enabled rows — nothing to do.")
        return

    # ------------------------------------------------------------------
    # Blob service client
    # ------------------------------------------------------------------
    blob_svc = BlobServiceClient(
        f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )

    # Counters for the final summary
    downloaded = 0
    skipped = 0

    # ------------------------------------------------------------------
    # Iterate through each (league, season) pair
    # ------------------------------------------------------------------
    for league_id, folder_alias, first_season, last_season in work_items:
        for season in range(first_season, last_season + 1):
            blob_path = f"leagues/{folder_alias}/{season_suffix(season)}/league.json"

            # If the blob already exists and --overwrite is NOT set,
            # count it as skipped and move on.
            if not args.overwrite and blob_exists(blob_svc, blob_path):
                skipped += 1
                continue

            # Otherwise, fetch from the API and store in Blob
            try:
                payload = fetch_api(league_id, season)
                write_blob(blob_svc, blob_path, payload)
                downloaded += 1
            except Exception as e:
                print(f"error     {folder_alias} {season}: {e}")

    # ------------------------------------------------------------------
    # Close DB cursor and connection
    # ------------------------------------------------------------------
    cur.close()
    conn.close()

    # ------------------------------------------------------------------
    # Summary line
    # ------------------------------------------------------------------
    print(f"Finished: downloaded {downloaded} new blobs, skipped {skipped}.")



if __name__ == "__main__":
    main()
