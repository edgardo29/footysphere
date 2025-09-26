#!/usr/bin/env python
"""
fetch_missing_venues.py
────────────────────────────────────────────────────────────────────────────
High-level goal
    • For the latest season of every league that has teams in our database,
      identify stadiums (venue_id) whose `country` OR `city` field is still
      NULL in *production* `venues`.
    • Download those missing details from the API-Football `/venues?id=` endpoint.
    • Write ONE blob per league-season:

            raw/venues_fills/<league_slug>/<season_label>/venues.json

      (overwrites if re-run for the same season → idempotent).
    • No database writes are done here; a separate loader will read the
      JSON and update `venues`.

Why two different SQL paths?
    • If table `team_league_seasons` is present & populated, we can restrict
      the NULL scan to only stadiums that actually appear in that league-
      season (efficient).
    • If the mapping table is missing or empty (first-time migrations) we
      fall back to a global scan on `venues` so nothing is skipped.

Typical console output

    team_league_seasons table missing or empty — fallback to global NULL scan (first-run behaviour)
    [major_league_soccer 2025_26] fetching 8 venue(s)…
          ↳ wrote 8 payloads to venues_fills/major_league_soccer/2025_26/venues.json

    fetch_missing_venues summary
    ----------------------------
    json_files_written   : 8
"""

# ──────────────────────────────────────────────────────────────────────
# Standard library + project-specific imports
# ----------------------------------------------------------------------
#  • sys.path tweaking lets us import shared helpers (`credentials` and
#    `get_db_conn`) without changing PYTHONPATH.
#  • Azure SDK handles blob uploads; requests hits the football API.
# ──────────────────────────────────────────────────────────────────────
import os
import sys
import json
import time
import requests
from typing import List, Dict, Any

sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])

from credentials import API_KEYS, AZURE_STORAGE            # API key + blob creds
from get_db_conn import get_db_connection                  # psycopg connector
from azure.storage.blob import BlobServiceClient           # Azure SDK

# ──────────────────────────────────────────────────────────────────────
# Configuration constants
# ----------------------------------------------------------------------
#  RATE_DELAY_SEC   – gap between API calls (stay under 60/min limit).
#  CONTAINER        – Azure blob container name where we store JSON.
# ──────────────────────────────────────────────────────────────────────
RATE_DELAY_SEC: float = 1.2
CONTAINER: str = "raw"

# ──────────────────────────────────────────────────────────────────────
# Helper: latest_seasons()
# ----------------------------------------------------------------------
#  Purpose: return ONE dict per league containing
#           • league_id        – PK from `leagues`
#           • season_year      – max season in team_ingest_status
#           • slug             – "premier_league" (spaces → underscores)
#           • label            – "2025_26"   (used in blob path)
#
#  Why: we only ever fetch /venues info for the most-recent campaign,
#       so older seasons don’t trigger duplicate work.
# ──────────────────────────────────────────────────────────────────────
def latest_seasons(cur) -> List[Dict[str, Any]]:
    cur.execute("""
        SELECT l.league_id,
               l.season_year,
               regexp_replace(lower(ln.league_name), '\s+', '_', 'g') AS slug
        FROM (
            SELECT league_id, MAX(season_year) AS season_year
            FROM   team_ingest_status
            GROUP  BY league_id
        ) l
        JOIN leagues ln USING (league_id)
        ORDER  BY league_id
    """)
    rows = cur.fetchall()

    out = []
    for league_id, season_year, slug in rows:
        # Create label like 2025 → "2025_26"
        label = f"{season_year}_{(season_year % 100) + 1:02d}"
        out.append({
            "league_id": league_id,
            "season_year": season_year,
            "slug": slug,
            "label": label
        })
    return out

# ──────────────────────────────────────────────────────────────────────
# Helper: mapping_exists()
# ----------------------------------------------------------------------
#  Purpose: detect if table `team_league_seasons` both *exists* and
#           *contains* at least one row.
#  Reason:  first-time migrations may not have that table yet; we need
#           to fall back gracefully rather than crash.
# ──────────────────────────────────────────────────────────────────────
def mapping_exists(cur) -> bool:
    # Does the table exist?
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'team_league_seasons'
        );
    """)
    if not cur.fetchone()[0]:
        return False
    # Does it have any data?
    cur.execute("SELECT 1 FROM team_league_seasons LIMIT 1;")
    return bool(cur.fetchone())

# ──────────────────────────────────────────────────────────────────────
# Helper: venue_ids_missing_geo()
# ----------------------------------------------------------------------
#  Purpose: return venue_ids whose geo fields are NULL.
#
#  Two modes:
#    • use_mapping=True  → limit to venue_ids that appear in the specific
#                          league+season (efficient, our preferred path).
#    • use_mapping=False → global scan for NULL country/city (safe fallback).
# ──────────────────────────────────────────────────────────────────────
def venue_ids_missing_geo(cur, league_id: int, season_year: int, use_mapping: bool):
    if use_mapping:
        # Scoped query via team_league_seasons mapping
        cur.execute("""
            SELECT DISTINCT v.venue_id
            FROM   venues v
            JOIN   teams  t   ON t.venue_id = v.venue_id
            JOIN   team_league_seasons tls ON tls.team_id = t.team_id
            WHERE  tls.league_id   = %s
              AND  tls.season_year = %s
              AND  (v.country IS NULL OR v.city IS NULL)
        """, (league_id, season_year))
    else:
        # Fallback: any stadium with missing geo data
        cur.execute("""
            SELECT venue_id
            FROM   venues
            WHERE  country IS NULL OR city IS NULL
        """)
    return [row[0] for row in cur.fetchall()]

# ──────────────────────────────────────────────────────────────────────
# Helper: fetch_json()
# ----------------------------------------------------------------------
#  Purpose: call API-Football /venues?id=<venue_id>
#  Returns: parsed JSON *or* None if error / 0 results.
#  Note   : prints a per-venue status line so you can debug quota issues.
# ──────────────────────────────────────────────────────────────────────
def fetch_json(venue_id: int):
    url = "https://v3.football.api-sports.io/venues"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}

    try:
        r = requests.get(url, headers=headers, params={"id": venue_id}, timeout=10)
        r.raise_for_status()
        js = r.json()

        if js.get("results", 0) == 0:
            print(f"   – {venue_id}: 0 results")
            return None
        return js

    except Exception as e:
        print(f"   – {venue_id}: ERROR → {e}")
        return None

# ──────────────────────────────────────────────────────────────────────
# Main routine: orchestrates the entire fetch-and-blob process
# ----------------------------------------------------------------------
#  1. Determine whether mapping table exists → choose SQL mode.
#  2. For each latest league-season:
#       • Find venue_ids needing geo fields.
#       • Fetch details via API (rate-limited).
#       • Write JSON array to Blob (one file).
#  3. Print grand summary.
# ──────────────────────────────────────────────────────────────────────
def main():
    # Establish DB & Blob connections up-front (reuse = faster)
    conn = get_db_connection()
    cur  = conn.cursor()

    # Decide query mode based on mapping table presence
    use_mapping = mapping_exists(cur)
    if not use_mapping:
        print("team_league_seasons table missing or empty — "
              "fallback to global NULL scan (first-run behaviour)")

    leagues = latest_seasons(cur)
    if not leagues:
        print("No leagues found in team_ingest_status.")
        cur.close(); conn.close(); return

    blob_service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential  =AZURE_STORAGE["access_key"])

    total_payloads = 0  # grand total for summary

    # -------- loop over each league's newest season ------------------
    for cfg in leagues:
        lid   = cfg["league_id"]
        year  = cfg["season_year"]
        slug  = cfg["slug"]
        label = cfg["label"]

        # Get venue_ids still missing geo
        vids = venue_ids_missing_geo(cur, lid, year, use_mapping)
        if not vids:
            print(f"[{slug} {label}] all venues complete – skip.")
            continue

        print(f"[{slug} {label}] fetching {len(vids)} venue(s)…")
        payloads = []

        for vid in vids:
            js = fetch_json(vid)
            if js:
                payloads.append(js)
            time.sleep(RATE_DELAY_SEC)   # throttle API calls

        # Build blob path and overwrite if it exists
        key = f"venues/{slug}/{label}/venues.json"
        blob_service.get_blob_client(CONTAINER, key).upload_blob(
            json.dumps(payloads, indent=2), overwrite=True)

        print(f"      ↳ wrote {len(payloads)} payloads to {key}")
        total_payloads += len(payloads)

    # -------- final summary ------------------------------------------
    print("\nfetch_missing_venues summary")
    print("----------------------------")
    print(f"{'json_files_written':<20}: {total_payloads}")

    # tidy up connections
    cur.close()
    conn.close()

# ──────────────────────────────────────────────────────────────────────
# Script entry-point
# ----------------------------------------------------------------------
#  Run with:    python fetch_missing_venues.py
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()