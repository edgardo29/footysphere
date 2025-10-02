#!/usr/bin/env python3
"""
load_stg_fixtures.py
────────────────────────────────────────────────────────────────────────────
Stage fixture JSON from Azure Blob Storage into stg_fixtures.

Flag logic
----------
baseline_loaded  = FALSE   →  First time for this league-season:
                              • load newest baseline in /full/
                              • then every /inc/ file newer than that baseline
                              • set baseline_loaded = TRUE on success

baseline_loaded  = TRUE    →  Skip /full/, load only /inc/ files.
                              Used for ad-hoc updates when you have just
                              fetched a delta file with --mode inc --days N.

A truncate step is assumed *outside* this script (e.g., cleanup_stg.py) so
stg_fixtures is empty before each load.

CLI
---
• No args                 → process every league where
                            backfill_done = TRUE AND full_done = TRUE
                            AND baseline_loaded = FALSE
• --league-id <INT>       → process only that league,
                            respecting its baseline_loaded flag
"""

import os
import sys
import json
import argparse
import datetime as dt
from typing import List, Tuple
from azure.storage.blob import BlobServiceClient

# ── shared import paths -----------------------------------------------------
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.extend([
    os.path.join(BASE, "config"),        # credentials.py
    os.path.join(BASE, "test_scripts"),  # get_db_conn.py
])

from credentials import AZURE_STORAGE                      # noqa: E402
from get_db_conn import get_db_connection                  # noqa: E402

CHUNK_SIZE = 1_000
RAW_CONTAINER = "raw"


# ═════════════════════════════════════════════════════════════════════
# Azure helpers
# ═════════════════════════════════════════════════════════════════════
def list_blobs(prefix: str) -> List[str]:
    """Return sorted blob paths under *prefix* (JSON files only)."""
    svc = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )
    cont = svc.get_container_client(RAW_CONTAINER)
    return sorted(
        b.name for b in cont.list_blobs(name_starts_with=prefix)
        if b.name.endswith(".json")
    )


def fetch_json_blob(path: str) -> dict:
    """Download a JSON blob and return the parsed dict."""
    svc = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )
    blob = svc.get_blob_client(container=RAW_CONTAINER, blob=path)
    return json.loads(blob.download_blob().readall())


# ═════════════════════════════════════════════════════════════════════
# Database helpers
# ═════════════════════════════════════════════════════════════════════
def get_leagues_to_stage(one_league: int | None) -> List[Tuple[int, str, int, bool]]:
    """
    Return (league_id, folder_alias, season_year, baseline_loaded) rows.

    • If one_league is None → select every league ready for its first baseline.
    • If one_league is provided  → select that row regardless of baseline flag.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    if one_league is None:
        cur.execute("""
            SELECT league_id,
                   folder_alias,
                   last_season  AS season_year,
                   baseline_loaded
            FROM   league_catalog
            WHERE  is_enabled       = TRUE
              AND  backfill_done    = TRUE
              AND  full_done        = TRUE
              AND  baseline_loaded  = FALSE
            ORDER BY league_id;
        """)
    else:
        cur.execute("""
            SELECT league_id,
                   folder_alias,
                   last_season  AS season_year,
                   baseline_loaded
            FROM   league_catalog
            WHERE  league_id = %s
              AND  is_enabled = TRUE
              AND  backfill_done = TRUE
              AND  full_done     = TRUE;
        """, (one_league,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def insert_chunk(cur, rows):
    """Bulk-insert rows into stg_fixtures."""
    if not rows:
        return

    placeholders = []
    values = []
    for r in rows:
        placeholders.append("(" + ", ".join(["%s"] * len(r)) + ", true)")
        values.extend(r)

    cur.execute(f"""
        INSERT INTO stg_fixtures (
            fixture_id, league_id, season_year,
            home_team_id, away_team_id,
            fixture_date, status, round,
            home_score, away_score, venue_id,
            is_valid
        )
        VALUES {", ".join(placeholders)}
    """, tuple(values))


def upsert_payload(conn, payload: dict, league_id: int, season: int):
    """Insert / update fixture rows from one API payload."""
    rows = []
    cur = conn.cursor()

    for item in payload.get("response", []):
        fx = item.get("fixture")
        if not fx:
            continue
        rows.append((
            fx["id"],
            league_id,
            season,
            item["teams"]["home"]["id"],
            item["teams"]["away"]["id"],
            fx["date"],
            fx["status"]["short"],
            item["league"]["round"],
            item["goals"]["home"],
            item["goals"]["away"],
            fx["venue"]["id"],
        ))
        if len(rows) >= CHUNK_SIZE:
            insert_chunk(cur, rows)
            rows.clear()
    if rows:
        insert_chunk(cur, rows)
    cur.close()


def mark_baseline_loaded(conn, league_id: int):
    """Set baseline_loaded = TRUE for the league."""
    cur = conn.cursor()
    cur.execute(
        "UPDATE league_catalog "
        "SET baseline_loaded = TRUE, upd_date = NOW() "
        "WHERE league_id = %s;",
        (league_id,),
    )
    conn.commit()
    cur.close()



# ═════════════════════════════════════════════════════════════════════
# Loader core
# ═════════════════════════════════════════════════════════════════════
def load_league(league_id: int, alias: str, season: int, load_baseline: bool):
    """
    Load fixtures for one league-season.
    • If load_baseline is True  → newest /full/ file first, then /inc/ newer.
    • Else                       → skip /full/, load only /inc/ files.
    """
    yr_folder = f"{season}_{(season % 100) + 1:02d}"
    base_path = f"fixtures/{alias}/{yr_folder}"
    full_prefix = f"{base_path}/full/"
    inc_prefix = f"{base_path}/inc/"

    # locate files
    full_files = list_blobs(full_prefix)
    latest_full = full_files[-1] if full_files else None
    inc_files = list_blobs(inc_prefix)

    # when loading baseline, filter inc newer than that baseline (compare timestamps)
    if load_baseline and latest_full:
        base_dt = dt.datetime.strptime(os.path.basename(latest_full), "full_%Y%m%d.json")
        inc_files = [
            p for p in inc_files
            if dt.datetime.strptime(os.path.basename(p), "inc_%Y%m%dT%H%MZ.json") > base_dt
        ]

    # incremental-only runs: keep only the newest inc file
    if not load_baseline and inc_files:
        inc_files = inc_files[-1:]  # list_blobs() returns sorted; last is newest

    if not load_baseline and not inc_files:
        print(f"  No incremental files for league_id={league_id}. Skipping.")
        return

    conn = get_db_connection()
    try:
        if load_baseline:
            if not latest_full:
                raise RuntimeError("No baseline file found.")
            upsert_payload(conn, fetch_json_blob(latest_full), league_id, season)
            print(f"  Loaded baseline {os.path.basename(latest_full)}")

        for path in inc_files:
            upsert_payload(conn, fetch_json_blob(path), league_id, season)
            print(f"  Loaded inc {os.path.basename(path)}")

        conn.commit()

        if load_baseline:
            mark_baseline_loaded(conn, league_id)
            print("  baseline_loaded flag set")
    except Exception as exc:
        conn.rollback()
        print(f"  ERROR: {exc}")
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════
# CLI entry
# ═════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage fixtures from Blob → stg_fixtures."
    )
    parser.add_argument(
        "--league-id",
        type=int,
        help="Stage only this league (otherwise auto-select ready leagues).",
    )
    args = parser.parse_args()

    leagues = get_leagues_to_stage(args.league_id)
    if not leagues:
        print("No leagues to stage.")
        sys.exit(0)

    for league_id, alias, season, baseline_loaded in leagues:
        print(f"Staging league_id={league_id} season={season}")
        load_league(
            league_id=league_id,
            alias=alias,
            season=season,
            load_baseline=not baseline_loaded,
        )
