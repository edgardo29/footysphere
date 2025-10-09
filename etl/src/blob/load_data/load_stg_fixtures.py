#!/usr/bin/env python3
"""
load_stg_fixtures.py  (DB-driven)
Stage fixture JSON from Azure Blob Storage into stg_fixtures.

Modes
-----
--mode baseline  → select leagues where baseline_loaded=FALSE
                    load newest /full then any newer /inc; flip baseline_loaded→TRUE
--mode inc       → select leagues where baseline_loaded=TRUE
                    load only the newest /inc file
[--league-id ID] → optional override to scope to a single league

Assumption: cleanup_stg has already truncated stg_fixtures.
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
def list_blobs(prefix: str):
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
    svc = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )
    blob = svc.get_blob_client(container=RAW_CONTAINER, blob=path)
    return json.loads(blob.download_blob().readall())


# ═════════════════════════════════════════════════════════════════════
# Database helpers
# ═════════════════════════════════════════════════════════════════════
def select_leagues(mode: str, one_league: int | None) -> List[Tuple[int, str, int, bool]]:
    """
    Return (league_id, folder_alias, season_year, baseline_loaded) rows,
    DB-driven based on mode & flags.
    """
    assert mode in ("baseline", "inc")
    conn = get_db_connection()
    cur = conn.cursor()

    if one_league is not None:
        cur.execute("""
            SELECT league_id, folder_alias, last_season AS season_year, baseline_loaded
              FROM league_catalog
             WHERE league_id = %s
               AND is_enabled = TRUE
               AND backfill_done = TRUE
               AND full_done = TRUE
             ORDER BY league_id;
        """, (one_league,))
    elif mode == "baseline":
        cur.execute("""
            SELECT league_id, folder_alias, last_season AS season_year, baseline_loaded
              FROM league_catalog
             WHERE is_enabled = TRUE
               AND backfill_done = TRUE
               AND full_done = TRUE
               AND baseline_loaded = FALSE
             ORDER BY league_id;
        """)
    else:  # mode == "inc"
        cur.execute("""
            SELECT league_id, folder_alias, last_season AS season_year, baseline_loaded
              FROM league_catalog
             WHERE is_enabled = TRUE
               AND backfill_done = TRUE
               AND full_done = TRUE
               AND baseline_loaded = TRUE
             ORDER BY league_id;
        """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def insert_chunk(cur, rows):
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
    cur = conn.cursor()
    cur.execute(
        "UPDATE league_catalog SET baseline_loaded = TRUE, upd_date = NOW() WHERE league_id = %s;",
        (league_id,),
    )
    conn.commit()
    cur.close()


# ═════════════════════════════════════════════════════════════════════
# Loader core
# ═════════════════════════════════════════════════════════════════════
def load_league(league_id: int, alias: str, season: int, load_baseline: bool):
    """
    baseline mode → newest /full then newer /inc; set baseline_loaded=TRUE
    inc mode      → newest /inc only
    """
    yr_folder = f"{season}_{(season % 100) + 1:02d}"
    base_path = f"fixtures/{alias}/{yr_folder}"
    full_prefix = f"{base_path}/full/"
    inc_prefix = f"{base_path}/inc/"

    full_files = list_blobs(full_prefix)
    latest_full = full_files[-1] if full_files else None
    inc_files = list_blobs(inc_prefix)

    if load_baseline:
        if not latest_full:
            # hard fail: you can't baseline without a baseline file uploaded
            raise RuntimeError(f"No baseline file found at {full_prefix}")
        base_dt = dt.datetime.strptime(os.path.basename(latest_full), "full_%Y%m%d.json")
        inc_files = [
            p for p in inc_files
            if dt.datetime.strptime(os.path.basename(p), "inc_%Y%m%dT%H%MZ.json") > base_dt
        ]
    else:
        # inc mode: only the newest inc file (if any)
        inc_files = inc_files[-1:] if inc_files else []

    conn = get_db_connection()
    try:
        if load_baseline:
            upsert_payload(conn, fetch_json_blob(latest_full), league_id, season)
            print(f"  Loaded baseline {os.path.basename(latest_full)}")
        for p in inc_files:
            upsert_payload(conn, fetch_json_blob(p), league_id, season)
            print(f"  Loaded inc {os.path.basename(p)}")
        conn.commit()
        if load_baseline:
            mark_baseline_loaded(conn, league_id)
            print("  baseline_loaded flag set")
    except Exception as exc:
        conn.rollback()
        print(f"  ERROR: {exc}")
        raise
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════
# CLI entry
# ═════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage fixtures from Blob → stg_fixtures (DB-driven).")
    parser.add_argument("--mode", choices=["baseline", "inc"], required=True,
                        help="baseline = baseline_loaded=FALSE selection; inc = baseline_loaded=TRUE selection.")
    parser.add_argument("--league-id", type=int, help="Optional single-league override.")
    args = parser.parse_args()

    leagues = select_leagues(args.mode, args.league_id)
    if not leagues:
        print("No leagues to stage.")
        sys.exit(0)

    for league_id, alias, season, baseline_loaded in leagues:
        print(f"Staging league_id={league_id} season={season} ({'baseline' if args.mode=='baseline' else 'inc'})")
        load_league(
            league_id=league_id,
            alias=alias,
            season=season,
            load_baseline=(args.mode == "baseline"),
        )
