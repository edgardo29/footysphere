#!/usr/bin/env python
"""
load_leagues.py
────────────────
Load every `league.json` from Azure Blob into:
  • leagues
  • league_seasons

Counts:
  Inserted = PK not present before this run.
  Updated  = at least one tracked column changed.

Run:
  python load_leagues.py
"""

import json
import os
import sys
from typing import List, Tuple
from azure.storage.blob import BlobServiceClient

# ── project imports (adjust if your tree differs) ──────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(os.path.join(BASE_DIR, "config"))        # credentials.py
sys.path.append(os.path.join(BASE_DIR, "test_scripts"))  # get_db_conn.py
from credentials import AZURE_STORAGE
from get_db_conn import get_db_connection

def season_suffix(year: int) -> str:
    return f"{year}_{str(year + 1)[-2:]}"

def build_work_list(cur) -> List[Tuple[int, str, int, int]]:
    """
    Returns (league_id, folder_alias, first_season, last_season).
    If last_season is NULL, use max(ld.seasons).
    """
    cur.execute("""
        SELECT lc.league_id,
               lc.folder_alias,
               lc.first_season,
               lc.last_season,
               ld.seasons
        FROM   league_catalog lc
        JOIN   league_directory ld USING (league_id)
        WHERE  lc.is_enabled
    """)
    out: list[tuple[int, str, int, int]] = []
    for lid, alias, first, last, seasons_json in cur.fetchall():
        seasons_list = seasons_json if isinstance(seasons_json, list) else json.loads(seasons_json)
        if last is None:
            last = max(seasons_list)
        out.append((lid, alias, first, last))
    return out

def blob_json(blob_svc: BlobServiceClient, path: str) -> dict | None:
    blob = blob_svc.get_blob_client(container="raw", blob=path)
    if not blob.exists():
        return None
    return json.loads(blob.download_blob().readall())

def main() -> None:
    # 1) DB
    conn = get_db_connection()
    cur  = conn.cursor()

    work_items = build_work_list(cur)
    if not work_items:
        print("league_catalog has no enabled rows — nothing to load.")
        return

    # 2) Blob
    blob_svc = BlobServiceClient(
        f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )

    # 3) Counters
    leagues_inserted = leagues_updated = 0
    seasons_inserted = seasons_updated = 0
    missing_blobs    = 0

    # 4) Process
    for league_id, alias, first_season, last_season in work_items:
        for yr in range(first_season, last_season + 1):
            label    = season_suffix(yr)
            blob_path = f"leagues/{alias}/{label}/league.json"
            payload   = blob_json(blob_svc, blob_path)
            if payload is None:
                print(f"missing   raw/{blob_path} (fetch step?)")
                missing_blobs += 1
                continue

            league_info = payload["response"][0]

            # ---- LEAGUES ------------------------------------------------
            def slugify(name: str) -> str:
                return "_".join(name.lower().split())
            new_alias = alias or slugify(league_info["league"]["name"])

            new_league_vals = (
                league_info["league"]["name"],
                league_info["league"]["logo"],
                league_info["country"]["name"],
                new_alias,
            )

            cur.execute(
                "SELECT league_name, league_logo_url, league_country, folder_alias "
                "FROM leagues WHERE league_id = %s",
                (league_id,)
            )
            current = cur.fetchone()

            if current is None:
                cur.execute(
                    "INSERT INTO leagues "
                    "(league_id, league_name, league_logo_url, league_country, folder_alias) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (league_id, *new_league_vals),
                )
                leagues_inserted += 1
            elif current != new_league_vals:
                cur.execute(
                    "UPDATE leagues "
                    "SET league_name=%s, league_logo_url=%s, league_country=%s, folder_alias=%s "
                    "WHERE league_id=%s",
                    (*new_league_vals, league_id),
                )
                leagues_updated += 1
            # else identical → no-op

            # ---- LEAGUE_SEASONS ----------------------------------------
            season_obj = next((s for s in league_info["seasons"] if s.get("year") == yr), None)
            if season_obj is None:
                print(f"warning: season {yr} missing in JSON for league {league_id}")
                continue

            new_start = (season_obj.get("start") or "")[:10]
            new_end   = (season_obj.get("end")   or "")[:10]
            new_curr  = bool(season_obj.get("current", False))
            new_season_vals = (new_start, new_end, new_curr, label)

            cur.execute(
                "SELECT start_date::text, end_date::text, is_current, season_str "
                "FROM league_seasons WHERE league_id=%s AND season_year=%s",
                (league_id, yr),
            )
            row = cur.fetchone()
            if row:
                current_season_row = (
                    (row[0] or "")[:10],
                    (row[1] or "")[:10],
                    row[2],
                    row[3] or None,
                )
            else:
                current_season_row = None

            if current_season_row is None:
                # INSERT with season_str; timestamps use DEFAULTs
                cur.execute(
                    "INSERT INTO league_seasons "
                    "(league_id, season_year, start_date, end_date, is_current, season_str) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (league_id, yr, *new_season_vals),
                )
                seasons_inserted += 1
            elif current_season_row != new_season_vals:
                # UPDATE only when something changed; bump upd_date
                cur.execute(
                    "UPDATE league_seasons "
                    "SET start_date=%s, end_date=%s, is_current=%s, season_str=%s, upd_date=NOW() "
                    "WHERE league_id=%s AND season_year=%s",
                    (*new_season_vals, league_id, yr),
                )
                seasons_updated += 1
            # else identical → no-op

    # 5) Commit + close
    conn.commit()
    cur.close()
    conn.close()

    # 6) Summary
    print("\nSummary")
    print("───────")
    print("  leagues table:")
    print(f"      inserted: {leagues_inserted:3}    updated: {leagues_updated:3}")
    print("  league_seasons table:")
    print(f"      inserted: {seasons_inserted:3}    updated: {seasons_updated:3}")
    print(f"  Missing blobs: {missing_blobs}")

if __name__ == "__main__":
    main()
