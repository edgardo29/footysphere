#!/usr/bin/env python
"""
load_team_league_seasons.py
────────────────────────────────────────────────────────────────────────────
PURPOSE
    Populate (or refresh) the bridge table `team_league_seasons`, which
    records every combination of   team_id • league_id • season_year
    that appears in your teams blobs.

DATA FLOW
    1.  Read the **exact blob_path** for the *latest* season of every league
        from `team_ingest_status` –– no hard-coded “_25” suffixes.
    2.  For each blob:
          • download JSON once from Azure
          • extract all unique team_id values
          • bulk-insert with one  INSERT … ON CONFLICT … DO UPDATE  that
            sets   load_date   on first insert,  upd_date   on every refresh
    3.  Print per-league stats and a grand total.


RUN ORDER
    • Call this **after** update_teams.py (so stg_teams is already merged).
      Example DAG section:

          load_stg_teams → check_stg_teams → update_teams
          **load_team_league_seasons.py**   ← here
          (fetch venue fills …)

HOW TO RUN
    $ source footysphere_venv/bin/activate
    $ python src/procs/load_team_league_seasons.py
"""

# ───────────────────────── Imports & search-paths ─────────────────────
import os
import sys
import json
from collections import defaultdict
from typing import List, Tuple

import psycopg2.extras as pgex
from azure.storage.blob import BlobServiceClient

# Add helper modules to sys.path
sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])

from credentials import AZURE_STORAGE                     # blob credentials
from get_db_conn import get_db_connection                 # psycopg connector

# ───────────────────────── Helper: open a blob & parse JSON ───────────
def load_json_from_blob(bs: BlobServiceClient, path: str) -> dict:
    """
    Download teams.json as bytes -> dict.
    Raises blob errors upward; caller will catch & log.
    """
    blob = bs.get_blob_client("raw", path)
    raw  = blob.download_blob().readall()
    return json.loads(raw)

# ───────────────────────── Helper: latest season rows ─────────────────
LATEST_SEASON_SQL = """
    SELECT league_id,
           season_year,
           blob_path
    FROM   team_ingest_status tis
    WHERE  season_year = (
              SELECT MAX(season_year)
              FROM   team_ingest_status t2
              WHERE  t2.league_id = tis.league_id
          )
      AND  teams_load_date IS NOT NULL          -- blob was staged
    ORDER  BY league_id;
"""

# ───────────────────────── Main loader routine ────────────────────────
def main() -> None:
    print("=== Populating team_league_seasons ===")

    # 1. Open PostgreSQL + Blob connections once (reuse = faster).
    conn = get_db_connection()
    cur  = conn.cursor()
    blob_service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential  =AZURE_STORAGE["access_key"],
    )

    # 2. Build the “work list” – one row per league.
    cur.execute(LATEST_SEASON_SQL)
    league_rows = cur.fetchall()
    if not league_rows:
        print("Nothing to process – team_ingest_status empty.")
        cur.close(); conn.close(); return

    grand_ins, grand_upd = 0, 0  # running totals for final summary

    # 3. Process each league-season in turn
    for league_id, season_year, blob_path in league_rows:
        # 3a. Download & parse the teams blob
        try:
            payload = load_json_from_blob(blob_service, blob_path)
        except Exception as err:
            print(f"[L{league_id} {season_year}] blob error → {err}")
            continue

        # 3b. Extract all unique team_id values from JSON
        team_ids = {
            item.get("team", {}).get("id")
            for item in payload.get("response", [])
            if item.get("team", {}).get("id")
        }
        if not team_ids:
            print(f"[L{league_id} {season_year}] no team IDs found – skip.")
            continue

        # 3c. Build VALUES list for bulk insert
        rows: List[Tuple[int, int, int]] = [
            (tid, league_id, season_year) for tid in team_ids
        ]

        # 3d. ONE bulk upsert: insert new rows, refresh upd_date on existing
        insert_sql = """
            INSERT INTO team_league_seasons (team_id, league_id, season_year)
            VALUES %s
            ON CONFLICT (team_id, league_id, season_year) DO
              UPDATE SET upd_date = NOW()
            RETURNING xmax = 0 AS inserted_flag;   -- TRUE when row was inserted
        """
        pgex.execute_values(cur, insert_sql, rows, page_size=1000)

        # 3e. Count inserts vs updates via the xmax flag
        results   = cur.fetchall()
        ins_count = sum(1 for flag, in results if flag)
        upd_count = len(results) - ins_count
        conn.commit()

        print(f"[L{league_id} {season_year}] +{ins_count} inserts, {upd_count} updates")
        grand_ins  += ins_count
        grand_upd  += upd_count

    # 4. Grand summary
    print("\nteam_league_seasons summary")
    print("---------------------------")
    print(f"{'rows_inserted':<16}: {grand_ins}")
    print(f"{'rows_updated':<16}: {grand_upd}")

    # 5. Clean-up connections
    cur.close()
    conn.close()


# ──────────────────────────────────────────────────────────────────────
#  Entry-point guard – allows “python load_team_league_seasons.py”
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()