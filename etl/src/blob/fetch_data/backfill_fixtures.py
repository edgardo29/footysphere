#!/usr/bin/env python3
"""
backfill_fixtures.py

One-off back-fill:
• For each league where backfill_done = FALSE,
  - fetch every season in full mode
  - upload snapshots (skipping blobs that already exist)
• If the league finishes without errors, set
     backfill_done = TRUE
     full_done     = TRUE
"""

import os
import sys

# ── local imports -------------------------------------------------------------
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.extend([
    os.path.join(BASE, "config"),        # credentials.py
    os.path.join(BASE, "test_scripts"),  # get_db_conn.py
    os.path.dirname(__file__)            # fetch_fixtures.py
])

from get_db_conn import get_db_connection                       # noqa: E402
from fetch_fixtures import call_api, make_blob_path, upload_blob  # noqa: E402

RAW_CONTAINER = "raw"


def backfill_all_leagues() -> None:
    """Back-fill every league where backfill_done = FALSE."""
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"ERROR: cannot connect to database: {e}")
        sys.exit(1)

    cur = conn.cursor()
    cur.execute("""
        SELECT league_id, folder_alias, first_season, last_season
          FROM league_catalog
         WHERE is_enabled = TRUE
           AND backfill_done = FALSE;
    """)
    leagues = cur.fetchall()

    if not leagues:
        print("No leagues pending back-fill.")
        cur.close()
        conn.close()
        return

    for league_id, alias, first_season, last_season in leagues:
        # Build list of season folders (e.g. 2024_25, 2025_26)
        season_labels = [
            f"{yr}_{(yr % 100) + 1:02d}"
            for yr in range(first_season, last_season + 1)
        ]
        header = ("season" if len(season_labels) == 1 else "seasons")
        print(f"Back-filling {alias}: {header} {', '.join(season_labels)}")

        league_failed = False

        for season_folder in season_labels:
            season_year = int(season_folder.split("_")[0])
            print(f"  Fetching {alias} {season_folder} [full]")

            # 1) Call API for this league-season (full snapshot)
            try:
                data = call_api(league_id, season_year, "full")
            except Exception as e:
                print(f"    ERROR fetching {alias} {season_folder}: {e}")
                league_failed = True
                continue

            # 2) Build blob path and upload (immutable; upload_blob handles 'exists')
            path = make_blob_path(alias, season_year, "full")
            try:
                upload_blob(path, data)
            except Exception as e:
                print(f"    ERROR uploading {RAW_CONTAINER}/{path}: {e}")
                league_failed = True

        # 3) Flip flags only if every season succeeded (or was already present)
        if not league_failed:
            try:
                cur.execute(
                    """
                    UPDATE league_catalog
                       SET backfill_done = TRUE,
                           full_done     = TRUE
                     WHERE league_id = %s;
                    """,
                    (league_id,)
                )
                conn.commit()
                print(f"  backfill_done and full_done set to TRUE for {alias}")
            except Exception as e:
                print(f"  ERROR updating flags for {alias}: {e}")
        else:
            print(f"  WARNING: incomplete back-fill for {alias}; flags unchanged")

    cur.close()
    conn.close()


if __name__ == "__main__":
    backfill_all_leagues()
