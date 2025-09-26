#!/usr/bin/env python
"""
load_stg_match_statistics.py
──────────────────────────────────────────────────────────────────────────────
Purpose
-------
1. Discover all statistics-JSON files that belong to **one season**.
2. Parse each file into flat rows (fixture_id • team_id • ~20 stats).
3. Bulk-insert those rows into the staging table `stg_match_statistics`.

Two mutually-exclusive modes
----------------------------
• --season YYYY      →  walk the *initial* folder for that season only  
• --incremental      →  auto-detect the **newest** dated sub-folder inside the
                        incremental tree for the season you pass from Airflow
                        via the CURR_SEASON environment variable.

Why two modes?
• Back-fill DAGs need the *initial* folder (one-off bulk dump).  
• The weekly / daily DAG should always pick up just the latest incremental
  folder without hard-coding the date.
"""

# ────────────────────────────────────────────────────────────────────────────
# 0. Imports & PATH plumbing
# ────────────────────────────────────────────────────────────────────────────
import os
import sys
import json
import re
import datetime as dt
from typing import List

from azure.storage.blob import BlobServiceClient          # Azure Blob SDK

# Add repo-local helper modules (DB connection & secrets) to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../test_scripts")))

from credentials import AZURE_STORAGE                     # noqa: E402
from get_db_conn import get_db_connection                 # noqa: E402

# ────────────────────────────────────────────────────────────────────────────
# 1. Global constants
# ────────────────────────────────────────────────────────────────────────────
CHUNK_SIZE = 1_000           # how many (fixture, team) rows per DB INSERT


# ════════════════════════════════════════════════════════════════════════════
# 2. JSON-to-tuple parser
# ════════════════════════════════════════════════════════════════════════════
def parse_stats_json(fixture_id: str, data: dict) -> List[tuple]:
    """
    Transform the raw API-Football statistics payload into a **flat** tuple
    that matches the column order in `stg_match_statistics`.

    Parameters
    ----------
    fixture_id : str   – same ID that appears in the blob file-name
    data       : dict  – deserialized JSON from the API

    Returns
    -------
    List[tuple] – one tuple per *team* (home & away), each containing:
        fixture_id • team_id • shots_on_goal … goals_prevented
    """
    rows = []

    # The API returns an array with two objects: one per team
    for team_block in data.get("response", []):
        team_id = team_block.get("team", {}).get("id")

        # Map external stat names → our concise column names
        api_to_col = {
            "Shots on Goal":     "shots_on_goal",
            "Shots off Goal":    "shots_off_goal",
            "Shots insidebox":   "shots_inside_box",
            "Shots outsidebox":  "shots_outside_box",
            "Blocked Shots":     "blocked_shots",
            "Goalkeeper Saves":  "goalkeeper_saves",
            "Total Shots":       "total_shots",
            "Corner Kicks":      "corners",
            "Offsides":          "offsides",
            "Fouls":             "fouls",
            "Ball Possession":   "possession",
            "Yellow Cards":      "yellow_cards",
            "Red Cards":         "red_cards",
            "Total passes":      "total_passes",
            "Passes accurate":   "passes_accurate",
            "Passes %":          "pass_accuracy",
            "expected_goals":    "expected_goals",
            "goals_prevented":   "goals_prevented",
        }

        # Start every column as NULL/None so missing stats ≠ crash
        stats = {v: None for v in api_to_col.values()}

        # Walk the  “statistics”  array and copy values into our dict
        for s in team_block.get("statistics", []):
            col = api_to_col.get(s.get("type"))
            if not col:
                continue                      # ignore stats we don’t track
            val = s.get("value")

            # Normalise % strings like "67%" → int(67)
            if col in {"possession", "pass_accuracy"} and isinstance(val, str):
                val = val.rstrip("%")

            # Cast to numeric where sensible; leave bad data as None
            try:
                stats[col] = float(val) if col.startswith(("expected", "goals")) else int(val)
            except Exception:
                stats[col] = None

        # Order here **must** match INSERT column list later on
        rows.append((
            fixture_id,
            team_id,
            stats["shots_on_goal"],  stats["shots_off_goal"],
            stats["shots_inside_box"], stats["shots_outside_box"],
            stats["blocked_shots"],  stats["goalkeeper_saves"],   stats["total_shots"],
            stats["corners"],        stats["offsides"],           stats["fouls"], stats["possession"],
            stats["yellow_cards"],   stats["red_cards"],
            stats["total_passes"],   stats["passes_accurate"],    stats["pass_accuracy"],
            stats["expected_goals"], stats["goals_prevented"],
        ))
    return rows


# ════════════════════════════════════════════════════════════════════════════
# 3. DB helper – chunked INSERT into staging table
# ════════════════════════════════════════════════════════════════════════════
def insert_chunk_stg_stats(cur, rows_buffer: List[tuple]) -> None:
    """
    Build a single `INSERT … VALUES (…), (…), …` statement for a chunk of rows
    and execute it in one round-trip for performance.
    """
    if not rows_buffer:
        return                                  # nothing to do

    # Build "(%s,%s,…)" placeholders × row-count  ➜ "(?,?,?), (?,?,?), …"
    placeholder = "(" + ", ".join(["%s"] * len(rows_buffer[0])) + ", true)"
    all_placeholders = ", ".join([placeholder] * len(rows_buffer))

    # Flatten list-of-tuples → flat list to match psycopg2 parameter order
    flat_values = [val for row in rows_buffer for val in row]

    cur.execute(f"""
        INSERT INTO stg_match_statistics (
          fixture_id, team_id,
          shots_on_goal, shots_off_goal, shots_inside_box, shots_outside_box,
          blocked_shots, goalkeeper_saves, total_shots,
          corners, offsides, fouls, possession,
          yellow_cards, red_cards,
          total_passes, passes_accurate, pass_accuracy,
          expected_goals, goals_prevented,
          is_valid
        ) VALUES {all_placeholders}
    """, flat_values)


# ════════════════════════════════════════════════════════════════════════════
# 4. Main control-flow
# ════════════════════════════════════════════════════════════════════════════
def main() -> None:
    """
    1. Parse CLI flags  (--season OR --incremental)
    2. Decide which Azure Blob prefix to scan
    3. Stream each JSON file → parse → buffer → chunk-insert to DB
    4. Print a concise summary so Airflow’s log tail is readable
    """
    # ── 4.1 CLI flags ────────────────────────────────────────────────────
    import argparse
    p = argparse.ArgumentParser(
        description="Load fixture-stat JSON into stg_match_statistics."
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--season", type=int,
                       help="Season start-year; loads *initial* folder for that season")
    group.add_argument("--incremental", action="store_true",
                       help="Load newest incremental folder for CURR_SEASON env-var")
    args = p.parse_args()

    # ── 4.2 Connect to Azure Blob once (re-used for every download) ─────
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_service = BlobServiceClient(account_url, credential=AZURE_STORAGE["access_key"])
    container    = blob_service.get_container_client("raw")

    # ── 4.3 Work out WHICH folder to scan based on CLI mode ──────────────
    # • --season YYYY      → match_statistics/YYYY_YY/initial/
    # • --incremental      → match_statistics/<CURR_SEASON>/incremental/<latest date>/
    if args.incremental:
        season = int(os.getenv("CURR_SEASON", "2024"))          # Airflow passes env-var
        season_str  = f"{season}_{(season % 100)+1}"
        inc_root    = f"match_statistics/{season_str}/incremental"

        # Find *most recent* dated sub-folder (YYYY-MM-DD)
        date_regex  = re.compile(rf"^{re.escape(inc_root)}/(\d{{4}}-\d{{2}}-\d{{2}})/")
        latest_date = None
        for blob in container.list_blobs(name_starts_with=inc_root):
            m = date_regex.match(blob.name)
            if m:
                d = dt.date.fromisoformat(m.group(1))
                if latest_date is None or d > latest_date:
                    latest_date = d
        if latest_date is None:
            print(f"No incremental folders under {inc_root} – nothing to load.")
            return
        blob_prefix = f"{inc_root}/{latest_date:%Y-%m-%d}"
    else:
        season      = args.season
        season_str  = f"{season}_{(season % 100)+1}"
        blob_prefix = f"match_statistics/{season_str}/initial"

    print(f"Scanning blobs under → {blob_prefix}")

    # ── 4.4 Begin DB session & stream files ──────────────────────────────
    conn = get_db_connection()
    cur  = conn.cursor()
    buffer = []
    total, parsed, failed = 0, 0, 0

    for blob in container.list_blobs(name_starts_with=blob_prefix):
        total += 1
        blob_name = blob.name
        m = re.search(r"statistics_(\d+)\.json$", blob_name)
        if not m:
            continue                                # skip folder markers etc.

        fixture_id = m.group(1)
        try:
            raw = container.get_blob_client(blob).download_blob().readall()
            rows = parse_stats_json(fixture_id, json.loads(raw))
            buffer.extend(rows)
            parsed += 1
            if len(buffer) >= CHUNK_SIZE:
                insert_chunk_stg_stats(cur, buffer); conn.commit(); buffer.clear()
        except Exception as exc:
            failed += 1
            print(f"Error on {blob_name} → {exc}")

    # flush remaining rows
    if buffer:
        insert_chunk_stg_stats(cur, buffer); conn.commit()

    cur.close(); conn.close()

    # ── 4.5 Summary banner (Airflow log tail) ────────────────────────────
    print("\n=========  STG MATCH_STATISTICS LOAD SUMMARY  =========")
    print(f"Blobs scanned      : {total}")
    print(f"Blobs parsed OK    : {parsed}")
    print(f"Blobs failed       : {failed}")
    if total == 0:
        print("WARNING: no files matched the prefix above!")
    print("========================================================\n")


if __name__ == "__main__":
    main()
