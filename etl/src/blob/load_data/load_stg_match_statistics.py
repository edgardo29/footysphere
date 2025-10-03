#!/usr/bin/env python
"""
load_stg_match_statistics.py  (alias-first blob layout, per-league latest run)
──────────────────────────────────────────────────────────────────────────────
Loads API-Football match statistics JSON from Azure Blob into the staging table
`stg_match_statistics`.

USAGE (examples)
---------------
Initial (full-season backfill, all leagues):
  python load_stg_match_statistics.py --mode initial --season 2025

Initial (single league via league_id):
  python load_stg_match_statistics.py --mode initial --season 2025 --league-id 39

Incremental (latest run, all leagues):
  python load_stg_match_statistics.py --mode incremental --season 2025

Incremental (single league via league_id):
  python load_stg_match_statistics.py --mode incremental --season 2025 --league-id 39

Behavior
--------
• Resolves <season> → season_str "YYYY_YY" (e.g., 2025 → "2025_26").
• If --league-id is given, resolves folder_alias from league_catalog;
  otherwise discovers all aliases present in blob storage for that season/mode.
• For each alias, selects the **latest** timestamp run folder
  (YYYY-MM-DD_HHMMSS) under:
    raw/match_statistics/<alias>/<season_str>/<mode>/<stamp>/*.json
• Streams each "statistics_<fixture>.json", flattens to two rows (home/away),
  and bulk-inserts into stg_match_statistics (with is_valid/error_reason).

Notes
-----
• This script **does not** truncate the staging table. Run your cleanup step
  separately before calling this loader.
• Staging schema columns assumed exactly as provided (load_timestamp has DEFAULT).
"""

import os
import sys
import re
import json
import argparse
import logging
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from azure.storage.blob import BlobServiceClient

# Repo-local helpers
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from credentials import AZURE_STORAGE                     # noqa: E402
from get_db_conn import get_db_connection                 # noqa: E402

# ────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────
CHUNK_SIZE = 1_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load_stg_match_statistics")
for noisy in ("azure.storage", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# Mapping from API-Football stat labels → staging column names
# Include both spellings for inside/outside box variants seen in the wild.
API_TO_COL = {
    "Shots on Goal":     "shots_on_goal",
    "Shots off Goal":    "shots_off_goal",
    "Shots insidebox":   "shots_inside_box",
    "Shots inside box":  "shots_inside_box",
    "Shots outsidebox":  "shots_outside_box",
    "Shots outside box": "shots_outside_box",
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
    # Advanced metrics (appear only for some leagues/feeds)
    "expected_goals":    "expected_goals",
    "goals_prevented":   "goals_prevented",
}

RUNSTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{6}$")
BLOB_STATS_RE = re.compile(r"statistics_(\d+)\.json$")


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────
def season_to_str(season_start: int) -> str:
    return f"{season_start}_{(season_start % 100) + 1:02d}"


def get_blob_service() -> BlobServiceClient:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])


def resolve_alias_for_league(cur, league_id: int) -> str:
    """
    Map league_id → folder_alias via league_catalog, fallback to 'league_<id>'.
    """
    cur.execute(
        "SELECT COALESCE(NULLIF(folder_alias, ''), CONCAT('league_', %s)) "
        "FROM league_catalog WHERE league_id = %s",
        (league_id, league_id),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else f"league_{league_id}"


def discover_aliases(container, season_str: str, mode: str) -> List[str]:
    """
    Enumerate aliases present for given season/mode by scanning blob names.
    Looks under: match_statistics/*/<season_str>/<mode>/
    """
    prefix = f"match_statistics/"
    aliases: set = set()
    # We scan a narrow prefix that must include season_str/mode for efficiency.
    # Azure doesn't have true dirs; we parse the name segments.
    for blob in container.list_blobs(name_starts_with=prefix):
        # Expect: match_statistics/<alias>/<season_str>/<mode>/...
        parts = blob.name.split("/")
        if len(parts) < 5:
            continue
        if parts[0] != "match_statistics":
            continue
        alias, season_part, mode_part = parts[1], parts[2], parts[3]
        if season_part == season_str and mode_part == mode:
            aliases.add(alias)
    return sorted(aliases)


def latest_runstamp_for_alias(container, alias: str, season_str: str, mode: str) -> Optional[str]:
    """
    Return the latest YYYY-MM-DD_HHMMSS run folder name for the alias/season/mode,
    or None if none found.
    """
    root = f"match_statistics/{alias}/{season_str}/{mode}/"
    stamps: set = set()
    for blob in container.list_blobs(name_starts_with=root):
        # Names look like: match_statistics/<alias>/<season_str>/<mode>/<stamp>/statistics_XXXX.json
        parts = blob.name.split("/")
        if len(parts) < 6:
            continue
        stamp = parts[4]
        if RUNSTAMP_RE.match(stamp):
            stamps.add(stamp)
    if not stamps:
        return None
    # Lexicographic max works for YYYY-MM-DD_HHMMSS format
    return max(stamps)


def parse_stats_json(fixture_id: int, data: dict) -> List[Tuple]:
    """
    Convert a single fixture's statistics payload into 2 tuples (home/away),
    aligned with stg_match_statistics columns (excluding load_timestamp).
    """
    out: List[Tuple] = []

    response = data.get("response")
    if not isinstance(response, list) or len(response) == 0:
        # No team blocks; emit a single invalid row for traceability
        out.append((
            fixture_id, None,           # fixture_id, team_id
            None, None, None, None,     # shots_on_goal … shots_outside_box
            None, None, None,           # blocked_shots … total_shots
            None, None, None, None,     # corners … possession
            None, None,                 # yellow_cards, red_cards
            None, None, None,           # total_passes, passes_accurate, pass_accuracy
            None, None,                 # expected_goals, goals_prevented
            False, "empty or invalid 'response' array"
        ))
        return out

    for team_block in response:
        team_id = team_block.get("team", {}).get("id")
        # Initialize all tracked stats to None
        stats: Dict[str, Optional[float]] = {v: None for v in API_TO_COL.values()}

        # Copy values from API list
        for s in team_block.get("statistics", []) or []:
            t = s.get("type")
            if t not in API_TO_COL:
                continue
            col = API_TO_COL[t]
            val = s.get("value")

            # Normalize percent strings like "67%" to int 67
            if col in {"possession", "pass_accuracy"} and isinstance(val, str):
                val = val.rstrip("%")

            try:
                if col in {"expected_goals", "goals_prevented"}:
                    stats[col] = float(val) if val is not None else None
                else:
                    stats[col] = int(val) if val is not None else None
            except Exception:
                # Leave as None if non-numeric
                stats[col] = None

        row = (
            int(fixture_id),
            int(team_id) if team_id is not None else None,

            stats["shots_on_goal"],  stats["shots_off_goal"],
            stats["shots_inside_box"], stats["shots_outside_box"],
            stats["blocked_shots"],  stats["goalkeeper_saves"], stats["total_shots"],

            stats["corners"], stats["offsides"], stats["fouls"], stats["possession"],
            stats["yellow_cards"], stats["red_cards"],

            stats["total_passes"], stats["passes_accurate"], stats["pass_accuracy"],
            stats["expected_goals"], stats["goals_prevented"],

            True,    # is_valid
            None,    # error_reason
        )
        out.append(row)

    return out


def insert_chunk(cur, rows: Sequence[Tuple]) -> None:
    if not rows:
        return
    # Columns to insert (excluding load_timestamp which has DEFAULT)
    col_list = (
        "fixture_id, team_id, "
        "shots_on_goal, shots_off_goal, shots_inside_box, shots_outside_box, "
        "blocked_shots, goalkeeper_saves, total_shots, "
        "corners, offsides, fouls, possession, "
        "yellow_cards, red_cards, "
        "total_passes, passes_accurate, pass_accuracy, "
        "expected_goals, goals_prevented, "
        "is_valid, error_reason"
    )
    placeholders_one = "(" + ", ".join(["%s"] * 22) + ")"
    placeholders = ", ".join([placeholders_one] * len(rows))
    flat: List = [v for row in rows for v in row]
    cur.execute(f"INSERT INTO stg_match_statistics ({col_list}) VALUES {placeholders}", flat)


# ────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Load match statistics JSON into stg_match_statistics.")
    ap.add_argument("--mode", required=True, choices=["initial", "incremental"],
                    help="Select which blob tree to load (initial or incremental).")
    ap.add_argument("--season", required=True, type=int,
                    help="Season start year (e.g., 2025 for 2025_26).")
    ap.add_argument("--league-id", type=int,
                    help="Optional league_id to load only one league (alias resolved via DB).")
    args = ap.parse_args()

    season_str = season_to_str(args.season)
    mode = args.mode

    # Azure
    svc = get_blob_service()
    container = svc.get_container_client("raw")

    # DB connection (for alias resolution & inserts)
    conn = get_db_connection()
    cur = conn.cursor()

    # Determine which aliases to process
    if args.league_id is not None:
        alias = resolve_alias_for_league(cur, args.league_id)
        aliases = [alias]
        log.info("Single-league mode → league_id=%s resolved alias=%s", args.league_id, alias)
    else:
        aliases = discover_aliases(container, season_str, mode)
        if not aliases:
            log.warning("No aliases found under match_statistics/*/%s/%s/ → nothing to load.", season_str, mode)
            cur.close(); conn.close()
            return
        log.info("Discovered %d alias(es) for season=%s mode=%s", len(aliases), season_str, mode)

    total_files = total_parsed = total_failed_files = 0
    total_rows = 0

    buffer: List[Tuple] = []

    for alias in aliases:
        stamp = latest_runstamp_for_alias(container, alias, season_str, mode)
        if not stamp:
            log.warning("SKIP alias=%s • No run-stamp folder under %s/%s/%s/",
                        alias, season_str, mode, alias)
            continue

        prefix = f"match_statistics/{alias}/{season_str}/{mode}/{stamp}/"
        log.info("Processing alias=%s • stamp=%s • prefix=%s", alias, stamp, prefix)

        files_scanned = files_parsed = files_failed = 0
        rows_inserted_alias = 0

        for blob in container.list_blobs(name_starts_with=prefix):
            name = blob.name
            m = BLOB_STATS_RE.search(name)
            if not m:
                continue
            files_scanned += 1
            fixture_id = int(m.group(1))

            try:
                raw = container.get_blob_client(blob=name).download_blob().readall()
                data = json.loads(raw)
                rows = parse_stats_json(fixture_id, data)
                buffer.extend(rows)
                files_parsed += 1
                rows_inserted_alias += len(rows)

                if len(buffer) >= CHUNK_SIZE:
                    insert_chunk(cur, buffer)
                    conn.commit()
                    buffer.clear()
            except Exception as exc:
                files_failed += 1
                # Emit a file-level invalid row so issues are visible in staging
                buffer.append((
                    fixture_id, None,
                    None, None, None, None,
                    None, None, None,
                    None, None, None, None,
                    None, None,
                    None, None, None,
                    None, None,
                    False, f"exception: {type(exc).__name__}: {exc}"
                ))
                if len(buffer) >= CHUNK_SIZE:
                    insert_chunk(cur, buffer)
                    conn.commit()
                    buffer.clear()

        total_files += files_scanned
        total_parsed += files_parsed
        total_failed_files += files_failed
        total_rows += rows_inserted_alias

        log.info("alias=%s • scanned=%d parsed=%d failed=%d rows=%d",
                 alias, files_scanned, files_parsed, files_failed, rows_inserted_alias)

    # Flush any remaining rows
    if buffer:
        insert_chunk(cur, buffer)
        conn.commit()
        buffer.clear()

    cur.close(); conn.close()

    print("\n=========  STG MATCH_STATISTICS LOAD SUMMARY  =========")
    print(f"Season            : {season_str}")
    print(f"Mode              : {mode}")
    print(f"Files scanned     : {total_files}")
    print(f"Files parsed OK   : {total_parsed}")
    print(f"Files failed      : {total_failed_files}")
    print(f"Rows inserted     : {total_rows}")
    if total_files == 0:
        print("WARNING: no files matched the selected prefixes!")
    print("========================================================\n")


if __name__ == "__main__":
    main()
