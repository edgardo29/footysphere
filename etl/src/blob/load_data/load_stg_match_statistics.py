#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load_stg_match_statistics.py — minimal params, DB-resolved season/alias
──────────────────────────────────────────────────────────────────────────────
Loads API-Football match statistics JSON from Azure Blob into stg_match_statistics.

CLI:
  --mode {initial, incremental}
  [--league-id <int> ...]   # now supports one or more specific leagues

Resolves from DB:
  - alias  : league_catalog.folder_alias (fallback: league_<id>)
  - season : prefer league_seasons.is_current=TRUE (max), else active-by-date (max),
             else absolute max(season_year)

Reads from:
  raw/match_statistics/<alias>/<YYYY_YY>/<mode>/<YYYY-MM-DD_HHMMSS>/statistics_<fixture>.json

Behavior when no run exists for chosen mode:
  - Logs a warning like:
      "No run found for league=X alias=Y season=Z mode=M; skipping."
  - Continues with other leagues (no previous-season fallback).
"""

from __future__ import annotations
import os, sys, re, json, argparse, logging
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from azure.storage.blob import BlobServiceClient

# ───────────────────────── robust repo-local imports ─────────────────────────
HERE = Path(__file__).resolve().parent
CANDIDATE_PATHS = [
    HERE / "../../../config",
    HERE / "../../../test_scripts",
    HERE / "../../../../config",
    HERE / "../../../../test_scripts",
]
FOOTY_ROOT = os.environ.get("FOOTY_ROOT", "")
if FOOTY_ROOT:
    CANDIDATE_PATHS += [
        Path(FOOTY_ROOT) / "etl" / "config",
        Path(FOOTY_ROOT) / "etl" / "test_scripts",
        Path(FOOTY_ROOT) / "config",
        Path(FOOTY_ROOT) / "test_scripts",
    ]
for p in CANDIDATE_PATHS:
    try:
        rp = p.resolve()
    except Exception:
        continue
    if rp.exists():
        sp = str(rp)
        if sp not in sys.path:
            sys.path.insert(0, sp)

try:
    from credentials import AZURE_STORAGE       # noqa: E402
    from get_db_conn import get_db_connection   # noqa: E402
except ModuleNotFoundError as e:
    tried = "\n".join(f" - {str(Path(x).resolve())}" for x in CANDIDATE_PATHS)
    raise SystemExit(
        "Could not import credentials/get_db_conn. Looked in:\n" + tried
    ) from e

# ───────────────────────────────── config / logging ─────────────────────────────────
CHUNK_SIZE = 1_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load_stg_match_statistics")
for noisy in ("azure.storage", "azure.core.pipeline.policies.http_logging_policy"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# ───────────────────────────────── mappings / regex ─────────────────────────────────
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
    "expected_goals":    "expected_goals",
    "goals_prevented":   "goals_prevented",
}
RUNSTAMP_RE   = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{6}$")
BLOB_STATS_RE = re.compile(r"statistics_(\d+)\.json$")

# ───────────────────────────────── helpers ─────────────────────────────────
def season_to_str(season_start: int) -> str:
    return f"{season_start}_{(season_start % 100) + 1:02d}"

def get_blob_service() -> BlobServiceClient:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])

def resolve_alias_for_league(cur, league_id: int) -> str:
    cur.execute(
        "SELECT COALESCE(NULLIF(folder_alias, ''), CONCAT('league_', %s)) "
        "FROM league_catalog WHERE league_id = %s",
        (league_id, league_id),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else f"league_{league_id}"

def get_enabled_leagues(cur, league_ids: Optional[List[int]]) -> List[int]:
    """
    Return league_id list.
      - If league_ids is provided → restrict to those IDs (even if disabled).
      - Else → all enabled leagues.
    """
    if league_ids:
        cur.execute(
            "SELECT league_id FROM league_catalog WHERE league_id = ANY(%s) ORDER BY league_id",
            (league_ids,),
        )
        return [int(r[0]) for r in cur.fetchall()]
    cur.execute("SELECT league_id FROM league_catalog WHERE is_enabled = TRUE ORDER BY league_id")
    return [int(r[0]) for r in cur.fetchall()]

def resolve_season_year(cur, league_id: int) -> Optional[int]:
    # 1) explicit current
    cur.execute(
        "SELECT MAX(season_year) FROM league_seasons WHERE league_id=%s AND is_current=TRUE",
        (league_id,),
    )
    s = cur.fetchone()[0]
    if s:
        return int(s)
    # 2) active-by-date
    cur.execute(
        """
        SELECT MAX(season_year)
          FROM league_seasons
         WHERE league_id=%s
           AND (start_date IS NULL OR start_date <= CURRENT_DATE)
           AND (end_date   IS NULL OR end_date   >= CURRENT_DATE)
        """,
        (league_id,),
    )
    s = cur.fetchone()[0]
    if s:
        return int(s)
    # 3) fallback max
    cur.execute("SELECT MAX(season_year) FROM league_seasons WHERE league_id=%s", (league_id,))
    s = cur.fetchone()[0]
    return int(s) if s else None

def latest_runstamp_for_alias(container, alias: str, season_str: str, mode: str) -> Optional[str]:
    root = f"match_statistics/{alias}/{season_str}/{mode}/"
    stamps: set = set()
    for blob in container.list_blobs(name_starts_with=root):
        # match_statistics/<alias>/<season>/<mode>/<stamp>/statistics_XXX.json
        parts = blob.name.split("/")
        if len(parts) < 6:
            continue
        stamp = parts[4]
        if RUNSTAMP_RE.match(stamp):
            stamps.add(stamp)
    return max(stamps) if stamps else None

def parse_stats_json(fixture_id: int, data: dict) -> List[Tuple]:
    out: List[Tuple] = []
    response = data.get("response")
    if not isinstance(response, list) or len(response) == 0:
        out.append((
            fixture_id, None,
            None, None, None, None,
            None, None, None,
            None, None, None, None,
            None, None,
            None, None, None,
            None, None,
            False, "empty or invalid 'response' array"
        ))
        return out

    stats_template: Dict[str, Optional[float]] = {v: None for v in API_TO_COL.values()}
    for team_block in response:
        team_id = team_block.get("team", {}).get("id")
        stats = dict(stats_template)
        for s in team_block.get("statistics", []) or []:
            t = s.get("type")
            if t not in API_TO_COL:
                continue
            col = API_TO_COL[t]
            val = s.get("value")
            if col in {"possession", "pass_accuracy"} and isinstance(val, str):
                val = val.rstrip("%")
            try:
                if col in {"expected_goals", "goals_prevented"}:
                    stats[col] = float(val) if val is not None else None
                else:
                    stats[col] = int(val) if val is not None else None
            except Exception:
                stats[col] = None

        out.append((
            int(fixture_id),
            int(team_id) if team_id is not None else None,
            stats["shots_on_goal"],  stats["shots_off_goal"],
            stats["shots_inside_box"], stats["shots_outside_box"],
            stats["blocked_shots"],  stats["goalkeeper_saves"], stats["total_shots"],
            stats["corners"], stats["offsides"], stats["fouls"], stats["possession"],
            stats["yellow_cards"], stats["red_cards"],
            stats["total_passes"], stats["passes_accurate"], stats["pass_accuracy"],
            stats["expected_goals"], stats["goals_prevented"],
            True, None
        ))
    return out

def insert_chunk(cur, rows: Sequence[Tuple]) -> None:
    if not rows:
        return
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
    flat = [v for row in rows for v in row]
    cur.execute(f"INSERT INTO stg_match_statistics ({col_list}) VALUES {placeholders}", flat)

# ───────────────────────────────── main ─────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Load match statistics JSON into stg_match_statistics.")
    ap.add_argument("--mode", required=True, choices=["initial", "incremental"],
                    help="Which blob tree to load (initial or incremental).")
    # CHANGED: accept 1+ integers for league IDs
    ap.add_argument("--league-id", type=int, nargs="+",
                    help="Optional: load only these league IDs; default is all enabled.")
    args = ap.parse_args()

    svc = get_blob_service()
    container = svc.get_container_client("raw")

    with get_db_connection() as conn:
        cur = conn.cursor()

        # Decide which leagues to load
        league_ids = get_enabled_leagues(cur, args.league_id)
        if not league_ids:
            log.warning("No leagues to process; exiting.")
            return

        total_files = total_parsed = total_failed_files = 0
        total_rows = 0
        leagues_skipped_no_run = 0
        buffer: List[Tuple] = []

        for lid in league_ids:
            alias = resolve_alias_for_league(cur, lid)
            season_year = resolve_season_year(cur, lid)
            if season_year is None:
                log.warning("League %s has no season resolved; skipping.", lid)
                leagues_skipped_no_run += 1
                continue

            season_str = season_to_str(season_year)

            # Find latest run for this league/season/mode (strict: current/latest season only)
            stamp = latest_runstamp_for_alias(container, alias, season_str, args.mode)
            if not stamp:
                log.warning("No run found for league=%s alias=%s season=%s mode=%s; skipping.",
                            lid, alias, season_str, args.mode)
                leagues_skipped_no_run += 1
                continue

            prefix = f"match_statistics/{alias}/{season_str}/{args.mode}/{stamp}/"
            log.info("Loading league=%s alias=%s season=%s mode=%s stamp=%s",
                     lid, alias, season_str, args.mode, stamp)

            files_scanned = files_parsed = files_failed = rows_inserted_alias = 0

            for blob in container.list_blobs(name_starts_with=prefix):
                m = BLOB_STATS_RE.search(blob.name)
                if not m:
                    continue
                files_scanned += 1
                fixture_id = int(m.group(1))
                try:
                    raw = container.get_blob_client(blob=blob.name).download_blob().readall()
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

            total_files       += files_scanned
            total_parsed      += files_parsed
            total_failed_files += files_failed
            total_rows        += rows_inserted_alias

            log.info("league=%s alias=%s • scanned=%d parsed=%d failed=%d rows=%d",
                     lid, alias, files_scanned, files_parsed, files_failed, rows_inserted_alias)

        # flush remaining
        if buffer:
            insert_chunk(cur, buffer)
            conn.commit()
            buffer.clear()

        cur.close()

    print("\n=========  STG MATCH_STATISTICS LOAD SUMMARY  =========")
    print(f"Mode                        : {args.mode}")
    print(f"Files scanned               : {total_files}")
    print(f"Files parsed OK             : {total_parsed}")
    print(f"Files failed                : {total_failed_files}")
    print(f"Rows inserted               : {total_rows}")
    print(f"Leagues skipped (no run)    : {leagues_skipped_no_run}")
    if total_files == 0:
        print("WARNING: no files matched the selected prefixes!")
    print("========================================================\n")

if __name__ == "__main__":
    main()
