#!/usr/bin/env python3
"""
load_stg_match_events.py
───────────────────────────────────────────────────────────────────────────────
Purpose:
  Read event snapshots from Azure Blob Storage (produced by fetch_match_events.py)
  and insert one row per event into the staging table `stg_match_events`.

Modes:
  --mode initial      → read from .../<league>/<season>/initial/<latest_run>/
  --mode incremental  → read from .../<league>/<season>/incremental/<latest_run>/

League scope:
  If --league-ids is omitted → use ALL enabled leagues from league_catalog.
  If provided               → only those leagues (space-separated IDs).

Season resolution per league (DB-driven):
  Prefer league_seasons.is_current = TRUE;
  else a season whose date range contains CURRENT_DATE;
  else MAX(season_year) in league_seasons;
  else league_catalog.last_season.
  Season folder comes from league_seasons.season_str when present (e.g., "2025_26"),
  otherwise it is formatted as "YYYY_YY".

Blob path pattern (container = "raw"):
  match_events/<folder_alias>/<season_folder>/<mode>/<mode>_<YYYYMMDD_HHMMSS>Z/events_<FIXTURE>.json

Inserts (one row per item in JSON["response"]):
  fixture_id, minute, minute_extra, team_id, player_id, assist_player_id,
  event_type, event_detail, comments
  (stg_match_events has defaults for is_valid=TRUE, load_timestamp, etc.)

No deletes, no dedupe, no limits — exactly as requested.

Examples:
  python load_stg_match_events.py --mode initial
  python load_stg_match_events.py --mode initial --league-ids 78 71 39
  python load_stg_match_events.py --mode incremental
  python load_stg_match_events.py --mode incremental --league-ids 78 71 39
"""

from __future__ import annotations

# ───────────────────────────────────────────────────────────────────────────────
# Stdlib
# ───────────────────────────────────────────────────────────────────────────────
import argparse
import json
import os
import re
import sys
from typing import Iterable, List, Optional, Tuple

# ───────────────────────────────────────────────────────────────────────────────
# Third-party
# ───────────────────────────────────────────────────────────────────────────────
from azure.storage.blob import BlobServiceClient  # type: ignore
import psycopg2.extras  # type: ignore

# ───────────────────────────────────────────────────────────────────────────────
# Local imports / repo paths
# ───────────────────────────────────────────────────────────────────────────────
# ROOT points to the repo "etl" dir: etl/src/...
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../test_scripts")))

from credentials import AZURE_STORAGE
from get_db_conn import get_db_connection

# ───────────────────────────────────────────────────────────────────────────────
# Azure Blob client (single container = "raw")
# ───────────────────────────────────────────────────────────────────────────────
ACCOUNT_URL = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
BLOB = BlobServiceClient(ACCOUNT_URL, credential=AZURE_STORAGE["access_key"])
RAW_CONTAINER = "raw"

# Regex to extract the run folder timestamp: "<mode>/<mode>_YYYYMMDD_HHMMSSZ/"
RUN_DIR_RE = re.compile(r"^(?P<mode>initial|incremental)/\1_(?P<ts>\d{8}_\d{6})Z/")

# Regex to fallback fixture id from filename "events_<FIXTURE>.json"
FIXTURE_FILE_RE = re.compile(r"events_(?P<fx>\d+)\.json$")


# ───────────────────────────────────────────────────────────────────────────────
# DB helpers: figure out (league_folder, season_folder) per target league
# ───────────────────────────────────────────────────────────────────────────────
def resolve_league_seasons(conn, league_ids: Optional[List[int]]) -> List[Tuple[int, str, int, str]]:
    """
    Return rows: [(league_id, league_folder, season_year, season_folder), ...]
    Season picking logic matches your fetch script’s behavior.
    """
    use_filter = bool(league_ids)
    scope_cte = (
        """
        WITH scope AS (
          SELECT lc.league_id,
                 lc.folder_alias,
                 lc.last_season
            FROM league_catalog lc
           WHERE lc.is_enabled = TRUE
        )
        """
        if not use_filter else
        """
        WITH scope AS (
          SELECT lc.league_id,
                 lc.folder_alias,
                 lc.last_season
            FROM league_catalog lc
            JOIN (SELECT unnest(%(ids)s::int[]) AS league_id) i
              ON i.league_id = lc.league_id
        )
        """
    )

    season_year_expr = (
        "COALESCE( "
        "  (SELECT ls1.season_year FROM league_seasons ls1 "
        "    WHERE ls1.league_id = s.league_id AND ls1.is_current = TRUE "
        "    ORDER BY ls1.season_year DESC LIMIT 1), "
        "  (SELECT ls2.season_year FROM league_seasons ls2 "
        "    WHERE ls2.league_id = s.league_id "
        "      AND CURRENT_DATE BETWEEN ls2.start_date AND ls2.end_date "
        "    ORDER BY ls2.season_year DESC LIMIT 1), "
        "  (SELECT MAX(ls3.season_year) FROM league_seasons ls3 "
        "    WHERE ls3.league_id = s.league_id), "
        "  s.last_season "
        ")"
    )

    sql = f"""
    {scope_cte},
    chosen AS (
      SELECT s.league_id,
             s.folder_alias,
             {season_year_expr} AS season_year
        FROM scope s
    )
    SELECT c.league_id,
           c.folder_alias AS league_folder,
           c.season_year,
           COALESCE(ls.season_str,
                    to_char(c.season_year, 'FM9999') || '_' ||
                    to_char(mod(c.season_year, 100) + 1, 'FM00')) AS season_folder
      FROM chosen c
      LEFT JOIN league_seasons ls
        ON ls.league_id = c.league_id
       AND ls.season_year = c.season_year
     WHERE c.season_year IS NOT NULL
     ORDER BY c.league_id;
    """

    params = {}
    if use_filter:
        params["ids"] = league_ids

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    # rows: [(league_id, league_folder, season_year, season_folder)]
    return rows


# ───────────────────────────────────────────────────────────────────────────────
# Azure helpers
# ───────────────────────────────────────────────────────────────────────────────
def list_blob_names(prefix: str) -> Iterable[str]:
    """
    Yield blob names under container/prefix. Azure has flat namespace; we filter by prefix.
    """
    container = BLOB.get_container_client(RAW_CONTAINER)
    # name starts with prefix (exact match of hierarchy)
    for blob in container.list_blobs(name_starts_with=prefix):
        yield blob.name


def find_latest_run_folder(prefix_root: str, mode: str) -> Optional[str]:
    """
    Given a prefix like: "match_events/<folder>/<season>/<mode>/"
    inspect the blob names under it and identify the latest run folder:
      "<mode>/<mode>_YYYYMMDD_HHMMSSZ/"
    Returns the run folder path *relative to prefix_root*, e.g.:
      "initial/initial_20251009_043116Z/"
    or None if nothing found.
    """
    latest_key = None  # comparable key: "YYYYMMDD_HHMMSS"
    latest_rel = None  # relative folder string

    # We list all blobs under prefix_root and extract the first two segments after it.
    # That gives us candidate run folders.
    seen = set()
    for name in list_blob_names(prefix_root):
        # strip the root to analyze the remainder
        if not name.startswith(prefix_root):
            continue
        remainder = name[len(prefix_root):]  # e.g. "initial/initial_20251009_.../events_1388308.json"
        # Take the first two path segments to test against RUN_DIR_RE
        parts = remainder.split("/", 3)
        if len(parts) < 2:
            continue
        rel_candidate = f"{parts[0]}/{parts[1]}/"  # e.g. "initial/initial_20251009_043116Z/"
        if rel_candidate in seen:
            continue
        seen.add(rel_candidate)

        m = RUN_DIR_RE.match(rel_candidate)
        if not m:
            continue
        if m.group("mode") != mode:
            continue
        ts = m.group("ts")  # "YYYYMMDD_HHMMSS"
        if (latest_key is None) or (ts > latest_key):
            latest_key = ts
            latest_rel = rel_candidate

    return latest_rel


def iter_event_blobs(run_root_prefix: str) -> Iterable[Tuple[str, bytes]]:
    """
    Iterate all event blobs within a chosen run root prefix, yielding (blob_name, content_bytes).
    Example run_root_prefix:
      "match_events/<folder>/<season>/<mode>/<mode>_YYYYMMDD_HHMMSSZ/"
    """
    container = BLOB.get_container_client(RAW_CONTAINER)
    for blob in container.list_blobs(name_starts_with=run_root_prefix):
        if blob.name.endswith(".json") and "/events_" in blob.name:
            content = container.download_blob(blob.name).readall()
            yield blob.name, content


# ───────────────────────────────────────────────────────────────────────────────
# Insert helper (fast batch inserts)
# ───────────────────────────────────────────────────────────────────────────────
def insert_events_batch(conn, rows: List[Tuple[int, Optional[int], Optional[int],
                                               Optional[int], Optional[int], Optional[int],
                                               Optional[str], Optional[str], Optional[str]]]) -> int:
    """
    Insert many rows into stg_match_events at once.
    Returns count inserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO stg_match_events (
            fixture_id, minute, minute_extra, team_id, player_id, assist_player_id,
            event_type, event_detail, comments
        )
        VALUES %s
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, template=None, page_size=1000)
    conn.commit()
    return len(rows)


# ───────────────────────────────────────────────────────────────────────────────
# JSON parsing
# ───────────────────────────────────────────────────────────────────────────────
def parse_fixture_id(blob_name: str, root_obj: dict) -> Optional[int]:
    """
    Prefer fixture from JSON: data["parameters"]["fixture"], else fallback to filename.
    """
    try:
        fx = root_obj.get("parameters", {}).get("fixture")
        if fx is not None:
            return int(fx)
    except Exception:
        pass

    m = FIXTURE_FILE_RE.search(os.path.basename(blob_name))
    if m:
        try:
            return int(m.group("fx"))
        except Exception:
            return None
    return None


def rows_from_event_json(blob_name: str, payload: dict) -> List[Tuple]:
    """
    Convert one events_<FIXTURE>.json into many staging rows.
    """
    out: List[Tuple] = []
    fixture_id = parse_fixture_id(blob_name, payload)
    if fixture_id is None:
        return out  # ignore unparseable fixture id

    for ev in payload.get("response", []) or []:
        t = ev.get("time") or {}
        team = ev.get("team") or {}
        player = ev.get("player") or {}
        assist = ev.get("assist") or {}

        minute = t.get("elapsed")
        minute_extra = t.get("extra")
        team_id = team.get("id")
        player_id = player.get("id")
        assist_id = assist.get("id")
        ev_type = ev.get("type")
        ev_detail = ev.get("detail")
        comments = ev.get("comments")

        out.append((
            fixture_id,
            minute,
            minute_extra,
            team_id,
            player_id,
            assist_id,
            ev_type,
            ev_detail,
            comments,
        ))
    return out


# ───────────────────────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # CLI
    ap = argparse.ArgumentParser(
        description="Load match events from Azure Blob snapshots into stg_match_events."
    )
    ap.add_argument(
        "--mode",
        choices=["initial", "incremental"],
        required=True,
        help="Which snapshot tree to load from."
    )
    ap.add_argument(
        "--league-ids",
        nargs="+",
        type=int,
        metavar="LEAGUE_ID",
        help="Optional space-separated league IDs (e.g., --league-ids 78 71). "
             "If omitted, all enabled leagues are used.",
    )
    args = ap.parse_args()
    mode: str = args.mode

    # DB connection
    conn = get_db_connection()

    # Build the worklist: (league_id, league_folder, season_year, season_folder)
    targets = resolve_league_seasons(conn, args.league_ids)

    total_files = 0
    total_rows = 0
    total_targets = 0

    for league_id, league_folder, _season_year, season_folder in targets:
        # Prefix to scan: match_events/<league_folder>/<season_folder>/<mode>/
        base_prefix = f"match_events/{league_folder}/{season_folder}/{mode}/"
        full_root = base_prefix  # prefix within container

        # Identify latest run folder *under this mode*
        latest_rel = find_latest_run_folder(prefix_root=f"match_events/{league_folder}/{season_folder}/", mode=mode)
        if not latest_rel:
            # No snapshots under this mode for this league/season; skip
            continue

        # Ensure the latest we found sits under the chosen mode
        if not latest_rel.startswith(f"{mode}/"):
            continue

        # Full run folder prefix, e.g. ".../<mode>/<mode>_YYYYMMDD_HHMMSSZ/"
        run_prefix = f"match_events/{league_folder}/{season_folder}/{latest_rel}"

        batch_rows: List[Tuple] = []
        file_count = 0

        for blob_name, content in iter_event_blobs(run_prefix):
            try:
                data = json.loads(content.decode("utf-8"))
            except Exception:
                # Bad JSON → ignore that file and continue
                continue

            rows = rows_from_event_json(blob_name, data)
            if rows:
                batch_rows.extend(rows)
                file_count += 1

        if batch_rows:
            inserted = insert_events_batch(conn, batch_rows)
            total_files += file_count
            total_rows += inserted
            total_targets += 1

            print(f"[OK] league_id={league_id} folder={league_folder} season={season_folder} "
                  f"run={latest_rel.strip('/')} files={file_count} rows={inserted}")

    conn.close()
    print(f"Finished: targets_with_data={total_targets} files={total_files} rows_inserted={total_rows}")


if __name__ == "__main__":
    main()
