#!/usr/bin/env python3
"""
load_stg_players.py
────────────────────────────────────────────────────────────────────
Manual loader for player identities with a simple CLI:

Usage:
  # load all targeted leagues for the window
  python load_stg_players.py --window summer

  # load a single targeted league (no quotes around the id)
  python load_stg_players.py --window winter --league_id 207

Behavior:
- Reads target leagues from league_catalog where players_target_windows contains <window> and is_enabled = true.
- Gets season_str by joining league_seasons (is_current = true).
- Loads JSONs from Azure Blob into stg_players.
- After a league successfully inserts rows, removes <window> from players_target_windows for that league.
"""

import os
import sys
import json
import argparse
from typing import List, Tuple, Optional, Set

from azure.storage.blob import BlobServiceClient

# repo paths for config + db conn helper
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from credentials import AZURE_STORAGE
from get_db_conn import get_db_connection

CHUNK_SIZE = 1000  # tune as needed


# ---------- Azure Blob helpers ----------

def get_blob_service_client() -> BlobServiceClient:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])


def fetch_blob_json(container_name: str, blob_path: str) -> dict:
    bsc = get_blob_service_client()
    bc = bsc.get_blob_client(container=container_name, blob=blob_path)
    raw = bc.download_blob().readall()
    return json.loads(raw)


def list_snapshot_labels(container_client, base_season_prefix: str) -> List[str]:
    labels: Set[str] = set()
    prefix = f"{base_season_prefix}/"
    for blob in container_client.list_blobs(name_starts_with=prefix):
        name = blob.name
        tail = name[len(prefix):]
        if "/" in tail:
            snapshot = tail.split("/", 1)[0]
            if snapshot:
                labels.add(snapshot)
    return sorted(labels)


def resolve_snapshot_label(container_client, base_season_prefix: str, window_arg: str) -> str:
    want = window_arg.strip().lower()
    if want in {"summer", "winter", "latest"}:
        labels = list_snapshot_labels(container_client, base_season_prefix)
        if not labels:
            raise RuntimeError(f"No snapshots found under {base_season_prefix}/")
        if want == "latest":
            return labels[-1]
        prefixed = [x for x in labels if x.startswith(f"{want}_")]
        if not prefixed:
            raise RuntimeError(f"No '{want}_*' snapshots found under {base_season_prefix}/")
        return prefixed[-1]
    # allow explicit label if ever passed (e.g., "summer_20250930_2130")
    return window_arg


# ---------- JSON parser ----------

def parse_player_json(data: dict) -> Optional[Tuple]:
    resp = data.get("response") or []
    if not resp or not isinstance(resp, list):
        return None
    p_obj = resp[0].get("player") if isinstance(resp[0], dict) else None
    if not p_obj or not isinstance(p_obj, dict):
        return None

    player_id = p_obj.get("id")
    full_name = p_obj.get("name")
    firstname = p_obj.get("firstname")
    lastname  = p_obj.get("lastname")

    birth = p_obj.get("birth") or {}
    birth_date = birth.get("date")
    birth_country = birth.get("country")

    nationality = p_obj.get("nationality")
    height      = p_obj.get("height")
    weight      = p_obj.get("weight")
    photo_url   = p_obj.get("photo")

    if not player_id:
        return None

    return (
        player_id,
        firstname,
        lastname,
        full_name,
        birth_date,
        birth_country,
        nationality,
        height,
        weight,
        photo_url,
    )


# ---------- DB inserts (chunked) ----------

def insert_chunk_stg_players(cur, rows_buffer: List[Tuple]):
    if not rows_buffer:
        return
    placeholders_list = []
    values_list = []
    for row in rows_buffer:
        placeholders = "(" + ", ".join(["%s"] * len(row)) + ", true, NULL)"
        placeholders_list.append(placeholders)
        values_list.extend(row)

    sql = f"""
        INSERT INTO stg_players (
            player_id, firstname, lastname, full_name,
            birth_date, birth_country, nationality,
            height, weight, photo_url,
            is_valid, error_reason
        )
        VALUES {", ".join(placeholders_list)}
    """
    cur.execute(sql, tuple(values_list))


# ---------- Target discovery & bookkeeping ----------

def season_fallback_from_calendar() -> str:
    # July boundary: seasons run Jul -> Jun
    import datetime as _dt
    now = _dt.datetime.utcnow()
    y = now.year if now.month >= 7 else now.year - 1
    return f"{y}_{(y+1)%100:02d}"


def discover_targets(conn, window: str, league_id: Optional[int]) -> List[Tuple[int, str, str]]:
    """
    Returns (league_id, league_folder, season_str) to load.

    Source of truth:
      league_catalog.players_target_windows contains <window>
      league_catalog.is_enabled = true
      league_seasons.is_current = true for season_str

    Optional: restrict to a single league_id.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lc.league_id,
                   lc.folder_alias AS league_folder,
                   ls.season_str
            FROM league_catalog lc
            JOIN league_seasons ls
              ON ls.league_id = lc.league_id
             AND ls.is_current = TRUE
            WHERE lc.is_enabled = TRUE
              AND lc.players_target_windows @> ARRAY[%s]::text[]
              {id_filter}
            ORDER BY lc.league_id
            """.format(id_filter="AND lc.league_id = %s" if league_id else ""),
            (window,) if not league_id else (window, league_id),
        )
        rows = cur.fetchall()

    if rows:
        return rows

    # Safety net: if nothing came back (e.g., seasons not flagged current yet),
    # fallback to a calendar-derived season for any catalog targets.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lc.league_id, lc.folder_alias
            FROM league_catalog lc
            WHERE lc.is_enabled = TRUE
              AND lc.players_target_windows @> ARRAY[%s]::text[]
              {id_filter}
            ORDER BY lc.league_id
            """.format(id_filter="AND lc.league_id = %s" if league_id else ""),
            (window,) if not league_id else (window, league_id),
        )
        fallback = cur.fetchall()
    if not fallback:
        return []

    fallback_season = season_fallback_from_calendar()
    return [(lid, lf, fallback_season) for (lid, lf) in fallback]


def drop_window_from_targets(conn, window: str, league_ids: List[int]):
    if not league_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE league_catalog
               SET players_target_windows = array_remove(players_target_windows, %s)
             WHERE league_id = ANY(%s)
            """,
            (window, league_ids),
        )
    conn.commit()


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(
        description="Load player identities from Blob into stg_players (window-only CLI, optional league_id)."
    )
    parser.add_argument("--window", required=True,
                        choices=["summer", "winter", "latest"],
                        help="Which snapshot window to load.")
    parser.add_argument("--league_id", type=int,
                        help="Optional single league_id to restrict the run (integer, no quotes).")
    parser.add_argument("--dry_run", action="store_true",
                        help="List intended loads; do not write.")
    parser.add_argument("--strict", action="store_true",
                        help="Exit non-zero if a targeted league had no matching snapshot.")
    args = parser.parse_args()

    container_name = "raw"
    bsc = get_blob_service_client()
    container_client = bsc.get_container_client(container_name)

    conn = get_db_connection()
    cur = conn.cursor()

    targets = discover_targets(conn, args.window, args.league_id)
    if not targets:
        scope = f"league_id={args.league_id}" if args.league_id else "all targeted leagues"
        print(f"[info] No targets found for window={args.window} ({scope}). Nothing to do.")
        cur.close(); conn.close()
        return

    print("[preflight]")
    for lid, lf, ss in targets:
        print(f"  - league_id={lid} folder={lf} season={ss} window={args.window}")
    if args.dry_run:
        print("[dry-run] Exiting without loading.")
        cur.close(); conn.close()
        return

    total_files = 0
    total_success = 0
    total_failed = 0
    processed_league_ids: List[int] = []
    skipped_no_snapshot: List[int] = []

    for league_id, league_folder, season_str in targets:
        base_season_prefix = f"players/{league_folder}/{season_str}"
        try:
            snapshot_label = resolve_snapshot_label(container_client, base_season_prefix, args.window)
        except Exception as e:
            print(f"[warn] {league_folder} ({season_str}) — no matching snapshot for window '{args.window}': {e}")
            skipped_no_snapshot.append(league_id)
            continue

        details_prefix = f"{base_season_prefix}/{snapshot_label}/player_details"
        print(f"\n[load] league={league_folder} season={season_str} window={args.window} snapshot={snapshot_label}")
        print(f"[load] prefix: {details_prefix}")

        rows_buffer: List[Tuple] = []
        league_files = 0
        league_success = 0
        league_failed = 0

        for blob in container_client.list_blobs(name_starts_with=f"{details_prefix}/"):
            blob_name = blob.name
            if not blob_name.endswith(".json"):
                continue
            league_files += 1
            try:
                data = fetch_blob_json(container_name, blob_name)
                row = parse_player_json(data)
                if row:
                    rows_buffer.append(row)
                    if len(rows_buffer) >= CHUNK_SIZE:
                        insert_chunk_stg_players(cur, rows_buffer)
                        conn.commit()
                        rows_buffer.clear()
                    league_success += 1
                else:
                    league_failed += 1
            except Exception as e:
                print(f"[error] {blob_name}: {e}")
                league_failed += 1

        if rows_buffer:
            insert_chunk_stg_players(cur, rows_buffer)
            conn.commit()
            rows_buffer.clear()

        print(f"[summary] files={league_files} ok={league_success} bad={league_failed}")
        total_files += league_files
        total_success += league_success
        total_failed += league_failed

        if league_success > 0:
            processed_league_ids.append(league_id)

    if processed_league_ids:
        drop_window_from_targets(conn, args.window, processed_league_ids)

    cur.close()
    conn.close()

    print("\n========== STAGING LOAD SUMMARY ==========")
    print(f"Window                  : {args.window}")
    print(f"Targets processed       : {len(targets)}")
    print(f"Total JSON files found  : {total_files}")
    print(f"Success (parsed/insert) : {total_success}")
    print(f"Failed/invalid          : {total_failed}")
    if skipped_no_snapshot:
        print(f"Skipped (no snapshot)   : {len(skipped_no_snapshot)} leagues: {sorted(set(skipped_no_snapshot))}")
    if args.strict and skipped_no_snapshot:
        print("[strict] Some targets had no snapshot; exiting with non-zero status.")
        sys.exit(2)


if __name__ == "__main__":
    main()
