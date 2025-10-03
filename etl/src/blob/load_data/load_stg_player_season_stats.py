#!/usr/bin/env python3
# load_stg_player_season_stats.py
#
# Purpose
# -------
# Load per-player, per-competition season stats from Azure Blob into
# **stg_player_season_stats** using your snapshot layout:
#
#   players/<folder_alias>/<season_str>/<snapshot>/player_details/player_*.json
#
# Key ideas
# ---------
# • You do NOT type league folder or season on the CLI. This script discovers
#   them from Postgres (league_catalog + league_seasons).
# • CLI only needs:
#       --window  (summer | winter | latest | explicit label like summer_20250930_2130)
#   Optionally:
#       --league_id 39                (single league filter)
#       --league_ids 39,78            (multi league filter)
#       --no_strict_competition       (keep ALL statistics[] blocks; by default we
#                                     keep only blocks matching the league_id)
# • This script ONLY INSERTS into staging. Truncate/check/merge are separate steps.
#
# Table expectation (staging)
# ---------------------------
# stg_player_season_stats should contain at least:
#   player_id, team_id, league_id, season, position, number,
#   appearances, lineups, minutes_played, goals, assists, saves,
#   dribbles_attempts, dribbles_success, fouls_committed, tackles,
#   passes_total, passes_key, pass_accuracy, yellow_cards, red_cards
#
# Safety & robustness
# -------------------
# • Snapshot selection: newest "summer_*" / "winter_*" / newest overall ("latest")
#   or a fixed explicit snapshot label.
# • Type coercion: strings like "6.700000" safely cast to numeric; blanks → NULL.
# • Strict league filter: default keeps only statistics blocks for the intended
#   competition (league_id from DB), preventing cups/UCL from leaking in.

import os
import sys
import re
import json
import argparse
from typing import List, Tuple, Optional, Dict

from azure.storage.blob import BlobServiceClient

# --- Project helpers (credentials + DB conn) -----------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from credentials import AZURE_STORAGE
from get_db_conn import get_db_connection

CHUNK_SIZE = 1000  # rows per bulk insert


# ---------------------------- Azure helpers -----------------------------

def _bsc() -> BlobServiceClient:
    """Return an authenticated Azure BlobServiceClient."""
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])


def _fetch_json(container: str, blob_path: str) -> dict:
    """Download a small JSON blob and return it as a Python dict."""
    bc = _bsc().get_blob_client(container=container, blob=blob_path)
    data = bc.download_blob().readall()
    return json.loads(data)


def _list_snapshot_labels(container_client, season_base_prefix: str) -> List[str]:
    """
    Enumerate snapshot folder names directly under:
      players/<folder_alias>/<season_str>/
    Example return: ["summer_20250930_2130", "winter_20260201_0815"]
    """
    labels = set()
    prefix = season_base_prefix.rstrip("/") + "/"
    for b in container_client.list_blobs(name_starts_with=prefix):
        rel = b.name[len(prefix):]
        if "/" in rel:
            labels.add(rel.split("/", 1)[0])
    return sorted(labels)


def _resolve_snapshot(container_client, season_base_prefix: str, window: str) -> str:
    """
    Decide which snapshot folder to use based on --window.
      • "summer"/"winter" → newest snapshot with that prefix
      • "latest"          → newest snapshot regardless of prefix
      • explicit label    → used as-is
    Raises RuntimeError if nothing matches.
    """
    w = window.strip().lower()
    if w in {"summer", "winter", "latest"}:
        labels = _list_snapshot_labels(container_client, season_base_prefix)
        if not labels:
            raise RuntimeError(f"No snapshots found under {season_base_prefix}/")
        if w == "latest":
            return labels[-1]
        filtered = [x for x in labels if x.startswith(f"{w}_")]
        if not filtered:
            raise RuntimeError(f"No '{w}_*' snapshots found under {season_base_prefix}/")
        return filtered[-1]
    return window  # explicit label such as "summer_20250930_2130"


# ----------------------------- DB helpers -------------------------------

def get_league_worklist(
    league_id: Optional[int] = None,
    league_ids: Optional[List[int]] = None
) -> List[Dict]:
    """
    Build the list of leagues to process:
      [{league_id, folder_alias, season_str}, ...]
    Source tables:
      • league_catalog (is_enabled flag, optional folder_alias)
      • league_seasons (is_current season, season_str)
      • leagues        (fallback folder_alias or league_name)

    Filtering:
      • If league_id is provided → exact match (= %s).
      • Else if league_ids list is provided → ANY(%s) with a tuple.
      • Else → all enabled current leagues.

    Returns a list; empty if nothing matches.
    """
    base_sql = """
        SELECT
        lc.league_id,
        COALESCE(
            lc.folder_alias,
            l.folder_alias,
            lower(regexp_replace(l.league_name, '[^a-z0-9]+', '_', 'g')) || '_' || lc.league_id
        ) AS folder_alias,
        ls.season_str AS season_str
        FROM league_catalog lc
        JOIN league_seasons ls
        ON ls.league_id = lc.league_id
        AND ls.is_current = TRUE
        LEFT JOIN leagues l
        ON l.league_id = lc.league_id
        WHERE lc.is_enabled = TRUE
    """
    order = " ORDER BY lc.league_id"

    with get_db_connection() as conn:
        cur = conn.cursor()
        if league_id is not None:
            sql = base_sql + " AND lc.league_id = %s" + order
            cur.execute(sql, (league_id,))
        elif league_ids:
            sql = base_sql + " AND lc.league_id = ANY(%s)" + order
            cur.execute(sql, (tuple(league_ids),))  # tuple-of-ids wrapped once
        else:
            sql = base_sql + order
            cur.execute(sql)

        rows = cur.fetchall()

    return [{"league_id": r[0], "folder_alias": r[1], "season_str": r[2]} for r in rows]


# ---------------------------- parsing helpers ---------------------------

def _to_int(x) -> Optional[int]:
    """Coerce value to int; return None on failure/empty."""
    if x is None or x == "":
        return None
    try:
        return int(float(x))  # handles "6", "6.0", "6.700000"
    except Exception:
        return None


def _to_num(x) -> Optional[float]:
    """Coerce value to float; return None on failure/empty."""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def parse_stats_rows(payload: dict, strict_league_id: Optional[int]) -> List[Tuple]:
    """
    Convert one /players payload to 0..N rows (one per statistics[] block),
    matching **stg_player_season_stats** insert order.
    If strict_league_id is set, keep only blocks where league.id == strict_league_id.
    """
    resp = payload.get("response") or []
    if not resp:
        return []

    player = resp[0].get("player") or {}
    stats  = resp[0].get("statistics") or []
    player_id = _to_int(player.get("id"))
    if not player_id or not stats:
        return []

    rows: List[Tuple] = []
    for blk in stats:
        team    = blk.get("team") or {}
        league  = blk.get("league") or {}
        games   = blk.get("games") or {}
        goals   = blk.get("goals") or {}
        drib    = blk.get("dribbles") or {}
        fouls   = blk.get("fouls") or {}
        tackles = blk.get("tackles") or {}
        passes  = blk.get("passes") or {}
        cards   = blk.get("cards") or {}

        team_id   = _to_int(team.get("id"))
        league_id = _to_int(league.get("id"))
        season    = _to_int(league.get("season"))

        if strict_league_id is not None and league_id != strict_league_id:
            continue
        if not (player_id and team_id and league_id and season):
            continue

        rows.append((
            player_id,                         # player_id
            team_id,                           # team_id
            league_id,                         # league_id
            season,                            # season (int from JSON)
            games.get("position"),             # position
            _to_int(games.get("number")),      # number
            _to_int(games.get("appearences")), # appearances (API typo)
            _to_int(games.get("lineups")),     # lineups
            _to_int(games.get("minutes")),     # minutes_played
            _to_int(goals.get("total")),       # goals
            _to_int(goals.get("assists")),     # assists
            _to_int(goals.get("saves")),       # saves (GK)
            _to_int(drib.get("attempts")),     # dribbles_attempts
            _to_int(drib.get("success")),      # dribbles_success
            _to_int(fouls.get("committed")),   # fouls_committed
            _to_int(tackles.get("total")),     # tackles
            _to_int(passes.get("total")),      # passes_total
            _to_int(passes.get("key")),        # passes_key
            _to_num(passes.get("accuracy")),   # pass_accuracy (numeric)
            _to_int(cards.get("yellow")),      # yellow_cards
            _to_int(cards.get("red")),         # red_cards
        ))

    return rows




# ----------------------------- DB insert --------------------------------

def insert_chunk(cur, rows: List[Tuple]) -> None:
    """Bulk-insert buffered rows into stg_player_season_stats."""
    if not rows:
        return
    cols = (
        "player_id, team_id, league_id, season, "
        "position, number, appearances, lineups, minutes_played, "
        "goals, assists, saves, dribbles_attempts, dribbles_success, "
        "fouls_committed, tackles, passes_total, passes_key, pass_accuracy, "
        "yellow_cards, red_cards"
    )
    placeholder = "(" + ", ".join(["%s"] * 21) + ")"
    sql = f"INSERT INTO stg_player_season_stats ({cols}) VALUES " + ", ".join([placeholder] * len(rows))
    flat = [v for row in rows for v in row]
    cur.execute(sql, tuple(flat))


# -------------------------------- main ----------------------------------

def main():
    ap = argparse.ArgumentParser(description="Load player season stats into staging from Blob snapshots (DB-driven).")
    ap.add_argument("--window", required=True,
                    help="summer | winter | latest | explicit label (e.g., summer_20250930_2130)")
    ap.add_argument("--league_id", type=int, help="Optional single league to process (e.g., 39)")
    ap.add_argument("--league_ids", help="Optional comma-separated list to process (e.g., 39,78)")
    ap.add_argument("--no_strict_competition", action="store_true",
                    help="If set, do NOT filter statistics[] by the league_id from DB.")
    args = ap.parse_args()

    # Parse optional multi-select list (ignored if --league_id provided)
    subset_list = None
    if args.league_id is not None:
        subset_single = args.league_id
    else:
        subset_single = None
        if args.league_ids:
            subset_list = [int(x.strip()) for x in args.league_ids.split(",") if x.strip()]

    # Build worklist from DB (no manual typing of folder/season)
    work = get_league_worklist(league_id=subset_single, league_ids=subset_list)
    if not work:
        print("No enabled, current leagues matched. Nothing to do.")
        return

    container = "raw"
    cc = _bsc().get_container_client(container)

    overall_files = overall_rows = overall_skipped = 0
    leagues_done = 0

    for row in work:
        league_id  = row["league_id"]
        folder     = row["folder_alias"]
        season_str = row["season_str"]

        base = f"players/{folder}/{season_str}"
        try:
            snapshot = _resolve_snapshot(cc, base, args.window)
        except Exception as e:
            print(f"[league {league_id}] No snapshot for window '{args.window}' under {base}/ → SKIP ({e})")
            continue

        details_prefix = f"{base}/{snapshot}/player_details/"
        strict_filter = None if args.no_strict_competition else league_id

        print(f"\n=== League {league_id} • folder='{folder}' • season={season_str}")
        print(f"    Snapshot: {snapshot}")
        print(f"    Reading:  {details_prefix}")

        files = rows_inserted = skipped = 0
        buffer: List[Tuple] = []

        with get_db_connection() as conn:
            cur = conn.cursor()
            for blob in cc.list_blobs(name_starts_with=details_prefix):
                name = blob.name
                if not name.endswith(".json"):
                    continue
                files += 1
                try:
                    payload = _fetch_json(container, name)
                    rows = parse_stats_rows(payload, strict_filter)
                    if rows:
                        buffer.extend(rows)
                        if len(buffer) >= CHUNK_SIZE:
                            insert_chunk(cur, buffer)
                            conn.commit()
                            rows_inserted += len(buffer)
                            buffer.clear()
                    else:
                        skipped += 1
                except Exception as e:
                    print(f"    ! Error parsing {name}: {e}")
                    skipped += 1

            if buffer:
                insert_chunk(cur, buffer)
                conn.commit()
                rows_inserted += len(buffer)
                buffer.clear()

        print(f"    → files processed: {files}, rows inserted: {rows_inserted}, skipped/errors: {skipped}")

        leagues_done   += 1
        overall_files  += files
        overall_rows   += rows_inserted
        overall_skipped+= skipped

    # Summary
    print("\n===== STG LOAD SUMMARY (player_season_stats) =====")
    print(f"Leagues processed : {leagues_done}")
    print(f"Files processed   : {overall_files}")
    print(f"Rows inserted     : {overall_rows}")
    print(f"Files skipped     : {overall_skipped}")
    print("Done.")


if __name__ == "__main__":
    main()
