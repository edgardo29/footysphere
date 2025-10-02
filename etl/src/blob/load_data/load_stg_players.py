# load_stg_players.py
#
# Purpose
# -------
# Load basic player identities from Azure Blob into staging table stg_players.
# Compatible with files produced by the new "league-pages" fetch AND the old per-player fetch.
#
# Snapshot selection
# ------------------
# We store snapshots under:
#   players/<league_folder>/<season_str>/<window_label>/player_details/
# where:
#   - league_folder = "<sanitized_league_name>_<league_id>"  e.g., "serie_a_135"
#   - season_str    = "YYYY_YY"                              e.g., "2025_26"
#   - window_label  = "summer_YYYYMMDD_HHMM" or "winter_YYYYMMDD_HHMM"
#
# This loader accepts --window values:
#   - "summer"  -> auto-select the latest "summer_*" snapshot under the given league+season
#   - "winter"  -> auto-select the latest "winter_*" snapshot under the given league+season
#   - "latest"  -> auto-select the latest snapshot regardless of prefix
#   - explicit  -> e.g., "summer_20250930_2130" is used as-is
#
# Notes
# -----
# - We only stage identity fields. Stats in the JSON are ignored at this stage.
# - Chunked multi-row inserts for speed.
# - Invalid/malformed JSONs are counted and skipped.

import os
import sys
import json
import argparse
from typing import List, Tuple, Optional, Set

from azure.storage.blob import BlobServiceClient

# Add your config + test_scripts paths for credentials + DB
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
    """
    Download JSON blob and parse to dict.
    """
    bsc = get_blob_service_client()
    bc = bsc.get_blob_client(container=container_name, blob=blob_path)
    raw = bc.download_blob().readall()
    return json.loads(raw)


def list_snapshot_labels(container_client, base_season_prefix: str) -> List[str]:
    """
    Enumerate 'virtual directories' (snapshot labels) under:
      players/<league_folder>/<season_str>/
    by scanning blob names and capturing the path segment after base_season_prefix.
    """
    labels: Set[str] = set()
    prefix = f"{base_season_prefix}/"  # ensure trailing slash
    for blob in container_client.list_blobs(name_starts_with=prefix):
        name = blob.name
        # Expect: players/<league>/<season>/<snapshot>/...
        tail = name[len(prefix):]
        if "/" in tail:
            snapshot = tail.split("/", 1)[0]
            if snapshot:  # ignore empty
                labels.add(snapshot)
    return sorted(labels)  # lexical sort; works with YYYYMMDD_HHMM timestamps


def resolve_snapshot_label(container_client, base_season_prefix: str, window_arg: str) -> str:
    """
    Resolve the actual snapshot folder name to read from.
    - If window_arg is 'summer'/'winter', pick the newest label with that prefix.
    - If window_arg is 'latest', pick the newest snapshot label (any prefix).
    - Otherwise, treat window_arg as an explicit label and return it.
    """
    want = window_arg.strip().lower()
    if want in {"summer", "winter", "latest"}:
        labels = list_snapshot_labels(container_client, base_season_prefix)
        if not labels:
            raise RuntimeError(f"No snapshots found under {base_season_prefix}/")
        if want == "latest":
            return labels[-1]
        # Filter to prefix-specific
        prefixed = [x for x in labels if x.startswith(f"{want}_")]
        if not prefixed:
            raise RuntimeError(f"No '{want}_*' snapshots found under {base_season_prefix}/")
        return prefixed[-1]
    # explicit label passed (e.g., "summer_20250930_2130")
    return window_arg


# ---------- JSON parser ----------

def parse_player_json(data: dict) -> Optional[Tuple]:
    """
    Given a /players JSON (either league-scoped or all-competitions),
    extract the 'player' object as a row for stg_players.
    Returns:
      (player_id, firstname, lastname, full_name, birth_date, birth_country,
       nationality, height, weight, photo_url)
    or None if malformed/missing.
    """
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
    birth_date = birth.get("date")        # e.g. "1997-08-09"
    birth_country = birth.get("country")  # e.g. "Chile"

    nationality = p_obj.get("nationality")
    height      = p_obj.get("height")     # strings like "182" or "182 cm"
    weight      = p_obj.get("weight")     # strings like "74"  or "74 kg"
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
        photo_url
    )


# ---------- DB insert (chunked) ----------

def insert_chunk_stg_players(cur, rows_buffer: List[Tuple]):
    """
    Single multi-row INSERT into stg_players for the buffered rows.
    All rows are inserted with is_valid=true; malformed ones never reach here.
    """
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


# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser(description="Load player identities from Blob into stg_players.")
    parser.add_argument("--league_folder", required=True,
                        help="Folder name for the league, e.g. 'serie_a_135' (always <name>_<league_id>).")
    parser.add_argument("--season_str", required=True,
                        help="Season string 'YYYY_YY', e.g., '2025_26'.")
    parser.add_argument("--window", required=True,
                        help="One of: 'summer' | 'winter' | 'latest' | explicit snapshot label like 'summer_20250930_2130'.")
    args = parser.parse_args()

    league_folder = args.league_folder
    season_str = args.season_str
    container_name = "raw"

    # Connect to Blob
    bsc = get_blob_service_client()
    container_client = bsc.get_container_client(container_name)

    # Resolve the actual snapshot label
    base_season_prefix = f"players/{league_folder}/{season_str}"
    snapshot_label = resolve_snapshot_label(container_client, base_season_prefix, args.window)

    # Player details live here:
    details_prefix = f"{base_season_prefix}/{snapshot_label}/player_details"
    print(f"Loading from blob prefix: {details_prefix}")

    # Connect DB
    conn = get_db_connection()
    cur = conn.cursor()

    rows_buffer: List[Tuple] = []
    total_files = 0
    success = 0
    failed = 0

    # Iterate all player_<id>.json in that prefix
    for blob in container_client.list_blobs(name_starts_with=f"{details_prefix}/"):
        blob_name = blob.name
        if not blob_name.endswith(".json"):
            continue

        total_files += 1
        try:
            data = fetch_blob_json(container_name, blob_name)
            row = parse_player_json(data)
            if row:
                rows_buffer.append(row)
                # Flush by chunk
                if len(rows_buffer) >= CHUNK_SIZE:
                    insert_chunk_stg_players(cur, rows_buffer)
                    conn.commit()
                    rows_buffer.clear()
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"Error loading {blob_name}: {e}")
            failed += 1

    # Flush remaining
    if rows_buffer:
        insert_chunk_stg_players(cur, rows_buffer)
        conn.commit()
        rows_buffer.clear()

    cur.close()
    conn.close()

    print("\n========== STAGING LOAD SUMMARY ==========")
    print(f"Snapshot label          : {snapshot_label}")
    print(f"Total JSON files found  : {total_files}")
    print(f"Success (parsed/insert) : {success}")
    print(f"Failed/invalid          : {failed}")
    print("Finished loading stg_players.")

if __name__ == "__main__":
    main()
