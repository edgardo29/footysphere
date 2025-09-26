#!/usr/bin/env python
"""
load_stg_player_season_stats.py
────────────────────────────────────────────────────────────────────────────
Bulk-loads API-Football *player-season statistics* JSON files from Azure
Blob Storage into **stg_player_season_stats** (staging).

Operating modes
---------------
initial      → players/initial/<league>/<season>/run_<date>/player_details/*
incremental  → players/incremental/<league>/<season>/<date>/*

On each run the script

1.  Detects the newest run_<date> (or YYYY-MM-DD) folder.
2.  Streams every `player_<id>.json` inside it.
3.  Parses the JSON’s `statistics` array → 0-N DB rows per file.
4.  Buffers rows in CHUNK_SIZE batches, inserts with one VALUES clause.
5.  Prints a concise summary.

The staging → production merge is handled later by the Airflow DAG.
"""

# ────────────────────────── standard-library imports ────────────────────
import os
import sys
import json
import logging
from pathlib import Path
from typing import List, Tuple

# ───────────────────────── Azure SDK import  ────────────────────────────
from azure.storage.blob import BlobServiceClient

# ──────────────────────── project helper imports  ───────────────────────
# make sure <repo>/config and <repo>/test_scripts are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../test_scripts")))

from credentials import AZURE_STORAGE           # storage credentials dict
from get_db_conn  import get_db_connection      # helper returning psycopg2.conn

# ────────────────────────── logging setup  ─────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                    level=logging.INFO)
log = logging.getLogger(__name__)
# silence Azure SDK INFO chatter
logging.getLogger("azure").setLevel(logging.WARNING)

# ─────────────────────────── constants  ─────────────────────────────────
CHUNK_SIZE = 1_000   # flush to DB after this many rows

# ─────────────────────────── Azure helpers  ────────────────────────────
def _blob_client() -> BlobServiceClient:
    """
    Return a ready-to-use BlobServiceClient so we don’t repeat URL /
    credential glue everywhere.
    """
    url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=url,
                             credential=AZURE_STORAGE["access_key"])


def _fetch_blob_json(container: str, blob_path: str) -> dict:
    """
    Download a *small* JSON blob (<1 MB) and return it as a Python dict.
    Any network / SDK exception bubbles up to caller.
    """
    bytestr = (_blob_client()
               .get_blob_client(container=container, blob=blob_path)
               .download_blob()
               .readall())
    return json.loads(bytestr)

# ────────────────────────────── parsing  ────────────────────────────────
def parse_stats_rows(payload: dict) -> List[Tuple]:
    """
    Convert one `/players?id=` payload to 0-N tuples in the exact column
    order of **stg_player_season_stats**.

    • One player JSON may contain several `statistics` blocks (one per
      competition they appeared in). Each block becomes one DB row.
    • We skip blocks missing mandatory keys (team_id, league_id, season).
    • Every value we don’t care about is left as None.
    """
    # Payload top-level shape: {"response":[{ "player":{…}, "statistics":[…] }]}
    resp = payload.get("response", [])
    if not resp:                      # empty response → nothing to load
        return []

    player = resp[0].get("player", {})
    stats  = resp[0].get("statistics", [])
    player_id = player.get("id")

    # If we somehow have no player_id OR no statistics array, bail out.
    if not (player_id and stats):
        return []

    rows: List[Tuple] = []
    for block in stats:
        # Pull out nested sub-dicts; default to empty dict to avoid KeyError
        team    = block.get("team",   {})
        league  = block.get("league", {})
        games   = block.get("games",  {})
        goals   = block.get("goals",  {})
        drib    = block.get("dribbles", {})
        fouls   = block.get("fouls", {})
        tackles = block.get("tackles", {})
        passes  = block.get("passes", {})
        cards   = block.get("cards",  {})

        # Mandatory identifiers
        team_id   = team.get("id")
        league_id = league.get("id")
        season    = league.get("season")

        if not (team_id and league_id and season):
            # Skip if basic identifiers are missing (corrupt block).
            continue

        # Build tuple in column order. API misspells "appearences".
        rows.append((
            player_id, team_id, league_id, season,
            games.get("position"), games.get("number"),
            games.get("appearences"), games.get("lineups"), games.get("minutes"),
            goals.get("total"), goals.get("assists"), goals.get("saves"),
            drib.get("attempts"), drib.get("success"),
            fouls.get("committed"), tackles.get("total"),
            passes.get("total"), passes.get("key"), passes.get("accuracy"),
            cards.get("yellow"), cards.get("red")
        ))

    return rows

# ───────────────────────────── DB insert  ───────────────────────────────
def insert_chunk(cur, rows: List[Tuple]) -> None:
    """
    Bulk-insert buffered rows into **stg_player_season_stats**.
    Adds an `is_valid = true` flag to each VALUES group.
    """
    if not rows:
        return

    # Build "(%s,%s,…,true)" placeholders × len(rows)
    placeholders = ", ".join(
        "(" + ", ".join(["%s"] * len(rows[0])) + ", true)" for _ in rows
    )
    flat_values = [val for row in rows for val in row]

    cur.execute(f"""
        INSERT INTO stg_player_season_stats (
          player_id, team_id, league_id, season,
          position, number,
          appearances, lineups, minutes_played,
          goals, assists, saves,
          dribbles_attempts, dribbles_success,
          fouls_committed, tackles,
          passes_total, passes_key, pass_accuracy,
          yellow_cards, red_cards,
          is_valid
        )
        VALUES {placeholders}
    """, tuple(flat_values))

# ─────────────────────────────── main  ──────────────────────────────────
def main() -> None:
    """
    • Parse CLI flags.
    • Resolve latest run folder for chosen mode / league / season.
    • Stream JSON blobs → parse → buffer → insert.
    """
    # ---- CLI parsing ---------------------------------------------------
    import argparse
    cli = argparse.ArgumentParser("Load player-season stats into staging.")
    cli.add_argument("--league_folder", required=True)
    cli.add_argument("--season_str",    required=True)
    cli.add_argument("--run_mode", choices=["initial", "incremental"],
                     default="incremental")
    cli.add_argument("--player_list_path")  # ignored; keeps Bash cmd symmetric
    args = cli.parse_args()

    league_folder, season_str, run_mode = args.league_folder, args.season_str, args.run_mode
    container = "raw"

    # ---- Resolve latest run folder ------------------------------------
    base_prefix = f"players/{run_mode}/{league_folder}/{season_str}/"
    client = _blob_client().get_container_client(container)

    # Gather run_<date> folders that *actually* contain player_details JSONs.
    valid_runs = set()
    for blob in client.list_blobs(name_starts_with=base_prefix):
        parts = blob.name.split("/")
        # Expecting: players / mode / league / season / <run> / player_details / file.json
        if (
            len(parts) >= 7
            and parts[5] == "player_details"      # ensures we’re not in /squads/
            and parts[-1].endswith(".json")
        ):
            valid_runs.add(parts[4])              # collect the <run> folder

    if not valid_runs:
        log.error("No player_details blobs found under %s", base_prefix)
        sys.exit(1)

    latest = max(valid_runs)  # string compare OK (YYYY-MM-DD)
    blob_prefix = (
        f"{base_prefix}{latest}/player_details" if run_mode == "initial"
        else f"{base_prefix}{latest}"
    )
    log.info("Resolved Blob prefix → %s", blob_prefix)

    # ---- Iterate blobs & load DB --------------------------------------
    buffer: List[Tuple] = []
    files_processed = rows_inserted = files_skipped = 0

    with get_db_connection() as conn:
        cur = conn.cursor()

        for blob in client.list_blobs(name_starts_with=blob_prefix):
            if not blob.name.endswith(".json"):
                continue  # skip directory placeholders

            files_processed += 1
            try:
                rows = parse_stats_rows(_fetch_blob_json(container, blob.name))

                if rows:
                    buffer.extend(rows)
                    # Flush buffer when threshold hit
                    if len(buffer) >= CHUNK_SIZE:
                        insert_chunk(cur, buffer)
                        conn.commit()
                        rows_inserted += len(buffer)
                        buffer.clear()
                else:
                    files_skipped += 1  # no stats rows parsed

            except Exception as exc:
                log.warning("Error parsing %s → %s", blob.name, exc)
                files_skipped += 1

        # Final flush
        if buffer:
            insert_chunk(cur, buffer)
            conn.commit()
            rows_inserted += len(buffer)

    # ---- Summary ------------------------------------------------------
    log.info("──── STAGING LOAD SUMMARY (player_season_stats) ────")
    log.info("League / Season      : %s / %s", league_folder, season_str)
    log.info("Run mode             : %s", run_mode)
    log.info("Blob prefix used     : %s", blob_prefix)
    log.info("JSON files processed : %d", files_processed)
    log.info("Rows inserted        : %d", rows_inserted)
    log.info("Files skipped/errors : %d", files_skipped)
    log.info("Finished stg load – OK")

# ─────────────────────────── bootstrap  ────────────────────────────────
if __name__ == "__main__":
    main()
