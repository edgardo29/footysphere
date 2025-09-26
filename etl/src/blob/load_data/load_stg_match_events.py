#!/usr/bin/env python3
"""
load_stg_match_events.py
───────────────────────────────────────────────────────────────────────────────
Bulk-loads JSON match-event files (one JSON per fixture) from Azure Blob Storage
into the **stg_match_events** table.

Key points
----------
1. **Stage folder flexibility** – The script now accepts a CLI flag
   `--stage_folder` so you can target either *initial* back-fills **or** your
   new *incremental* drops without changing any code.
2. **Chunked inserts** – Uses psycopg2.extras.execute_values() to push rows in
   pages of 1 000 at a time (tweak BATCH_SZ if needed).
3. **Single responsibility** – Loads exactly one `<league>/<season>` pair per
   invocation so Airflow (or a parent batch runner) can parallelise or retry
   failures cleanly.

Directory layout expected in Azure
----------------------------------
raw/
└── match_events/
    ├── initial/
    │   └── <league_folder>/<season_folder>/events_<fixture_id>.json
    └── incremental/
        └── <league_folder>/<season_folder>/events_<fixture_id>.json
"""

import os, sys, json, argparse
from azure.storage.blob import BlobServiceClient
import psycopg2.extras

# ---------------------------------------------------------------------------
# 0)  Ensure helpers on PYTHONPATH  (credentials + DB connection helper)
# ---------------------------------------------------------------------------

HERE = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(HERE, "../../../config")),
    os.path.abspath(os.path.join(HERE, "../../../test_scripts")),
])
from credentials import AZURE_STORAGE
from get_db_conn import get_db_connection          # returns psycopg2 connection

# ---------------------------------------------------------------------------
# 1)  CLI arguments
# ---------------------------------------------------------------------------

cli = argparse.ArgumentParser(
    description="Load match-event JSON blobs into stg_match_events."
)
cli.add_argument("--league_folder", required=True,
                 help="Name of the league folder, e.g. 'premier_league'")
cli.add_argument("--season_folder", required=True,
                 help="Season folder, e.g. '2024_25'")
cli.add_argument("--stage_folder", default="initial",
                 choices=["initial", "incremental"],
                 help=("Top-level folder under match_events/ (default: "
                       "'initial'). For new weekly runs, pass 'incremental'."))
args = cli.parse_args()

# Construct the blob prefix dynamically so the exact source location is
#  raw/match_events/<stage_folder>/<league_folder>/<season_folder>/…
blob_prefix = (
    f"match_events/{args.stage_folder}/"
    f"{args.league_folder}/"
    f"{args.season_folder}/"
)

# ---------------------------------------------------------------------------
# 2)  Azure Blob client setup
# ---------------------------------------------------------------------------

account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
blob_service      = BlobServiceClient(account_url,
                                      credential=AZURE_STORAGE["access_key"])
container_name    = "raw"
container_client  = blob_service.get_container_client(container_name)

# ---------------------------------------------------------------------------
# 3)  Database connection
# ---------------------------------------------------------------------------

conn = get_db_connection()   # uses creds from your config; autocommit disabled

def insert_batch(rows):
    """
    Helper that flushes a list of rows into the database in one go.

    Parameters
    ----------
    rows : list[tuple]
        Each tuple matches the stg_match_events column order.
    """
    if not rows:
        return

    psycopg2.extras.execute_values(
        conn.cursor(),
        """
        INSERT INTO stg_match_events (
            fixture_id,
            minute,
            minute_extra,
            team_id,
            player_id,
            assist_player_id,
            event_type,
            event_detail,
            comments
        ) VALUES %s
        """,
        rows,
        page_size=1000      # internal chunk size for execute_values
    )
    conn.commit()

# ---------------------------------------------------------------------------
# 4)  Walk blobs, build batches, flush in chunks
# ---------------------------------------------------------------------------

BATCH_SZ     = 2000          # app-level batch size (feel free to tune)
batch        = []            # in-memory row accumulator
total_files  = 0             # just for a progress message at the end

print(f"Scanning Azure prefix: {container_name}/{blob_prefix}")

for blob in container_client.list_blobs(name_starts_with=blob_prefix):
    # Skip directories or odd files – we only want the actual JSONs
    if not blob.name.endswith(".json"):
        continue

    total_files += 1

    # Download the blob into memory (they're tiny, typical ~2–10 KB each)
    blob_data = (
        blob_service
        .get_blob_client(container_name, blob.name)
        .download_blob()
        .readall()
    )
    payload = json.loads(blob_data)

    # The fixture ID is embedded in the filename: …/events_<fixture_id>.json
    fixture_id = int(blob.name.split("_")[-1].split(".")[0])

    # Each entry in payload["response"] is a single event dict
    for ev in payload.get("response", []):
        t = ev["time"]          # shorthand for time block
        batch.append((
            fixture_id,
            t["elapsed"],
            t.get("extra"),              # may be null
            ev["team"]["id"],
            ev.get("player", {}).get("id"),
            ev.get("assist", {}).get("id"),
            ev["type"],
            ev["detail"],
            ev.get("comments"),
        ))

        # Periodically flush to DB to keep memory bounded
        if len(batch) >= BATCH_SZ:
            insert_batch(batch)
            batch.clear()

# Final flush (remaining < BATCH_SZ rows)
insert_batch(batch)
conn.close()

print(f"Loaded events from {total_files:,} fixture JSON files "
      f"into stg_match_events.")
