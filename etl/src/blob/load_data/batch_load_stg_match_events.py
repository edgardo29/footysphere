#!/usr/bin/env python3
"""
batch_load_stg_match_events.py
───────────────────────────────────────────────────────────────────────────────
Discovers every `<league_folder>/<season_folder>` pair that *actually exists*
under a chosen stage folder (*initial* or *incremental*) and calls
`load_stg_match_events.py` once for each pair.

Why have a batch wrapper?
-------------------------
* **Resilience** – If one league fails you see which one; you can re-run only
  the failures.
* **Simplicity** – The single-league loader stays ultra-focused and keeps its
  own SQL connection logic isolated.
* **Scalability** – Airflow, GNU parallel, or any orchestration tool can break
  this runner into separate tasks if needed (one per league/season).

Example hierarchy scanned
-------------------------
raw/match_events/incremental/
└── premier_league/2024_25/events_123.json
└── premier_league/2024_25/events_456.json
└── la_liga/2023_24/events_999.json
"""

import os, sys, subprocess, argparse
from pathlib import Path
from azure.storage.blob import BlobServiceClient

# ---------------------------------------------------------------------------
# 0)  CLI – choose stage folder to scan
# ---------------------------------------------------------------------------

cli = argparse.ArgumentParser(
    description="Auto-detect league/season folders and load them into staging."
)
cli.add_argument("--stage_folder", default="incremental",
                 choices=["initial", "incremental"],
                 help=("Top-level directory under raw/match_events/ that will be "
                       "scanned (default: 'incremental')."))
args = cli.parse_args()

# ---------------------------------------------------------------------------
# 1)  Azure Blob client setup
# ---------------------------------------------------------------------------

# Add config directory to path so we can import credentials
HERE = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(HERE, "../../../config")))
from credentials import AZURE_STORAGE

account_url      = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
blob_service     = BlobServiceClient(account_url,
                                     credential=AZURE_STORAGE["access_key"])
container_name   = "raw"
container_client = blob_service.get_container_client(container_name)

# Prefix we will walk, e.g. "match_events/incremental/"
PREFIX      = f"match_events/{args.stage_folder}/"
prefix_len  = len(PREFIX)     # so we can strip it later

print(f"Scanning Azure prefix: {container_name}/{PREFIX} …")

# ---------------------------------------------------------------------------
# 2)  Discover every <league>/<season> that has at least one JSON
# ---------------------------------------------------------------------------

pairs = set()     # {("premier_league", "2024_25"), ...}

for blob in container_client.list_blobs(name_starts_with=PREFIX):
    # We only care about the JSON leaves named events_<fixture>.json
    if not blob.name.endswith(".json"):
        continue

    # Strip the prefix so we see only "<league>/<season>/events_123.json"
    subpath = blob.name[prefix_len:]

    # Split only the first two segments: ["<league>", "<season>", "events_…"]
    parts = subpath.split("/", 2)
    if len(parts) < 2:
        # Malformed path – skip rather than crash
        continue

    league_folder, season_folder = parts[0], parts[1]
    pairs.add((league_folder, season_folder))

print(f"Found {len(pairs)} league/season folders:")
for lg, ss in sorted(pairs):
    print(f"  • {lg} / {ss}")

if not pairs:
    print("Nothing to load; exiting peacefully.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# 3)  Helper to run the single-league loader for one pair
# ---------------------------------------------------------------------------

LOADER = Path(__file__).with_name("load_stg_match_events.py")

def run_loader(league: str, season: str) -> None:
    """
    Spins up a child Python process to load one league/season.

    We invoke the other script via subprocess so each run has a clean
    interpreter and its own DB connection. Any failure bubbles up as a
    CalledProcessError, which we trap below.
    """
    cmd = [
        sys.executable,
        str(LOADER),
        "--league_folder", league,
        "--season_folder", season,
        "--stage_folder", args.stage_folder,   # propagate user choice
    ]
    print(f"\n→ Loading events for {league} / {season}")
    subprocess.check_call(cmd)    # will raise if the child exits non-zero
    print(f"✓ Finished {league} / {season}")

# ---------------------------------------------------------------------------
# 4)  Loop through everything we discovered
# ---------------------------------------------------------------------------

for league_folder, season_folder in sorted(pairs):
    try:
        run_loader(league_folder, season_folder)
    except subprocess.CalledProcessError:
        # Fail fast so Airflow (or your CI) can mark the job red
        print(f"✗ Loader failed for {league_folder}/{season_folder}. Aborting.")
        sys.exit(1)

print("\n✔ All discovered match-event folders successfully loaded "
      "into stg_match_events.")
