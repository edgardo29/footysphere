"""
fetch_player_season_stats.py
────────────────────────────────────────────────────────────────────────────
Purpose
-------
Call the API-Sports “/players” endpoint for a given league + season (and
optionally a restricted list of player IDs) and upload each JSON response
to Azure Blob Storage.

Directory conventions
---------------------
• **Initial back-fill**
  players/initial/<league_folder>/<season_str>/player_details/
      └── player_<id>.json

• **Weekly incremental updates**
  players/incremental/<league_folder>/<season_str>/<YYYY-MM-DD>/
      └── player_<id>.json
  The <YYYY-MM-DD> folder = the script’s execution date in UTC; this keeps
  deltas grouped by load day and lets you purge old deltas later.

Key flags
---------
--league_folder      Folder alias used in your Blob path (e.g. 'bundesliga')
--league_id          Numeric league_id required by roster endpoints (e.g. 78)
--season_str         Season part of the path (e.g. '2024_25')
--season_year        Numeric year required by the API (e.g. 2024)
--run_mode           'initial' or 'incremental'
--player_list_path   Text file of player IDs (required for incremental mode)

Design notes
------------
* One JSON file per player keeps re-runs idempotent (easy overwrite).
* The script *never* touches Postgres; all it cares about is getting fresh
  JSONs into Blob so a downstream loader can pick them up.
* All network/Blob errors are logged and counted; the script exits non-zero
  only if **every** request fails (so Airflow marks a red task).

Example usage
-------------
# Full back-fill (first time)
python fetch_player_season_stats.py \
       --league_folder bundesliga --league_id 78 --season_str 2024_25 \
       --season_year 2024 --run_mode initial

# Weekly incremental
python fetch_player_season_stats.py \
       --league_folder bundesliga --season_str 2024_25 --season_year 2024 \
       --run_mode incremental --player_list_path /tmp/ids.txt
"""

# ────────────────────────── standard library imports ─────────────────────
import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

# ─────────────────────────── third-party imports ─────────────────────────
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

# ─────────────────────── project helper imports ──────────────────────────
sys.path.extend(
    [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
    ]
)
from credentials import AZURE_STORAGE, API_KEYS   # Must contain API_KEYS["api_football"]

# ─────────────────────────── global logger set-up ────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── NEW ▶ stop Azure SDK from spamming per-request logs ─────────────────
logging.getLogger("azure").setLevel(logging.WARNING)

# ─────────────────────────── Azure helper funcs ──────────────────────────
def _get_blob_client() -> BlobServiceClient:
    url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    return BlobServiceClient(account_url=url,
                             credential=AZURE_STORAGE["access_key"])


def _upload_json(container, blob_path: str, payload: Dict) -> None:
    """Upload `payload` (already a dict) to `blob_path` as pretty-printed JSON."""
    json_bytes = json.dumps(payload, indent=2).encode("utf-8")
    container.upload_blob(
        name=blob_path,
        data=json_bytes,
        overwrite=True,                               # idempotent overwrite
        content_settings=ContentSettings(content_type="application/json"),
    )

# ─────────────────────────── API helper funcs ────────────────────────────
API_BASE = "https://v3.football.api-sports.io"

def _call_player_endpoint(player_id: int, season_year: int) -> Dict:
    """Hit `/players?id=<id>&season=<year>` and return the JSON dict."""
    headers = {
        "x-apisports-key": API_KEYS["api_football"],
        "Accept": "application/json",
    }
    params = {"id": player_id, "season": season_year}
    resp = requests.get(f"{API_BASE}/players", headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ───────────── NEW ► helpers to discover full roster for initial load ────
# (kept for reference but no longer used when snapshot is mandatory)
def _call_teams_endpoint(league_id: int, season_year: int) -> List[int]:
    """Return the team_ids participating in league_id / season_year."""
    headers = {
        "x-apisports-key": API_KEYS["api_football"],
        "Accept": "application/json",
    }
    resp = requests.get(
        f"{API_BASE}/teams",
        headers=headers,
        params={"league": league_id, "season": season_year},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    return [team["team"]["id"] for team in payload["response"]]

# NEW ► generic /players fallback (used when /squads returns empty)
def _call_team_players_endpoint(team_id: int, season_year: int) -> List[int]:
    """Return player_ids via /players (used if /squads is empty)."""
    headers = {
        "x-apisports-key": API_KEYS["api_football"],
        "Accept": "application/json",
    }
    resp = requests.get(
        f"{API_BASE}/players",
        headers=headers,
        params={"team": team_id, "season": season_year},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    return [p["player"]["id"] for p in payload["response"]]

def _call_squad_endpoint(team_id: int, season_year: int) -> List[int]:
    """
    Return all player_ids on a squad.

    • Primary call → /players/squads  
    • If that returns an empty list (happens at the very start of a season),
      fall back to /players?team=<id>&season=<year>.
    """
    headers = {
        "x-apisports-key": API_KEYS["api_football"],
        "Accept": "application/json",
    }
    resp = requests.get(
        f"{API_BASE}/players/squads",
        headers=headers,
        params={"team": team_id, "season": season_year},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    if not payload["response"]:                    # ← API hasn’t loaded the squad yet
        return _call_team_players_endpoint(team_id, season_year)

    squad = payload["response"][0]["players"]
    return [p["id"] for p in squad]

# ────────── NEW ► helper to read frozen roster snapshot (Blob) ───────────
def _load_snapshot_player_ids(league_folder: str, season_str: str) -> List[int]:
    """
    Find *latest* run_YYYY-MM-DD/squads.json under
      squads/initial/<league_folder>/<season_str>/
    and return a sorted list of unique player_ids.
    """
    container = _get_blob_client().get_container_client("raw")
    prefix = f"squads/initial/{league_folder}/{season_str}/"
    blobs = list(container.list_blobs(name_starts_with=prefix))

    # Extract distinct run_YYYY-MM-DD folder names (index 4 in the key)
    run_folders = sorted(
        {blob.name.split("/")[4] for blob in blobs if blob.name.count("/") >= 4},
        reverse=True,
    )
    if not run_folders:
        raise FileNotFoundError(f"No roster snapshot under {prefix}")

    latest_folder = run_folders[0]                         # newest snapshot
    blob_name = f"{prefix}{latest_folder}/squads.json"
    logger.info("Using roster snapshot → %s", blob_name)

    snap_bytes = container.download_blob(blob_name).readall()
    squads     = json.loads(snap_bytes)

    # Flatten {team_id:[player_ids]} into one set
    return sorted(
        {pid for roster in squads.get("teams", {}).values() for pid in roster}
    )

# ───────────────────────────── main routine ──────────────────────────────
def main() -> None:
    """
    1. Parse CLI flags & validate.
    2. Build list of player IDs to fetch.
       • incremental → from ids.txt
       • initial     → read frozen roster snapshot (mandatory)
    3. For each ID: call API, upload JSON to Blob.
    4. Log summary; exit non-zero only if every call failed.
    """
    import argparse

    parser = argparse.ArgumentParser("Fetch player-season stats into Azure Blob.")
    parser.add_argument("--league_folder", required=True)
    parser.add_argument("--league_id",    type=int, required=True,
                        help="Numeric league_id for roster endpoints")
    parser.add_argument("--season_str",   required=True)
    parser.add_argument("--season_year",  type=int, required=True,
                        help="Numeric season year required by API (e.g., 2024)")
    parser.add_argument("--run_mode", choices=["initial", "incremental"],
                        default="incremental")
    parser.add_argument("--player_list_path",
                        help="Text file of player IDs (required for incremental)")
    args = parser.parse_args()

    # ── build list of player IDs ────────────────────────────────────────
    if args.run_mode == "incremental":
        if not args.player_list_path:
            logger.error("--player_list_path required for incremental mode")
            sys.exit(1)
        id_file = Path(args.player_list_path)
        if not id_file.is_file():
            logger.error("ID file not found: %s", id_file)
            sys.exit(1)
        player_ids = [int(x.strip()) for x in id_file.read_text().splitlines() if x.strip()]
        if not player_ids:
            logger.info("ID file empty → nothing to fetch")
            sys.exit(0)

    else:  # ─────────── initial mode → **frozen snapshot only** ──────────
        try:
            player_ids = _load_snapshot_player_ids(args.league_folder, args.season_str)
        except Exception as exc:
            logger.error("Roster snapshot lookup failed → %s", exc)
            sys.exit(1)

        if not player_ids:
            logger.error("Snapshot contained zero players → aborting")
            sys.exit(1)

        logger.info("Initial mode • %d unique players loaded from snapshot", len(player_ids))

    # ── build blob path prefix ─────────────────────────────────────────
    exec_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if args.run_mode == "initial":
        run_date   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        blob_prefix = (
            f"players/initial/{args.league_folder}/{args.season_str}/"
            f"{run_date}/player_details"
        )
    else:
        blob_prefix = f"players/incremental/{args.league_folder}/{args.season_str}/{exec_date}"

    blob_container = _get_blob_client().get_container_client("raw")
    success = fail = 0

    # ── iterate over IDs, call API, upload JSON ────────────────────────
    for pid in player_ids:
        try:
            payload = _call_player_endpoint(pid, args.season_year)
            blob_path = f"{blob_prefix}/player_{pid}.json"
            _upload_json(blob_container, blob_path, payload)
            success += 1
            time.sleep(0.2)  # API rate-limit guard
        except Exception as exc:
            logger.warning("player %s → %s", pid, exc)
            fail += 1

    logger.info("SUMMARY | fetched=%d  success=%d  failed=%d",
                len(player_ids), success, fail)
    if success == 0:
        sys.exit(1)

# ───────────────────────── bootstrap ────────────────────────────────────
if __name__ == "__main__":
    main()
