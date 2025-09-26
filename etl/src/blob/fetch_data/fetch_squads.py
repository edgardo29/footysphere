#!/usr/bin/env python
"""
fetch_squads.py
────────────────────────────────────────────────────────────────────────────
One-shot script – run once per league + season (or whenever you want a
fresh roster) **before** you fetch player-season stats.

Steps
=====
1. `/teams` → list every club playing league_id / season_year.
2. For each club:
      • `/players/squads`  → preferred, richer endpoint
      • fallback to `/players` if the squad array is empty (common for
        newly-promoted or lower-tier teams).
3. Build a dict  {team_id: [player_id, …]}.
4. Upload the single JSON to Azure Blob:

   raw/squads/initial/<league_folder>/<season_str>/run_<YYYY-MM-DD>/squads.json

Why store squads?
-----------------
* **Faster** – the expensive roster discovery happens once.
* **Reproducible** – player fetches always read from an immutable blob.
* **Auditable** – you can diff two run_* folders to see roster churn.

Usage
-----
python fetch_squads.py \
       --league_folder bundesliga \
       --league_id     78 \
       --season_str    2024_25 \
       --season_year   2024
"""

# ───────── std-lib ───────────────────────────────────────────────────────
import os, sys, json, time, logging
from datetime import datetime, timezone
from pathlib import Path
from typing  import Dict, List

# ───────── 3rd-party ─────────────────────────────────────────────────────
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

# ───────── project secrets / config ──────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
from credentials import API_KEYS, AZURE_STORAGE

# ───────── logging ───────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                    level=logging.INFO)
logging.getLogger("azure").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ───────── constants ─────────────────────────────────────────────────────
API_BASE = "https://v3.football.api-sports.io"

# ───────── helper functions ──────────────────────────────────────────────
def _blob_container():
    """Return a handle to the `raw` container."""
    service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )
    return service.get_container_client("raw")

def _api_get(endpoint: str, params: Dict) -> Dict:
    """Thin wrapper around requests.get with retryable raise_for_status()."""
    resp = requests.get(
        f"{API_BASE}/{endpoint}",
        headers={"x-apisports-key": API_KEYS["api_football"], "Accept": "application/json"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def _team_ids(league_id: int, season_year: int) -> List[int]:
    """Return all club IDs for a league/season combination."""
    payload = _api_get("teams", {"league": league_id, "season": season_year})
    return [item["team"]["id"] for item in payload["response"]]

def _player_ids_for_team(team_id: int, season_year: int) -> List[int]:
    """
    Preferred: `/players/squads`.
    Fallback : `/players` (some squads come back empty early in the season).
    """
    payload = _api_get("players/squads", {"team": team_id, "season": season_year})
    if payload["response"]:
        return [p["id"] for p in payload["response"][0]["players"]]

    # Fallback path
    payload = _api_get("players", {"team": team_id, "season": season_year})
    return [row["player"]["id"] for row in payload["response"]]

# ───────── main ──────────────────────────────────────────────────────────
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser("Fetch squads and store one JSON snapshot")
    parser.add_argument("--league_folder", required=True, help="e.g. premier_league")
    parser.add_argument("--league_id",    type=int, required=True, help="numeric API league ID")
    parser.add_argument("--season_str",   required=True, help="e.g. 2024_25")
    parser.add_argument("--season_year",  type=int, required=True, help="e.g. 2024")
    parser.add_argument("--run_date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Folder name component; default = today (UTC)")
    args = parser.parse_args()

    log.info("▶ building squads  league=%s  season=%s", args.league_folder, args.season_str)

    team_ids = _team_ids(args.league_id, args.season_year)
    log.info("• %d teams found", len(team_ids))

    squads: Dict[int, List[int]] = {}
    for tid in team_ids:
        try:
            squads[tid] = _player_ids_for_team(tid, args.season_year)
            time.sleep(0.2)        # conservative rate-limit guard
        except Exception as exc:
            log.warning("  – team %s skipped (%s)", tid, exc)

    # -------- upload ----------------------------------------------------
    blob_path = (f"squads/initial/{args.league_folder}/{args.season_str}/"
                 f"run_{args.run_date}/squads.json")
    payload = {
        "meta": vars(args),
        "teams": squads,               # {team_id: [player_ids]}
    }
    _blob_container().upload_blob(
        name   = blob_path,
        data   = json.dumps(payload, indent=2).encode("utf-8"),
        overwrite = True,               # overwrite if run twice the same day
        content_settings = ContentSettings(content_type="application/json"),
    )
    log.info("✔ uploaded %s  teams=%d  players=%d",
             blob_path, len(squads), sum(len(v) for v in squads.values()))

if __name__ == "__main__":
    main()
