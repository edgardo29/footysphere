# players_fetch.py
#
# Purpose
# -------
# Fetch player identities for a league-season into Azure Blob with **low API usage**:
#   - PRIMARY: /players?league=<id>&season=<yyyy>&page=<n> (league-level paging)
#   - SUPPLEMENT: /players/squads?team=<id> (one per team, optional) to catch zero-minute players
# Writes one JSON per player under player_details/ so your existing loader works unchanged.
#
# Snapshot Foldering
# ------------------
# You provide a base window name: "summer" or "winter".
# The script **automatically** expands it to "summer_YYYYMMDD_HHMM" (UTC) on each run.
# If you pass a custom label (e.g., "summer_20251001_0915"), it uses it as-is.
#
# Final blob path:
#   raw/players/<sanitized_league_name>_<league_id>/<YYYY_YY>/<window_label>/{squads,player_details}
# Example:
#   raw/players/serie_a_135/2025_26/summer_20250930_1640/player_details/player_19054.json
#
# Idempotence
# -----------
# - Re-running with the **same** window label will **skip existing files** (no overwrites).
# - New run on the **same day** creates a **new label** (different minute) → a fresh folder.
#
# CLI
# ---
# Required:
#   --window {summer|winter|custom_label}   # "summer" or "winter" auto-expands to summer_YYYYMMDD_HHMM (UTC)
#
# Optional:
#   --league_ids "71,135"     # only process these leagues (comma-separated); omit to process all current
#   --season_year 2025        # override season across all processed leagues
#   --no_squads               # skip the squads supplement (league pages only)
#
# Notes
# -----
# - Massive call reduction vs per-player endpoint (hundreds → dozens per league).
# - Detects API-Football daily-limit messages and stops further API calls gracefully.

import os
import sys
import json
import time
import argparse
import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from azure.storage.blob import BlobServiceClient

# Local imports (adjusted to your repo)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from credentials import API_KEYS, AZURE_STORAGE
from get_db_conn import get_db_connection


# =============== Naming helpers ===============

def sanitize_name(s: str) -> str:
    """Lowercase + underscores; safe for blob folders."""
    return (
        str(s)
        .lower()
        .replace(' ', '_')
        .replace('/', '_')
        .replace('&', '_')
        .replace(',', '_')
        .replace('-', '_')
    )

def make_league_folder(league_name: str, league_id: int) -> str:
    """ALWAYS '<sanitized_league_name>_<league_id>' (e.g., 'serie_a_135')."""
    return f"{sanitize_name(league_name)}_{league_id}"

def make_season_dir(season_year: int) -> str:
    """'YYYY_YY' (e.g., 2025_26)."""
    return f"{season_year}_{(season_year % 100) + 1:02d}"

def resolve_window_label(window_arg: str) -> str:
    """
    If user passes 'summer' or 'winter', expand to 'summer_YYYYMMDD_HHMM' (UTC).
    If user passes a custom string (already contains an underscore timestamp), use as-is.
    """
    base = window_arg.strip().lower()
    if base in {"summer", "winter"}:
        ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M")
        return f"{base}_{ts}"
    # accept custom labels like 'summer_20251001_0915' or anything else
    return window_arg


# =============== HTTP helpers ===============

def http_get_json(session: requests.Session, url: str, headers: dict, params: dict,
                  max_retries: int = 4, base_delay: float = 0.6, backoff: float = 1.7) -> dict:
    """GET with light retries for 429/5xx."""
    attempt = 0
    while True:
        try:
            resp = session.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    resp.raise_for_status()
                time.sleep(base_delay * (backoff ** attempt))
                attempt += 1
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            if attempt >= max_retries:
                raise
            time.sleep(base_delay * (backoff ** attempt))
            attempt += 1

def api_daily_limit_hit(payload: Dict[str, Any]) -> bool:
    """Detect API-Football 'request limit' messages that arrive in the 'errors' field."""
    err = payload.get("errors")
    if not err:
        return False
    if isinstance(err, dict):
        msg = " ".join(str(v) for v in err.values() if v is not None).lower()
        return ("request limit" in msg) or ("reached the limit" in msg) or ("upgrade" in msg)
    return False


# =============== DB lookups ===============

def get_current_league_seasons() -> List[Tuple[int, str, int]]:
    """
    Default league set:
      [(league_id, league_name, season_year), ...]
    Driven by league_seasons.is_current = true.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT ls.league_id,
               l.league_name,
               ls.season_year
          FROM league_seasons ls
          JOIN leagues l ON l.league_id = ls.league_id
         WHERE ls.is_current = TRUE
         ORDER BY ls.league_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_team_ids_for_league_season(league_id: int, season_year: int) -> List[int]:
    """Team IDs participating in the league-season."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT team_id
          FROM team_league_seasons
         WHERE league_id = %s
           AND season_year = %s
         ORDER BY team_id
    """, (league_id, season_year))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


# =============== API wrappers ===============

def fetch_league_players_page(session: requests.Session, league_id: int, season_year: int, page: int) -> dict:
    """GET /players?league=<id>&season=<yyyy>&page=<n> (≈20 players per page)."""
    url = "https://v3.football.api-sports.io/players"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"league": league_id, "season": season_year, "page": page}
    return http_get_json(session, url, headers, params)

def fetch_squad(session: requests.Session, team_id: int) -> dict:
    """GET /players/squads?team=<team_id> (supplement)."""
    url = "https://v3.football.api-sports.io/players/squads"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"team": team_id}
    return http_get_json(session, url, headers, params)


# =============== Blob helpers ===============

def get_blob_client(container: str, blob_path: str):
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    bsc = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
    return bsc.get_blob_client(container=container, blob=blob_path)

def upload_json_to_blob(data: dict, container: str, blob_path: str, overwrite: bool = False) -> bool:
    """
    Upload dict->JSON.
    - overwrite=False: skip if exists (idempotent within a snapshot)
    - returns True if written, False if skipped
    """
    bc = get_blob_client(container, blob_path)
    if not overwrite and bc.exists():
        return False
    bc.upload_blob(json.dumps(data, indent=4), overwrite=True)
    return True


# =============== Writers (per-player JSON) ===============

def write_player_from_page(container: str, details_path: str, league_id: int, season_year: int, obj: Dict[str, Any]) -> bool:
    """
    Build a /players-like single-player payload from a league page row and upload it.
    Keeps your loader compatible (it reads response[0].player).
    """
    pid = obj.get("player", {}).get("id")
    if not pid:
        return False
    payload = {
        "get": "players",
        "parameters": {"league": str(league_id), "season": str(season_year), "id": str(pid)},
        "errors": [],
        "results": 1,
        "paging": {"current": 1, "total": 1},
        "response": [obj],
    }
    return upload_json_to_blob(payload, container, f"{details_path}/player_{pid}.json", overwrite=False)

def write_minimal_player_from_squad(container: str, details_path: str, league_id: int, season_year: int, p: Dict[str, Any]) -> bool:
    """
    If a player wasn't seen in league pages, synthesize a minimal identity from the squad list.
    """
    pid = p.get("id")
    if not pid:
        return False
    player_obj = {
        "id": pid,
        "name": p.get("name"),
        "firstname": None,
        "lastname": None,
        "age": p.get("age"),
        "birth": {"date": None, "place": None, "country": None},
        "nationality": None,
        "height": None,
        "weight": None,
        "injured": False,
        "photo": p.get("photo"),
    }
    payload = {
        "get": "players",
        "parameters": {"league": str(league_id), "season": str(season_year), "id": str(pid)},
        "errors": [],
        "results": 1,
        "paging": {"current": 1, "total": 1},
        "response": [{"player": player_obj, "statistics": []}],
    }
    return upload_json_to_blob(payload, container, f"{details_path}/player_{pid}.json", overwrite=False)


# =============== Main ===============

def main():
    parser = argparse.ArgumentParser(description="Fetch league-season players into Azure Blob (snapshot-based).")
    parser.add_argument(
        "--window",
        required=True,
        help="Base label 'summer' or 'winter' (auto-expands to '<base>_YYYYMMDD_HHMM' UTC), "
             "or pass a custom snapshot label like 'summer_20251001_0915'."
    )
    parser.add_argument(
        "--league_ids",
        default="",
        help="(Optional) Comma-separated league_ids to process (e.g., '71,135'). Omit for all current leagues."
    )
    parser.add_argument(
        "--season_year",
        type=int,
        default=None,
        help="(Optional) Override season year for all processed leagues (e.g., 2025)."
    )
    parser.add_argument(
        "--no_squads",
        action="store_true",
        help="If set, do NOT call /players/squads (league pages only)."
    )
    args = parser.parse_args()

    # Resolve snapshot label
    resolved_window = resolve_window_label(args.window)  # auto-expands summer/winter
    print(f"Requested window: {args.window}")
    print(f"Resolved snapshot window: {resolved_window} (UTC)")

    # Determine league set
    leagues = get_current_league_seasons()
    if not leagues:
        print("No current leagues found. Exiting.")
        return

    if args.league_ids.strip():
        allow: Set[int] = {int(x.strip()) for x in args.league_ids.split(",") if x.strip()}
        leagues = [row for row in leagues if row[0] in allow]

    if args.season_year is not None:
        leagues = [(lid, lname, int(args.season_year)) for (lid, lname, _sy) in leagues]

    # Clients + counters
    session = requests.Session()
    container_name = "raw"

    total_leagues = len(leagues)
    total_pages = 0
    total_teams = 0
    total_players_written = 0
    total_players_skipped = 0
    daily_limit_tripped = False

    print(f"Leagues to process: {total_leagues}")

    # Process each league
    for (league_id, league_name, season_year) in leagues:
        league_folder = make_league_folder(league_name, league_id)
        season_dir = make_season_dir(season_year)

        base_prefix  = f"players/{league_folder}/{season_dir}/{resolved_window}"
        squads_path  = f"{base_prefix}/squads"
        details_path = f"{base_prefix}/player_details"

        print(f"\n=== LeagueID={league_id}, '{league_name}', Season={season_year}")
        print(f"    folder='{league_folder}' -> {base_prefix}")

        # Track which player_ids we've already written
        seen: Set[int] = set()

        # League-level pages (PRIMARY)
        page = 1
        while True:
            try:
                payload = fetch_league_players_page(session, league_id, season_year, page)
            except Exception as e:
                print(f"   ! league page {page} fetch error: {e}")
                break

            if api_daily_limit_hit(payload):
                print("   ! Daily request limit reported by API-Football. Stopping further API calls.")
                daily_limit_tripped = True
                break

            resp = payload.get("response", []) or []
            paging = payload.get("paging", {}) or {}
            total = int(paging.get("total", 1) or 1)

            if not resp:
                break

            for obj in resp:
                pid = obj.get("player", {}).get("id")
                if not pid:
                    continue
                if write_player_from_page(container_name, details_path, league_id, season_year, obj):
                    total_players_written += 1
                else:
                    total_players_skipped += 1
                seen.add(pid)

            total_pages += 1
            if page >= total:
                break
            page += 1
            time.sleep(0.08)

        # Squads (supplement)
        if not args.no_squads and not daily_limit_tripped:
            team_ids = get_team_ids_for_league_season(league_id, season_year)
            if team_ids:
                print(f"   -> Teams discovered: {len(team_ids)}")
                total_teams += len(team_ids)

                for tid in team_ids:
                    if daily_limit_tripped:
                        break
                    try:
                        squad = fetch_squad(session, tid)
                    except Exception as e:
                        print(f"   ! squad fetch error for team_id={tid}: {e}")
                        time.sleep(0.2)
                        continue

                    if api_daily_limit_hit(squad):
                        print("   ! Daily request limit reported during squads. Stopping further API calls.")
                        daily_limit_tripped = True
                        break

                    # Store raw squad (idempotent)
                    try:
                        upload_json_to_blob(squad, container_name, f"{squads_path}/squad_{tid}.json", overwrite=False)
                    except Exception as e:
                        print(f"   ! upload error for squad team_id={tid}: {e}")

                    # Synthesize minimal player JSONs for unseen IDs
                    sresp = squad.get("response", []) or []
                    if sresp:
                        players = (sresp[0] or {}).get("players", []) or []
                        for p in players:
                            pid = p.get("id")
                            if not pid or pid in seen:
                                continue
                            if write_minimal_player_from_squad(container_name, details_path, league_id, season_year, p):
                                total_players_written += 1
                            else:
                                total_players_skipped += 1
                            seen.add(pid)
                    time.sleep(0.08)

        if daily_limit_tripped:
            print("   -> Stopped due to daily limit; snapshot contains partial results for this league.")

    # Summary
    print("\n================ SUMMARY ================")
    print(f"Snapshot window        : {resolved_window}")
    print(f"Leagues processed      : {total_leagues}")
    print(f"League pages fetched   : {total_pages}")
    print(f"Teams (squads) touched : {total_teams}")
    print(f"Per-player files       : written={total_players_written}, skipped(existing)={total_players_skipped}")
    print(f"Daily limit tripped?   : {daily_limit_tripped}")
    print("========================================")


if __name__ == "__main__":
    main()
