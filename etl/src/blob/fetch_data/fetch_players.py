import os
import sys
import json
import requests
from azure.storage.blob import BlobServiceClient

# Add your config/test_scripts paths for DB connection & credentials
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from credentials import API_KEYS, AZURE_STORAGE
from get_db_conn import get_db_connection

def make_league_folder_name(league_name):
    """
    Creates a folder-friendly string from league_name.
    Example: "Premier League" -> "premier_league"
    """
    return str(league_name).lower()\
                          .replace(' ', '_')\
                          .replace('/', '_')\
                          .replace('&', '_')\
                          .replace(',', '_')\
                          .replace('-', '_')

def get_current_league_seasons():
    """
    Retrieves (league_id, league_name, season_year) from league_seasons + leagues
    where league_seasons.is_current=true.
    Adjust or remove conditions if your logic differs.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT ls.league_id,
               l.league_name,
               ls.season_year
          FROM league_seasons ls
          JOIN leagues l ON l.league_id = ls.league_id
         WHERE ls.is_current = true
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows  # e.g. [(39, 'Premier League', 2024), (140, 'La Liga', 2024), ...]

def get_team_ids_for_league_season(league_id, season_year):
    """
    Retrieves team_ids from team_league_seasons for the given (league_id, season_year).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT team_id
          FROM team_league_seasons
         WHERE league_id = %s
           AND season_year = %s
    """
    cur.execute(query, (league_id, season_year))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [r[0] for r in rows]

def fetch_squad(team_id):
    """
    GET /players/squads?team={team_id} => returns the squad for that team.
    """
    base_url = "https://v3.football.api-sports.io/players/squads"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"team": team_id}

    resp = requests.get(base_url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def fetch_player_details(player_id, season_year):
    """
    GET /players?id={player_id}&season={season_year} => detailed player info.
    """
    base_url = "https://v3.football.api-sports.io/players"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"id": player_id, "season": season_year}

    resp = requests.get(base_url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def upload_json_to_blob(data, container, blob_path):
    """
    Uploads dict -> JSON to Azure Blob at blob_path, skipping if it exists.
    """
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)

    if blob_client.exists():
        # Minimal log
        return

    json_str = json.dumps(data, indent=4)
    blob_client.upload_blob(json_str, overwrite=True)

def main():
    """
    1) Joins league_seasons+leagues => (league_id, league_name, season_year) for .is_current=true
    2) For each => obtains team IDs from team_league_seasons => fetch squads => parse => fetch details => store in blob
    3) Minimal logs for player details
    """
    # 1) get current leagues+seasons
    league_seasons = get_current_league_seasons()
    if not league_seasons:
        print("No current league_seasons found. Exiting.")
        sys.exit(0)

    container_name = "raw"
    print(f"Found {len(league_seasons)} league_season records to process...")

    for (league_id, league_name, season_year) in league_seasons:
        league_folder = make_league_folder_name(league_name)
        season_str = f"{season_year}_{(season_year % 100)+1}"

        print(f"\n=== LeagueID={league_id}, '{league_name}', Year={season_year} => folder='{league_folder}', season='{season_str}' ===")

        # 2) get team_ids from bridging table
        team_ids = get_team_ids_for_league_season(league_id, season_year)
        if not team_ids:
            print(f"   -> No team_ids for league_id={league_id}, season_year={season_year}. Skipping.")
            continue

        squads_path = f"players/initial/{league_folder}/{season_str}/squads"
        details_path = f"players/initial/{league_folder}/{season_str}/player_details"

        print(f"   -> {len(team_ids)} teams found: {team_ids}")

        # 3) For each team => fetch squads => parse => fetch details
        for tid in team_ids:
            try:
                squad_data = fetch_squad(tid)
                squad_blob = f"{squads_path}/squad_{tid}.json"
                upload_json_to_blob(squad_data, container_name, squad_blob)

                resp_arr = squad_data.get("response", [])
                if not resp_arr:
                    continue

                # Usually "response":[ { "team":..., "players":[...] } ]
                players_list = resp_arr[0].get("players", [])
                print(f"   -> TeamID={tid} squad has {len(players_list)} players. Fetching details...")

                for p in players_list:
                    pid = p.get("id")
                    if pid:
                        try:
                            details = fetch_player_details(pid, season_year)
                            det_blob = f"{details_path}/player_{pid}.json"
                            upload_json_to_blob(details, container_name, det_blob)
                        except Exception as e:
                            # minimal log for error
                            print(f"   Error fetching details for player_id={pid}, ignoring. {e}")
            except Exception as e:
                print(f"   Error fetching squad for team_id={tid}, ignoring. {e}")

    print("\nAll leagues processed with minimal logging done.")

if __name__ == "__main__":
    main()
