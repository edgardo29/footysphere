import os
import sys
import json
from azure.storage.blob import BlobServiceClient

# Add your config + test_scripts paths for credentials + DB
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from credentials import AZURE_STORAGE
from get_db_conn import get_db_connection

CHUNK_SIZE = 1000

def fetch_blob_json(container_name, blob_path):
    """
    Downloads the JSON file from Azure Blob Storage, returns it as a dict.
    """
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    raw_data = blob_client.download_blob().readall()
    return json.loads(raw_data)

def parse_player_json(data):
    """
    Given the entire JSON from /players?id={player_id}&season=..., extracts the 'player' object.
    We only need the single item from data["response"][0]["player"], ignoring 'statistics'.
    
    Returns a tuple matching stg_players columns:
      (player_id, firstname, lastname, full_name, birth_date, birth_country,
       nationality, height, weight, photo_url)
    or None if data is malformed.
    """
    resp = data.get("response", [])
    if not resp:
        return None
    
    # Usually resp[0] => { "player": {...}, "statistics":[...] }
    p_obj = resp[0].get("player", {})
    if not p_obj:
        return None
    
    # Extract fields
    player_id = p_obj.get("id")
    firstname = p_obj.get("firstname")
    lastname  = p_obj.get("lastname")
    full_name = p_obj.get("name")
    
    # birth info
    birth     = p_obj.get("birth", {})
    birth_date = birth.get("date")  # e.g. "1997-08-09"
    birth_country = birth.get("country")  # e.g. "Chile"
    
    nationality = p_obj.get("nationality")
    height      = p_obj.get("height")   # e.g. "178 cm"
    weight      = p_obj.get("weight")   # e.g. "72 kg"
    photo_url   = p_obj.get("photo")
    
    # Validate minimum
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

def insert_chunk_stg_players(cur, rows_buffer):
    """
    Performs a single multi-row INSERT into stg_players for the rows in rows_buffer.
    Each row = (player_id, firstname, lastname, full_name, birth_date, birth_country,
                nationality, height, weight, photo_url)
    We'll add is_valid=true for all rows by default.
    """
    if not rows_buffer:
        return

    placeholders_list = []
    values_list = []
    for row in rows_buffer:
        placeholders = "(" + ", ".join(["%s"] * len(row)) + ", true)"
        placeholders_list.append(placeholders)
        values_list.extend(row)

    insert_sql = f"""
    INSERT INTO stg_players (
       player_id, firstname, lastname, full_name,
       birth_date, birth_country, nationality,
       height, weight, photo_url,
       is_valid
    )
    VALUES {", ".join(placeholders_list)}
    """

    cur.execute(insert_sql, tuple(values_list))

def main():
    """
    1) We read every player_{id}.json in a given Blob path, e.g.:
       raw/players/initial/ligue_1/2024_25/player_details/player_*.json
    2) For each file, parse 'player' object => build row => chunked multi-row insert into stg_players.
    3) We skip the 'statistics' array here; that will be loaded into stg_player_season_stats separately.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Load basic player info from Blob -> stg_players in chunked mode.")
    parser.add_argument("--league_folder", required=True,
                        help="Folder name for the league, e.g. 'ligue_1' or 'premier_league'.")
    parser.add_argument("--season_str", required=True,
                        help="Season string, e.g. '2024_25'.")
    args = parser.parse_args()

    league_folder = args.league_folder
    season_str = args.season_str

    container_name = "raw"
    # Path where we stored each player's detail JSON
    base_blob_prefix = f"players/initial/{league_folder}/{season_str}/player_details"

    print(f"Loading players from blob prefix => {base_blob_prefix}")

    # Initialize DB
    conn = get_db_connection()
    cur = conn.cursor()

    rows_buffer = []
    total_files = 0
    success_count = 0
    fail_count = 0

    # List all blobs under that prefix
    from azure.storage.blob import BlobServiceClient
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs(name_starts_with=base_blob_prefix)

    for blob in blob_list:
        blob_name = blob.name
        # Check if it's in the pattern player_{id}.json
        if not blob_name.endswith(".json"):
            continue

        total_files += 1

        # Download & parse
        try:
            data = fetch_blob_json(container_name, blob_name)
            player_row = parse_player_json(data)
            if player_row:
                rows_buffer.append(player_row)
                if len(rows_buffer) >= CHUNK_SIZE:
                    insert_chunk_stg_players(cur, rows_buffer)
                    conn.commit()
                    rows_buffer.clear()
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            print(f"Error loading {blob_name}: {e}")
            fail_count += 1

    # Insert leftover
    if rows_buffer:
        insert_chunk_stg_players(cur, rows_buffer)
        conn.commit()
        rows_buffer.clear()

    cur.close()
    conn.close()

    print("\n========== STAGING LOAD SUMMARY ==========")
    print(f"Total JSON files found: {total_files}")
    print(f"Success (parsed/inserted): {success_count}")
    print(f"Failed/invalid: {fail_count}")
    print("Finished loading stg_players.")

if __name__ == "__main__":
    main()
