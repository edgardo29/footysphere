import os
import sys
import json
from azure.storage.blob import BlobServiceClient

# Add your config and test_scripts paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection
from credentials import AZURE_STORAGE

def fetch_data_from_blob(container_name, blob_path):
    try:
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)
    except Exception as e:
        print(f"Error fetching blob data at {blob_path}: {e}")
        raise

def load_venues_from_teams_json(data):
    """
    Goes through data["response"], each item has 'venue' dict:
      {
         "id": 556,
         "name": "Old Trafford",
         "city": "Manchester",
         "country": "England",
         ...
      }
    Insert into the `venues` table if not present.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for item in data.get("response", []):
            venue = item.get("venue", {})
            venue_id = venue.get("id")
            if not venue_id:
                continue  # Some API results might have null venue

            name = venue.get("name")
            city = venue.get("city")
            country = venue.get("country")
            # You can parse capacity, address, etc. if you want.

            # Check if venue already exists
            cur.execute("SELECT COUNT(*) FROM venues WHERE venue_id=%s", (venue_id,))
            exists = cur.fetchone()[0] > 0

            if exists:
                # Optionally update info if you want
                # update_sql = """ UPDATE venues SET ... WHERE venue_id=%s """
                # cur.execute(update_sql, (..., venue_id))
                pass
            else:
                insert_sql = """
                    INSERT INTO venues (venue_id, venue_name, city, country)
                    VALUES (%s, %s, %s, %s)
                """
                cur.execute(insert_sql, (venue_id, name, city, country))
                print(f"[venues] Inserted venue_id={venue_id}, name='{name}'")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error loading venues from teams JSON: {e}")
        raise

if __name__ == "__main__":
    # Read your leagues config
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config/leagues.json"))
    with open(config_path, "r") as f:
        leagues_config = json.load(f)

    print("=== Loading Venues from the same teams.json files ===")

    for league in leagues_config:
        league_id = league["id"]
        league_name = league["name"]
        season = league["season"]

        blob_path = f"teams/{league_name.lower().replace(' ', '_')}/{season}_25/teams.json"
        print(f"\n-- Venues: {league_name} (ID={league_id}, season={season}) from {blob_path}")

        try:
            data = fetch_data_from_blob("raw", blob_path)
            load_venues_from_teams_json(data)
        except Exception as e:
            print(f"Error loading venues for league={league_id}: {e}")
