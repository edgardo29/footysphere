import os
import sys
import json
from azure.storage.blob import BlobServiceClient

# Add the `test_scripts` and `config` directories to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config")))

from get_db_conn import get_db_connection  # Reuse modular connection logic
from credentials import AZURE_STORAGE


def fetch_data_from_blob(container_name, blob_path):
    """
    Fetches data from Azure Blob Storage.
    Args:
        container_name (str): The name of the Azure Blob Storage container.
        blob_path (str): The path to the blob in the container.
    Returns:
        dict: Parsed JSON data from the blob.
    """
    try:
        # Create BlobServiceClient
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])

        # Get the blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        # Download the blob content and parse it as JSON
        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)
    except Exception as e:
        print(f"Error fetching data from Azure Blob Storage at {blob_path}: {e}")
        raise


def insert_league_data_into_db(data):
    """
    Inserts league data into the `leagues` table in the database.
    Args:
        data (dict): The league data to insert.
    """
    try:
        # Extract league details from the response
        league_info = data["response"][0]  # Get the first item in the response array
        league_id = league_info["league"]["id"]
        league_name = league_info["league"]["name"]
        logo_url = league_info["league"]["logo"]
        country = league_info["country"]["name"]

        # Reuse modular connection logic
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if the league already exists
        cur.execute("SELECT COUNT(*) FROM leagues WHERE league_id = %s", (league_id,))
        if cur.fetchone()[0] > 0:
            print(f"League with ID {league_id} already exists. Skipping insertion.")
        else:
            # Prepare and execute the insertion query
            query = """
            INSERT INTO leagues (league_id, name, logo_url, country)
            VALUES (%s, %s, %s, %s)
            """
            values = (league_id, league_name, logo_url, country)
            cur.execute(query, values)
            conn.commit()
            print(f"League {league_name} inserted successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting league data into the database: {e}")
        raise


if __name__ == "__main__":
    # Load league metadata from leagues.json
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/leagues.json"))
    with open(config_path, "r") as f:
        leagues = json.load(f)

    print("Processing leagues from leagues.json...")

    for league in leagues:
        league_name = league["name"]
        season = league["season"]

        # Generate dynamic blob path
        blob_path = f"raw/leagues/{league_name.lower().replace(' ', '_')}/{season}_25/league.json"

        print(f"Processing League: {league_name} (ID: {league['id']}, Season: {season})")

        try:
            # Fetch data from Azure Blob Storage
            league_data = fetch_data_from_blob("raw", blob_path)

            # Insert data into the database
            insert_league_data_into_db(league_data)

        except Exception as e:
            print(f"Error processing League: {league_name}: {e}")
