import os
import sys
import json
from azure.storage.blob import BlobServiceClient

# Add the `config` and `test_scripts` directories to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

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
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)
    except Exception as e:
        print(f"Error fetching data from Azure Blob Storage at {blob_path}: {e}")
        raise


def insert_teams_into_db(data, league_id):
    """
    Inserts team data into the `teams` table in the database.
    Args:
        data (dict): The API response containing team data.
        league_id (int): The league ID for the teams.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for item in data["response"]:
            team = item["team"]
            team_id = team["id"]
            team_name = team["name"]
            logo_url = team["logo"]

            # Check if the team already exists
            cur.execute("SELECT COUNT(*) FROM teams WHERE team_id = %s", (team_id,))
            if cur.fetchone()[0] > 0:
                print(f"Team {team_name} (ID: {team_id}) already exists. Skipping.")
                continue

            # Insert the team into the database
            query = """
            INSERT INTO teams (team_id, league_id, name, logo_url)
            VALUES (%s, %s, %s, %s)
            """
            cur.execute(query, (team_id, league_id, team_name, logo_url))
            conn.commit()
            print(f"Team {team_name} (ID: {team_id}) inserted successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting team data into the database: {e}")
        raise


if __name__ == "__main__":
    # Load league metadata from leagues.json
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config/leagues.json"))
    with open(config_path, "r") as f:
        leagues = json.load(f)

    print("Processing teams for each league...")

    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]
        season = league["season"]

        print(f"Processing teams for League: {league_name} (ID: {league_id}, Season: {season})")

        try:
            # Generate dynamic blob path
            blob_path = f"teams/{league_name.lower().replace(' ', '_')}/{season}_25/teams.json"

            # Fetch data from Azure Blob Storage
            team_data = fetch_data_from_blob("raw", blob_path)

            # Insert team data into the database
            insert_teams_into_db(team_data, league_id)

        except Exception as e:
            print(f"Error processing teams for League: {league_name}: {e}")
