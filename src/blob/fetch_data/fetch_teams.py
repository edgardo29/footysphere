import os
import sys
import json
import requests
from azure.storage.blob import BlobServiceClient

# Add `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))

from credentials import API_KEYS, AZURE_STORAGE


def fetch_teams_from_api(league_id, season):
    """
    Fetches teams for a specific league and season from the API.
    Args:
        league_id (int): The ID of the league.
        season (int): The season year.
    Returns:
        dict: The API response containing team data.
    """
    url = "https://v3.football.api-sports.io/teams"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"league": league_id, "season": season}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching teams for League ID {league_id}, Season {season}: {e}")
        raise


def upload_to_blob(data, container_name, blob_path):
    """
    Uploads data to Azure Blob Storage in JSON format.
    Args:
        data (dict): The data to upload.
        container_name (str): Name of the Azure Blob Storage container.
        blob_path (str): The path within the container to store the blob.
    """
    try:
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        if blob_client.exists():
            print(f"Blob already exists at {blob_path}. Skipping upload.")
            return False

        json_data = json.dumps(data, indent=4)
        blob_client.upload_blob(json_data, overwrite=True)
        print(f"Data successfully uploaded to {container_name}/{blob_path}")
        return True
    except Exception as e:
        print(f"Error uploading data to Azure Blob Storage: {e}")
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
        # Fetch teams from API
        team_data = fetch_teams_from_api(league_id, season)

        # Corrected blob path
        blob_path = f"teams/{league_name.lower().replace(' ', '_')}/{season}_25/teams.json"

        # Save raw data to Azure Blob Storage
        upload_to_blob(team_data, "raw", blob_path)

    except Exception as e:
        print(f"Error processing teams for League: {league_name}: {e}")

