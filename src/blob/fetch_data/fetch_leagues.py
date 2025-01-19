import os
import sys
import json
import requests
from azure.storage.blob import BlobServiceClient

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))

from credentials import API_KEYS, AZURE_STORAGE


def fetch_league_data(league_id, season):
    """
    Fetches league data from the API for a specific league and season.
    Args:
        league_id (int): The ID of the league to fetch.
        season (int): The season year (e.g., 2024).
    Returns:
        dict: Parsed JSON response from the API.
    """
    api_url = "https://v3.football.api-sports.io/leagues"
    params = {"id": league_id, "season": season}
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}

    try:
        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching league data for League ID {league_id}, Season {season}: {e}")
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
        # Create BlobServiceClient
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])

        # Get the blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        # Check if the blob already exists
        if blob_client.exists():
            print(f"Blob already exists at {blob_path}. Skipping upload.")
            return False

        # Convert the data to a formatted JSON string
        json_data = json.dumps(data, indent=4)

        # Upload the JSON string
        blob_client.upload_blob(json_data, overwrite=True)
        print(f"Data successfully uploaded to {container_name}/{blob_path}")
        return True
    except Exception as e:
        print(f"Error uploading data to Azure Blob Storage at {blob_path}: {e}")
        raise


if __name__ == "__main__":
    # Load league metadata from leagues.json
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config/leagues.json"))
    with open(config_path, "r") as f:
        leagues = json.load(f)

    print("Processing leagues from leagues.json...")

    # Process each league
    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]
        season = league["season"]

        print(f"Processing League: {league_name} (ID: {league_id}, Season: {season})")

        try:
            # Fetch league data from the API
            league_data = fetch_league_data(league_id, season)

            # Generate dynamic blob path (without duplicating `raw`)
            blob_path = f"leagues/{league_name.lower().replace(' ', '_')}/{season}_25/league.json"

            # Upload to Azure Blob Storage
            upload_to_blob(league_data, "raw", blob_path)

        except Exception as e:
            print(f"Error processing League: {league_name} (ID: {league_id}): {e}")
