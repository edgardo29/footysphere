import os
import sys
import json
import requests
from azure.storage.blob import BlobServiceClient

# Add `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))

from credentials import API_KEYS, AZURE_STORAGE


def fetch_fixtures_from_api(league_id, season):
    """
    Fetches fixtures for a specific league and season from the API.
    Args:
        league_id (int): The ID of the league.
        season (int): The season year.
    Returns:
        dict: The API response containing fixture data.
    """
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"league": league_id, "season": season}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching fixtures for League ID {league_id}, Season {season}: {e}")
        raise


def check_blob_exists(container_name, blob_path):
    """
    Checks if a blob already exists in Azure Blob Storage.
    Args:
        container_name (str): The name of the Azure Blob Storage container.
        blob_path (str): The path to the blob in the container.
    Returns:
        bool: True if the blob exists, False otherwise.
    """
    try:
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        return blob_client.exists()
    except Exception as e:
        print(f"Error checking blob existence: {e}")
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

        # Convert the data to a formatted JSON string
        json_data = json.dumps(data, indent=4)

        # Upload the JSON string
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

    print("Processing fixtures for each league...")

    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]
        season = league["season"]

        # Generate dynamic blob path
        blob_path = f"fixtures/{league_name.lower().replace(' ', '_')}/{season}_25/fixtures.json"

        print(f"Processing fixtures for League: {league_name} (ID: {league_id}, Season: {season})")

        try:
            # Check if the blob already exists
            if check_blob_exists("raw", blob_path):
                print(f"Blob already exists at {blob_path}. Skipping fetch and upload.")
                continue

            # Fetch fixtures from API
            fixture_data = fetch_fixtures_from_api(league_id, season)

            # Save raw data to Azure Blob Storage
            upload_to_blob(fixture_data, "raw", blob_path)

        except Exception as e:
            print(f"Error processing fixtures for League: {league_name}: {e}")
