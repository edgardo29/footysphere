import os
import sys
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))

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

        json_data = json.dumps(data, indent=4)
        blob_client.upload_blob(json_data, overwrite=True)
        print(f"Data successfully uploaded to {container_name}/{blob_path}")
        return True
    except Exception as e:
        print(f"Error uploading data to Azure Blob Storage: {e}")
        raise


def validate_and_transform(raw_data):
    """
    Validates and transforms raw fixture data into a clean format.
    Args:
        raw_data (dict): Raw data fetched from the API.
    Returns:
        list[dict]: List of transformed fixture records.
        list[str]: List of error messages encountered during processing.
    """
    transformed_data = []
    error_logs = []

    for item in raw_data.get("response", []):
        try:
            # Extract required fields
            fixture = {
                "fixture_id": item["fixture"]["id"],
                "league_id": item["league"]["id"],
                "season": item["league"]["season"],
                "home_team_id": item["teams"]["home"]["id"],
                "away_team_id": item["teams"]["away"]["id"],
                "date": item["fixture"]["date"].split("T")[0],
                "time": item["fixture"]["date"].split("T")[1].split("+")[0],
                "home_score": item["goals"]["home"] if item["goals"]["home"] is not None else 0,
                "away_score": item["goals"]["away"] if item["goals"]["away"] is not None else 0,
                "status": item["fixture"]["status"]["short"]
            }
            transformed_data.append(fixture)
        except KeyError as e:
            # Log missing fields
            error_logs.append(
                f"[{datetime.now()}] ERROR: Missing field {str(e)} in fixture data: {json.dumps(item)}"
            )

    return transformed_data, error_logs


def save_logs_to_blob(error_logs, league_name, season):
    """
    Saves error logs to the `transformed` container in Azure Blob Storage.
    Args:
        error_logs (list[str]): List of error messages to log.
        league_name (str): Name of the league.
        season (int): Season year.
    """
    if not error_logs:
        print("No errors to log.")
        return

    try:
        log_data = "\n".join(error_logs)
        blob_path = f"fixtures/{league_name.lower().replace(' ', '_')}/{season}_25/logs/fixtures_log.txt"
        upload_to_blob(log_data, "transformed", blob_path)
        print(f"Error logs saved to {blob_path}")
    except Exception as e:
        print(f"Error saving logs to Blob Storage: {e}")
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

        # Fetch raw data path
        raw_blob_path = f"fixtures/{league_name.lower().replace(' ', '_')}/{season}_25/fixtures.json"
        transformed_blob_path = f"fixtures/{league_name.lower().replace(' ', '_')}/{season}_25/fixtures.json"

        print(f"Processing fixtures for League: {league_name} (ID: {league_id}, Season: {season})")

        try:
            # Fetch raw data from Blob Storage
            raw_data = fetch_data_from_blob("raw", raw_blob_path)

            # Validate and transform raw data
            transformed_data, error_logs = validate_and_transform(raw_data)

            # Upload transformed data to Blob Storage
            upload_to_blob(transformed_data, "transformed", transformed_blob_path)

            # Save error logs to Blob Storage
            save_logs_to_blob(error_logs, league_name, season)

        except Exception as e:
            print(f"Error processing fixtures for League: {league_name}: {e}")
