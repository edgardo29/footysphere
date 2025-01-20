import json
import os
import sys
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
from credentials import AZURE_STORAGE, API_KEYS

def calculate_date_range():
    """Calculate the previous full week's Monday to Sunday."""
    today = datetime.now(timezone.utc).date()
    # Find the most recent Sunday
    last_sunday = today - timedelta(days=today.weekday() + 1)
    # Calculate the Monday for the same week
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday

def fetch_all_fixtures(api_key, league_id, season):
    """Fetch all fixtures for a league and season."""
    url = f"https://v3.football.api-sports.io/fixtures"
    headers = {
        "x-api-key": api_key,
    }
    params = {
        "league": league_id,
        "season": season,
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def filter_fixtures_by_date(fixtures, start_date, end_date):
    """Filter fixtures to include only those within the specified date range."""
    filtered = []
    for fixture in fixtures.get("response", []):
        fixture_date = datetime.fromisoformat(fixture["fixture"]["date"].replace("Z", "+00:00")).date()
        if start_date <= fixture_date <= end_date:
            filtered.append(fixture)
    return filtered

def upload_to_blob_storage(container_name, blob_path, data):
    """Upload data to Azure Blob Storage."""
    blob_service_client = BlobServiceClient(
        f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        AZURE_STORAGE['access_key']
    )
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    print(f"Uploaded to blob: {blob_path}")

def main():
    # Define the league details
    league = {"id": 39, "name": "Premier League", "season": 2024}

    # Calculate the date range for the previous week
    from_date, to_date = calculate_date_range()
    print(f"Fetching fixtures from {from_date} to {to_date} for {league['name']}...")

    # Fetch all fixtures for the league and season
    try:
        fixtures = fetch_all_fixtures(API_KEYS["api_football"], league["id"], league["season"])
        # Filter fixtures for the specific date range
        filtered_fixtures = filter_fixtures_by_date(fixtures, from_date, to_date)

        if filtered_fixtures:
            # Define blob storage path
            blob_path = f"fixtures/{league['name'].replace(' ', '_')}/{league['season']}/week_{from_date}_to_{to_date}.json"

            # Upload filtered fixtures to Blob Storage
            upload_to_blob_storage("raw", blob_path, filtered_fixtures)
        else:
            print(f"No fixtures found for the week {from_date} to {to_date}.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {league['name']}: {e}")

if __name__ == "__main__":
    main()
