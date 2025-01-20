import os
import sys
import time
import requests
import json

from azure.storage.blob import BlobServiceClient

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))


from credentials import API_KEYS, AZURE_STORAGE
from get_db_conn import get_db_connection  # Using your database connection logic

# Configuration
API_KEY = API_KEYS["api_football"]
API_URL = "https://v3.football.api-sports.io/fixtures/events"
blob_service_client = BlobServiceClient(
    account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
    credential=AZURE_STORAGE["access_key"]
)
RAW_CONTAINER = "raw"

RATE_LIMIT = 300  # Max requests per minute
REQUEST_INTERVAL = 60 / RATE_LIMIT  # Time between requests


def get_fixtures_from_db(league_id, season):
    """
    Fetches fixture IDs for a given league and season from the database.
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = """
            SELECT fixture_id
            FROM fixtures
            WHERE league_id = %s AND season = %s AND status = %s
            """
            cursor.execute(query, (league_id, season, 'FT'))
            fixtures = cursor.fetchall()
        conn.close()
        return [row[0] for row in fixtures]  # Extract fixture_id from results
    except Exception as e:
        print(f"Error fetching fixtures: {e}")
        raise


def fetch_match_stats(fixture_id):
    """
    Fetches match stats for a given fixture ID from the API.
    """
    headers = {"x-rapidapi-key": API_KEY}
    params = {"fixture": fixture_id}

    response = requests.get(API_URL, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching stats for fixture {fixture_id}: {response.text}")
        return None


def store_in_blob(blob_client, league_name, season, fixture_id, data):
    """
    Stores the raw JSON data into Azure Blob Storage in a formatted and organized structure.
    """
    blob_path = f"match_stats/{league_name}/{season}/{fixture_id}.json"
    try:
        # Serialize the dictionary to a pretty-printed JSON string
        data_json = json.dumps(data, indent=4)  # Pretty-print with 4 spaces

        # Upload the JSON string
        blob_client.get_blob_client(container=RAW_CONTAINER, blob=blob_path).upload_blob(
            data_json, overwrite=True
        )
        print(f"Stored fixture {fixture_id} stats in {blob_path} (organized)")
    except Exception as e:
        print(f"Error storing fixture {fixture_id} in Blob Storage: {e}")


def process_league(league_id, league_name, season):
    """
    Fetches and stores match stats for all fixtures of a given league and season.
    """
    fixtures = get_fixtures_from_db(league_id, season)

    for count, fixture_id in enumerate(fixtures, start=1):
        data = fetch_match_stats(fixture_id)
        if data:
            store_in_blob(blob_service_client, league_name, season, fixture_id, data)

        # Respect API rate limits
        if count % RATE_LIMIT == 0:
            print(f"Reached {RATE_LIMIT} requests, waiting 60 seconds...")
            time.sleep(60)
        else:
            time.sleep(REQUEST_INTERVAL)


if __name__ == "__main__":
    # Example: Processing Ligue 1 for the 2024 season
    LEAGUE_ID = 61  # Ligue 1
    LEAGUE_NAME = "ligue_1"
    SEASON = "2024"

    print(f"Starting processing for {LEAGUE_NAME}, season {SEASON}...")
    process_league(LEAGUE_ID, LEAGUE_NAME, SEASON)
    print(f"Completed processing for {LEAGUE_NAME}, season {SEASON}.")
