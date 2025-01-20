import os
import sys
import json
from psycopg2 import sql
from azure.storage.blob import BlobServiceClient

# Add `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection  # Reuse modular connection logic


from credentials import AZURE_STORAGE


def fetch_transformed_data(container_name, blob_path):
    """
    Fetches transformed data from Azure Blob Storage.
    Args:
        container_name (str): The name of the Azure Blob Storage container.
        blob_path (str): The path to the blob in the container.
    Returns:
        list[dict]: Parsed JSON data from the blob.
    """
    try:
        account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE["access_key"])
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data)
    except Exception as e:
        print(f"Error fetching transformed data from Blob Storage at {blob_path}: {e}")
        raise


def insert_or_update_fixtures(data):
    """
    Inserts or updates fixtures in the database.
    Args:
        data (list[dict]): List of transformed fixture records.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Prepare insertion query with ON CONFLICT for updates
        query = """
        INSERT INTO fixtures (
            fixture_id, league_id, season, home_team_id, away_team_id,
            date, time, home_score, away_score, status
        )
        VALUES (
            %(fixture_id)s, %(league_id)s, %(season)s, %(home_team_id)s, %(away_team_id)s,
            %(date)s, %(time)s, %(home_score)s, %(away_score)s, %(status)s
        )
        ON CONFLICT (fixture_id) DO UPDATE SET
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            status = EXCLUDED.status;
        """

        for fixture in data:
            cur.execute(query, fixture)

        conn.commit()
        print(f"Processed {len(data)} fixtures successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting or updating fixtures in the database: {e}")
        raise


if __name__ == "__main__":
    # Load league metadata from leagues.json
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config/leagues.json"))
    with open(config_path, "r") as f:
        leagues = json.load(f)

    print("Loading fixtures into the database...")

    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]
        season = league["season"]

        # Transformed data path
        transformed_blob_path = f"fixtures/{league_name.lower().replace(' ', '_')}/{season}_25/fixtures.json"

        print(f"Loading fixtures for League: {league_name} (ID: {league_id}, Season: {season})")

        try:
            # Fetch transformed data from Blob Storage
            transformed_data = fetch_transformed_data("transformed", transformed_blob_path)

            # Insert or update fixtures in the database
            insert_or_update_fixtures(transformed_data)

        except Exception as e:
            print(f"Error loading fixtures for League: {league_name}: {e}")
