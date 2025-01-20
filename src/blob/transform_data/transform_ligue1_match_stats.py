import os
import sys
import json
import datetime
from azure.storage.blob import BlobServiceClient

# Add the `config` directory to sys.path for credentials import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))

from credentials import AZURE_STORAGE

# Configuration
blob_service_client = BlobServiceClient(
    account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
    credential=AZURE_STORAGE["access_key"]
)
RAW_CONTAINER = "raw"
TRANSFORMED_CONTAINER = "transformed"
LOG_CONTAINER = "logs"

def load_raw_data(blob_client, league_name, season):
    """
    Loads raw data files from Blob Storage.
    """
    prefix = f"match_stats/{league_name}/{season}/"
    blobs = blob_client.get_container_client(RAW_CONTAINER).list_blobs(name_starts_with=prefix)
    return [blob.name for blob in blobs]

def validate_data(data):
    """
    Validates the raw JSON data.
    Returns a tuple: (is_valid, error_message)
    """
    # Check if the "response" field exists and is a list
    if "response" not in data or not isinstance(data["response"], list):
        return False, "Missing or invalid field: response"

    # Ensure each response item has a "team" object with a valid team_id
    for event in data["response"]:
        if "team" not in event or "id" not in event["team"]:
            return False, f"Missing team_id in event: {event}"

    return True, None

def transform_data(data, fixture_id):
    """
    Transforms the raw JSON data into a cleaned format for the transformed container.
    Aggregates stats for each team_id in the fixture.
    """
    team_stats = {}

    for event in data["response"]:
        team_id = event["team"]["id"]
        team_stats.setdefault(team_id, {
            "fixture_id": fixture_id,
            "team_id": team_id,
            "yellow_cards": 0,
            "red_cards": 0,
            "fouls": 0,
            "total_shots": None,  # Placeholder for missing data
            "ball_possession": None,  # Placeholder for missing data
        })

        # Count yellow cards
        if event["type"] == "Card" and event["detail"] == "Yellow Card":
            team_stats[team_id]["yellow_cards"] += 1

        # Count red cards
        if event["type"] == "Card" and event["detail"] == "Red Card":
            team_stats[team_id]["red_cards"] += 1

        # Count fouls
        if event.get("comments") == "Foul":
            team_stats[team_id]["fouls"] += 1

    return list(team_stats.values())

def write_log_to_blob(log_type, league_name, season, log_content):
    """
    Writes log content to the logs container in Azure Blob Storage.
    """
    log_blob_path = f"{log_type}/{league_name}/{season}/{datetime.datetime.now().strftime('%Y-%m-%d')}_log.txt"
    try:
        log_blob_client = blob_service_client.get_blob_client(container=LOG_CONTAINER, blob=log_blob_path)
        log_blob_client.upload_blob(log_content, overwrite=True)
        print(f"Uploaded log to Blob Storage: {log_blob_path}")
    except Exception as e:
        print(f"Error uploading log: {e}")

def process_files(league_name, season):
    """
    Validates, transforms, and stores files from the raw container to the transformed container.
    """
    raw_container_client = blob_service_client.get_container_client(RAW_CONTAINER)
    transformed_container_client = blob_service_client.get_container_client(TRANSFORMED_CONTAINER)

    validation_logs = []
    transformation_logs = []

    # Get all raw files
    raw_files = load_raw_data(blob_service_client, league_name, season)
    print(f"Found {len(raw_files)} raw files to process.")

    for file_name in raw_files:
        try:
            print(f"Processing file: {file_name}")
            # Extract fixture_id from the file name
            fixture_id = file_name.split("/")[-1].replace(".json", "")  # Extract 1213747 from the file path

            # Load raw data
            raw_blob_client = raw_container_client.get_blob_client(file_name)
            raw_blob = raw_blob_client.download_blob().readall()
            data = json.loads(raw_blob)

            # Inject fixture_id into the data
            data["fixture_id"] = int(fixture_id)

            # Validate
            is_valid, error_message = validate_data(data)
            if not is_valid:
                validation_logs.append(f"{file_name}: Validation error - {error_message}")
                print(f"Validation failed for file: {file_name}")
                continue

            print(f"Validation passed for file: {file_name}")

            # Transform
            transformed_data = transform_data(data, fixture_id)
            print(f"Transformed data for file: {file_name}")

            # Save transformed data
            transformed_blob_name = file_name.replace("raw", "transformed")
            transformed_blob_client = transformed_container_client.get_blob_client(transformed_blob_name)
            transformed_blob_client.upload_blob(
                json.dumps(transformed_data, indent=4),
                overwrite=True
            )
            transformation_logs.append(f"{file_name}: Successfully transformed")
            print(f"File successfully transformed and saved: {file_name}")

        except Exception as e:
            validation_logs.append(f"{file_name}: Unexpected error - {str(e)}")
            print(f"Error processing file: {file_name}, {str(e)}")

    # Ensure error log is created even if no errors
    if not validation_logs:
        validation_logs.append("No errors occurred during validation or transformation.")

    # Upload validation logs
    write_log_to_blob("validation", league_name, season, "\n".join(validation_logs))


if __name__ == "__main__":
    LEAGUE_NAME = "ligue_1"
    SEASON = "2024"

    print(f"Starting transformation for {LEAGUE_NAME}, season {SEASON}...")
    process_files(LEAGUE_NAME, SEASON)
    print(f"Transformation completed for {LEAGUE_NAME}, season {SEASON}.")
