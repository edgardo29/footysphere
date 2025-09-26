#!/usr/bin/env python
"""
Fetch every league for one season and upload the payload to Azure Blob Storage
  • Container:  raw
  • Path:       league_directory/<season>/leagues.json
Usage:
  python fetch_league_directory.py --season 2024
"""

import os
import sys
import json
import argparse
import requests
from azure.storage.blob import BlobServiceClient

# config + DB helper paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
from credentials import API_KEYS, AZURE_STORAGE

def fetch_leagues(season: int) -> dict:
    url = "https://v3.football.api-sports.io/leagues"
    params = {"season": season}
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def upload_to_blob(data: dict, season: int) -> None:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_svc   = BlobServiceClient(account_url=account_url,
                                   credential=AZURE_STORAGE["access_key"])
    blob_path  = f"league_directory/{season}/leagues.json"
    blob_client = blob_svc.get_blob_client(container="raw", blob=blob_path)

    # overwrite is fine – directory is refreshed each run
    blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)
    print(f"✓ Uploaded discovery file → raw/{blob_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True,
                        help="Season year to query (e.g. 2024)")
    args = parser.parse_args()

    payload = fetch_leagues(args.season)
    upload_to_blob(payload, args.season)
