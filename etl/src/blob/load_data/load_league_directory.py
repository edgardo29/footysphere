#!/usr/bin/env python
"""
Load raw league list from blob into league_directory.
Usage:
  python load_league_directory.py --season 2024
"""

import os
import sys
import json
import argparse
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# helper paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")))
from credentials import AZURE_STORAGE
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection


def download_blob(season: int) -> dict:
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_svc   = BlobServiceClient(account_url=account_url,
                                   credential=AZURE_STORAGE["access_key"])
    blob_path  = f"league_directory/{season}/leagues.json"
    blob_client = blob_svc.get_blob_client(container="raw", blob=blob_path)
    content = blob_client.download_blob().readall()
    return json.loads(content)


def upsert_directory(data: dict) -> None:
    rows = []
    for entry in data["response"]:
        league = entry["league"]
        country = entry.get("country", {})
        seasons = [s["year"] for s in entry.get("seasons", [])]

        rows.append(
            (
                league["id"],
                league["name"],
                country.get("name"),
                league.get("logo"),
                json.dumps(seasons),
            )
        )

    sql = """
        INSERT INTO league_directory
            (league_id, league_name, country, logo_url, seasons, load_date, upd_date)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (league_id) DO UPDATE
           SET league_name = EXCLUDED.league_name,
               country     = EXCLUDED.country,
               logo_url    = EXCLUDED.logo_url,
               seasons     = EXCLUDED.seasons,
               upd_date    = NOW();
    """

    conn = get_db_connection()
    cur  = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✓ Upserted {len(rows)} rows into league_directory")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()

    payload = download_blob(args.season)
    upsert_directory(payload)
