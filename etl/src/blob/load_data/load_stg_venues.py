#!/usr/bin/env python
"""
Stage venues from teams blobs (latest season only).
"""

import os, sys, json
from azure.storage.blob import BlobServiceClient

sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])

from credentials  import AZURE_STORAGE
from get_db_conn  import get_db_connection


# ── helpers ───────────────────────────────────────────────────────────────
def fetch_json(blob_service: BlobServiceClient, key: str) -> dict:
    blob = blob_service.get_blob_client("raw", key)
    return json.loads(blob.download_blob().readall())


def upsert_venue(cur, v: dict) -> int:
    sql = """
        INSERT INTO stg_venues (
            venue_id, venue_name, city, country, capacity
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (venue_id) DO NOTHING;
    """
    cur.execute(sql, (
        v.get("id"),
        v.get("name"),
        v.get("city"),
        v.get("country"),
        v.get("capacity"),
    ))
    return cur.rowcount


def insert_from_payload(cur, payload: dict) -> int:
    n = 0
    for item in payload.get("response", []):
        v = item.get("venue") or {}
        if v.get("id"):
            n += upsert_venue(cur, v)
    return n


# ── main ───────────────────────────────────────────────────────────────────
def main() -> None:
    conn = get_db_connection()
    cur  = conn.cursor()

    cur.execute("TRUNCATE stg_venues;")

    # latest-season-per-league CTE + venues_load_date predicate
    cur.execute("""
        WITH latest AS (
            SELECT league_id, MAX(season_year) AS season_year
            FROM   team_ingest_status
            GROUP  BY league_id
        )
        SELECT tis.blob_path, tis.league_id, tis.season_year
        FROM   team_ingest_status tis
        JOIN   latest l
          ON   l.league_id   = tis.league_id
         AND   l.season_year = tis.season_year
        WHERE  tis.venues_load_date IS NULL
           OR  tis.venues_load_date < tis.upd_date
        ORDER  BY tis.league_id, tis.season_year;
    """)
    todo = cur.fetchall()

    if not todo:
        print("load_stg_venues: nothing to stage.")
        cur.close(); conn.close(); return

    print(f"load_stg_venues: {len(todo)} blob(s) will be staged.")

    blob_service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )

    total_rows = 0

    for blob_path, league_id, season_year in todo:
        try:
            payload   = fetch_json(blob_service, blob_path)
            new_rows  = insert_from_payload(cur, payload)
            total_rows += new_rows

            # stamp via SQL NOW() – avoids tz confusion
            cur.execute("""
                UPDATE team_ingest_status
                   SET venues_load_date = NOW()
                 WHERE league_id = %s
                   AND season_year = %s;
            """, (league_id, season_year))

            conn.commit()
            print(f"{blob_path}: +{new_rows} venues")

        except Exception as err:
            conn.rollback()
            print(f"{blob_path}: ERROR → {err}")

    cur.close(); conn.close()
    print(f"load_stg_venues COMPLETE — {total_rows} new venue rows staged.")


if __name__ == "__main__":
    main()
