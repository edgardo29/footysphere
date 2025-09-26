#!/usr/bin/env python
"""
load_teams_to_stg.py
────────────────────────────────────────────────────────────────────────────
PURPOSE
    • Read only those team-JSON blobs that have never been staged
      (load_date IS NULL) or were refreshed after the last stage
      (load_date < upd_date) as recorded in team_ingest_status.
    • INSERT their rows into stg_teams, tagging each row with
      league_id and season_year for later QA / merge steps.
    • Stamp load_date so the same blob is ignored next run.

    **does NOT** TRUNCATE stg_teams.  Those steps happen in their
    own scripts / DAG tasks.

RUN
    source footysphere_venv/bin/activate
    python src/blob/load_data/load_teams_to_stg.py
"""

# ───────────── Imports & helper-path setup ────────────────────────────────
import os, sys, json
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# add project helper dirs to PYTHONPATH
sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])

from credentials  import AZURE_STORAGE           # secrets (account + key)
from get_db_conn  import get_db_connection       # returns psycopg2 connection

# ─────────────── Blob download helper ─────────────────────────────────────
def fetch_json_from_blob(blob_service: BlobServiceClient,
                         container: str,
                         key: str) -> dict:
    """Download and parse a blob into a Python dict."""
    blob = blob_service.get_blob_client(container, key)
    payload = blob.download_blob().readall()
    return json.loads(payload)

# ─────────────── Staging insert helper ────────────────────────────────────
def stage_teams(cur, json_payload, league_id, season_year):
    """Insert each team row into stg_teams (no duplicates check here)."""
    insert_sql = """
        INSERT INTO stg_teams (
            team_id, team_name, team_country, team_logo_url,
            venue_id, league_id, season_year
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    for item in json_payload.get("response", []):
        team = item.get("team",  {})
        ven  = item.get("venue", {})
        cur.execute(insert_sql, (
            team.get("id"),
            team.get("name"),
            team.get("country"),
            team.get("logo"),
            ven.get("id"),          # may be None
            league_id,
            season_year,
        ))

# ─────────────── Main loader logic ───────────────────────────────────────
def main() -> None:
    # 1) Connect to Postgres & Azure Blob
    conn = get_db_connection()
    cur  = conn.cursor()

    blob_service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )

    # 2) Find blobs that still need staging (based on load_date / upd_date)
    # 2) Find blobs that still need staging – *newest season only*
    cur.execute("""
        WITH latest AS (                             -- one row per league
            SELECT league_id, MAX(season_year) AS max_year
            FROM   team_ingest_status
            GROUP  BY league_id
        )
        SELECT s.blob_path,
               s.league_id,
               s.season_year
        FROM   team_ingest_status s
        JOIN   latest l USING (league_id)
        WHERE  s.season_year = l.max_year            -- keep ONLY latest
          AND (s.teams_load_date IS NULL             -- never staged
               OR s.teams_load_date < s.upd_date)    -- or blob was refreshed
        ORDER  BY s.league_id, s.season_year;
    """)

    todo = cur.fetchall()

    if not todo:
        print("load_teams_to_stg: nothing new to stage.")
        cur.close(); conn.close(); return

    print(f"load_teams_to_stg: {len(todo)} blob(s) will be staged.")

    staged = 0  # for summary

    # 3) Loop through each blob and insert into stg_teams
    for blob_path, league_id, season_year in todo:
        try:
            payload = fetch_json_from_blob(blob_service, "raw", blob_path)
            stage_teams(cur, payload, league_id, season_year)

            # mark blob as staged
            cur.execute("""
                UPDATE team_ingest_status
                   SET teams_load_date = NOW()
                 WHERE league_id = %s
                   AND season_year = %s;
            """, (league_id, season_year))

            conn.commit()
            staged += 1
            print(f"{blob_path}: rows inserted into stg_teams.")

        except Exception as err:
            conn.rollback()
            print(f"{blob_path}: ERROR → {err}")

    # 4) wrap-up
    cur.close(); conn.close()
    print(f"load_teams_to_stg COMPLETE — {staged} blob(s) staged this run.")

# ─────────────── Entrypoint guard ─────────────────────────────────────────
if __name__ == "__main__":
    main()
