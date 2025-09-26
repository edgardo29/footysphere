#!/usr/bin/env python
"""
fetch_teams.py
────────────────────────────────────────────────────────────────────────────
Pull any league-season that appears in `league_seasons` but has **no row**
yet in `team_ingest_status`.  For each missing pair the script either

  • uploads a brand-new JSON blob  → prints “…:   fetched (N teams)”
  • or, if the blob already exists, inserts the status row only
                                     → prints “…:   blob present”

A concise summary prints at the end, showing where each kind of change went.
Nothing is printed for league-seasons that were already up-to-date.
"""

# ───────────────────────── Imports & path setup ───────────────────────────
import os, sys, json, time, requests
sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])

from credentials        import API_KEYS, AZURE_STORAGE   # secrets module (kept out of repo)
from get_db_conn        import get_db_connection         # returns psycopg2 or similar conn
from azure.storage.blob import BlobServiceClient         # Azure Python SDK

# ──────────────────────────── Constants ───────────────────────────────────
API_URL        = "https://v3.football.api-sports.io/teams"
RATE_DELAY_SEC = 1.2    # 50 requests/minute ≪ 300-req/min limit

# ───────────────────────── Helper functions ───────────────────────────────
def slugify(name: str) -> str:
    """Fallback: turn 'Major League Soccer' → 'major_league_soccer'."""
    return "_".join(name.lower().split())

def blob_exists(blob_service: BlobServiceClient, key: str) -> bool:
    """Cheap HEAD request so we don’t download the whole blob."""
    return blob_service.get_blob_client("raw", key).exists()

def fetch_from_api(league_id: int, season_year: int) -> dict:
    """Call API-Football and return the JSON body (raises on non-200)."""
    hdrs = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"league": league_id, "season": season_year}
    resp = requests.get(API_URL, headers=hdrs, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def upload_json(blob_service: BlobServiceClient, key: str, payload: dict) -> None:
    """Pretty-print and upload JSON to Azure Blob."""
    blob_service.get_blob_client("raw", key).upload_blob(
        json.dumps(payload, indent=4), overwrite=True
    )

def upsert_receipt(cur, lid: int, season: int, key: str, rows: int) -> None:
    """
    Insert (or update) one row in team_ingest_status.

    • INSERT relies on DEFAULTs to populate load_date / upd_date.
    • UPDATE bumps upd_date so you can see when we last touched the row.
    """
    cur.execute(
        """
        INSERT INTO team_ingest_status
              (league_id, season_year, blob_path, row_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (league_id, season_year) DO UPDATE
        SET blob_path = EXCLUDED.blob_path,
            row_count = EXCLUDED.row_count,
            upd_date  = NOW();
        """,
        (lid, season, key, rows),
    )

def discover_missing(cur):
    """
    Return list[dict] of league‑seasons that are in league_seasons but NOT yet
    in team_ingest_status.

    IMPORTANT:
    • Use the canonical alias from leagues.folder_alias AS-IS (no extra _<id>).
    • If folder_alias is NULL (edge case), fall back to slug + _<league_id>
      to preserve uniqueness and keep pathing consistent.
    """
    cur.execute(
        """
        SELECT l.league_id,
               l.league_name,
               l.folder_alias,
               COALESCE(ls.season_str, ls.season_year::text) AS label,
               ls.season_year
        FROM   league_seasons      ls
        JOIN   leagues             l  USING (league_id)
        LEFT   JOIN team_ingest_status t
               ON  t.league_id   = ls.league_id
               AND t.season_year = ls.season_year
        WHERE  t.league_id IS NULL
        ORDER  BY l.league_id, ls.season_year;
        """
    )

    cols  = [c.name for c in cur.description]
    rows  = [dict(zip(cols, r)) for r in cur.fetchall()]

    # post‑process rows into the structure the rest of the script expects
    todo = []
    for r in rows:
        alias = r["folder_alias"] or f"{slugify(r['league_name'])}_{r['league_id']}"
        todo.append({
            "league_id"   : r["league_id"],
            "season_year" : r["season_year"],
            "alias"       : alias,          # ← use canonical alias; no extra suffix
            "label"       : r["label"],
        })
    return todo


# ───────────────────────────── Main routine ───────────────────────────────
def main() -> None:
    conn = get_db_connection()
    cur  = conn.cursor()

    # Azure Blob client (single session for speed)
    blob_service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )

    # Build work list once at start
    todo = discover_missing(cur)
    if not todo:
        cur.close(); conn.close()
        return  # everything already ingested

    uploaded_blobs = 0
    status_rows_written = 0

    for job in todo:
        lid, season = job["league_id"], job["season_year"]
        alias, label = job["alias"], job["label"]

        # ── ensure label is “YYYY_YY” when season_str is NULL ────────────
        if "_" not in label:                # e.g. label came in as "2024"
            label = f"{season}_{str(season + 1)[-2:]}"   # → "2024_25"

        blob_key = f"teams/{alias}/{label}/teams.json"

        try:
            if blob_exists(blob_service, blob_key):
                # Blob is already present — just backfill the missing status row
                raw = blob_service.get_blob_client("raw", blob_key).download_blob().readall()
                row_count = len(json.loads(raw).get("response", []))
                upsert_receipt(cur, lid, season, blob_key, row_count)
                conn.commit()
                status_rows_written += 1
                print(f"{alias}/{label}:   blob present")
            else:
                # Need to hit the API, upload, write status row
                payload = fetch_from_api(lid, season)
                upload_json(blob_service, blob_key, payload)
                row_count = len(payload.get("response", []))
                upsert_receipt(cur, lid, season, blob_key, row_count)
                conn.commit()
                uploaded_blobs += 1
                status_rows_written += 1
                print(f"{alias}/{label}:   fetched ({row_count} teams)")

            time.sleep(RATE_DELAY_SEC)  # keep under the API rate limit

        except Exception as err:
            conn.rollback()             # leave DB consistent, move on
            print(f"{alias}/{label}:   ERROR → {err}")

    # ────────────── summary block ──────────────
    print("\n--- summary ---")
    print(f"uploaded blobs       : {uploaded_blobs}   -> raw container")
    print(f"status rows written  : {status_rows_written}   -> team_ingest_status")

    cur.close()
    conn.close()

# ───────────────────────── Entrypoint guard ──────────────────────────────
if __name__ == "__main__":
    main()
