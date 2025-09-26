#!/usr/bin/env python
"""
fetch_match_statistics.py
──────────────────────────────────────────────────────────────────────────────
Pulls per-fixture statistics from API-Football and stores each response as a
pretty-printed JSON blob in Azure Blob Storage.

It supports **two modes** controlled by --mode:

  1.  initial      → one-time, full-season back-fill
  2.  incremental  → rolling window (e.g. last 7 days) for Airflow runs

Directory layout in Azure Blob
──────────────────────────────
raw/
 └─ match_statistics/
     ├─ initial/
     │    └─ 2024_25/statistics_<fixture>.json
     └─ incremental/
          └─ 2025-06-17/statistics_<fixture>.json  ← one folder per run-date
"""

# ────────────────────────────────────────────────────────────────────────────
#  Standard-lib / third-party imports
# ────────────────────────────────────────────────────────────────────────────
import os
import sys
import json
import argparse
import datetime as dt
import logging
from typing import List

import requests
from azure.storage.blob import BlobServiceClient

# ────────────────────────────────────────────────────────────────────────────
#  Local helper modules  (paths already exist in your repo)
# ────────────────────────────────────────────────────────────────────────────
#   credentials.py   →   API keys + Azure Storage account/key
#   get_db_conn.py   →   returns a psycopg2 connection to Postgres
# ---------------------------------------------------------------------------

# Append the two known relative paths so Python can import them
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config"))
)
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts"))
)

from credentials import API_KEYS, AZURE_STORAGE  # noqa: E402  (import after sys.path edits)
from get_db_conn import get_db_connection        # noqa: E402

# ────────────────────────────────────────────────────────────────────────────
#  Logging — plain stdout with timestamps so Airflow captures it automatically
# ────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ↓ ADD THIS — silences Azure request/response chatter
for noisy in (
    "azure.storage",                         # storage-blob internals
    "azure.core.pipeline.policies.http_logging_policy",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)


# ════════════════════════════════════════════════════════════════════════════
#  1. API helper
# ════════════════════════════════════════════════════════════════════════════
def fetch_stats_api(fixture_id: str) -> dict:
    """
    One network call to /fixtures/statistics
    Raises HTTPError automatically if the API returns a non-200.
    """
    base_url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    params = {"fixture": fixture_id}

    resp = requests.get(base_url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ════════════════════════════════════════════════════════════════════════════
#  2. Azure Blob helper
# ════════════════════════════════════════════════════════════════════════════
def upload_fixture_stats(data: dict, *, container: str, blob_path: str) -> None:
    """
    Writes one JSON document to Azure Blob Storage **iff** it is not already
    present, so the script can be safely re-run without clobbering files.
    """
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    blob_service = BlobServiceClient(account_url=account_url,
                                     credential=AZURE_STORAGE["access_key"])
    blob = blob_service.get_blob_client(container=container, blob=blob_path)

    # Check-before-insert keeps things idempotent
    if blob.exists():
        return

    blob.upload_blob(json.dumps(data, indent=4), overwrite=False)


# ════════════════════════════════════════════════════════════════════════════
#  3. SQL helpers to fetch the list of fixture_id values
# ════════════════════════════════════════════════════════════════════════════
def _fetchall(sql: str, params: tuple) -> List[str]:
    """
    Executes a simple SELECT and returns the first column of all rows
    as a list[str].  Re-used by the two helper functions below so we don’t
    repeat cursor boiler-plate.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return [str(r[0]) for r in cur.fetchall()]


def fixture_ids_for_season(season: int) -> List[str]:
    """
    All fixture_id values for one season-year.
    Pulls only finished games (`status = 'FT'`) so stats are complete.
    """
    return _fetchall(
        """
        SELECT fixture_id
          FROM fixtures
         WHERE season_year = %s
           AND status = 'FT'          -- remove this line if you want every status
        """,
        (season,),
    )


def fixture_ids_by_date_window(start_date: dt.date,
                               end_date: dt.date) -> List[str]:
    """
    All fixture_id values whose fixture_date is >= start_date and < end_date.
    Also limited to finished matches.
    """
    return _fetchall(
        """
        SELECT fixture_id
          FROM fixtures
         WHERE fixture_date >= %s
           AND fixture_date <  %s
           AND status = 'FT'          -- remove if you want LIVE/NS too
        """,
        (start_date, end_date),
    )



# ════════════════════════════════════════════════════════════════════════════
#  4. Main script entry-point
# ════════════════════════════════════════════════════════════════════════════
def main() -> None:
  # ── 4.1 Command-line arguments ────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description="Fetch API-Football match statistics and upload to Azure."
    )
    parser.add_argument(
        "--mode",
        choices=["initial", "incremental"],
        default="incremental",
        help="initial = full season  •  incremental = rolling window",
    )
    parser.add_argument(
        "--season",
        type=int,
        required=True,                     # ← season is now mandatory
        help="Season start-year, e.g. 2024 → folders under 2024_25",
    )

    parser.add_argument(
        "--lookback-days",
        type=int,
        default=8,
        help="How many days back from today for incremental mode (ignored in initial).",
    )
    parser.add_argument(
        "--start-date",
        help="YYYY-MM-DD (optional explicit window start for incremental mode)",
    )
    parser.add_argument(
        "--end-date",
        help="YYYY-MM-DD (optional explicit window *end*, exclusive, for incremental)",
    )
    args = parser.parse_args()

    # ── 4.2 Derive fixture list + Blob folder name ────────────────────────
    # We now *always* require --season so every file lives under
    #   raw/match_statistics/<season_str>/...
    if args.season is None:
        parser.error("--season is required in both modes")

    season_str = f"{args.season}_{(args.season % 100) + 1}"

    if args.mode == "initial":
        # —— full-season back-fill ————————————————————————————————
        fixture_ids = fixture_ids_for_season(args.season)
        blob_prefix = f"match_statistics/{season_str}/initial"

        log.info(
            "INITIAL load  •  season=%s  •  fixtures=%d",
            season_str,
            len(fixture_ids),
        )
    else:
        # —— rolling window for weekly/daily jobs ————————————————
        today = dt.date.today()

    # ---------- build rolling window ---------------------------------
    if args.start_date:
        start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = today - dt.timedelta(days=args.lookback_days)

    if args.end_date:
        end_date = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = today          # stop at 23:59 of *today*


        fixture_ids = fixture_ids_by_date_window(start_date, end_date)
        # Files land in:  …/<season>/incremental/<run-date>/…
        blob_prefix = (
            f"match_statistics/{season_str}/incremental/{today:%Y-%m-%d}"
        )

        log.info(
            "INCREMENTAL load  •  season=%s  •  window=[%s, %s)  •  fixtures=%d",
            season_str,
            start_date,
            end_date,
            len(fixture_ids),
        )

    if not fixture_ids:
        log.warning("No fixtures found → nothing to do (exit 0)")
        return


    # ── 4.3 Loop over fixtures, download JSON, upload to Blob ─────────────
    container_name = "raw"
    success = failed = 0

    for i, fid in enumerate(fixture_ids, 1):
        try:
            stats_json = fetch_stats_api(fid)
            upload_fixture_stats(
                stats_json,
                container=container_name,
                blob_path=f"{blob_prefix}/statistics_{fid}.json",
            )
            success += 1

            # Light progress log every 100 files
            if success % 100 == 0:
                log.info("Uploaded %d / %d JSON files …", success, len(fixture_ids))

        except Exception as exc:  # noqa: BLE001  (keep broad, we only count)
            failed += 1
            log.error("Fixture %s → %s", fid, exc)

    # ── 4.4 Human-readable summary for Airflow log tail ───────────────────
    print(
        "\n======== FETCH MATCH STATISTICS — SUMMARY ========\n"
        f"Mode          : {args.mode}\n"
        f"Total fixtures: {len(fixture_ids)}\n"
        f"Successfully  : {success}\n"
        f"Failed        : {failed}\n"
        "==================================================\n"
    )


if __name__ == "__main__":
    main()
