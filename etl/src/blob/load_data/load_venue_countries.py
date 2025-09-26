#!/usr/bin/env python
"""
load_venue_fills.py
────────────────────────────────────────────────────────────────────────────
Read every blob at
    raw/venues/<league_slug>/<season_label>/venues.json
and fill missing `city` or `country` values in the   public.venues   table.

"""

# ──────────────────────────────────────────────────────────────────────
# Imports & helper-module paths
# ──────────────────────────────────────────────────────────────────────
from __future__ import annotations

import os
import sys
import json
import logging
from typing import Set, Iterator, Tuple, Dict, Any

from azure.storage.blob import BlobServiceClient, BlobClient
import psycopg2.extras as pgex

# project helpers – adjust if your repo layout changes
sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])
from credentials  import AZURE_STORAGE            # noqa: E402
from get_db_conn  import get_db_connection        # noqa: E402

# ──────────────────────────────────────────────────────────────────────
# Logging setup
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# suppress noisy Azure SDK HTTP logs
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def iter_venue_blobs(bs: BlobServiceClient) -> Iterator[Tuple[str, str, BlobClient]]:
    """
    Recursively yield every blob that ends with '/venues.json' under
    the *raw* container.

    Yields
    ------
    (league_slug, season_label, BlobClient)
    """
    container = bs.get_container_client("raw")

    # list_blobs is already flat / recursive
    for blob_props in container.list_blobs(name_starts_with="venues/"):
        if blob_props.name.endswith("/venues.json"):
            # Split once: ['venues', 'premier_league', '2025_26', 'venues.json']
            _, league_slug, season_label, _ = blob_props.name.split("/", 3)
            yield league_slug, season_label, container.get_blob_client(blob_props.name)


def extract_venue(js_obj: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Convert a JSON element to the inner venue dict, if recognizable.

    Accepts two shapes:
        A. { "id": 310, "name": "...", "city": "...", "country": "..." }
        B. { "get": "venues", "response": [ { "id": 310, ... } ] }

    Returns
    -------
    dict | None
        Inner venue dict or None if shape is unrecognized.
    """
    if "id" in js_obj:                     # shape A (flat)
        return js_obj
    if js_obj.get("response"):             # shape B (API wrapper)
        return js_obj["response"][0]
    return None


def update_venue_row(cur, v: Dict[str, Any]) -> bool:
    """
    Update one row if city OR country is NULL.

    Returns True only when the row was actually modified.
    """
    vid = v.get("id")
    if vid is None:                  # defensive: id missing
        return False

    cur.execute(
        """
        UPDATE public.venues
           SET venue_name = COALESCE(%s, venue_name),
               city       = COALESCE(%s, city),
               country    = COALESCE(%s, country),
               upd_date   = NOW()
         WHERE venue_id   = %s
           AND (country IS NULL OR city IS NULL)
        """,
        (v.get("name"), v.get("city"), v.get("country"), vid),
    )
    return cur.rowcount > 0          # >0 covers trigger side-effects


# ──────────────────────────────────────────────────────────────────────
# Main routine
# ──────────────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=== Loading venue fill blobs into `venues` table ===")

    # 1. Connections
    blob_service = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential  =AZURE_STORAGE["access_key"],
    )
    conn = get_db_connection()
    cur  = conn.cursor()

    grand_updates: int = 0           # across all blobs for this run
    seen_ids: Set[int] = set()       # avoid duplicate updates within one run

    # 2. Iterate over each venues.json blob
    for league, season, bclient in iter_venue_blobs(blob_service):
        log.info("Blob ➜ %s %s", league, season)

        # 2a. Download & parse JSON
        try:
            data_raw = bclient.download_blob().readall()
            data = json.loads(data_raw)
            if not isinstance(data, list):
                log.warning("[%s %s] JSON root not list → skip", league, season)
                continue
        except Exception as err:
            log.error("[%s %s] blob error → %s", league, season, err)
            continue

        updates_this_blob = 0

        # 2b. Iterate each element in the array
        for elem in data:
            venue = extract_venue(elem)
            if not venue:
                log.warning("[%s %s] unknown element shape → skip element", league, season)
                continue

            vid = venue.get("id")
            if vid in seen_ids:
                continue                     # skip duplicates
            seen_ids.add(vid)

            if update_venue_row(cur, venue):
                updates_this_blob += 1
                grand_updates     += 1

        # 2c. Commit & log per-blob stats
        if updates_this_blob:
            conn.commit()
            log.info("[%s %s] %d venues updated", league, season, updates_this_blob)
        else:
            conn.rollback()  # nothing changed – keep txn clean

    # 3. Grand summary
    log.info("-------------------------------")
    log.info("load_venue_fills summary")
    log.info("venues_updated : %d", grand_updates)

    # 4. Cleanup
    cur.close()
    conn.close()
    log.info("=== Done ===")


# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
