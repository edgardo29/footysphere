#!/usr/bin/env python3
"""
fetch_news.py
───────────────────────────────────────────────────────────────────────────────
Fetch Guardian football news for three query shapes (transfers, injuries, match
reports) and upload raw artifacts to Azure Blob Storage.

Layout (per run; shared UTC second-stamp across tabs):
raw/news/guardian/
  search_football_transfers/     run_ts=YYYY-MM-DDTHH-MM-SS/{request.txt,params.json,response.json,_meta.json}
  search_football_injuries/      run_ts=YYYY-MM-DDTHH-MM-SS/{...}
  search_football_matchreports/  run_ts=YYYY-MM-DDTHH-MM-SS/{...}

Fixed params for ALL tabs:
  page-size=10, order-by=newest, show-fields=thumbnail,trailText,shortUrl,byline

Tabs (FOOTBALL-ONLY):

  transfers →
    section=football
    tag=football/football,tone/news,type/article,-tone/minutebyminute,-type/liveblog,-tone/comment,-tone/analysis,-tone/matchreports
    q=(transfer terms) AND NOT (injury terms)
    query-fields=headline

  injuries  →
    section=football
    tag=football/football,tone/news,type/article,-tone/minutebyminute,-type/liveblog,-tone/comment,-tone/analysis,-tone/matchreports
    q=(injury terms) AND NOT (noise terms incl. transfer/loan/etc.)
    query-fields=headline

  matchreports →
    section=football
    tag=football/football,tone/matchreports,type/article

Secrets (NEVER written to blob):
  from etl/config/credentials.py → NEWS_API_KEYS["api_news"], AZURE_STORAGE

Run:
  python etl/src/blob/fetch_data/fetch_news.py
"""

import os
import sys
import json
import time
import datetime as dt
from urllib.parse import urlencode

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings

# ── credentials import ────────────────────────────────────────────────────────
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.append(os.path.join(BASE, "config"))
from credentials import NEWS_API_KEYS, AZURE_STORAGE  # type: ignore

# ── constants ─────────────────────────────────────────────────────────────────
RAW_CONTAINER = "raw"
OUT_PREFIX    = "news/guardian"
BASE_URL      = "https://content.guardianapis.com/search"
GUARDIAN_KEY  = NEWS_API_KEYS["api_news"]

COMMON = {
    "order-by": "newest",
    "show-fields": "thumbnail,trailText,shortUrl,byline",
    "page-size": 10,
}

# FOOTBALL-ONLY queries (finalized)
TABS = [
    # Transfers: football news articles, exclude non-news/analysis/liveblogs/match reports
    ("transfers", "search_football_transfers", {
        "section": "football",
        "tag": "football/football,tone/news,type/article,-tone/minutebyminute,-type/liveblog,-tone/comment,-tone/analysis,-tone/matchreports",
        "q": "("
              "transfer OR transfers OR loan OR loans OR \"on loan\" OR swap OR swaps OR fee OR fees "
              "OR deal OR deals OR sign OR signs OR signed OR signing OR \"medical\" OR \"clause\" OR \"contract\""
             ") AND NOT ("
              "injury OR injuries OR injured OR \"ruled out\" OR \"out for\" OR hamstring OR groin OR ankle OR knee "
              "OR ACL OR MCL OR Achilles OR concussion OR fracture OR fractured OR broken OR meniscus OR calf OR thigh "
              "OR sprain OR tear"
             ")",
        "query-fields": "headline",
    }),

    # Injuries: football news articles, exclude non-news/analysis/liveblogs/match reports + transfer/loan noise
    ("injuries", "search_football_injuries", {
        "section": "football",
        "tag": "football/football,tone/news,type/article,-tone/minutebyminute,-type/liveblog,-tone/comment,-tone/analysis,-tone/matchreports",
        "q": "("
              "injury OR injuries OR injured OR \"ruled out\" OR \"out for\" OR hamstring OR groin OR ankle OR knee "
              "OR ACL OR MCL OR Achilles OR concussion OR fracture OR fractured OR broken OR meniscus OR calf OR thigh "
              "OR sprain OR tear"
             ") AND NOT ("
              "crisis OR preview OR legal OR court OR owner OR lawsuit OR case OR dies OR death OR obituary OR riot OR police "
              "OR derby OR fan OR loan OR transfer OR swap OR fee OR deal OR signs OR signed"
             ")",
        "query-fields": "headline",
    }),

    # Match reports: pure match report tone
    ("matchreports", "search_football_matchreports", {
        "section": "football",
        "tag": "football/football,tone/matchreports,type/article",
        # No q / query-fields needed; we want all match reports
    }),
]

SLEEP_SEC   = 1.0
TIMEOUT_SEC = 10


def now_stamp_sec() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H-%M-%S")


def blob_service() -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )


def upload_text(bs: BlobServiceClient, blob_path: str, text: str):
    bs.get_blob_client(container=RAW_CONTAINER, blob=blob_path).upload_blob(
        text,
        overwrite=False,
        content_settings=ContentSettings(content_type="text/plain; charset=utf-8"),
    )
    print(f"stored    {RAW_CONTAINER}/{blob_path}")


def upload_json(bs: BlobServiceClient, blob_path: str, obj: dict):
    bs.get_blob_client(container=RAW_CONTAINER, blob=blob_path).upload_blob(
        json.dumps(obj, ensure_ascii=False, indent=2),
        overwrite=False,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"stored    {RAW_CONTAINER}/{blob_path}")


def main() -> int:
    run_ts = now_stamp_sec()
    print(f"[fetch_news] run_ts={run_ts}")

    bs = blob_service()
    failures: list[tuple[str, str]] = []

    for i, (tab, dir_name, extra) in enumerate(TABS):
        base_dir = f"{OUT_PREFIX}/{dir_name}/run_ts={run_ts}"

        # Real vs sanitized params/URL (never write the key)
        params = dict(COMMON); params.update(extra); params["api-key"] = GUARDIAN_KEY
        safe   = dict(COMMON); safe.update(extra);   safe["api-key"]   = "REDACTED"
        url_safe = f"{BASE_URL}?{urlencode(safe)}"

        t0 = time.time()
        try:
            r = requests.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)
            status = r.status_code
            r.raise_for_status()
            payload = r.json()
            dur_ms = round((time.time() - t0) * 1000, 1)

            if not isinstance(payload, dict) or "response" not in payload:
                raise RuntimeError("Unexpected payload (missing 'response').")

            upload_text(bs, f"{base_dir}/request.txt", url_safe)
            upload_json(bs, f"{base_dir}/params.json", safe)
            upload_json(bs, f"{base_dir}/response.json", payload)
            upload_json(bs, f"{base_dir}/_meta.json", {
                "fetched_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "http_status": status,
                "duration_ms": dur_ms,
                "source": "guardian",
                "endpoint": "/search",
                "tab": tab,
            })

        except Exception as e:
            failures.append((tab, str(e)))
            try:
                upload_text(bs, f"{base_dir}/request.txt", url_safe)
                upload_json(bs, f"{base_dir}/params.json", safe)
                upload_json(bs, f"{base_dir}/_meta.json", {
                    "fetched_at_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "http_status": None,
                    "duration_ms": None,
                    "source": "guardian",
                    "endpoint": "/search",
                    "tab": tab,
                    "error": str(e),
                })
            except Exception:
                pass

        if i < len(TABS) - 1:
            time.sleep(SLEEP_SEC)

    if failures:
        print("[fetch_news] Completed with errors:")
        for tab, msg in failures:
            print(f"  - {tab}: {msg}")
        return 1

    print("[fetch_news] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
