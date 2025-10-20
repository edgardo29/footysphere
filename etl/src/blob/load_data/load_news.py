#!/usr/bin/env python3
"""
load_stg_news.py
────────────────────────────────────────────────────────────────────────────
Loads the LATEST Guardian news batch per tab (transfers / injuries / matchreports)
from Azure Blob raw artifacts into stg_news.

Notes:
  • No TRUNCATE here (handled elsewhere).
  • Picks the lexicographically newest run_ts=... per tab by finding .../response.json.
  • Normalizes: summary strips HTML, prefers fields.shortUrl over webUrl.
  • Bulk upsert via execute_values for speed.

Run:
  source footysphere_venv/bin/activate
  python etl/src/blob/load_data/load_stg_news.py
"""

# ───────────── Imports & helper-path setup ────────────────────────────────
import os, sys, re, json, html
from typing import Optional, Tuple, List
from azure.storage.blob import BlobServiceClient
from psycopg2.extras import execute_values, Json

# add project helper dirs to PYTHONPATH
sys.path.extend([
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../config")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")),
])

from credentials  import AZURE_STORAGE           # secrets (account + key)
from get_db_conn  import get_db_connection       # returns psycopg2 connection

RAW_CONTAINER = "raw"
OUT_PREFIX    = "news/guardian"

# tab -> directory name under OUT_PREFIX
TAB_DIRS = {
    "transfers":    "search_football_transfers",
    "injuries":     "search_football_injuries",
    "matchreports": "search_football_matchreports",
}

MANDATORY_KEYS = ("id", "webTitle", "webPublicationDate")

# ─────────────── Blob helpers ─────────────────────────────────────────────
def list_latest_run_ts_for_tab(bs: BlobServiceClient, tab_dir: str) -> Optional[Tuple[str, str]]:
    """
    Return (run_ts, base_dir) for the newest batch for a given tab dir,
    where base_dir is like: news/guardian/<tab_dir>/run_ts=YYYY-MM-DDTHH-MM-SS
    """
    prefix = f"{OUT_PREFIX}/{tab_dir}/"
    # List only response.json files; they're our "batch existence" signals.
    blobs = bs.get_container_client(RAW_CONTAINER).list_blobs(name_starts_with=prefix)
    candidates = []
    for b in blobs:
        name = b.name
        if not name.endswith("/response.json"):
            continue
        # Expect .../run_ts=YYYY-MM-DDTHH-MM-SS/response.json
        m = re.search(r"run_ts=([^/]+)/response\.json$", name)
        if m:
            run_ts = m.group(1)
            # base_dir = path up to run_ts=... (without trailing file)
            base_dir = name.rsplit("/", 1)[0]  # drop 'response.json'
            candidates.append((run_ts, base_dir))

    if not candidates:
        return None
    # run_ts is ISO-like; lexicographic max = newest
    candidates.sort(key=lambda t: t[0])
    return candidates[-1]


def fetch_json_from_blob(bs: BlobServiceClient, blob_path: str) -> dict:
    blob = bs.get_blob_client(RAW_CONTAINER, blob_path)
    return json.loads(blob.download_blob().readall())

# ─────────────── Transform helpers ────────────────────────────────────────
def strip_html(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    # fast tag-strip + unescape entities
    return html.unescape(re.sub(r"<[^>]+>", "", text)).strip() or None


def run_ts_to_iso_utc(run_ts: str) -> str:
    """
    Convert 'YYYY-MM-DDTHH-MM-SS' -> 'YYYY-MM-DDTHH:MM:SSZ'
    """
    date_part, time_part = run_ts.split("T", 1)
    hh, mm, ss = time_part.split("-")
    return f"{date_part}T{hh}:{mm}:{ss}Z"


def rows_from_response(tab: str, response: dict, fetched_at_utc: Optional[str], fallback_run_ts: Optional[str]) -> List[tuple]:
    """
    Build rows for stg_news insert.
    Returns list of tuples in the order of INSERT columns.
    """
    results = (response.get("response") or {}).get("results") or []
    rows = []

    # decide fetched_at_utc
    fetched = fetched_at_utc or (run_ts_to_iso_utc(fallback_run_ts) if fallback_run_ts else None)

    for item in results:
        article_id = item.get("id")
        title      = item.get("webTitle")
        pub_utc    = item.get("webPublicationDate")
        fields     = item.get("fields") or {}
        url        = fields.get("shortUrl") or item.get("webUrl")
        img        = fields.get("thumbnail")
        summary    = strip_html(fields.get("trailText"))

        is_valid = True
        error_reason = None

        for k in MANDATORY_KEYS:
            if not item.get(k):
                is_valid = False
                error_reason = f"missing {k}"
                break
        if not url:
            is_valid = False
            error_reason = (error_reason + "; missing url") if error_reason else "missing url"

        rows.append((
            article_id,            # article_id
            tab,                   # tab
            title,                 # title
            summary,               # summary
            img,                   # image_url
            "The Guardian",        # source
            url,                   # url
            pub_utc,               # published_at_utc (string OK; PG parses ISO)
            fetched,               # fetched_at_utc (string ISO Z)
            Json(item),                  # raw (JSONB)
            is_valid,              # is_valid
            error_reason           # error_reason
        ))
    return rows

# ─────────────── Main loader ──────────────────────────────────────────────
def main() -> None:
    # 1) Connect to Postgres & Azure Blob
    conn = get_db_connection()
    cur  = conn.cursor()
    bs   = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net",
        credential=AZURE_STORAGE["access_key"],
    )

    total_rows = 0
    per_tab_counts = {}

    try:
        all_rows = []

        for tab, tab_dir in TAB_DIRS.items():
            latest = list_latest_run_ts_for_tab(bs, tab_dir)
            if not latest:
                print(f"[{tab}] No batches found under {OUT_PREFIX}/{tab_dir}/ — skipping.")
                continue

            run_ts, base_dir = latest
            # required files
            resp_key = f"{base_dir}/response.json"
            meta_key = f"{base_dir}/_meta.json"

            try:
                response_json = fetch_json_from_blob(bs, resp_key)
            except Exception as e:
                print(f"[{tab}] ERROR reading {resp_key}: {e}")
                continue

            fetched_at_utc = None
            try:
                meta_json = fetch_json_from_blob(bs, meta_key)
                fetched_at_utc = (meta_json or {}).get("fetched_at_utc")
            except Exception:
                # tolerate missing _meta.json; derive from run_ts
                fetched_at_utc = None

            rows = rows_from_response(tab, response_json, fetched_at_utc, run_ts)
            per_tab_counts[tab] = len(rows)
            all_rows.extend(rows)

        if not all_rows:
            print("load_stg_news: nothing to insert.")
            cur.close(); conn.close(); return

        # 2) Bulk UPSERT
        insert_sql = """
            INSERT INTO stg_news (
                article_id, tab, title, summary, image_url, source, url,
                published_at_utc, fetched_at_utc, raw, is_valid, error_reason
            )
            VALUES %s
            ON CONFLICT (article_id) DO UPDATE SET
                tab              = EXCLUDED.tab,
                title            = EXCLUDED.title,
                summary          = EXCLUDED.summary,
                image_url        = EXCLUDED.image_url,
                source           = EXCLUDED.source,
                url              = EXCLUDED.url,
                published_at_utc = EXCLUDED.published_at_utc,
                fetched_at_utc   = EXCLUDED.fetched_at_utc,
                raw              = EXCLUDED.raw,
                is_valid         = EXCLUDED.is_valid,
                error_reason     = EXCLUDED.error_reason;
        """

        execute_values(cur, insert_sql, all_rows, page_size=500)
        conn.commit()

        total_rows = len(all_rows)
        print(f"load_stg_news: inserted/updated {total_rows} rows "
              f"(transfers={per_tab_counts.get('transfers',0)}, "
              f"injuries={per_tab_counts.get('injuries',0)}, "
              f"matchreports={per_tab_counts.get('matchreports',0)}).")

    except Exception as err:
        conn.rollback()
        print(f"load_stg_news: ERROR → {err}")
        raise
    finally:
        cur.close()
        conn.close()


# ─────────────── Entrypoint ───────────────────────────────────────────────
if __name__ == "__main__":
    main()
