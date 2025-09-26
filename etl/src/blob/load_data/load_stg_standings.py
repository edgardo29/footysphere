#!/usr/bin/env python3
"""
load_stg_standings.py — DB‑driven, group‑aware loader (verbose)
───────────────────────────────────────────────────────────────────────────────
Purpose
-------
Stage the *latest* standings snapshot from Azure Blob into **stg_standings**.

Design (long‑term best practice)
--------------------------------
• **DB‑driven scope**: select league‑seasons that are IN‑SEASON:
    - league_catalog.is_enabled = TRUE
    - league_seasons.is_current = TRUE
    - CURRENT_DATE between start_date and end_date + grace_days
• **Group‑aware**: writes the `group_label` (e.g., "Eastern Conference").
  This matches how API‑Football returns multiple tables per league‑season.
• **Single file per league‑season**: loads the **newest** snapshot only
  (whether it's a full_ or inc_ filename). Staging is truncate‑and‑load,
  so there is no incremental chaining for standings.

Folder layout written by fetch_standings.py
-------------------------------------------
  raw/standings/{folder_alias}/{season_str}/<full|inc>_YYYY-MM-DD_HH-MM.json

How to run (manual)
-------------------
  export PYTHON="$HOME/footysphere_venv/bin/python"
  export ROOT="$HOME/footysphere"
  # optional, one-off limit:
  export LEAGUE_IDS="244,253,71"
  # optional, post‑season buffer days (default 7):
  export STANDINGS_GRACE_DAYS=7

  $PYTHON $ROOT/src/procs/cleanup_stg.py --table stg_standings
  $PYTHON $ROOT/src/blob/load_data/load_stg_standings.py
"""

# ── PYTHON STD LIB ───────────────────────────────────────────────────────────
import os
import sys
import json
from typing import List, Sequence, Tuple, Optional

# ── AZURE SDK ────────────────────────────────────────────────────────────────
from azure.storage.blob import BlobServiceClient

# ── PROJECT HELPERS (credentials + DB) ───────────────────────────────────────
THIS_DIR = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(THIS_DIR, "../../../config")))
sys.path.append(os.path.abspath(os.path.join(THIS_DIR, "../../../test_scripts")))

from credentials import AZURE_STORAGE                          # type: ignore
from get_db_conn import get_db_connection                      # type: ignore

CHUNK_SIZE = 1_000
DEFAULT_GRACE_DAYS = 7

# ─────────────────────────────────────────────────────────────────────────────
# 1) Azure Blob helpers
# ─────────────────────────────────────────────────────────────────────────────
def list_blob_names(container: str, prefix: str) -> List[str]:
    """
    Return all JSON blobs under prefix/, sorted lexicographically.
    Filenames embed timestamps so lexicographic order == chronological.
    """
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    svc  = BlobServiceClient(account_url, credential=AZURE_STORAGE["access_key"])
    cont = svc.get_container_client(container)
    return sorted(
        b.name for b in cont.list_blobs(name_starts_with=prefix)
        if b.name.endswith(".json")
    )

def download_json(container: str, blob_path: str) -> dict:
    """
    Download a single blob → return parsed JSON dict.
    """
    account_url = f"https://{AZURE_STORAGE['account_name']}.blob.core.windows.net"
    svc   = BlobServiceClient(account_url, credential=AZURE_STORAGE["access_key"])
    data  = svc.get_blob_client(container, blob_path).download_blob().readall()
    return json.loads(data)

# ─────────────────────────────────────────────────────────────────────────────
# 2) DB selector — mirror fetch_standings selector (no preseason)
# ─────────────────────────────────────────────────────────────────────────────
BASE_SELECTOR_SQL = r"""
SELECT
    l.league_id,
    ls.season_year,
    ls.season_str,
    COALESCE(lc.folder_alias,
             l.folder_alias,
             LOWER(REGEXP_REPLACE(l.league_name, '\s+', '_', 'g')) || '_' || l.league_id::text
    ) AS folder_alias
FROM leagues l
JOIN league_seasons ls USING (league_id)
JOIN league_catalog lc   USING (league_id)
WHERE lc.is_enabled = TRUE
  AND ls.is_current = TRUE
  AND CURRENT_DATE BETWEEN ls.start_date AND (ls.end_date + (%s || ' days')::interval)
ORDER BY l.league_id, ls.season_year;
"""

def select_scope(grace_days: int = DEFAULT_GRACE_DAYS,
                 league_ids: Optional[Sequence[int]] = None) -> List[Tuple]:
    """
    Returns tuples: (league_id, season_year, season_str, folder_alias).
    Optional `league_ids` limits the scope for one-off runs.
    """
    params = [grace_days]
    sql = BASE_SELECTOR_SQL
    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        sql = sql.replace("ORDER BY", f"  AND l.league_id IN ({placeholders})\nORDER BY")
        params.extend(list(league_ids))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# 3) Inserts into stg_standings (group‑aware, matches column order)
# ─────────────────────────────────────────────────────────────────────────────
def insert_chunk(cur, rows: list[tuple]) -> None:
    """
    Bulk insert a chunk of rows with a single multi‑row INSERT.
    The `is_valid` column is set TRUE at the SQL level.
    """
    if not rows:
        return
    placeholder_single = "(" + (", ".join(["%s"] * len(rows[0]))) + ", TRUE)"
    placeholders_all = ", ".join([placeholder_single] * len(rows))

    sql = f"""
        INSERT INTO stg_standings (
            league_id, season_year, team_id,
            rank, points, played,
            win, draw, lose,
            goals_for, goals_against, goals_diff,
            group_label,                -- ← group/phase descriptor ('' if none)
            is_valid
        )
        VALUES {placeholders_all}
    """
    flat_values = [v for row in rows for v in row]
    cur.execute(sql, flat_values)

def parse_payload_and_insert(conn, payload: dict, league_id: int, season: int) -> int:
    """
    Flatten the payload (iterates ALL groups), build tuples, and insert in CHUNK_SIZE.
    Returns the number of rows inserted for this payload.
    """
    buf: list[tuple] = []
    total = 0
    cur = conn.cursor()

    for resp in payload.get("response", []):
        league_block = resp.get("league", {})
        standings = league_block.get("standings", [])
        if not standings:
            continue

        # standings is a list of group-lists; iterate every group list
        for group_list in standings:
            for row in group_list:
                team      = row["team"]
                aggregate = row["all"]
                goals     = aggregate["goals"]
                group_lbl = row.get("group") or ''   # store '' when there is no group

                buf.append((
                    league_id,
                    season,
                    team["id"],
                    row["rank"],
                    row["points"],
                    aggregate["played"],
                    aggregate["win"],
                    aggregate["draw"],
                    aggregate["lose"],
                    goals["for"],
                    goals["against"],
                    row["goalsDiff"],
                    group_lbl,                         # new field at the end
                ))

                if len(buf) >= CHUNK_SIZE:
                    insert_chunk(cur, buf)
                    conn.commit()
                    total += len(buf)
                    buf.clear()

    if buf:
        insert_chunk(cur, buf)
        conn.commit()
        total += len(buf)

    cur.close()
    return total

# ─────────────────────────────────────────────────────────────────────────────
# 4) Load ONE newest snapshot per league‑season
# ─────────────────────────────────────────────────────────────────────────────
def load_one_league_season(league_id: int, season_year: int,
                           season_str: str, folder_alias: str) -> int:
    """
    Pick the lexicographically-last (newest) file under the league/season folder,
    load it into staging, and return inserted row count.
    """
    prefix = f"standings/{folder_alias}/{season_str}/"
    print(f"\n>>> {league_id}/{season_year}  folder={folder_alias}  season={season_str}")

    names = list_blob_names("raw", prefix)
    candidates = [n for n in names if "/full_" in n or "/inc_" in n]
    if not candidates:
        print("   no snapshot found – skipping.")
        return 0

    latest = candidates[-1]  # filenames embed timestamp; last = newest
    conn = get_db_connection()
    inserted = 0
    try:
        payload = download_json("raw", latest)
        inserted = parse_payload_and_insert(conn, payload, league_id, season_year)
        print(f"   loaded {os.path.basename(latest)} · rows={inserted}")
        print("   commit OK")
    except Exception as exc:
        conn.rollback()
        print(f"   rolled back – {exc}")
        raise
    finally:
        conn.close()

    return inserted

# ─────────────────────────────────────────────────────────────────────────────
# 5) Entrypoint — DB‑driven scope (optional one‑off limit via env)
# ─────────────────────────────────────────────────────────────────────────────
def parse_league_ids_env() -> Optional[Sequence[int]]:
    """
    Optional runtime limit without code/DB changes:
    export LEAGUE_IDS="244,253,71"
    """
    csv = os.getenv("LEAGUE_IDS", "").strip()
    if not csv:
        return None
    out: List[int] = []
    for tok in csv.split(","):
        tok = tok.strip()
        if tok:
            out.append(int(tok))
    return out

if __name__ == "__main__":
    grace_days = int(os.getenv("STANDINGS_GRACE_DAYS", str(DEFAULT_GRACE_DAYS)))
    league_ids = parse_league_ids_env()

    scope = select_scope(grace_days=grace_days, league_ids=league_ids)
    if not scope:
        print("No in‑season league‑seasons selected; nothing to load.")
        sys.exit(0)

    total_rows = 0
    for league_id, season_year, season_str, folder_alias in scope:
        total_rows += load_one_league_season(league_id, season_year, season_str, folder_alias)

    print(f"\nDone. Inserted {total_rows} row(s) into stg_standings.")
