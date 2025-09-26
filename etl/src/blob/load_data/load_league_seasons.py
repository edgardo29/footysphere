#!/usr/bin/env python
"""
load_league_seasons.py
────────────────────────────────────────────────────────────────────────────
Populate/refresh `league_seasons` for all enabled leagues in league_catalog,
using first_season..last_season. Optionally scope to a single --league-id.

Usage:
  python load_league_seasons.py
  python load_league_seasons.py --league-id 244
"""

import argparse
import os
import sys
import time
import requests

# ── path setup (match your tree) ───────────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../test_scripts")))

from get_db_conn import get_db_connection
from credentials import API_KEYS

API_URL = "https://v3.football.api-sports.io/leagues"
RATE_DELAY_SEC = 1.0  # polite; well under 300 rpm

def season_label(year: int) -> str:
    """Return 'YYYY_YY' label (calendar-year leagues become 'YYYY_YY' with same YY)."""
    return f"{year}_{str(year + 1)[-2:]}"

def fetch_league_meta(league_id: int) -> dict:
    """GET /leagues?id=<league_id>; return the JSON body (raises on non-200)."""
    headers = {"x-rapidapi-key": API_KEYS["api_football"]}
    resp = requests.get(API_URL, headers=headers, params={"id": league_id}, timeout=30)
    resp.raise_for_status()
    return resp.json()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--league-id", type=int, help="Limit to one league_id")
    args = parser.parse_args()

    conn = get_db_connection()
    cur = conn.cursor()

    # 1) Load enabled leagues (+ optional scope)
    if args.league_id:
        cur.execute("""
            SELECT league_id, league_name, first_season, last_season
            FROM   league_catalog
            WHERE  is_enabled = TRUE AND league_id = %s
        """, (args.league_id,))
    else:
        cur.execute("""
            SELECT league_id, league_name, first_season, last_season
            FROM   league_catalog
            WHERE  is_enabled = TRUE
            ORDER  BY league_id
        """)
    leagues = cur.fetchall()

    if not leagues:
        print("No enabled leagues found in league_catalog.")
        cur.close(); conn.close()
        return

    for league_id, league_name, first_season, last_season in leagues:
        try:
            meta = fetch_league_meta(league_id)

            if not meta.get("response"):
                print(f"{league_id} {league_name}: API returned no response; skipping.")
                time.sleep(RATE_DELAY_SEC)
                continue

            seasons = meta["response"][0].get("seasons", [])
            by_year = {s.get("year"): s for s in seasons if "year" in s}

            total = 0
            missing = 0

            for year in range(int(first_season), int(last_season) + 1):
                s = by_year.get(year)
                if not s:
                    missing += 1
                    continue

                start_date = s.get("start") or None
                end_date   = s.get("end") or None
                is_current = bool(s.get("current", False))
                label      = season_label(year)

                # Upsert (idempotent)
                cur.execute(
                    """
                    INSERT INTO league_seasons
                          (league_id, season_year, start_date, end_date,
                           is_current, season_str, load_date, upd_date)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (league_id, season_year) DO UPDATE
                       SET start_date = EXCLUDED.start_date,
                           end_date   = EXCLUDED.end_date,
                           is_current = EXCLUDED.is_current,
                           season_str = EXCLUDED.season_str,
                           upd_date   = NOW();
                    """,
                    (league_id, year, start_date, end_date, is_current, label)
                )
                total += 1

            conn.commit()
            ok = total
            print(f"{league_id} {league_name}: upserted {ok} season rows"
                  + (f", missing_in_api={missing}" if missing else ""))

        except Exception as e:
            conn.rollback()
            print(f"{league_id} {league_name}: ERROR → {e}")

        time.sleep(RATE_DELAY_SEC)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
