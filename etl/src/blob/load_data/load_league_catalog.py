#!/usr/bin/env python
"""
load_league_catalog.py
────────────────────────────────────────────────────────────────────────────
Enable a league for ingestion by inserting (or updating) a row in
`league_catalog`.

USAGE
  # Search by name, pick from menu if >1 match
  python load_league_catalog.py "major league soccer" --first-season 2024

  # Add directly by league_id
  python load_league_catalog.py 253 --first-season 2024

  # Enable or extend through 2025 in one call
  python load_league_catalog.py 253 --first-season 2024 --last-season 2025
"""

import argparse
import os
import re
import sys

# ── path setup: must match your working tree ───────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../test_scripts")))

from get_db_conn import get_db_connection  # resolves to your DB connector


def slugify(name: str) -> str:
    """lowercase, replace whitespace with '_'"""
    return re.sub(r"\s+", "_", name.strip().lower())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("search",
                        help="league_id (int) or part of league_name to search")
    parser.add_argument("--first-season", type=int, required=True,
                        help="Earliest season year to ingest (e.g. 2024)")
    parser.add_argument("--last-season", type=int,
                        help="Latest season year to ingest (e.g. 2025). "
                             "Defaults to --first-season if omitted.")
    args = parser.parse_args()

    last_season_val = args.last_season or args.first_season

    conn = get_db_connection()
    cur  = conn.cursor()

    # 1) Find candidate leagues in league_directory
    if args.search.isdigit():
        cur.execute("""
            SELECT league_id, league_name
            FROM   league_directory
            WHERE  league_id = %s
        """, (int(args.search),))
    else:
        cur.execute("""
            SELECT league_id, league_name
            FROM   league_directory
            WHERE  league_name ILIKE %s
        """, ('%' + args.search + '%',))
    matches = cur.fetchall()

    if not matches:
        print(f"No league found for '{args.search}'. "
              "Run discover_leagues.py for the latest season and try again.")
        cur.close(); conn.close()
        return

    # 2) Resolve multiple matches
    if len(matches) > 1 and not args.search.isdigit():
        print("Multiple matches:")
        for idx, (lid, lname) in enumerate(matches, 1):
            print(f"{idx}) {lid} – {lname}")
        choice = int(input("Pick #> "))
        league_id, league_name = matches[choice - 1]
    else:
        league_id, league_name = matches[0]

    # 3) Build canonical alias: always "<slug>_<league_id>"
    base = slugify(league_name)
    suffix = f"_{league_id}"
    folder_alias = base if base.endswith(suffix) else (base + suffix)

    # 4) Upsert catalog row
    cur.execute("""
        INSERT INTO league_catalog
              (league_id, folder_alias, league_name,
               first_season, last_season, is_enabled,
               load_date,    upd_date)
        VALUES (%s, %s, %s, %s, %s, TRUE, NOW(), NOW())
        ON CONFLICT (league_id) DO UPDATE
           SET folder_alias = EXCLUDED.folder_alias,
               league_name  = EXCLUDED.league_name,
               first_season = EXCLUDED.first_season,
               -- extend forward only
               last_season  = GREATEST(
                                 COALESCE(league_catalog.last_season, 0),
                                 EXCLUDED.last_season),
               is_enabled   = TRUE,
               upd_date     = NOW();
    """, (league_id, folder_alias, league_name, args.first_season, last_season_val))

    conn.commit()
    print(f"✓ {league_name} ({league_id}) enabled from "
          f"{args.first_season} through {last_season_val} "
          f"→ folder_alias='{folder_alias}'")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
