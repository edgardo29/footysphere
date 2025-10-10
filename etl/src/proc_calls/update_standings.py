#!/usr/bin/env python3
"""
update_standings.py
───────────────────────────────────────────────────────────────────────────────
Batch runner for updating the `standings` table from staging using the
set-based function:

    update_standings_run(p_league_id INT, p_season_year INT, p_form_window INT DEFAULT 5)
    RETURNS (rows_ins INT, rows_upd INT, rows_skipped_nulls INT, rows_no_form INT)

Usage examples:
  # Run for every (league_id, season_year) present in stg_standings
  python update_standings.py

  # Run only for one league/season
  python update_standings.py --league-id 39 --season 2025

  # Use a different form window (e.g., last 8 matches)
  python update_standings.py --window 8
"""

import argparse
import os
import sys
import time

# DB connection helper
THIS_DIR = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(THIS_DIR, "../../test_scripts")))
from get_db_conn import get_db_connection  # type: ignore


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Update standings from staging via update_standings_run().")
    p.add_argument("--league-id", type=int, help="Limit to this league_id")
    p.add_argument("--season", type=int, help="Limit to this season_year (requires --league-id if used)")
    p.add_argument("--window", type=int, default=5, help="Form window N (default: 5)")
    return p.parse_args()


def ensure_function_exists(cur) -> None:
    cur.execute("""
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'update_standings_run'
          AND n.nspname = ANY (current_schemas(TRUE));
    """)
    if cur.fetchone() is None:
        raise RuntimeError(
            "Missing function update_standings_run(...). "
            "Create it before running this script."
        )


def fetch_targets(cur, league_id: int | None, season: int | None) -> list[tuple[int, int]]:
    if league_id is not None and season is not None:
        cur.execute(
            """
            SELECT DISTINCT league_id, season_year
            FROM stg_standings
            WHERE is_valid = TRUE
              AND league_id = %s
              AND season_year = %s
            ORDER BY league_id, season_year
            """,
            (league_id, season),
        )
    elif league_id is not None and season is None:
        cur.execute(
            """
            SELECT DISTINCT league_id, season_year
            FROM stg_standings
            WHERE is_valid = TRUE
              AND league_id = %s
            ORDER BY league_id, season_year
            """,
            (league_id,),
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT league_id, season_year
            FROM stg_standings
            WHERE is_valid = TRUE
            ORDER BY league_id, season_year
            """
        )
    return [(int(r[0]), int(r[1])) for r in cur.fetchall()]


def main() -> None:
    args = parse_args()
    conn = get_db_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Preflight: ensure function exists
        ensure_function_exists(cur)

        # Gather targets
        targets = fetch_targets(cur, args.league_id, args.season)
        if not targets:
            print("No targets found in stg_standings (is_valid=TRUE) matching the filters.")
            return

        print(f"Running update_standings_run() for {len(targets)} target(s). "
              f"form_window={args.window}")

        total_ins = total_upd = total_skipped = total_no_form = 0
        failures: list[tuple[int, int, str]] = []

        for league_id, season_year in targets:
            start = time.time()
            try:
                cur.execute(
                    """
                    SELECT rows_ins, rows_upd, rows_skipped_nulls, rows_no_form
                    FROM update_standings_run(%s, %s, %s);
                    """,
                    (league_id, season_year, args.window),
                )
                rows = cur.fetchone()
                if rows is None:
                    raise RuntimeError("Function returned no row")

                ins, upd, skipped, no_form = map(int, rows)
                conn.commit()

                total_ins += ins
                total_upd += upd
                total_skipped += skipped
                total_no_form += no_form

                elapsed_ms = int((time.time() - start) * 1000)
                print(f"[{league_id}-{season_year}] inserted={ins}, updated={upd}, "
                      f"skipped={skipped}, no_form={no_form} ({elapsed_ms} ms)")
            except Exception as e:
                conn.rollback()
                failures.append((league_id, season_year, str(e)))
                print(f"[{league_id}-{season_year}] ERROR: {e}")

        # Final summary
        print("────────────────────────────────────────────────────────")
        print(f"TOTAL → inserted={total_ins}, updated={total_upd}, "
              f"skipped={total_skipped}, no_form={total_no_form}")
        if failures:
            print(f"{len(failures)} target(s) failed:")
            for lg, sy, err in failures:
                print(f"  - [{lg}-{sy}] {err}")
            # Non-zero exit for pipeline visibility
            sys.exit(1)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
