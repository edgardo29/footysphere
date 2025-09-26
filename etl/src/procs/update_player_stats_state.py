#!/usr/bin/env python
"""
update_player_stats_state.py
────────────────────────────────────────────────────────────────────────────
Update (or insert) the tracking row in `player_stats_load_state` after a
successful load of player-season stats.

Called by the Airflow DAG as the very last step.

Usage
-----
python update_player_stats_state.py \
       --league_folder premier_league \
       --season_str    2024_25 \
       --run_mode      incremental
"""

# ───────── std-lib ───────────────────────────────────────────────────────
import argparse, logging, os, sys
from datetime import datetime, timezone

# ───────── project imports (db creds helper lives in test_scripts) ───────
sys.path.extend(
    [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_scripts")),
    ]
)
from get_db_conn import get_db_connection

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                    level=logging.INFO)
log = logging.getLogger(__name__)


# ───────── helper ────────────────────────────────────────────────────────
def _league_id_from_folder(folder_alias: str, cur) -> int:
    cur.execute("SELECT league_id FROM leagues WHERE folder_alias = %s", (folder_alias,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Unknown league_folder '{folder_alias}'")
    return row[0]


# ───────── main ──────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser("Update player_stats_load_state")
    parser.add_argument("--league_folder", required=True)
    parser.add_argument("--season_str",    required=True)          # e.g. 2024_25
    parser.add_argument("--run_mode",      required=True,
                        choices=["initial", "incremental"])
    args = parser.parse_args()

    season_year = int(args.season_str.split("_")[0])               # 2024_25 → 2024
    now_utc     = datetime.now(timezone.utc)

    with get_db_connection() as conn:
        cur = conn.cursor()

        # Resolve league_id
        league_id = _league_id_from_folder(args.league_folder, cur)

        # Upsert load-state
        cur.execute(
            """
            INSERT INTO player_stats_load_state
                 (league_id, season_year,
                  is_active_load, last_load_ts, last_mode)
            VALUES (%s, %s, true, %s, %s)
            ON CONFLICT (league_id, season_year)             -- PK/unique
            DO UPDATE SET
                last_load_ts = EXCLUDED.last_load_ts,
                last_mode    = EXCLUDED.last_mode;
            """,
            (league_id, season_year, now_utc, args.run_mode),
        )
        conn.commit()

    log.info("✔ load-state updated  league=%s  season=%s  mode=%s",
             args.league_folder, args.season_str, args.run_mode)


if __name__ == "__main__":
    main()
