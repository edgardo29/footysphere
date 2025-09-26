#!/usr/bin/env python
"""
auto_load_stg_player_season_stats.py
────────────────────────────────────────────────────────────────────────────
Load *all* active league/season combos into **stg_player_season_stats**
without typing any parameters.

• Reads the same tables the Airflow DAG uses to find targets:
      player_stats_load_state   (is_active_load = true)
      leagues
      league_seasons
• For each (league_folder, season_str, run_mode) it calls
  load_stg_player_season_stats.py.
"""

import subprocess
import sys
from pathlib import Path
import os

# project helpers
REPO_ROOT = Path(__file__).resolve().parents[3]           # .../footysphere
PYTHON     = sys.executable
LOADER     = REPO_ROOT / "src/blob/load_data/load_stg_player_season_stats.py"


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "../../../config")))
\
from get_db_conn import get_db_connection


def discover_targets(run_mode: str) -> list[tuple[str, str]]:
    """
    Return [(league_folder, season_str), …] for all active loads.
    """
    sql = """
        SELECT l.folder_alias,
               ls.season_str
        FROM   player_stats_load_state pls
        JOIN   leagues              l  ON l.league_id = pls.league_id
        JOIN   league_seasons       ls ON ls.league_id = pls.league_id
                                       AND ls.season_year = pls.season_year
        WHERE  pls.is_active_load = true
        ORDER  BY l.folder_alias, ls.season_str;
    """
    rows: list[tuple[str, str]] = []
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = [(lf, ss) for lf, ss in cur.fetchall()]
    return rows


def run_loader(league_folder: str, season_str: str, run_mode: str) -> None:
    cmd = [
        PYTHON, LOADER.as_posix(),
        "--league_folder", league_folder,
        "--season_str",    season_str,
        "--run_mode",      run_mode,
    ]
    print(" ".join(cmd))
    subprocess.check_call(cmd)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in ("initial", "incremental"):
        sys.exit("Usage:  auto_load_stg_player_season_stats.py  <initial|incremental>")

    run_mode = sys.argv[1]
    targets  = discover_targets(run_mode)

    if not targets:
        print("No active league/season targets found.")
        return

    for league_folder, season_str in targets:
        print(f"\n=== Loading {league_folder} / {season_str} ({run_mode}) ===")
        run_loader(league_folder, season_str, run_mode)

    print("\nAll active leagues loaded into stg_player_season_stats.")


if __name__ == "__main__":
    main()
