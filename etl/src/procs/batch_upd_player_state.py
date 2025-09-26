#!/usr/bin/env python
"""
batch_update_player_state.py
────────────────────────────────────────────────────────────────────────────
Fire-and-forget helper.  Run this from the command line whenever you want to
refresh `player_stats_load_state` for *every* active league/season (or a
subset you pick with --leagues).

It simply loops over rows in player_stats_load_state (WHERE is_active_load)
and re-executes   src/procs/update_player_stats_state.py   with the correct
arguments.

Usage (examples)
────────────────
# update ALL active rows with mode=incremental
$ python batch_update_player_state.py --run_mode incremental

# only certain leagues
$ python batch_update_player_state.py --run_mode initial \
      --leagues premier_league bundesliga
"""

# ───────── std-lib ───────────────────────────────────────────────────────
import os, sys, subprocess, logging, argparse, shlex
from pathlib import Path

# ───────── project imports & search-path tweaks ─────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]      # …/footysphere
for folder in ("src", "config", "test_scripts"):
    sys.path.append((PROJECT_ROOT / folder).as_posix())
    

from get_db_conn import get_db_connection    # same helper the DAG uses

# ───────── logging setup ────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                    level=logging.INFO)
log = logging.getLogger(__name__)

# ───────── constants ────────────────────────────────────────────────────
PYTHON_EXE = sys.executable                # use the *current* interpreter
UPDATE_SCRIPT = (PROJECT_ROOT /
                 "src/procs/update_player_stats_state.py").as_posix()

# ───────── helper: yield league/season info ─────────────────────────────
def iter_active_loads(only_leagues: set[str] | None = None):
    from get_db_conn import get_db_connection

    q = """
        SELECT
            l.folder_alias,
            ls.season_str
        FROM   player_stats_load_state   pls
        JOIN   leagues               l  ON l.league_id = pls.league_id
        JOIN   league_seasons        ls ON ls.league_id   = pls.league_id
                                        AND ls.season_year = pls.season_year
        WHERE  pls.is_active_load = true
    """

    with get_db_connection() as conn:
        cur = conn.cursor()          # ← get a cursor
        cur.execute(q)               # ← execute returns None
        for folder_alias, season_str in cur.fetchall():
            if only_leagues and folder_alias not in only_leagues:
                continue
            yield folder_alias, season_str


# ───────── main ─────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser("Batch-update player_stats_load_state")
    parser.add_argument("--run_mode", required=True,
                        choices=("initial", "incremental"),
                        help="'initial' or 'incremental' – stored in last_mode")
    parser.add_argument("--leagues", nargs="*",
                        help="Optional list of folder_aliases to limit the run")
    args = parser.parse_args()

    targets = list(iter_active_loads(set(args.leagues) if args.leagues else None))
    if not targets:
        log.warning("No matching league/season rows found → nothing to do.")
        return

    log.info("▶ running update for %d rows  (mode=%s)", len(targets), args.run_mode)

    for league_folder, season_str in targets:        # ← unpack tuple
        cmd = [
            PYTHON_EXE, UPDATE_SCRIPT,
            "--league_folder", league_folder,
            "--season_str",    season_str,
            "--run_mode",      args.run_mode,
        ]
        log.info("• %s", " ".join(shlex.quote(p) for p in cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            log.info("  ✔ success")
        else:
            log.error("  ✖ FAILED  rc=%s  stderr=%s",
                      proc.returncode, proc.stderr.strip())


if __name__ == "__main__":
    main()
