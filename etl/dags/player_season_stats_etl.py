"""
player_season_stats_etl.py
────────────────────────────────────────────────────────────────────────────
Weekly ETL for player_season_stats

Task list
---------
1. Build target list from player_stats_load_state.
2. Build ids.txt for incremental targets.
3. Combine targets + id paths for mapped BashOperators.
4. Fetch JSONs from API-Football to Azure Blob (Script A).
5. Truncate stg_player_season_stats.
6. Load JSONs from Blob into staging (Script B).
7. Merge staging into player_season_stats.
8. Update player_stats_load_state.
"""

from __future__ import annotations
import logging
import pendulum
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# 1.  Make local project modules importable for every PythonOperator
# ----------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]        # .../footysphere
for folder in ("src", "test_scripts", "config"):
    p = (PROJECT_ROOT / folder).as_posix()
    if p not in sys.path:
        sys.path.append(p)
# ----------------------------------------------------------------------

# Global ENV / ROOT constants (still needed for BashOperator subprocesses)
ROOT = "{{ var.value.FOOTY_ROOT }}"
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
    "PYTHONPATH": (
        "{{ var.value.FOOTY_ROOT }}/src:"
        "{{ var.value.FOOTY_ROOT }}/test_scripts:"
        "{{ var.value.FOOTY_ROOT }}/config"
    ),
}

logging.getLogger(__name__).info("player_season_stats_etl DAG parsed OK")

# ───────────────────────── Helper functions ─────────────────────────────
def get_player_targets(**_) -> list[dict]:
    """Helper for Task 1."""
    from get_db_conn import get_db_connection
    rows=[]
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ls.league_id,            -- <-- EDIT  (new)
                   l.folder_alias,
                   ls.season_str,
                   ls.season_year,
                   CASE WHEN pls.last_load_ts IS NULL
                        THEN 'initial' ELSE 'incremental' END
            FROM   league_seasons ls
            JOIN   leagues        l  ON l.league_id = ls.league_id
            JOIN   player_stats_load_state pls
                   ON pls.league_id   = ls.league_id
                  AND pls.season_year = ls.season_year
            WHERE  pls.is_active_load = true;
        """)
        for lid, lf, ss, yr, mode in cur.fetchall():           # <-- EDIT
            rows.append(
                dict(league_id=lid, league=lf, season=ss,      # <-- EDIT
                     year=yr, mode=mode)
            )
    return rows

def build_player_id_list(league:str, season:str, year:int, mode:str, **_) -> str:
    """Helper for Task 2."""
    if mode == "initial":
        return ""
    from pathlib import Path
    from get_db_conn import get_db_connection
    path = Path(f"/tmp/ids_{league}_{season}.txt")
    with get_db_conn() as conn, path.open("w") as fh:
        cur = conn.cursor()
        cur.execute("""
            WITH ref AS (
              SELECT last_load_ts
              FROM   player_stats_load_state pls
              JOIN   leagues l ON l.league_id = pls.league_id
              WHERE  l.folder_alias = %s AND pls.season_year = %s
            )
            SELECT DISTINCT player_id
            FROM   match_events me, ref
            WHERE  me.league_folder = %s
              AND  me.season_str    = %s
              AND  me.event_time   > ref.last_load_ts;
        """, (league, year, league, season))
        for (pid,) in cur.fetchall():
            fh.write(f"{pid}\n")
    return str(path)

def zip_targets_and_ids(targets, id_files, **_) -> list[dict]:
    """Helper for Task 3."""
    return [{"params": {**t, "id_file": p}} for t, p in zip(targets, id_files)]

# ───────────────────────────── DAG object ───────────────────────────────
with DAG(
    dag_id="player_season_stats_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="30 06 * * 1",
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "player_stats"],
    is_paused_upon_creation=True,
) as dag:

    # Task 1: build target list ------------------------------------------------
    build_targets = PythonOperator(
        task_id="build_player_targets",
        python_callable=get_player_targets,
    )

    # Task 2: build ids.txt (mapped)
    id_files = (
        PythonOperator
        .partial(task_id="build_player_id_list",
                 python_callable=build_player_id_list)
        .expand(op_kwargs=build_targets.output)
    )

    # Task 3: zip targets + ids ------------------------------------------------
    zipped = PythonOperator(
        task_id="zip_params",
        python_callable=zip_targets_and_ids,
        op_args=[build_targets.output, id_files.output],
    )

    # Task 4: fetch JSONs → Blob (Script A) ------------------------------------
    fetch_player_stats = (
        BashOperator
        .partial(
            task_id="fetch_player_stats",
            bash_command=(
                "$PYTHON " + ROOT + "/src/blob/fetch_data/fetch_player_season_stats.py "
                "--league_folder {{ params.league }} "
                "--league_id     {{ params.league_id }} "        # <-- EDIT
                "--season_str    {{ params.season }} "
                "--season_year   {{ params.year }} "
                "--run_mode      {{ params.mode }} "
                "{% if params.id_file %}--player_list_path {{ params.id_file }}{% endif %}"
            ),
            env=ENV,
        )
        .expand_kwargs(zipped.output)
    )

    # Task 5: truncate staging table ------------------------------------------
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/cleanup_stg.py "
                     "--table stg_player_season_stats",
        env=ENV,
    )

    # Task 6: load Blob → staging (Script B) -----------------------------------
    load_to_stg = (
        BashOperator
        .partial(
            task_id="load_to_stg",
            bash_command=(
                "$PYTHON " + ROOT + "/src/blob/load_data/load_stg_player_season_stats.py "
                "--league_folder {{ params.league }} "
                "--season_str    {{ params.season }} "
                "--run_mode      {{ params.mode }} "
                "{% if params.id_file %}--player_list_path {{ params.id_file }}{% endif %}"
                # league_id not needed by Script B; omitted
            ),
            env=ENV,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        .expand_kwargs(zipped.output)
    )


    # Task 7: Dadat validation --------------------------------------------
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/check_player_season_stats.py"
        ),
        env=ENV,
        # Checks row-count, NULLs, PK duplicates, etc.
    )

    # Task 8: merge staging → prod --------------------------------------------
    merge_to_main = BashOperator(
        task_id="merge_to_main",
        bash_command="$PYTHON " + ROOT + "/src/procs/insert_player_season_stats.py",
        env=ENV,
    )

    # Task 8: update load-state -------------------------------------------------
    update_state = (
        BashOperator
        .partial(
            task_id="update_state",
            bash_command=(
                "$PYTHON " + ROOT + "/src/procs/update_player_stats_state.py "
                "--league_folder {{ params.league }} "
                "--season_str    {{ params.season }} "
                "--run_mode      {{ params.mode }}"
            ),
            env=ENV,
        )
        .expand_kwargs(zipped.output)
    )

    # Dependencies
    build_targets >> id_files >> zipped >> fetch_player_stats >> cleanup_stg
    cleanup_stg   >> load_to_stg >> check_stg >> merge_to_main >> update_state
