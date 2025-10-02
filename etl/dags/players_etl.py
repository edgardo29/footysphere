# players_snapshot_manual.py  — Airflow 2.8+, MANUAL TRIGGER ONLY
#
# Manual, snapshot-based players ETL with zero hard-coding of leagues.
# Dynamically queries DB for league folders and maps a load task per league.
#
# Trigger JSON (Airflow UI → Trigger DAG):
# {
#   "window": "summer",          // or "winter" or explicit "summer_20250930_2130"
#   "season_str": "2025_26",     // required for loader path
#   "league_ids": "39,61,78"     // optional filter (comma-separated)
#   // "season_year": 2025       // optional (overrides fetch season)
# }

from __future__ import annotations
import os
import pendulum
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Shared env for Bash steps (your scripts read PYTHON/PG_CONN)
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"

with DAG(
    dag_id="players_snapshot_manual",
    description="Players snapshot → STG → CHECK → MERGE (manual; leagues from DB).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,            # manual only
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "players", "snapshot", "manual", "dynamic"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Fetch players into Blob (window auto-expands inside the script if "summer"/"winter")
    fetch_players = BashOperator(
        task_id="fetch_players",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_players.py "
            + "--window {{ dag_run.conf.get('window', 'summer') }}"
            + "{% if dag_run.conf.get('league_ids') %} --league_ids {{ dag_run.conf.get('league_ids') }}{% endif %}"
            + "{% if dag_run.conf.get('season_year') %} --season_year {{ dag_run.conf.get('season_year') }}{% endif %}"
        ),
        env=ENV,
        doc_md="Fetch league-pages (+ optional squads) into snapshot window.",
    )

    # 2) Truncate staging
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/cleanup_stg.py --table stg_players",
        env=ENV,
        doc_md="TRUNCATE stg_players prior to load.",
    )

    # 3) Query DB at runtime for league folders (no hard-coding)
    @task(task_id="list_league_folders")
    def list_league_folders() -> list[str]:
        """
        Returns a list of league folder names for current leagues.
        Priority for folder name:
          1) league_catalog.folder_alias (if set)
          2) leagues.folder_alias (if set)
          3) fallback: lower(regexp_replace(league_name,'[ /&,-]+','_','g')) || '_' || league_id
        Optional filter via dag_run.conf['league_ids'].
        """
        dsn = Variable.get("PG_CONN")
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        filter_ids = conf.get("league_ids")
        ids_clause = ""
        params = []
        if filter_ids:
            ids = [int(x.strip()) for x in str(filter_ids).split(",") if x.strip()]
            ids_clause = " AND ls.league_id = ANY(%s)"
            params.append(ids)

        sql = """
            SELECT
                ls.league_id,
                COALESCE(lc.folder_alias, l.folder_alias,
                         lower(regexp_replace(l.league_name, '[ /&,-]+', '_', 'g')) || '_' || ls.league_id
                ) AS league_folder
            FROM league_seasons ls
            JOIN leagues l ON l.league_id = ls.league_id
            LEFT JOIN league_catalog lc ON lc.league_id = ls.league_id
            WHERE ls.is_current = TRUE
            """ + ids_clause + """
            ORDER BY ls.league_id
        """
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                rows = cur.fetchall()
        # return just the folder names
        return [r[1] for r in rows]

    league_folders = list_league_folders()

    # 4) Build one load command per league (mapped)
    #    We keep window flexible:
    #    - "summer"/"winter" → loader auto-picks LATEST snapshot with that prefix for the season
    #    - explicit "summer_YYYYMMDD_HHMM" → loader uses it as-is
    @task(task_id="build_load_cmds")
    def build_load_cmds(league_folders: list[str]) -> list[str]:
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        season_str = conf["season_str"]  # required
        window = conf.get("window", "summer")
        py = Variable.get("PYTHON")
        root = Variable.get("FOOTY_ROOT")
        return [
            f"{py} {root}/src/blob/load_data/load_stg_players.py "
            f"--league_folder {lf} --season_str {season_str} --window {window}"
            for lf in league_folders
        ]

    load_cmds = build_load_cmds(league_folders)

    load_to_stg = BashOperator.partial(
        task_id="load_to_stg",
        env=ENV,
        doc_md="Load each league’s player_details → stg_players from the chosen snapshot.",
    ).expand(bash_command=load_cmds)

    # 5) DQ checks
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/check_stg_players.py",
        env=ENV,
        doc_md="Run DQ checks on stg_players.",
    )

    # 6) Merge/update prod
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command="$PYTHON " + ROOT + "/src/procs/update_players.py",
        env=ENV,
        doc_md="Merge stg_players → players via stored proc wrapper.",
    )

    done = EmptyOperator(task_id="done")

    # Graph
    fetch_players >> cleanup_stg >> league_folders >> load_cmds >> load_to_stg >> check_stg >> update_main_table >> done
