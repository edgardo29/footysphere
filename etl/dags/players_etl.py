# players_etl.py — Manual, DB-driven, bash-only
# Uses your ETL venv ($PYTHON) everywhere and calls the new window-only loader.

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Airflow Variables expected:
#  - PYTHON: /home/football_vmadmin/footysphere-repo/etl/etl_venv/bin/python
#  - FOOTY_ROOT: /home/football_vmadmin/footysphere-repo/etl
#  - PG_CONN: postgresql://.../footysphere_db?sslmode=require
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
    "FOOTY_ROOT": "{{ var.value.FOOTY_ROOT }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"

with DAG(
    dag_id="players_etl",
    description="Players snapshot → STG → CHECK → MERGE (manual; DB-driven; bash-only)",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # manual only
    catchup=False,
    default_args={"owner": "footysphere", "retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["footysphere", "players", "snapshot", "manual", "bash-only"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Fetch players into Blob (window "summer"/"winter"/explicit). Optional league_ids/season_year.
    fetch_players = BashOperator(
        task_id="fetch_players",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_players.py "
            + "--window {{ dag_run.conf.get('window', 'summer') }} "
            + "{% if dag_run.conf.get('league_ids') %}--league_ids {{ dag_run.conf.get('league_ids') }} {% endif %}"
            + "{% if dag_run.conf.get('season_year') %}--season_year {{ dag_run.conf.get('season_year') }} {% endif %}"
        ),
        env=ENV,
        doc_md="Fetch league player pages into the chosen snapshot window.",
    )

    # 2) Truncate staging
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/cleanup_stg.py --table stg_players",
        env=ENV,
        doc_md="TRUNCATE stg_players.",
    )

    # 3) Load from catalog targets (window-only; optional single league_id, no quotes)
    load_targets = BashOperator(
        task_id="load_targets",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/db/loaders/load_stg_players.py "
            + "--window {{ dag_run.conf.get('window', 'summer') }} "
            + "{% if dag_run.conf.get('league_id') %}--league_id {{ dag_run.conf.get('league_id') }} {% endif %}"
            + "{% if dag_run.conf.get('dry_run') %}--dry_run {% endif %}"
            + "{% if dag_run.conf.get('strict') %}--strict {% endif %}"
        ),
        env=ENV,
        doc_md=(
            "Load stg_players for all leagues targeted in league_catalog.players_target_windows "
            "for the chosen window; removes that window for leagues that actually loaded. "
            "Optionally restrict with an integer league_id."
        ),
    )

    # 4) DQ checks
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/check_stg_players.py",
        env=ENV,
        doc_md="Run DQ checks on stg_players.",
    )

    # 5) Merge to prod
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/update_players.py",
        env=ENV,
        doc_md="Merge stg_players → players.",
    )

    done = EmptyOperator(task_id="done")

    fetch_players >> cleanup_stg >> load_targets >> check_stg >> update_main_table >> done
s