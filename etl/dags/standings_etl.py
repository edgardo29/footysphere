#!/usr/bin/env python3
"""
standings_full_etl_dag.py
────────────────────────────────────────────────────────────────────
Nightly (or weekly) ETL for league standings:
  1) fetch_full_snapshot  ->  2) cleanup_stg
                           ->  3) load_to_stg
                           ->  4) check_stg
                           ->  5) update_main_table
                           ->  6) finished

Outputs
───────
* Azure Blob:  raw/standings/{folder_alias}/{season_str}/full_YYYY-MM-DD_HH-MM.json
* Postgres  :  stg_standings  (staging; truncate-and-load, group-aware)
* Postgres  :  standings      (prod; PK includes group_label)
"""

from __future__ import annotations
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────────────────────────────────────────────────────────
# Shared env / paths (Airflow Variables)
#   - PYTHON: path to your venv python
#   - PG_CONN: (optional) if any script shells out to psql (not required here)
#   - FOOTY_ROOT: repo root (absolute)
#   - STANDINGS_GRACE_DAYS: int, default 7 (post-season buffer)
# You can define these in the Airflow UI → Admin → Variables
# ─────────────────────────────────────────────────────────────────────────────
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
    # pass to loader via env; fetch takes it as a CLI flag
    "STANDINGS_GRACE_DAYS": "{{ var.value.get('STANDINGS_GRACE_DAYS', 7) | string }}",
    # optional one-off limit for BOTH fetch & load (Trigger DAG → conf: {"league_ids":"244,253,71"})
    "LEAGUE_IDS": "{{ dag_run.conf.get('league_ids', '') }}",
}

ROOT = "{{ var.value.FOOTY_ROOT }}"

logging.getLogger(__name__).info("standings_full_etl DAG file parsed successfully")

with DAG(
    dag_id="standings_etl",
    description="ETL for league standings (DB-driven, group-aware, newest snapshot only)",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    # Schedule:
    #   Nightly 02:40 UTC → "40 2 * * *"
    #   Weekly (Sun 02:40 UTC) → "40 2 * * 0"  or use @weekly
    schedule="0 3 * * MON",
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "standings", "full", "group-aware"],
    is_paused_upon_creation=True,
) as dag:

    # 1) Fetch the current standings snapshots (DB-driven scope; no preseason)
    fetch_standings = BashOperator(
        task_id="fetch_standings",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/blob/fetch_data/fetch_standings.py "
            "--mode full "
            "--grace-days {{ var.value.get('STANDINGS_GRACE_DAYS', 7) }} "
            "{% if dag_run.conf.get('league_ids') %}--league-ids {{ dag_run.conf['league_ids'] }}{% endif %}"
        ),
        env=ENV,
    )


    # 2) Truncate staging (standings) before load
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/proc_calls/cleanup_stg.py --table stg_standings"
        ),
        env=ENV,
        doc_md="Truncate `stg_standings`.",
    )

    # 3) Load newest snapshots into staging (group-aware; single latest file per league-season)
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/blob/load_data/load_stg_standings.py"
        ),
        env=ENV,  # passes STANDINGS_GRACE_DAYS and optional LEAGUE_IDS
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=pendulum.duration(minutes=20),
        doc_md="""
        Loads the **newest** snapshot (full_ or inc_) per league‑season into `stg_standings`.
        Group‑aware: stores `group_label` so multiple tables per season are distinct.
        """,
    )

    # 4) Validate staging (math checks, duplicates by group, FK, etc.)
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/proc_calls/check_stg_standings.py"
        ),
        env=ENV,
        doc_md="Runs `CALL check_stg_standings()` and prints a per‑rule summary.",
    )

    # 5) Upsert into production standings (true‑diff updates only)
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/proc_calls/update_standings.py"
        ),
        env=ENV,
        doc_md="""
        Executes `CALL update_standings()`:
        * Inserts new rows (sets `load_date`, `upd_date`).
        * Updates only when values truly changed (`upd_date` bumps).
        * Prints summary: “N inserted, M updated”.
        """,
    )

    # 6) Terminal success node
    finished = EmptyOperator(task_id="finished")

    # Graph
    fetch_standings >> cleanup_stg >> load_to_stg >> check_stg >> update_main_table >> finished
