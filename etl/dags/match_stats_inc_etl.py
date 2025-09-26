"""
match_stats_incremental_etl.py
──────────────────────────────────────────────────────────────────────────────
Scheduled pipeline that

1. pulls the last *N* days of finished fixtures from API-Football  
2. writes JSON files to Azure Blob under incremental/YYYY-MM-DD/  
3. stages them in Postgres (`stg_match_statistics`)  
4. merges into the production table (`match_statistics`)

Change the cron or the Airflow Variable **MATCH_STATS_LOOKBACK** to alter
how often and how far back you pull data.
"""
# ───────────────────────────── Imports ──────────────────────────────────────
from __future__ import annotations
import logging
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────── Global constants ──────────────────────────────
# ENV → passed to *every* BashOperator so the command lines stay short
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",    # full path to venv  python
    "PG_CONN": "{{ var.value.PG_CONN }}",  # psql conn-string for loaders
}

ROOT = "{{ var.value.FOOTY_ROOT }}"        # absolute project root

# Emit one log line at DAG-parse time; helps debug “DAG Import Error”
logging.getLogger(__name__).info("match_stats_incremental_etl DAG parsed OK")

# ───────────────────────────── DAG object ───────────────────────────────────
with DAG(
    dag_id="match_stats_inc_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    description="Weekly incremental ETL for match_statistics",
    # Monday 06:15 UTC  →  adjust to daily or another time as needed
    schedule="15 06 * * 1",
    catchup=False,                          # don’t back-fill old dates
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "match_stats", "incremental"],
    is_paused_upon_creation=True,           # keep paused until you switch it on
) as dag:

    # ───────────────────── Task 1  Fetch JSONs ────────────────────────────
    # Pulls fixture IDs from DB (rolling N-day window) then hits the API
    fetch_incremental_snapshot = BashOperator(
        task_id="fetch_incremental_snapshot",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_match_statistics.py "
            + "--mode incremental "
            + "--season {{ var.value.CURR_SEASON }} "
            + "--lookback-days "
            + "{{ dag_run.conf.get('lookback', var.value.MATCH_STATS_LOOKBACK | default(8) | int) }}"

        ),
        env=ENV,
        doc_md="""
        Runs the stats-fetcher in **incremental** mode.

        * Window size comes from Airflow Variable `MATCH_STATS_LOOKBACK`
          (default 8 days).
        * Each run saves JSON to  
          `raw/match_statistics/incremental/<run-date>/statistics_<id>.json`.
        """,
    )

    # ───────────────────── Task 2  Truncate staging ───────────────────────
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/cleanup_stg.py "
            + "--table stg_match_statistics"
        ),
        env=ENV,
        # Simple TRUNCATE; runs even if previous task had zero rows
    )

    # ───────────────────── Task 3  Load → stg_match_statistics ────────────
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/load_data/load_stg_match_statistics.py "
            + "--incremental"                   

        ),
        env={**ENV, "CURR_SEASON": "{{ var.value.CURR_SEASON }}"},
        # Only run if upstream tasks succeeded
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=pendulum.duration(minutes=20),
        doc_md="""
        Reads the **latest** incremental folder (based on run date)
        and bulk-loads JSON rows into `stg_match_statistics` using COPY.
        """,
    )

    # ───────────────────── Task 4  Basic data validation ──────────────────
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/check_match_statistics.py"
        ),
        env=ENV,
        # Checks row-count, NULLs, PK duplicates, etc.
    )

    # ───────────────────── Task 5  Merge into prod ────────────────────────
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/insert_match_statistics.py"
        ),
        env=ENV,
        doc_md="Calls the stored procedure `insert_match_statistics()`.",
    )



    # ─────────────── Dependency graph (left → right) ──────────────────────
    fetch_incremental_snapshot >> cleanup_stg >> load_to_stg >> check_stg \
        >> update_main_table
