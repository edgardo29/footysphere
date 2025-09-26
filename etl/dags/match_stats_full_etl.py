"""
match_stats_initial_load.py
──────────────────────────────────────────────────────────────────────────────
Back-fills an **entire season** of match statistics.

Usage
-----
Airflow UI → Trigger DAG → “Conf” →  `{ "season": 2024 }`
"""
from __future__ import annotations
import logging, pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

ENV  = {"PYTHON": "{{ var.value.PYTHON }}",
        "PG_CONN": "{{ var.value.PG_CONN }}"}
ROOT = "{{ var.value.FOOTY_ROOT }}"

logging.getLogger(__name__).info("match_stats_initial_load DAG parsed OK")

with DAG(
    dag_id="match_stats_initial_load",
    description="One-off full-season back-fill for match_statistics",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,                      # never runs on a schedule
    catchup=False,
    tags=["footysphere", "match_stats", "initial"],
    is_paused_upon_creation=True,       # stays paused until manually triggered
) as dag:

    # 1  Fetch the entire season (season passed via dag_run.conf)
    fetch_full_season = BashOperator(
        task_id="fetch_full_season",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_match_statistics.py "
            + "--mode initial "
            + "--season {{ dag_run.conf['season'] }}"
        ),
        env=ENV,
        doc_md="Downloads stats for **every** finished fixture in that season.",
    )

    # 2  Truncate staging table
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON "
        + ROOT + "/src/procs/cleanup_stg.py --table stg_match_statistics",
        env=ENV,
    )

    # 3  Load JSON → stg_match_statistics
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command="$PYTHON "
        + ROOT + "/src/blob/load_data/load_stg_match_statistics.py",
        env=ENV,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=pendulum.duration(minutes=40),
    )

    # 4  Validate staging rows
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON "
        + ROOT + "/src/procs/check_match_statistics.py",
        env=ENV,
    )

    # 5  Merge into prod
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command="$PYTHON "
        + ROOT + "/src/procs/update_match_statistics.py",
        env=ENV,
        doc_md="Executes `CALL update_match_statistics()`.",
    )

    finished = EmptyOperator(task_id="finished")

    fetch_full_season >> cleanup_stg >> load_to_stg >> check_stg \
        >> update_main_table >> finished
