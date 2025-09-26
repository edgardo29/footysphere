"""
fixtures_full_etl.py  (Airflow 2.8‑safe, manual only)

DAG graph
┌────────────────────┐
│    fetch_full      │   python fetch_fixtures.py --mode full
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    cleanup_stg     │   python cleanup_stg.py --table stg_fixtures
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    load_to_stg     │   python load_stg_fixtures.py
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│     check_stg      │   python check_stg_fixtures.py
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│ update_main_table  │   python update_fixtures.py   (calls proc inside)
└────────────────────┘

Purpose
  Create the season’s first full snapshot for leagues needing a baseline,
  then load to staging, validate, and merge into prod. Your fetch script
  is responsible for flipping full_done=TRUE per league when it completes.

Trigger
  Manual only (season rollover after setting full_done=FALSE).
"""

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Reusable environment
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"

with DAG(
    dag_id="fixtures_full_etl",
    description="Full-snapshot fixtures → STG → CHECK → MERGE (manual).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,                          # ← manual trigger only
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "fixtures", "full"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Fetch fresh full snapshot to Blob (script sets full_done=TRUE).
    fetch_full = BashOperator(
        task_id="fetch_full",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_fixtures.py --mode full"
        ),
        env=ENV,
        doc_md="Writes full_YYYY-MM-DD_HH-MM.json for leagues needing baseline.",
    )

    # 2) Truncate staging table before reload.
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/cleanup_stg.py --table stg_fixtures"
        ),
        env=ENV,
        doc_md="Truncate stg_fixtures prior to load.",
    )

    # 3) Load newest baseline blob(s) into staging.
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/load_data/load_stg_fixtures.py"
        ),
        env=ENV,
        doc_md="Load latest baseline JSON(s) → stg_fixtures.",
    )

    # 4) Validate the staged rows (dupes/NULLs/rule checks → data_load_errors).
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/check_stg_fixtures.py"
        ),
        env=ENV,
        doc_md="Run data quality checks; log issues to data_load_errors.",
    )

    # 5) Idempotent merge into prod fixtures via your Python proc wrapper.
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/update_fixtures.py"
        ),
        env=ENV,
        doc_md="Calls stored proc to MERGE stg_fixtures → fixtures (idempotent).",
    )

    fetch_full >> cleanup_stg >> load_to_stg >> check_stg >> update_main_table
