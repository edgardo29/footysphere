"""
discover_league_directory_dag.py
────────────────────────────────
Nightly snapshot:

1. Call discover_leagues.py  → writes
   raw/league_directory/<season>/leagues.json

2. Call load_league_directory.py → upserts into league_directory
"""
# ───────────────────────────── Imports ───────────────────────────────
from __future__ import annotations
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# ─────────────── Global constants (reuse project Variables) ──────────
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"

logging.getLogger(__name__).info("discover_league_directory_dag parsed OK")

# ───────────────────────────── DAG object ────────────────────────────
with DAG(
    dag_id="discover_league_directory",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # 02:00 UTC daily – adjust as needed
    schedule="0 2 1 * *",
    catchup=False,
    default_args={"owner": "footysphere", "retries": 1},
    tags=["footysphere", "leagues", "directory"],
) as dag:

    # 1) Fetch one big JSON snapshot for current season year
    discover_snapshot = BashOperator(
        task_id="discover_snapshot",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/blob/fetch_data/discover_leagues.py "
            + "--season {{ var.value.CURR_SEASON }}"
        ),
        env=ENV,
        doc_md="Fetch `/leagues?season=<CURR_SEASON>` and store blob.",
    )

    # 2) Load / upsert into league_directory
    load_directory = BashOperator(
        task_id="load_directory",
        bash_command=(
            "$PYTHON "
            + ROOT
            + "/src/blob/load_data/load_league_directory.py "
            + "--season {{ var.value.CURR_SEASON }}"
        ),
        env=ENV,
        doc_md="Upsert the snapshot into `league_directory`.",
    )

    discover_snapshot >> load_directory
