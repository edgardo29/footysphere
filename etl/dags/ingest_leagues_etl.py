"""
ingest_leagues_dag.py
─────────────────────
Main league ingest:

1. fetch_leagues.py – queries league_catalog (enabled rows) and
   downloads any missing league/season blobs.

2. load_leagues.py  – reads the blobs and upserts into
   `leagues` + `league_seasons`.
"""
# ───────────────────────────── Imports ───────────────────────────────
from __future__ import annotations
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"

logging.getLogger(__name__).info("ingest_leagues_dag parsed OK")

with DAG(
    dag_id="ingest_leagues_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Kick off 30 min after discover (02:30), then hourly until 20:30
    schedule="30 2-20/1 * * *",
    catchup=False,
    default_args={"owner": "footysphere", "retries": 1},
    tags=["footysphere", "leagues", "ingest"],
) as dag:

    # 1) Download new or missing blobs
    fetch_blobs = BashOperator(
        task_id="fetch_league_blobs",
        bash_command=(
            "$PYTHON " + ROOT + "/src/blob/fetch_data/fetch_leagues.py"
        ),
        env=ENV,
        execution_timeout=pendulum.duration(minutes=30),
        doc_md="""
        • Reads `league_catalog` for enabled leagues.  
        • Downloads `/leagues?id=<id>&season=<year>` only when the blob
          is missing (or overwritten via flag).""",
    )

    # 2) Load blobs into Postgres
    load_to_db = BashOperator(
        task_id="load_blobs_to_db",
        bash_command=(
            "$PYTHON " + ROOT + "/src/blob/load_data/load_leagues.py"
        ),
        env=ENV,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=pendulum.duration(minutes=30),
        doc_md="Upserts into `leagues` and `league_seasons`.",
    )

    fetch_blobs >> load_to_db
