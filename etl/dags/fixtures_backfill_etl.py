"""
fixtures_backfill_etl.py  (Airflow 2.8‑safe, manual only)

DAG graph
┌────────────────────┐
│   backfill_run     │   python backfill_fixtures.py   (history → blobs)
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│      finished      │
└────────────────────┘

Purpose
  One‑time history load for brand‑new leagues. Writes full snapshots for all
  seasons (first_season..last_season) and, on success per league, flips:
    backfill_done=TRUE, full_done=TRUE
  No staging/merge here.

Trigger
  Manual only (season bootstrap for a new league).
"""

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Reusable environment (Airflow Variables)
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",   # not used here, kept for parity
}
ROOT = "{{ var.value.FOOTY_ROOT }}"        # repo/project root

with DAG(
    dag_id="fixtures_backfill_etl",
    description="One‑off history load for brand‑new leagues (blobs only).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,                          # ← manual trigger only
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "fixtures", "backfill"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Heavy history fetch → writes full snapshots; flips flags on success.
    backfill_run = BashOperator(
        task_id="backfill_run",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/backfill_fixtures.py"
        ),
        env=ENV,
        doc_md="History load to Blob; marks backfill_done=TRUE, full_done=TRUE.",
    )

    finished = EmptyOperator(task_id="finished")
    backfill_run >> finished
