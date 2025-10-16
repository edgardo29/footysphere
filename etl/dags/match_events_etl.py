"""
match_events_etl.py

DAG graph
┌────────────────────┐
│   fetch_events     │   $PYTHON $ROOT/src/blob/fetch_data/fetch_match_events.py \
└─────────┬──────────┘       --mode incremental --lookback-days 8
          │
┌─────────▼──────────┐
│   cleanup_stg      │   $PYTHON $ROOT/src/procs/cleanup_stg.py --table stg_match_events
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│   load_to_stg      │   $PYTHON $ROOT/src/blob/load_data/load_stg_match_events.py \
└─────────┬──────────┘       --mode incremental
          │
┌─────────▼──────────┐
│     check_stg      │   $PYTHON $ROOT/src/procs/check_stg_match_events.py
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│  update_main_table │   $PYTHON $ROOT/src/procs/update_match_events.py
└────────────────────┘

Notes
- Fetch writes a fresh snapshot every run to:
  raw/match_events/<league_folder>/<season_folder>/incremental/incremental_<YYYYMMDD_HHMMSS>Z/events_<fixture>.json
  (season & league_folder resolved from DB; no --season needed)
- Loader auto-selects the latest snapshot per league/season under the incremental/ branch.
- Weekly cadence covers the prior week with --lookback-days 8 (captures late corrections).
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Reusable environment
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",   # path to venv interpreter
    "PG_CONN": "{{ var.value.PG_CONN }}", # Postgres conn string
}
ROOT = "{{ var.value.FOOTY_ROOT }}"       # project root (variable)

with DAG(
    dag_id="match_events_etl",
    description="↓ Match events → STG → FACT (weekly incremental; season resolved per-league from DB)",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 6 * * MON",                 # Monday 06:00 UTC
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "match_events", "weekly"],
) as dag:

    # 1) Fetch latest events (incremental snapshot; no --season needed)
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_match_events.py "
            + "--mode incremental "
            + "--lookback-days 8"
            # To target specific leagues, append: + " --league-ids 94 203"
        ),
        env=ENV,
    )

    # 2) Truncate staging before load (idempotent weekly refresh)
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/proc_calls/cleanup_stg.py --table stg_match_events"
        ),
        env=ENV,
    )

    # 3) Load latest incremental snapshot (DB resolves leagues & seasons)
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/load_data/load_stg_match_events.py "
            + "--mode incremental"
            # If you restricted leagues in fetch, mirror it here:
            # + " --league-ids 94 203"
        ),
        env=ENV,
    )

    # 4) Validate freshly loaded rows
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/proc_calls/check_stg_match_events.py"
        ),
        env=ENV,
    )

    # 5) Merge into production table
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/proc_calls/update_match_events.py"
        ),
        env=ENV,
    )

    # 6) Done marker
    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.ALL_SUCCESS)

    # Dependencies
    fetch_events >> cleanup_stg >> load_to_stg >> check_stg >> update_main_table >> done
