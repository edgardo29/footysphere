"""
match_events_etl.py   

DAG graph
┌────────────────────┐
│   fetch_events     │        python fetch_match_events.py --season <auto-season>
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│   cleanup_stg      │        python cleanup_stg.py --table stg_match_events
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│   load_to_stg      │        python batch_load_stg_match_events.py
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│     check_stg      │        python check_match_events.py
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│  update_main_table │        CALL update_match_events();
└────────────────────┘

Season logic ➜ The DAG derives the **season_start_year** automatically:
if the (weekend) data interval falls in **July or later**, season = current year; otherwise season = year − 1. This removes all manual edits when the new season starts.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── reusable environment: so we don't repeat long paths in every task ──
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",   # path to venv interpreter
    "PG_CONN": "{{ var.value.PG_CONN }}", # Postgres conn string
}

ROOT = "{{ var.value.FOOTY_ROOT }}"         # project root (variable)

# ── helper Jinja to compute season without a Variable ──────────────────
SEASON_JINJA = (
    "{{ ((data_interval_start - macros.timedelta(days=2)).year - 1) "
    "if ((data_interval_start - macros.timedelta(days=2)).month < 7) "
    "else (data_interval_start - macros.timedelta(days=2)).year }}"
)

with DAG(
    dag_id="match_events_full_etl",
    description="↓ Match events → STG → FACT (weekly, season auto-detect)",
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

    # ── 1 · Fetch latest events to blob ────────────────────────────────
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_match_events.py "
            + "--season " + SEASON_JINJA + " "
            + "--mode incremental "
            + "--run-date {{ ds }} "
            + "--lookback-days 8"
        ),
        env=ENV,
    )

    # ── 2 · Truncate staging table before re-loading it ────────────────
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/cleanup_stg.py --table stg_match_events"
        ),
        env=ENV,
    )

    # ── 3 · Load the newest blob file into staging ────────────────────
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/load_data/batch_load_stg_match_events.py "
            + "--stage_folder 'incremental/{{ ds }}'"
        ),
        env=ENV,
    )

    # ── 4 · Validate the freshly-loaded rows ──────────────────────────
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/check_match_events.py"
        ),
        env=ENV,
    )

    # ── 5 · Merge into production match_events table via stored proc ──
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/procs/update_match_events.py"
        ),
        env=ENV,
    )

    # ── 6 · Optional success marker for downstream DAG dependencies ───
    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.ALL_SUCCESS)

    # ── dependency graph ──
    fetch_events >> cleanup_stg >> load_to_stg >> check_stg >> update_main_table >> done
