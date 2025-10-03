# match_stats_etl.py
# ─────────────────────────────────────────────────────────────────────────────
# Combined ETL for match statistics.
# - Scheduled runs default to incremental mode (uses Airflow vars).
# - Manual trigger can run initial or incremental via dag_run.conf.
#
# Trigger examples (Airflow UI → Trigger DAG → “Conf”):
#   { "mode": "initial", "season": 2025 }
#   { "mode": "incremental", "season": 2025, "lookback": 8, "league_id": 39 }

from __future__ import annotations
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

ENV  = {"PYTHON": "{{ var.value.PYTHON }}",
        "PG_CONN": "{{ var.value.PG_CONN }}"}
ROOT = "{{ var.value.FOOTY_ROOT }}"

logging.getLogger(__name__).info("match_stats_etl DAG parsed OK")

with DAG(
    dag_id="match_stats_etl",
    description="Combined initial + incremental ETL for match_statistics",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Weekly schedule (incremental). Manual triggers can run initial.
    schedule="15 06 * * 1",
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "match_stats"],
    is_paused_upon_creation=True,
) as dag:

    # 1) Fetch stats JSONs (mode + season come from conf with sensible defaults)
    fetch_stats = BashOperator(
        task_id="fetch_stats",
        bash_command="""
{{ env.PYTHON }} {{ params.root }}/src/blob/fetch_data/fetch_match_statistics.py \
  --mode {{ dag_run.conf.get('mode', 'incremental') }} \
  --season {{ dag_run.conf.get('season', var.value.CURR_SEASON) }} \
  {% if dag_run.conf.get('mode', 'incremental') == 'incremental' -%}
  --lookback-days {{ dag_run.conf.get('lookback', (var.value.MATCH_STATS_LOOKBACK | default(8) | int)) }} \
  {%- endif -%}
  {% if dag_run.conf.get('league_id') -%}
  --league-id {{ dag_run.conf['league_id'] }} \
  {%- endif -%}
""",
        env=ENV,
        params={"root": ROOT},
        doc_md="""
**Fetch JSON** from API-Football.

- `mode`: from `dag_run.conf.mode` (default: `incremental`)
- `season`: from `dag_run.conf.season` (default: `var.CURR_SEASON`)
- `lookback`: used only for incremental (default: `var.MATCH_STATS_LOOKBACK`, fallback 8)
- optional `league_id` supported
""",
    )

    # 2) Truncate staging (fresh run each time)
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="{{ env.PYTHON }} " + ROOT + "/src/procs/cleanup_stg.py --table stg_match_statistics",
        env=ENV,
    )

    # 3) Load JSON → staging
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command="""
{{ env.PYTHON }} {{ params.root }}/src/blob/load_data/load_stg_match_statistics.py \
  --mode {{ dag_run.conf.get('mode', 'incremental') }} \
  --season {{ dag_run.conf.get('season', var.value.CURR_SEASON) }} \
  {% if dag_run.conf.get('league_id') -%}
  --league-id {{ dag_run.conf['league_id'] }} \
  {%- endif -%}
""",
        env=ENV,
        params={"root": ROOT},
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=pendulum.duration(minutes=40),
        doc_md="Load the **latest per-league run folder** into `stg_match_statistics`.",
    )

    # 4) Validate staging (compact summary, includes “fixtures missing 0 rows”)
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="{{ env.PYTHON }} " + ROOT + "/src/procs/check_stg_match_statistics.py",
        env=ENV,
        doc_md="Runs `CALL check_stg_match_statistics()` and prints a short summary.",
    )

    # 5) Promote complete fixtures → prod
    update_main = BashOperator(
        task_id="update_main",
        bash_command="{{ env.PYTHON }} " + ROOT + "/src/procs/update_match_statistics.py",
        env=ENV,
        doc_md="Runs `CALL update_match_statistics()` (only 2-row, valid, finished fixtures).",
    )

    finished = EmptyOperator(task_id="finished")

    fetch_stats >> cleanup_stg >> load_to_stg >> check_stg >> update_main >> finished
