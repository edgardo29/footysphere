# match_stats_etl.py
# Combined ETL for match statistics (DB-resolved season/league).
# Weekly schedule defaults to incremental. Manual trigger can pass JSON Conf.

from __future__ import annotations
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",       # e.g. /home/.../etl_venv/bin/python
    "PG_CONN": "{{ var.value.PG_CONN }}",
    "FOOTY_ROOT": "{{ var.value.FOOTY_ROOT }}",  # e.g. /home/.../footysphere-repo/etl
}

logging.getLogger(__name__).info("match_stats_etl DAG parsed OK")

with DAG(
    dag_id="match_stats_etl",
    description="Combined initial + incremental ETL for match_statistics (DB-resolved season/league).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="15 06 * * 1",  # weekly
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "match_stats"],
    is_paused_upon_creation=True,
) as dag:

    # Derive mode/league_id safely from dag_run.conf (or defaults)
    JINJA_HEADER = r"""
{% set _conf = dag_run.conf if dag_run and dag_run.conf else {} %}
{% set mode = _conf.get('mode', 'incremental') %}
{% set league_id = _conf.get('league_id') %}
"""

    # 1) Fetch stats JSONs → Azure Blob
    fetch_stats = BashOperator(
        task_id="fetch_stats",
        bash_command=JINJA_HEADER + r"""
$PYTHON $FOOTY_ROOT/src/blob/fetch_data/fetch_match_statistics.py --mode {{ mode }}{% if league_id %} --league-id {{ league_id }}{% endif %}
""",
        env=ENV,
        doc_md="Fetches match statistics (only --mode and optional --league-id). Season/aliases resolved from DB.",
    )

    # 2) Truncate staging (fresh each run)
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON $FOOTY_ROOT/src/procs/cleanup_stg.py --table stg_match_statistics",
        env=ENV,
    )

    # 3) Load latest per-league run folder → staging
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=JINJA_HEADER + r"""
$PYTHON $FOOTY_ROOT/src/blob/load_data/load_stg_match_statistics.py --mode {{ mode }}{% if league_id %} --league-id {{ league_id }}{% endif %}
""",
        env=ENV,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        execution_timeout=pendulum.duration(minutes=40),
        doc_md="Loads latest run under raw/match_statistics/<alias>/<YYYY_YY>/<mode>/<stamp>/ into stg_match_statistics.",
    )

    # 4) Validate staging
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON $FOOTY_ROOT/src/procs/check_stg_match_statistics.py",
        env=ENV,
        doc_md="Runs CALL check_stg_match_statistics(); prints compact summary.",
    )

    # 5) Promote complete fixtures → prod
    update_main = BashOperator(
        task_id="update_main",
        bash_command="$PYTHON $FOOTY_ROOT/src/procs/update_match_statistics.py",
        env=ENV,
        doc_md="Runs CALL update_match_statistics() (only 2-row, valid, finished fixtures).",
    )

    finished = EmptyOperator(task_id="finished")

    fetch_stats >> cleanup_stg >> load_to_stg >> check_stg >> update_main >> finished
