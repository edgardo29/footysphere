"""
fixtures_full_etl.py  (Airflow 2.8-safe: manual FULL or scheduled INC)

DAG graph
┌────────────────────┐
│  fetch_full/INC    │   python fetch_fixtures.py --mode full  (manual run)
│                    │   python fetch_fixtures.py --mode inc --days <N> (scheduled)
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    cleanup_stg     │   python cleanup_stg.py --table stg_fixtures
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    load_to_stg     │   python load_stg_fixtures.py
│                    │   or python load_stg_fixtures.py --league-id <ID> (INC loop)
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
  Create the season’s first full snapshot for leagues needing a baseline (manual),
  OR run weekly incrementals (scheduled) that fetch deltas, load staging, validate,
  and merge into prod. Your fetch script is responsible for flipping full_done=TRUE
  per league when FULL completes.

Trigger
  • Scheduled run → INC (uses FIXTURES_INC_DAYS & LEAGUE_IDS)
  • Manual run with conf {"mode":"full"} → FULL baseline path
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
# Incremental config (Airflow Variables)
LEAGUE_IDS = "{{ var.value.LEAGUE_IDS }}"  # e.g. "39 140 78 61 135 71 253 244"
INC_DAYS = "{{ var.value.FIXTURES_INC_DAYS | default(30) }}"

with DAG(
    dag_id="fixtures_full_etl",
    description="FULL (manual) or INC (scheduled) fixtures → STG → CHECK → MERGE.",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * MON",                 # ← weekly schedule for INC; FULL is manual via conf
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "fixtures", "full", "incremental"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Fetch fresh snapshot to Blob.
    #    • Manual with conf {"mode":"full"} → FULL
    #    • Otherwise (scheduled)           → INC with date window
    fetch_full = BashOperator(
        task_id="fetch_full",
        bash_command=(
            'if [ "{{ dag_run.conf.get(\'mode\', \'inc\') }}" = "full" ]; then '
            # FULL baseline
            '  echo "Running FULL baseline fetch"; '
            '  $PYTHON ' + ROOT + '/src/blob/fetch_data/fetch_fixtures.py --mode full; '
            'else '
            # INCREMENTAL
            '  echo "Running INC fetch --days ' + INC_DAYS + '"; '
            '  $PYTHON ' + ROOT + '/src/blob/fetch_data/fetch_fixtures.py --mode inc --days ' + INC_DAYS + '; '
            'fi'
        ),
        env=ENV,
        doc_md=(
            "Writes **full_YYYYMMDD.json** (FULL) or **inc_YYYYMMDDTHHMMZ.json** (INC) "
            "into league-season folders under Blob."
        ),
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

    # 3) Load newest baseline or newest incremental blob(s) into staging.
    #    • FULL → loader with no args (auto-picks baselines)
    #    • INC  → loop over LEAGUE_IDS and pass --league-id (loader now keeps newest inc only)
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            'if [ "{{ dag_run.conf.get(\'mode\', \'inc\') }}" = "full" ]; then '
            '  echo "Loading baselines into staging"; '
            '  $PYTHON ' + ROOT + '/src/blob/load_data/load_stg_fixtures.py; '
            'else '
            '  echo "Loading incrementals into staging for leagues: ' + LEAGUE_IDS + '"; '
            '  for L in ' + LEAGUE_IDS + '; do '
            '    echo "  → league $L"; '
            '    $PYTHON ' + ROOT + '/src/blob/load_data/load_stg_fixtures.py --league-id $L || exit 1; '
            '  done; '
            'fi'
        ),
        env=ENV,
        doc_md=(
            "Load latest **baseline** (FULL) or the **newest inc** per league (INC) into stg_fixtures."
        ),
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
