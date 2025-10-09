"""
fixtures_full_etl.py  (DB-driven: manual FULL or scheduled INC)

DAG graph
┌────────────────────┐
│  fetch_full/INC    │   python fetch_fixtures.py --mode full
│                    │   python fetch_fixtures.py --mode inc --days <N>
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    cleanup_stg     │   python cleanup_stg.py --table stg_fixtures
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    load_to_stg     │   python load_stg_fixtures.py --mode baseline|inc [--league-id <ID>]
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│     check_stg      │   python check_stg_fixtures.py
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│ update_main_table  │   python update_fixtures.py
└────────────────────┘

Conf (UI/CLI)
  - {"mode":"full"}              → Full onboarding path (baseline loader)
  - {"mode":"inc","days":14}     → Incremental (default 14 if not given)
  - Optional: {"league_id": 94}  → Target a single league for either path

Rules (DB is the source of truth)
  - fetch_fixtures.py:
       FULL → picks leagues with full_done=FALSE
       INC  → picks leagues with full_done=TRUE
  - load_stg_fixtures.py:
       --mode baseline → baseline_loaded=FALSE (loads /full then newer /inc; flips baseline_loaded→TRUE)
       --mode inc      → baseline_loaded=TRUE  (loads newest /inc only)
"""
from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Minimal vars; provide sane defaults if the Airflow Variables aren’t set
ENV = {
    "PYTHON": "{{ var.value.PYTHON | default('/home/football_vmadmin/footysphere-repo/etl_venv/bin/python') }}",
    "PG_CONN": "{{ var.value.PG_CONN | default('postgresql+psycopg://user:pass@host:5432/dbname') }}",
}
ROOT = "{{ var.value.FOOTY_ROOT | default('/home/football_vmadmin/footysphere-repo/etl') }}"

DEFAULT_INC_DAYS = 14  # used only if conf.days not supplied

with DAG(
    dag_id="fixtures_etl",
    description="FULL (manual) or INC (scheduled) fixtures → STG → CHECK → MERGE. DB-driven selection.",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * MON",     # weekly INC by default
    catchup=False,
    default_args={"owner": "footysphere", "retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["footysphere", "fixtures", "full", "incremental"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Fetch (FULL vs INC decided by conf.mode; INC days taken from conf.days or DEFAULT_INC_DAYS)
    fetch_full = BashOperator(
        task_id="fetch_full",
        bash_command=(
            'MODE="{{ dag_run.conf.get(\'mode\', \'inc\') }}"; '
            'DAYS="{{ dag_run.conf.get(\'days\', ' + str(DEFAULT_INC_DAYS) + ') }}"; '
            'LEAGUE="{{ dag_run.conf.get(\'league_id\', \'\') }}"; '
            'if [ "$MODE" = "full" ]; then '
            '  echo "Running FULL baseline fetch"; '
            '  $PYTHON ' + ROOT + '/src/blob/fetch_data/fetch_fixtures.py --mode full; '
            'else '
            '  echo "Running INC fetch --days $DAYS"; '
            '  if [ -n "$LEAGUE" ]; then '
            '    $PYTHON ' + ROOT + '/src/blob/fetch_data/fetch_fixtures.py --mode inc --days "$DAYS"; '
            '  else '
            '    $PYTHON ' + ROOT + '/src/blob/fetch_data/fetch_fixtures.py --mode inc --days "$DAYS"; '
            '  fi; '
            'fi'
        ),
        env=ENV,
        doc_md="Writes full_YYYYMMDD.json (FULL) or inc_YYYYMMDDTHHMMZ.json (INC) to Blob.",
    )

    # 2) Truncate staging
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/cleanup_stg.py --table stg_fixtures",
        env=ENV,
        doc_md="Truncate stg_fixtures prior to load.",
    )

    # 3) Load to staging (baseline vs inc decided by conf.mode; optional league_id override)
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            'MODE="{{ dag_run.conf.get(\'mode\', \'inc\') }}"; '
            'LEAGUE="{{ dag_run.conf.get(\'league_id\', \'\') }}"; '
            'if [ "$MODE" = "full" ]; then '
            '  echo "Loading BASELINES (baseline_loaded=FALSE leagues)"; '
            '  if [ -n "$LEAGUE" ]; then '
            '    $PYTHON ' + ROOT + '/src/blob/load_data/load_stg_fixtures.py --mode baseline --league-id "$LEAGUE"; '
            '  else '
            '    $PYTHON ' + ROOT + '/src/blob/load_data/load_stg_fixtures.py --mode baseline; '
            '  fi; '
            'else '
            '  echo "Loading INCREMENTALS (baseline_loaded=TRUE leagues)"; '
            '  if [ -n "$LEAGUE" ]; then '
            '    $PYTHON ' + ROOT + '/src/blob/load_data/load_stg_fixtures.py --mode inc --league-id "$LEAGUE"; '
            '  else '
            '    $PYTHON ' + ROOT + '/src/blob/load_data/load_stg_fixtures.py --mode inc; '
            '  fi; '
            'fi'
        ),
        env=ENV,
        doc_md="Baseline mode loads newest /full then newer /inc; INC mode loads newest /inc only.",
    )

    # 4) Validate staging
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/check_stg_fixtures.py",
        env=ENV,
        doc_md="Run data quality checks; log issues to data_load_errors.",
    )

    # 5) Merge into prod
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command="$PYTHON " + ROOT + "/src/procs/update_fixtures.py",
        env=ENV,
        doc_md="MERGE stg_fixtures → fixtures (idempotent).",
    )

    fetch_full >> cleanup_stg >> load_to_stg >> check_stg >> update_main_table
