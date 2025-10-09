"""
player_season_stats_snapshot_etl.py  (Airflow 2.8-safe: manual snapshot load)

DAG graph
┌────────────────────┐
│    cleanup_stg     │   python cleanup_stg.py --table stg_player_season_stats
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│    load_to_stg     │   python load_stg_player_season_stats.py
│                    │   --window <summer|winter|latest|label>
│                    │   [--league_id <ID> | --league_ids "<ID1,ID2>"]
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│     check_stg      │   python call_check_stg_player_season_stats.py  (PROC)
└─────────┬──────────┘
          │
┌─────────▼──────────┐
│   merge_to_main    │   python call_update_player_season_stats.py     (PROC)
└────────────────────┘

Purpose
  Load player season stats from an existing *Blob snapshot* → staging → validate → upsert to prod.
  • Strict league filter (only statistics blocks where league.id == league_id).
  • DB-driven discovery of leagues/seasons; snapshot resolved by --window.
  • No dependency on player_stats_load_state.

Trigger
  Manual runs only (2× per season). Examples:
    {"window":"summer"}                      # default if omitted
    {"window":"winter"}
    {"window":"latest"}
    {"window":"summer_20250930_2148"}        # explicit label
    {"window":"summer","league_id":39}       # single league
    {"window":"summer","league_ids":"39,78"} # subset
"""

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Reusable environment
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"  # e.g., /home/football_vmadmin/footysphere-repo/etl

with DAG(
    dag_id="player_season_stats_etl",
    description="Snapshot → STG → CHECK → MERGE for player_season_stats (strict, manual).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,                      # manual only
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 0,
    },
    tags=["footysphere", "player_stats", "snapshot"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Truncate staging table before reload.
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/cleanup_stg.py --table stg_player_season_stats",
        env=ENV,
        doc_md="Truncate **stg_player_season_stats** prior to load.",
    )

    # 2) Load Blob → staging (DB-driven loader picks leagues/seasons & resolves snapshot by --window).
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=(
            "$PYTHON " + ROOT + "/src/blob/load_data/load_stg_player_season_stats.py "
            "--window {{ dag_run.conf.get('window', 'summer') }}"
            "{% if dag_run.conf.get('league_id') %} --league_id {{ dag_run.conf['league_id'] }}{% endif %}"
            "{% if dag_run.conf.get('league_ids') %} --league_ids {{ dag_run.conf['league_ids'] }}{% endif %}"
            # strict competition by default (do NOT pass --no_strict_competition)
        ),
        env=ENV,
        doc_md=(
            "Reads `players/<folder>/<season>/<snapshot>/player_details/*.json` and inserts into staging. "
            "Strict filter keeps only statistics blocks for the target league."
        ),
    )

    # 3) Validate staging (PROC wrapper prints vertical summary + per-rule counts).
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/check_stg_player_season_stats.py ",
        env=ENV,
        doc_md="Run DQ checks; write issues to **data_load_errors**; print per-rule counts.",
    )

    # 4) Upsert staging → prod (PROC wrapper prints inserted/updated totals).
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command="$PYTHON " + ROOT + "/src/procs/update_player_season_stats.py ",
        env=ENV,
        doc_md="MERGE **stg_player_season_stats** → **player_season_stats** (idempotent).",
    )

    cleanup_stg >> load_to_stg >> check_stg >> update_main_table
