#!/usr/bin/env python
"""
teams_venues_weekly_etl.py
────────────────────────────────────────────────────────────────────────────
Weekly ETL that keeps **teams, venues, and the team-league bridge** in sync.

Required prerequisite
─────────────────────
Your separate “League-Directory” pipeline has already inserted
(league_id, season_year) rows into **league_seasons**.  
This DAG starts from there.

Correct task order (agreed 2025-07-07)
──────────────────────────────────────
    1. fetch_teams          ──>  download /teams blobs only
    2. venues lane          ──>  stg_venues → check → merge
    3. teams lane           ──>  stg_teams  → check (FK OK) → merge
    4. load_team_league_seasons
    5. venue geo back-fill  ──>  fetch_missing_venues → load_venue_fills
    6. done ✓
"""

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash  import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Airflow Variables (set once in UI) ─────────────────────────────────
ENV  = {
    "PYTHON": "{{ var.value.PYTHON }}",   # /home/.../footysphere_venv/bin/python
    "PG_CONN": "{{ var.value.PG_CONN }}", # postgres://user:pw@host/db
}
ROOT = "{{ var.value.FOOTY_ROOT }}"       # repo root, e.g. /home/footysphere

# auto-derive season start year
SEASON_JINJA = (
    "{{ ((data_interval_start - macros.timedelta(days=2)).year - 1) "
    "if ((data_interval_start - macros.timedelta(days=2)).month < 7) "
    "else (data_interval_start - macros.timedelta(days=2)).year }}"
)

with DAG(
    dag_id="teams_venues_etl",
    description="Teams & Venues ETL – assumes league_seasons already loaded",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="30 5 * * MON",     # Monday 05:30 UTC
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "teams", "venues", "weekly"],
) as dag:

    # ── 1 · Fetch teams blobs (no DB work yet) ─────────────────────────
    fetch_teams = BashOperator(
        task_id="fetch_teams",
        bash_command=(
            "$PYTHON " + ROOT + "/src/blob/fetch_data/fetch_teams.py "
            "--season " + SEASON_JINJA
        ),
        env=ENV,
    )

    # ── 2 · Venues lane (stg → check → merge) ─────────────────────────
    clean_stg_venues = BashOperator(
        task_id="clean_stg_venues",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/cleanup_stg.py --table stg_venues",
        env=ENV,
    )
    load_stg_venues = BashOperator(
        task_id="load_stg_venues",
        bash_command="$PYTHON " + ROOT + "/src/blob/load_data/load_stg_venues.py",
        env=ENV,
    )
    check_stg_venues = BashOperator(
        task_id="check_stg_venues",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/check_stg_venues.py",
        env=ENV,
    )
    update_venues = BashOperator(
        task_id="update_venues",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/update_venues.py",
        env=ENV,
    )

    # ── 3 · Teams lane (now that venues exist) ────────────────────────
    clean_stg_teams = BashOperator(
        task_id="clean_stg_teams",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/cleanup_stg.py --table stg_teams",
        env=ENV,
    )
    load_stg_teams = BashOperator(
        task_id="load_stg_teams",
        bash_command="$PYTHON " + ROOT + "/src/blob/load_data/load_stg_teams.py",
        env=ENV,
    )
    check_stg_teams = BashOperator(
        task_id="check_stg_teams",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/check_stg_teams.py",
        env=ENV,
    )
    update_teams = BashOperator(
        task_id="update_teams",
        bash_command="$PYTHON " + ROOT + "/src/proc_calls/update_teams.py",
        env=ENV,
    )

    # ── 4 · Bridge table (team_league_seasons) ────────────────────────
    load_team_league_seasons = BashOperator(
        task_id="load_team_league_seasons",
        bash_command="$PYTHON " + ROOT + "/src/blob/load_data/load_team_league_seasons.py",
        env=ENV,
    )

    # ── 5 · Geo back-fill for venues ──────────────────────────────────
    fetch_missing_venues = BashOperator(
        task_id="fetch_missing_venues",
        bash_command=(
            "$PYTHON " + ROOT + "/src/blob/fetch_data/fetch_missing_venues.py "
            "--season " + SEASON_JINJA
        ),
        env=ENV,
    )
    load_venue_fills = BashOperator(
        task_id="load_venue_countries",
        bash_command="$PYTHON " + ROOT + "/src/blob/load_data/load_venue_countries.py",
        env=ENV,
    )

    # ── 6 · Success marker ────────────────────────────────────────────
    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.ALL_SUCCESS)

    # ── Dependency graph ──────────────────────────────────────────────
    (
        fetch_teams
        >> clean_stg_venues >> load_stg_venues >> check_stg_venues >> update_venues
        >> clean_stg_teams  >> load_stg_teams  >> check_stg_teams  >> update_teams
        >> load_team_league_seasons
        >> fetch_missing_venues >> load_venue_fills
        >> done
    )
