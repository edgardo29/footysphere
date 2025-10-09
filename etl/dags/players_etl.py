# players_etl.py — Manual, DB-driven, bash-only
# Uses your ETL venv ($PYTHON) everywhere and calls existing load_stg_players.py.

from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Airflow Variables expected:
#  - PYTHON: /home/football_vmadmin/footysphere-repo/etl/etl_venv/bin/python
#  - FOOTY_ROOT: /home/football_vmadmin/footysphere-repo/etl
#  - PG_CONN: postgresql://.../footysphere_db?sslmode=require
ENV = {
    "PYTHON": "{{ var.value.PYTHON }}",
    "PG_CONN": "{{ var.value.PG_CONN }}",
    "FOOTY_ROOT": "{{ var.value.FOOTY_ROOT }}",
}
ROOT = "{{ var.value.FOOTY_ROOT }}"

with DAG(
    dag_id="players_etl",
    description="Players snapshot → STG → CHECK → MERGE (manual; DB-driven; bash-only)",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # manual only
    catchup=False,
    default_args={"owner": "footysphere", "retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["footysphere", "players", "snapshot", "manual", "bash-only"],
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:

    # 1) Fetch players into Blob (window "summer"/"winter"/explicit). Optional league_ids/season_year.
    fetch_players = BashOperator(
        task_id="fetch_players",
        bash_command=(
            "$PYTHON "
            + ROOT + "/src/blob/fetch_data/fetch_players.py "
            + "--window {{ dag_run.conf.get('window', 'summer') }} "
            + "{% if dag_run.conf.get('league_ids') %}--league_ids {{ dag_run.conf.get('league_ids') }} {% endif %}"
            + "{% if dag_run.conf.get('season_year') %}--season_year {{ dag_run.conf.get('season_year') }} {% endif %}"
        ),
        env=ENV,
        doc_md="Fetch league player pages into the chosen snapshot window.",
    )

    # 2) Truncate staging
    cleanup_stg = BashOperator(
        task_id="cleanup_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/cleanup_stg.py --table stg_players",
        env=ENV,
        doc_md="TRUNCATE stg_players.",
    )

    # 3) Resolve season_str + current leagues from DB, then call existing loader per league
    load_all = BashOperator(
        task_id="load_all",
        bash_command=r'''
set -euo pipefail
echo "[load_all] using PYTHON=$PYTHON FOOTY_ROOT=$FOOTY_ROOT"

"$PYTHON" - <<'PY'
import os, sys, subprocess, pendulum, psycopg2

DSN   = os.environ["PG_CONN"]
PYEXE = os.environ["PYTHON"]
ROOT  = os.environ["FOOTY_ROOT"]

# Rendered from Airflow at runtime (optional overrides; fine if empty)
WINDOW = "{{ dag_run.conf.get('window', 'summer') }}"
LIDS   = "{{ dag_run.conf.get('league_ids', '') }}"
SOVR   = "{{ dag_run.conf.get('season_str', '') }}"

def resolve_season_str(conn, override):
    if override:
        return override
    with conn.cursor() as cur:
        cur.execute("""
            SELECT season_str
            FROM league_seasons
            WHERE is_current = TRUE AND season_str IS NOT NULL
            ORDER BY season_year DESC NULLS LAST
            LIMIT 1
        """)
        row = cur.fetchone()
    if row and row[0]:
        return row[0]
    now = pendulum.now("UTC")
    y = now.year if now.month >= 7 else now.year - 1
    return f"{y}_{(y+1)%100:02d}"

def list_league_folders(conn, league_ids_txt):
    sql = """
        SELECT
            COALESCE(lc.folder_alias, l.folder_alias,
                     lower(regexp_replace(l.league_name,'[ /&,-]+','_','g')) || '_' || ls.league_id
            ) AS league_folder
        FROM league_seasons ls
        JOIN leagues l            ON l.league_id = ls.league_id
        LEFT JOIN league_catalog lc ON lc.league_id = ls.league_id
        WHERE ls.is_current = TRUE
    """
    params = []
    ids = [int(x) for x in league_ids_txt.split(",") if x.strip()] if league_ids_txt else None
    if ids:
        sql += " AND ls.league_id = ANY(%s::int[])"
        params.append(ids)
    sql += " ORDER BY ls.league_id"
    with conn.cursor() as cur:
        cur.execute(sql, params if params else None)
        return [r[0] for r in cur.fetchall()]

with psycopg2.connect(DSN) as conn:
    season_str = resolve_season_str(conn, SOVR or None)
    folders = list_league_folders(conn, LIDS)

if not folders:
    print("[load_all] No current leagues found (league_seasons.is_current=TRUE)", file=sys.stderr)
    sys.exit(1)

print(f"[load_all] season_str={season_str} window={WINDOW} leagues={len(folders)}", flush=True)

for lf in folders:
    cmd = [
        PYEXE, f"{ROOT}/src/blob/load_data/load_stg_players.py",
        "--league_folder", lf, "--season_str", season_str, "--window", WINDOW
    ]
    print("[load_all] RUN:", " ".join(cmd), flush=True)
    res = subprocess.run(cmd)
    if res.returncode != 0:
        sys.exit(res.returncode)
PY
''',
        env=ENV,
        doc_md="DB-driven season + is_current leagues; calls existing load_stg_players.py per league.",
    )

    # 4) DQ checks
    check_stg = BashOperator(
        task_id="check_stg",
        bash_command="$PYTHON " + ROOT + "/src/procs/check_stg_players.py",
        env=ENV,
        doc_md="Run DQ checks on stg_players.",
    )

    # 5) Merge to prod
    update_main_table = BashOperator(
        task_id="update_main_table",
        bash_command="$PYTHON " + ROOT + "/src/procs/update_players.py",
        env=ENV,
        doc_md="Merge stg_players → players.",
    )

    done = EmptyOperator(task_id="done")

    fetch_players >> cleanup_stg >> load_all >> check_stg >> update_main_table >> done
