"""
snapshot_squads.py
────────────────────────────────────────────────────────────────────────────
Freeze every club roster for all *current* league-seasons.

• Reads `leagues`   → folder_alias, league_id
• Reads `league_seasons` → season_year, season_str, is_current
• Calls fetch_squads.py once per row
• Uploads JSON to
    raw/squads/initial/<league_folder>/<season_str>/run_<YYYY-MM-DD>/squads.json
"""

from __future__ import annotations
import logging, pendulum, sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator

# ------------------------------------------------------------------ PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]            # …/footysphere
for folder in ("src", "test_scripts", "config"):
    p = (PROJECT_ROOT / folder).as_posix()
    if p not in sys.path:
        sys.path.append(p)
# ------------------------------------------------------------------ ENV CONSTS
ROOT = "{{ var.value.FOOTY_ROOT }}"
ENV = {
    "PYTHON":      "{{ var.value.PYTHON }}",
    "PG_CONN":     "{{ var.value.PG_CONN }}",
    "PYTHONPATH": (
        "{{ var.value.FOOTY_ROOT }}/src:"
        "{{ var.value.FOOTY_ROOT }}/test_scripts:"
        "{{ var.value.FOOTY_ROOT }}/config"
    ),
}
logging.getLogger(__name__).info("snapshot_squads DAG parsed OK")

# ────────────────────────── helper ───────────────────────────────────────────
def build_targets(**_) -> list[dict]:
    """
    Return one dict per *current* league-season.

    Dict example:
        {
          "league_id":     78,
          "league_folder": "bundesliga",
          "season_str":    "2024_25",
          "season_year":   2024,
        }
    """
    from get_db_conn import get_db_connection
    rows = []
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ls.league_id,
                   l.folder_alias,
                   ls.season_str,
                   ls.season_year
            FROM   league_seasons ls
            JOIN   leagues        l  ON l.league_id = ls.league_id
            WHERE  ls.is_current = true;        -- ← matches your schema
        """)
        for lid, folder, sstr, yr in cur.fetchall():
            rows.append(
                dict(league_id=lid,
                     league_folder=folder,
                     season_str=sstr,
                     season_year=yr)
            )
    logging.getLogger(__name__).info("snapshot_squads • %d targets", len(rows))
    return rows

# ─────────────────────────── DAG ─────────────────────────────────────────────
with DAG(
    dag_id="snapshot_squads",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@once",                     # manual or un-pause
    catchup=False,
    default_args={
        "owner": "footysphere",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["footysphere", "squads", "season_init"],
    is_paused_upon_creation=True,
) as dag:

    targets = PythonOperator(
        task_id="build_squad_targets",
        python_callable=build_targets,
    )

    snapshot = (
        BashOperator
        .partial(
            task_id="fetch_squads",
            bash_command=(
                "$PYTHON " + ROOT + "/src/blob/fetch_data/fetch_squads.py "
                "--league_folder {{ params.league_folder }} "
                "--league_id     {{ params.league_id }} "
                "--season_str    {{ params.season_str }} "
                "--season_year   {{ params.season_year }}"
            ),
            env=ENV,
        )
        # ❷ expand over the *params* field, not kwargs
        .expand(params=targets.output)
    )

    targets >> snapshot            # dependency unchanged