# create_team_ingest_status_table.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection


def create_team_ingest_status_table():
    """
    Creates `team_ingest_status`, a one-row-per-league-season receipt
    marking that the teams for that league/season have been fetched,
    uploaded to Blob, and counted.

      league_id    INT   (FK → leagues)
      season_year  INT   (e.g. 2025)
      blob_path    TEXT  (Azure location of the JSON)
      row_count    INT   (# teams in the payload)
      load_date    TIMESTAMP(0)  default now()   – first successful load
      upd_date     TIMESTAMP(0)  default now()   – last time row was upserted

    The composite PK keeps the table idempotent: one row per
    (league_id, season_year).
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS team_ingest_status (
        league_id    INT  NOT NULL,
        season_year  INT  NOT NULL,
        blob_path    TEXT NOT NULL,
        row_count    INT  NOT NULL,
        load_date    TIMESTAMP(0),
        upd_date     TIMESTAMP(0) NOT NULL DEFAULT NOW(),
        

        PRIMARY KEY (league_id, season_year),
        FOREIGN KEY (league_id) REFERENCES leagues(league_id)
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("`team_ingest_status` table created successfully.")
    except Exception as e:
        print(f"Error creating `team_ingest_status` table: {e}")
        raise


if __name__ == "__main__":
    create_team_ingest_status_table()
