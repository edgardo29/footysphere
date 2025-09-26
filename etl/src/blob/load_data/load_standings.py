import os
import sys
import logging

# Add your DB connection logic path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection

# Configure logging: logs go to both console & file "stg_upsert_standings.log"
logging.basicConfig(
    level=logging.INFO,   # logs INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),                 # console output
        logging.FileHandler("stg_upsert_standings.log")    # file output
    ]
)

CHUNK_SIZE = 2000  # how many rows to upsert per batch

def upsert_standings_in_chunks():
    """
    Upserts valid rows (is_valid=true) from stg_standings into the final standings table.
    Uses a chunked approach with LIMIT/OFFSET to avoid a huge transaction.
    Logs process details to both console & stg_upsert_standings.log.
    """

    conn = get_db_connection()
    cur = conn.cursor()

    offset = 0
    total_upserted = 0

    try:
        logging.info("Starting chunked upsert from stg_standings to standings...")

        while True:
            # We select up to CHUNK_SIZE valid rows at a time, sorted by league_id, season_year, team_id
            upsert_sql = f"""
            WITH chunk AS (
                SELECT league_id, season_year, team_id,
                       rank, points, played,
                       win, draw, lose, goals_for, goals_against, goals_diff
                FROM stg_standings
                WHERE is_valid = true
                ORDER BY league_id, season_year, team_id
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            )
            INSERT INTO standings (
                league_id, season_year, team_id,
                rank, points, played,
                win, draw, lose, goals_for, goals_against, goals_diff
            )
            SELECT
                league_id, season_year, team_id,
                rank, points, played,
                win, draw, lose, goals_for, goals_against, goals_diff
            FROM chunk
            ON CONFLICT (league_id, season_year, team_id)
            DO UPDATE SET
                rank          = EXCLUDED.rank,
                points        = EXCLUDED.points,
                played        = EXCLUDED.played,
                win           = EXCLUDED.win,
                draw          = EXCLUDED.draw,
                lose          = EXCLUDED.lose,
                goals_for     = EXCLUDED.goals_for,
                goals_against = EXCLUDED.goals_against,
                goals_diff    = EXCLUDED.goals_diff
            ;
            """

            cur.execute(upsert_sql)
            rowcount = cur.rowcount  # approximate # of inserted+updated
            conn.commit()

            logging.info(f"Upsert chunk offset={offset} => rowcount={rowcount}")

            # rowcount < CHUNK_SIZE => we've probably exhausted all valid rows
            if rowcount < CHUNK_SIZE:
                total_upserted += rowcount
                break

            total_upserted += rowcount
            offset += CHUNK_SIZE

        logging.info(f"Chunked upsert completed. Approx total rows handled: {total_upserted}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting stg_standings in chunks: {e}", exc_info=True)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    upsert_standings_in_chunks()
