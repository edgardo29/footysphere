import os
import sys

# Add your DB connection path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

CHUNK_SIZE = 2000  # Number of rows per batch (tweak as needed)

def upsert_valid_fixtures_in_chunks():
    """
    Upserts valid rows from stg_fixtures into the final fixtures table in batches of CHUNK_SIZE.
    Each chunk is done in its own transaction to avoid a single huge lock.
    """

    conn = get_db_connection()
    cur = conn.cursor()

    offset = 0
    total_upserted = 0

    try:
        while True:
            # Use a CTE to select a chunk of valid rows, then upsert them
            upsert_sql = f"""
            WITH chunk AS (
                SELECT fixture_id, league_id, season_year,
                       home_team_id, away_team_id,
                       fixture_date, status, round,
                       home_score, away_score, venue_id
                FROM stg_fixtures
                WHERE is_valid = true
                ORDER BY fixture_id
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            )
            INSERT INTO fixtures (
                fixture_id, league_id, season_year,
                home_team_id, away_team_id,
                fixture_date, status, round,
                home_score, away_score, venue_id
            )
            SELECT
                fixture_id, league_id, season_year,
                home_team_id, away_team_id,
                fixture_date, status, round,
                home_score, away_score, venue_id
            FROM chunk
            ON CONFLICT (fixture_id)
            DO UPDATE SET
                league_id    = EXCLUDED.league_id,
                season_year  = EXCLUDED.season_year,
                home_team_id = EXCLUDED.home_team_id,
                away_team_id = EXCLUDED.away_team_id,
                fixture_date = EXCLUDED.fixture_date,
                status       = EXCLUDED.status,
                round        = EXCLUDED.round,
                home_score   = EXCLUDED.home_score,
                away_score   = EXCLUDED.away_score,
                venue_id     = EXCLUDED.venue_id;
            """

            cur.execute(upsert_sql)
            rowcount = cur.rowcount
            # rowcount is the # of rows that were inserted+updated in this statement
            # note: rowcount can be tricky with ON CONFLICT. We rely on it for a rough measure.

            conn.commit()  # commit each chunk
            print(f"Chunk upsert: offset={offset}, rowcount={rowcount}")

            if rowcount < CHUNK_SIZE:
                # no more rows or fewer than chunk size => we're done
                total_upserted += rowcount
                break

            total_upserted += rowcount
            offset += CHUNK_SIZE

        print(f"Upsert in chunks completed. Total rows handled: ~{total_upserted} (approx).")

    except Exception as e:
        conn.rollback()
        print(f"Error upserting fixtures in chunks: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    upsert_valid_fixtures_in_chunks()
