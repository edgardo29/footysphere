#!/usr/bin/env python3
"""
Creates the staging table + indexes for raw match‑events.
Run once (or safely re‑run) after you have get_db_conn().
"""
import os, sys
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

DDL = """
CREATE TABLE IF NOT EXISTS stg_match_events (
    stg_event_id      BIGSERIAL PRIMARY KEY,
    fixture_id        INT,
    minute            INT,
    minute_extra      INT,
    team_id           INT,
    player_id         INT,
    assist_player_id  INT,
    event_type        VARCHAR(20),
    event_detail      VARCHAR(50),
    comments          TEXT,
    is_valid          BOOLEAN DEFAULT TRUE,
    error_reason      TEXT,
    load_timestamp    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_me_fixture ON stg_match_events (fixture_id);
CREATE INDEX IF NOT EXISTS idx_stg_me_player  ON stg_match_events (player_id);
"""

def main() -> None:
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute(DDL)
        conn.commit()
        print("stg_match_events created / verified.")
    except Exception as exc:
        conn.rollback()
        print("Error:", exc)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
