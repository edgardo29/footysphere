#!/usr/bin/env python3
"""
Creates the production match_events table + indexes.
"""
import os, sys
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

DDL = """
CREATE TABLE IF NOT EXISTS match_events (
    event_id          BIGSERIAL PRIMARY KEY,
    fixture_id        INT        NOT NULL,
    minute            INT        NOT NULL,
    minute_extra      INT,
    team_id           INT        NOT NULL,
    player_id         INT,
    assist_player_id  INT,
    event_type        VARCHAR(20) NOT NULL,
    event_detail      VARCHAR(50),
    comments          TEXT,
    load_date     TIMESTAMP(0) NOT NULL DEFAULT now(),
    upd_date      TIMESTAMP(0) NOT NULL DEFAULT now()
    CONSTRAINT fk_me_fixture FOREIGN KEY (fixture_id)
        REFERENCES fixtures (fixture_id) ON DELETE CASCADE,
    CONSTRAINT fk_me_team    FOREIGN KEY (team_id)
        REFERENCES teams (team_id),
    CONSTRAINT fk_me_player  FOREIGN KEY (player_id)
        REFERENCES players (player_id)
);

CREATE INDEX IF NOT EXISTS idx_me_fixture ON match_events (fixture_id);
CREATE INDEX IF NOT EXISTS idx_me_player  ON match_events (player_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_me_natural
            ON match_events (fixture_id,
                             minute, COALESCE(minute_extra,0),
                             team_id, COALESCE(player_id,0),
                             event_type);
"""

def main() -> None:
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute(DDL)
        conn.commit()
        print("Table match_events created / verified.")
    except Exception as exc:
        conn.rollback()
        print("Error:", exc)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
