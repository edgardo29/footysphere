import os
import sys

# Add your test_scripts dir for get_db_conn
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection


DDL = """
CREATE TABLE IF NOT EXISTS stg_news (
    article_id        TEXT PRIMARY KEY,                               -- Guardian "id"
    tab               TEXT NOT NULL CHECK (tab IN ('transfers','injuries','matchreports')),

    title             TEXT NOT NULL,                                  -- webTitle
    summary           TEXT,                                           -- stripped fields.trailText
    image_url         TEXT,                                           -- fields.thumbnail
    source            TEXT NOT NULL DEFAULT 'The Guardian',
    url               TEXT NOT NULL,                                  -- fields.shortUrl else webUrl
    published_at_utc  TIMESTAMPTZ NOT NULL,                           -- webPublicationDate (UTC)
    fetched_at_utc    TIMESTAMPTZ NOT NULL,                           -- our fetch time (UTC)

    raw               JSONB,                                          -- original item payload

    is_valid          BOOLEAN DEFAULT TRUE,
    error_reason      TEXT,
    load_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_stg_news_tab_published
    ON stg_news (tab, published_at_utc DESC);
"""


def create_stg_news_table():
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
        print("Created `stg_news` (tabs: transfers, injuries, matchreports).")
    except Exception as e:
        print(f"Error creating `stg_news`: {e}")
        raise


if __name__ == "__main__":
    create_stg_news_table()
