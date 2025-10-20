import os
import sys

# Add your test_scripts dir for get_db_conn
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))
from get_db_conn import get_db_connection


DDL_DROP_VIEW = "DROP VIEW IF EXISTS vw_news_by_tab_latest50;"

DDL_TABLE_AND_INDEXES = """
CREATE TABLE IF NOT EXISTS news (
    article_id        TEXT PRIMARY KEY,
    tab               TEXT NOT NULL CHECK (tab IN ('transfers','injuries','matchreports')),

    title             TEXT NOT NULL,
    summary           TEXT,
    image_url         TEXT,
    source            TEXT NOT NULL DEFAULT 'The Guardian',
    url               TEXT NOT NULL,
    published_at_utc  TIMESTAMPTZ NOT NULL,

    -- Audit (DB-level)
    load_date         TIMESTAMPTZ(0) NOT NULL DEFAULT NOW(),
    upd_date          TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_news_url ON news (url);
CREATE INDEX IF NOT EXISTS ix_news_tab_published ON news (tab, published_at_utc DESC);
"""

# Remove legacy lineage columns if they still exist
DDL_REMOVE_LINEAGE_COLS = """
ALTER TABLE IF EXISTS news
  DROP COLUMN IF EXISTS first_seen_utc,
  DROP COLUMN IF EXISTS last_seen_utc;
"""

DDL_TRIGGER = """
-- Keep upd_date fresh on UPDATEs
CREATE OR REPLACE FUNCTION trg_news_set_upd_date()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.upd_date := NOW();
  RETURN NEW;
END$$;

DROP TRIGGER IF EXISTS set_upd_date_news ON news;
CREATE TRIGGER set_upd_date_news
BEFORE UPDATE ON news
FOR EACH ROW EXECUTE FUNCTION trg_news_set_upd_date();
"""

DDL_VIEW = """
CREATE OR REPLACE VIEW vw_news_by_tab_latest50 AS
SELECT *
FROM (
  SELECT n.*, ROW_NUMBER() OVER (PARTITION BY tab ORDER BY published_at_utc DESC) AS rn
  FROM news n
) t
WHERE rn <= 50;
"""

def create_news():
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            # 1) Drop the view if present so we can alter the table shape safely
            cur.execute(DDL_DROP_VIEW)
            # 2) Ensure table + indexes exist (without lineage columns)
            cur.execute(DDL_TABLE_AND_INDEXES)
            # 3) Remove lineage columns if they exist from older deployments
            cur.execute(DDL_REMOVE_LINEAGE_COLS)
            # 4) Recreate trigger
            cur.execute(DDL_TRIGGER)
            # 5) Recreate the view
            cur.execute(DDL_VIEW)
        print("Created/updated `news`, trigger, and `vw_news_by_tab_latest50` (no lineage columns).")
    except Exception as e:
        print(f"Error creating/updating `news` objects: {e}")
        raise

if __name__ == "__main__":
    create_news()
