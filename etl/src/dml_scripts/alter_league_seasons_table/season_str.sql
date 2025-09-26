-- 1. Add the season_str column
ALTER TABLE league_seasons
    ADD COLUMN season_str text;

-- 2. Populate it for the 2024-25 season that’s already in the table
UPDATE league_seasons
SET    season_str = '2024_25'
WHERE  season_year = 2024;

-- 3. (future) Whenever you add a new season row, include season_str
-- example for 2025-26 season
-- INSERT INTO league_seasons (league_id, season_year, start_date, end_date, is_current, season_str)
-- VALUES (39, 2025, '2025-08-15', '2026-05-24', true, '2025_26');
