-- 1. Add the new column to hold the blob-folder name
ALTER TABLE leagues
    ADD COLUMN folder_alias text UNIQUE;

-- 2. Populate it for the five leagues you already have
UPDATE leagues SET folder_alias = 'premier_league' WHERE league_id = 39;
UPDATE leagues SET folder_alias = 'la_liga'        WHERE league_id = 140;
UPDATE leagues SET folder_alias = 'bundesliga'     WHERE league_id = 78;
UPDATE leagues SET folder_alias = 'serie_a'        WHERE league_id = 135;
UPDATE leagues SET folder_alias = 'ligue_1'        WHERE league_id = 61;

-- 3. (optional) if you add more leagues later, insert the alias the same way.
--    The UNIQUE constraint prevents accidental duplicates.
