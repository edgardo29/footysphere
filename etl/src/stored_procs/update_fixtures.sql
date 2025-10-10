CREATE OR REPLACE PROCEDURE public.update_fixtures()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_new_venues   INT := 0;  -- placeholders just created
    v_rows_ins     INT := 0;  -- fixtures inserted
    v_rows_upd     INT := 0;  -- fixtures updated
BEGIN
    -------------------------------------------------------------------------
    -- 1)  Add placeholders so the FK can’t fail
    -------------------------------------------------------------------------
    WITH ins AS (
        INSERT INTO venues (venue_id, venue_name, city, country)
        SELECT DISTINCT
               s.venue_id,
               'Unknown Venue',
               'Unknown City',
               'Unknown Country'
          FROM stg_fixtures s
         WHERE s.is_valid
           AND s.venue_id IS NOT NULL
           AND NOT EXISTS (
                 SELECT 1 FROM venues v WHERE v.venue_id = s.venue_id
             )
        ON CONFLICT (venue_id) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_new_venues FROM ins;

    RAISE NOTICE
      'update_fixtures(): added % placeholder venue row(s) (ID, ''Unknown Venue/City/Country'') to "venues" for missing venue_id references',
      v_new_venues;

    -------------------------------------------------------------------------
    -- 2)  Upsert fixtures  (unchanged logic)
    -------------------------------------------------------------------------
    WITH upsert AS (
        INSERT INTO fixtures AS f (
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
        FROM   stg_fixtures s
        WHERE  s.is_valid
        ON CONFLICT (fixture_id) DO UPDATE
            SET (league_id,  season_year,
                 home_team_id, away_team_id,
                 fixture_date, status, round,
                 home_score,  away_score, venue_id,
                 upd_date)
              = (EXCLUDED.league_id,  EXCLUDED.season_year,
                 EXCLUDED.home_team_id, EXCLUDED.away_team_id,
                 EXCLUDED.fixture_date, EXCLUDED.status, EXCLUDED.round,
                 EXCLUDED.home_score,  EXCLUDED.away_score, EXCLUDED.venue_id,
                 NOW())
            WHERE (f.league_id,    f.season_year,  f.home_team_id,
                   f.away_team_id, f.fixture_date, f.status,
                   f.round,        f.home_score,   f.away_score,
                   f.venue_id)
              IS DISTINCT FROM
                   (EXCLUDED.league_id,    EXCLUDED.season_year,  EXCLUDED.home_team_id,
                    EXCLUDED.away_team_id, EXCLUDED.fixture_date, EXCLUDED.status,
                    EXCLUDED.round,        EXCLUDED.home_score,   EXCLUDED.away_score,
                    EXCLUDED.venue_id)
        RETURNING xmax = 0 AS was_insert
    )
    SELECT  COUNT(*) FILTER (WHERE was_insert),
            COUNT(*) FILTER (WHERE NOT was_insert)
      INTO  v_rows_ins, v_rows_upd
      FROM  upsert;

    RAISE NOTICE
      'update_fixtures(): % fixture row(s) inserted, % updated',
      v_rows_ins, v_rows_upd;
END;
$procedure$
