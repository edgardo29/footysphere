CREATE OR REPLACE PROCEDURE public.update_teams()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    inserted_rows INT;
    updated_rows  INT;
BEGIN
    /* pick one row per team_id (latest season) ----------------------- */
    WITH d AS (
        SELECT DISTINCT ON (team_id)
               team_id,
               team_name,
               team_country,
               team_logo_url,
               venue_id
        FROM   stg_teams
        WHERE  is_valid
        ORDER  BY team_id, season_year DESC      -- keep newest season
    ),
    upsert AS (
        INSERT INTO teams AS t (
            team_id, team_name, team_country, team_logo_url, venue_id
        )
        SELECT *
        FROM   d
        ON CONFLICT (team_id) DO UPDATE
            SET team_name     = EXCLUDED.team_name,
                team_country  = EXCLUDED.team_country,
                team_logo_url = EXCLUDED.team_logo_url,
                venue_id      = COALESCE(EXCLUDED.venue_id, t.venue_id)
          WHERE  COALESCE(t.team_name,'')     <> COALESCE(EXCLUDED.team_name,'')
             OR  COALESCE(t.team_country,'')  <> COALESCE(EXCLUDED.team_country,'')
             OR  COALESCE(t.team_logo_url,'') <> COALESCE(EXCLUDED.team_logo_url,'')
             OR  (EXCLUDED.venue_id IS NOT NULL AND t.venue_id <> EXCLUDED.venue_id)
        RETURNING xmax = 0 AS was_insert
    )
    SELECT
        COUNT(*) FILTER (WHERE was_insert),
        COUNT(*) FILTER (WHERE NOT was_insert)
    INTO inserted_rows, updated_rows
    FROM upsert;

    RAISE NOTICE '[update_teams] % inserts, % updates',
                 inserted_rows, updated_rows;
END;
$procedure$
