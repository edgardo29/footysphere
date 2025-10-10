CREATE OR REPLACE PROCEDURE public.update_venues()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_inserted  INT;
    v_updated   INT;
BEGIN
    /*---------------------------------------------------------------
      1. Upsert using INSERT … ON CONFLICT … DO UPDATE.
         The WHERE clause in DO UPDATE prevents “no-op” updates.
    ---------------------------------------------------------------*/
    WITH upsert AS (
        INSERT INTO venues AS v (
            venue_id, venue_name, city, country
        )
        SELECT s.venue_id,
               s.venue_name,
               s.city,
               s.country
        FROM   stg_venues s
        WHERE  s.is_valid
        ON CONFLICT (venue_id) DO UPDATE
          SET venue_name = EXCLUDED.venue_name,
              city       = EXCLUDED.city,
              country    = EXCLUDED.country
          WHERE  COALESCE(v.venue_name,'') <> COALESCE(EXCLUDED.venue_name,'')
             OR  COALESCE(v.city,'')       <> COALESCE(EXCLUDED.city,'')
             OR  COALESCE(v.country,'')    <> COALESCE(EXCLUDED.country,'')
        RETURNING xmax = 0                       AS inserted_flag   -- true when it was an insert
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted_flag)        AS ins_cnt,
        COUNT(*) FILTER (WHERE NOT inserted_flag)    AS upd_cnt
    INTO   v_inserted, v_updated
    FROM   upsert;

    RAISE NOTICE '[update_venues] inserted % rows, updated % rows',
                 v_inserted, v_updated;
END;
$procedure$
