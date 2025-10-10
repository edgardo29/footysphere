CREATE OR REPLACE PROCEDURE public.update_players()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_inserts INT := 0;
    v_updates INT := 0;
BEGIN
    WITH src AS (
        SELECT DISTINCT ON (stg.player_id)
               stg.player_id,
               stg.firstname,
               stg.lastname,
               stg.full_name,
               stg.birth_date,
               stg.birth_country,
               stg.nationality,
               stg.height,
               stg.weight,
               stg.photo_url
        FROM stg_players stg
        WHERE stg.is_valid = true
        ORDER BY stg.player_id, stg.stg_player_id DESC
    ),
    upsert AS (
        INSERT INTO players (
            player_id, firstname, lastname, full_name,
            birth_date, birth_country, nationality,
            height, weight, photo_url
        )
        SELECT
            s.player_id, s.firstname, s.lastname, s.full_name,
            s.birth_date, s.birth_country, s.nationality,
            s.height, s.weight, s.photo_url
        FROM src s
        ON CONFLICT (player_id) DO UPDATE
        SET firstname     = EXCLUDED.firstname,
            lastname      = EXCLUDED.lastname,
            full_name     = EXCLUDED.full_name,
            birth_date    = EXCLUDED.birth_date,
            birth_country = EXCLUDED.birth_country,
            nationality   = EXCLUDED.nationality,
            height        = EXCLUDED.height,
            weight        = EXCLUDED.weight,
            photo_url     = EXCLUDED.photo_url,
            upd_date      = NOW()
        -- Only update if something actually changed:
        WHERE (players.firstname, players.lastname, players.full_name,
               players.birth_date, players.birth_country, players.nationality,
               players.height, players.weight, players.photo_url)
              IS DISTINCT FROM
              (EXCLUDED.firstname, EXCLUDED.lastname, EXCLUDED.full_name,
               EXCLUDED.birth_date, EXCLUDED.birth_country, EXCLUDED.nationality,
               EXCLUDED.height, EXCLUDED.weight, EXCLUDED.photo_url)
        RETURNING (xmax = 0)::INT AS inserted, (xmax <> 0)::INT AS updated
    )
    SELECT COALESCE(SUM(inserted),0), COALESCE(SUM(updated),0)
      INTO v_inserts, v_updates
      FROM upsert;

    IF v_inserts > 0 THEN
        RAISE NOTICE 'New players inserted: %', v_inserts;
    END IF;
    IF v_updates > 0 THEN
        RAISE NOTICE 'Players updated (changed fields): %', v_updates;
    END IF;
    IF v_inserts = 0 AND v_updates = 0 THEN
        RAISE NOTICE 'Players upsert complete. No changes.';
    END IF;
END;
$procedure$
