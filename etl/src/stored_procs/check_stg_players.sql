CREATE OR REPLACE PROCEDURE public.check_stg_players()
 LANGUAGE plpgsql
AS $procedure$DECLARE
    -- Scope cutoff: set to NULL for full-table validation.
    -- To limit to "recent batch", assign e.g.:
    -- p_since_ts := NOW() - INTERVAL '20 minutes';
    p_since_ts TIMESTAMP := NULL;

    v_rowcount           INT;
    v_total_invalidated  INT := 0;

    -- Per-rule counts (rows that violate the rule in-scope; a row can hit multiple rules)
    cnt_missing_id       INT := 0;
    cnt_blank_name       INT := 0;
    cnt_birthdate_bad    INT := 0;
    cnt_dupes            INT := 0;  -- counts the *extra* duplicate rows
    cnt_height_bad       INT := 0;
    cnt_weight_bad       INT := 0;

    -- Scope sizes
    scoped_total         INT := 0;
    final_invalid        INT := 0;
BEGIN
    RAISE NOTICE 'Validating players... (since_ts=%)', p_since_ts;

    ---------------------------------------------------------------------------
    -- 0) Normalize names (trim -> NULL) in-scope
    ---------------------------------------------------------------------------
    UPDATE stg_players sp
       SET firstname = NULLIF(BTRIM(firstname), ''),
           lastname  = NULLIF(BTRIM(lastname),  ''),
           full_name = NULLIF(BTRIM(full_name), '')
     WHERE (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);

    ---------------------------------------------------------------------------
    -- Precompute per-rule COUNTS (independent of is_valid)
    ---------------------------------------------------------------------------
    SELECT COUNT(*) INTO scoped_total
      FROM stg_players sp
     WHERE (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);

    SELECT COUNT(*) INTO cnt_missing_id
      FROM stg_players sp
     WHERE sp.player_id IS NULL
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);

    SELECT COUNT(*) INTO cnt_blank_name
      FROM stg_players sp
     WHERE (sp.firstname IS NULL AND sp.lastname IS NULL AND sp.full_name IS NULL)
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);

    SELECT COUNT(*) INTO cnt_birthdate_bad
      FROM stg_players sp
     WHERE sp.birth_date IS NOT NULL
       AND (sp.birth_date < DATE '1900-01-01' OR sp.birth_date > CURRENT_DATE)
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);

    WITH scoped AS (
        SELECT player_id
          FROM stg_players
         WHERE (p_since_ts IS NULL OR load_timestamp >= p_since_ts)
           AND player_id IS NOT NULL
    ),
    g AS (
        SELECT player_id, COUNT(*) AS cnt
          FROM scoped
         GROUP BY player_id
        HAVING COUNT(*) > 1
    )
    SELECT COALESCE(SUM(cnt - 1), 0) INTO cnt_dupes FROM g;

    WITH hw AS (
        SELECT
          NULLIF(regexp_replace(height, '[^0-9]', '', 'g'), '')::INT AS h_cm,
          NULLIF(regexp_replace(weight, '[^0-9]', '', 'g'), '')::INT AS w_kg
        FROM stg_players sp
        WHERE (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts)
    )
    SELECT COUNT(*) INTO cnt_height_bad
      FROM hw
     WHERE h_cm IS NOT NULL AND (h_cm < 140 OR h_cm > 220);

    WITH hw2 AS (
        SELECT
          NULLIF(regexp_replace(weight, '[^0-9]', '', 'g'), '')::INT AS w_kg
        FROM stg_players sp
        WHERE (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts)
    )
    SELECT COUNT(*) INTO cnt_weight_bad
      FROM hw2
     WHERE w_kg IS NOT NULL AND (w_kg < 40 OR w_kg > 150);

    ---------------------------------------------------------------------------
    -- 1) Invalidate: missing player_id
    ---------------------------------------------------------------------------
    UPDATE stg_players sp
       SET is_valid=false,
           error_reason = COALESCE(error_reason,'') || 'Missing player_id; '
     WHERE sp.player_id IS NULL
       AND sp.is_valid=true
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;  v_total_invalidated := v_total_invalidated + v_rowcount;

    ---------------------------------------------------------------------------
    -- 2) Invalidate: no usable name
    ---------------------------------------------------------------------------
    UPDATE stg_players sp
       SET is_valid=false,
           error_reason = COALESCE(error_reason,'') || 'Name fields blank; '
     WHERE (sp.firstname IS NULL AND sp.lastname IS NULL AND sp.full_name IS NULL)
       AND sp.is_valid=true
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;  v_total_invalidated := v_total_invalidated + v_rowcount;

    ---------------------------------------------------------------------------
    -- 3) Invalidate: birth_date out of range
    ---------------------------------------------------------------------------
    UPDATE stg_players sp
       SET is_valid=false,
           error_reason = COALESCE(error_reason,'') || 'Unrealistic birth_date; '
     WHERE sp.birth_date IS NOT NULL
       AND (sp.birth_date < DATE '1900-01-01' OR sp.birth_date > CURRENT_DATE)
       AND sp.is_valid=true
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;  v_total_invalidated := v_total_invalidated + v_rowcount;

    ---------------------------------------------------------------------------
    -- 4) Invalidate duplicates (keep newest stg_player_id per player_id)
    ---------------------------------------------------------------------------
    WITH keep AS (
        SELECT player_id, MAX(stg_player_id) AS keep_id
          FROM stg_players
         WHERE player_id IS NOT NULL
           AND (p_since_ts IS NULL OR load_timestamp >= p_since_ts)
         GROUP BY player_id
    )
    UPDATE stg_players sp
       SET is_valid=false,
           error_reason = COALESCE(error_reason,'') || 'Duplicate player_id; '
      FROM keep k
     WHERE sp.player_id = k.player_id
       AND sp.stg_player_id <> k.keep_id
       AND sp.is_valid=true;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;  v_total_invalidated := v_total_invalidated + v_rowcount;

    ---------------------------------------------------------------------------
    -- 5) Invalidate unrealistic height/weight (parse numeric)
    ---------------------------------------------------------------------------
    WITH hw AS (
        SELECT sp.stg_player_id,
               NULLIF(regexp_replace(sp.height, '[^0-9]', '', 'g'), '')::INT AS h_cm,
               NULLIF(regexp_replace(sp.weight, '[^0-9]', '', 'g'), '')::INT AS w_kg
          FROM stg_players sp
         WHERE (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts)
    )
    UPDATE stg_players sp
       SET is_valid = false,
           error_reason = COALESCE(sp.error_reason,'') ||
                          CASE WHEN hw.h_cm IS NOT NULL AND (hw.h_cm < 140 OR hw.h_cm > 220)
                               THEN 'Unrealistic height; ' ELSE '' END ||
                          CASE WHEN hw.w_kg IS NOT NULL AND (hw.w_kg < 40 OR hw.w_kg > 150)
                               THEN 'Unrealistic weight; ' ELSE '' END
      FROM hw
     WHERE sp.stg_player_id = hw.stg_player_id
       AND sp.is_valid = true
       AND (
             (hw.h_cm IS NOT NULL AND (hw.h_cm < 140 OR hw.h_cm > 220)) OR
             (hw.w_kg IS NOT NULL AND (hw.w_kg < 40 OR hw.w_kg > 150))
           );
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;  v_total_invalidated := v_total_invalidated + v_rowcount;

    ---------------------------------------------------------------------------
    -- 6) Log invalids
    ---------------------------------------------------------------------------
    INSERT INTO data_load_errors (staging_table, record_key, error_reason)
    SELECT 'stg_players',
           (COALESCE(player_id::text, 'NULL') || '-' || stg_player_id::text),
           error_reason
      FROM stg_players sp
     WHERE sp.is_valid = false
       AND sp.error_reason IS NOT NULL
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts)
    ON CONFLICT DO NOTHING;

    ---------------------------------------------------------------------------
    -- Final counts & summary print
    ---------------------------------------------------------------------------
    SELECT COUNT(*)
      INTO final_invalid
      FROM stg_players sp
     WHERE sp.is_valid = false
       AND (p_since_ts IS NULL OR sp.load_timestamp >= p_since_ts);

    RAISE NOTICE '========== PLAYER VALIDATION SUMMARY ==========';
    IF p_since_ts IS NULL THEN
        RAISE NOTICE 'Scope                 : ALL rows in stg_players';
    ELSE
        RAISE NOTICE 'Scope (since_ts)      : %', p_since_ts;
    END IF;
    RAISE NOTICE 'Records in scope       : %', scoped_total;
    RAISE NOTICE ' - Missing player_id   : %', cnt_missing_id;
    RAISE NOTICE ' - Blank name          : %', cnt_blank_name;
    RAISE NOTICE ' - Birthdate out range : %', cnt_birthdate_bad;
    RAISE NOTICE ' - Duplicate player_id : % (extra rows)', cnt_dupes;
    RAISE NOTICE ' - Unrealistic height  : %', cnt_height_bad;
    RAISE NOTICE ' - Unrealistic weight  : %', cnt_weight_bad;
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Invalid rows flagged   : % (this run updates)', v_total_invalidated;
    RAISE NOTICE 'Invalid rows now total : % (in scope)', final_invalid;
END;$procedure$
