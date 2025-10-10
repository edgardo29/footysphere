CREATE OR REPLACE PROCEDURE public.check_stg_match_events()
 LANGUAGE plpgsql
AS $procedure$DECLARE
    rows_reset      INT := 0;
    fk_count        INT := 0;
    minute_count    INT := 0;
    type_count      INT := 0;
    dup_count       INT := 0;
    errors_logged   INT := 0;
BEGIN
    -- 0) Reset flags (safe for first runs & reruns)
    UPDATE stg_match_events
       SET is_valid = TRUE,
           error_reason = NULL
     WHERE is_valid IS DISTINCT FROM TRUE;
    GET DIAGNOSTICS rows_reset = ROW_COUNT;

    -- 1) FK checks: fixture & team must exist
    UPDATE stg_match_events s
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'Missing fixture/team; '
     WHERE is_valid = TRUE
       AND (
            NOT EXISTS (SELECT 1 FROM fixtures f WHERE f.fixture_id = s.fixture_id) OR
            NOT EXISTS (SELECT 1 FROM teams    t WHERE t.team_id    = s.team_id)
           );
    GET DIAGNOSTICS fk_count = ROW_COUNT;

    -- 2) Minute sanity (allow –10 … 130; extra 0 … 40)
    UPDATE stg_match_events
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'Bad minute; '
     WHERE is_valid = TRUE
       AND (
            minute IS NULL OR
            minute < -10 OR minute > 130 OR
            (minute_extra IS NOT NULL AND (minute_extra < 0 OR minute_extra > 40))
           );
    GET DIAGNOSTICS minute_count = ROW_COUNT;

    -- 3) Event-type whitelist (case-insensitive)
    UPDATE stg_match_events
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'Unknown type; '
     WHERE is_valid = TRUE
       AND lower(event_type) NOT IN ('goal','card','subst','var');
    GET DIAGNOSTICS type_count = ROW_COUNT;

    -- 4) In-file duplicates (keep first) – include event_detail
    WITH dup AS (
      SELECT stg_event_id,
             ROW_NUMBER() OVER (
               PARTITION BY fixture_id,
                            minute,
                            COALESCE(minute_extra,0),
                            team_id,
                            COALESCE(player_id,0),
                            lower(event_type),
                            COALESCE(lower(event_detail), '')
               ORDER BY stg_event_id
             ) AS rn
        FROM stg_match_events
       WHERE is_valid = TRUE
    )
    UPDATE stg_match_events s
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'Duplicate; '
      FROM dup
     WHERE s.stg_event_id = dup.stg_event_id
       AND dup.rn > 1;
    GET DIAGNOSTICS dup_count = ROW_COUNT;

    -- 5) Log bad rows
    INSERT INTO data_load_errors (staging_table, record_key, error_reason)
    SELECT 'stg_match_events', stg_event_id::TEXT, error_reason
      FROM stg_match_events
     WHERE is_valid = FALSE;
    GET DIAGNOSTICS errors_logged = ROW_COUNT;

    -- 6) Per-rule summary (each on its own line)
    RAISE NOTICE 'fk:%', fk_count;
    RAISE NOTICE 'minute:%', minute_count;
    RAISE NOTICE 'type:%', type_count;
    RAISE NOTICE 'dup:%', dup_count;
    RAISE NOTICE 'logged:%', errors_logged;
END$procedure$
