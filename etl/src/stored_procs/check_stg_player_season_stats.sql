CREATE OR REPLACE PROCEDURE public.check_stg_player_season_stats()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    rowcount                 INT;
    newly_marked_invalid     INT := 0;

    -- per-rule counters
    missing_ids_count        INT := 0;
    negative_core_count      INT := 0;  -- appearances/lineups/minutes < 0
    lineups_gt_apps_count    INT := 0;

    -- summary counts
    total_rows               INT := 0;
    valid_rows               INT := 0;
    invalid_rows             INT := 0;
BEGIN
    RAISE NOTICE 'Validating stg_player_season_stats...';

    ----------------------------------------------------------------
    -- Rule 1: Missing required IDs
    ----------------------------------------------------------------
    UPDATE stg_player_season_stats
       SET is_valid = false,
           error_reason = COALESCE(error_reason,'') || 'Missing required IDs; '
     WHERE is_valid = true
       AND (player_id IS NULL OR team_id IS NULL OR league_id IS NULL OR season IS NULL);

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    missing_ids_count     := rowcount;
    newly_marked_invalid  := newly_marked_invalid + rowcount;

    ----------------------------------------------------------------
    -- Rule 2: Negative appearances/lineups/minutes
    ----------------------------------------------------------------
    UPDATE stg_player_season_stats
       SET is_valid = false,
           error_reason = COALESCE(error_reason,'') || 'Negative appearances/lineups/minutes; '
     WHERE is_valid = true
       AND (COALESCE(appearances,0) < 0
         OR COALESCE(lineups,0)     < 0
         OR COALESCE(minutes_played,0) < 0);

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    negative_core_count   := rowcount;
    newly_marked_invalid  := newly_marked_invalid + rowcount;

    ----------------------------------------------------------------
    -- Rule 3: lineups > appearances
    ----------------------------------------------------------------
    UPDATE stg_player_season_stats
       SET is_valid = false,
           error_reason = COALESCE(error_reason,'') || 'lineups > appearances; '
     WHERE is_valid = true
       AND lineups IS NOT NULL
       AND appearances IS NOT NULL
       AND lineups > appearances;

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    lineups_gt_apps_count := rowcount;
    newly_marked_invalid  := newly_marked_invalid + rowcount;

    ----------------------------------------------------------------
    -- Write invalids to data_load_errors (append)
    ----------------------------------------------------------------
    INSERT INTO data_load_errors (staging_table, record_key, error_reason)
    SELECT 'stg_player_season_stats',
           stg_stats_id::text,
           error_reason
      FROM stg_player_season_stats
     WHERE is_valid = false
       AND error_reason IS NOT NULL;

    ----------------------------------------------------------------
    -- Summary + vertical formatting
    ----------------------------------------------------------------
    SELECT COUNT(*) INTO total_rows   FROM stg_player_season_stats;
    SELECT COUNT(*) INTO invalid_rows FROM stg_player_season_stats WHERE is_valid = false;
    SELECT COUNT(*) INTO valid_rows   FROM stg_player_season_stats WHERE is_valid = true;

    RAISE NOTICE 'SUMMARY:';
    RAISE NOTICE '  Total rows: %', total_rows;
    RAISE NOTICE '  Valid rows: %', valid_rows;
    RAISE NOTICE '  Invalid rows: %', invalid_rows;
    RAISE NOTICE '  Newly marked invalid this run: %', newly_marked_invalid;

    RAISE NOTICE 'RULE COUNTS:';
    RAISE NOTICE '  Missing required IDs: %', missing_ids_count;
    RAISE NOTICE '  Negative appearances/lineups/minutes: %', negative_core_count;
    RAISE NOTICE '  lineups > appearances: %', lineups_gt_apps_count;
END;
$procedure$
