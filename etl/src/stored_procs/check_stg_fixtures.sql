CREATE OR REPLACE PROCEDURE public.check_stg_fixtures()
 LANGUAGE plpgsql
AS $procedure$DECLARE
    v_rule_rows    INTEGER;   -- generic holder for single-step updates
    v_rows_ins     INTEGER;   -- rows inserted into data_load_errors
    v_home_missing INTEGER := 0;
    v_away_missing INTEGER := 0;
BEGIN
    -------------------------------------------------------------------------
    -- 1) fixture_id IS NOT NULL
    -------------------------------------------------------------------------
    UPDATE stg_fixtures
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'missing_fixture_id; '
     WHERE fixture_id IS NULL
       AND is_valid;
    GET DIAGNOSTICS v_rule_rows = ROW_COUNT;
    RAISE NOTICE 'Rule-1 (missing fixture_id)  → % row(s)', v_rule_rows;

    -------------------------------------------------------------------------
    -- 2) home_team_id <> away_team_id
    -------------------------------------------------------------------------
    UPDATE stg_fixtures
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'home_eq_away; '
     WHERE home_team_id = away_team_id
       AND is_valid;
    GET DIAGNOSTICS v_rule_rows = ROW_COUNT;
    RAISE NOTICE 'Rule-2 (home = away team)    → % row(s)', v_rule_rows;

    -------------------------------------------------------------------------
    -- 3) both team IDs exist in teams  ➜ split into home / away checks
    -------------------------------------------------------------------------
    -- 3a) home_team_id missing
    UPDATE stg_fixtures s
       SET is_valid = FALSE,
           error_reason = COALESCE(error_reason,'') ||
                CASE
                    WHEN position('missing_home_team' IN COALESCE(error_reason,'')) = 0
                    THEN 'missing_home_team; '
                    ELSE ''
                END
     WHERE is_valid
       AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.team_id = s.home_team_id);
    GET DIAGNOSTICS v_home_missing = ROW_COUNT;
    RAISE NOTICE 'Rule-3a (missing HOME team FK) → % row(s)', v_home_missing;

    -- 3b) away_team_id missing
    UPDATE stg_fixtures s
       SET is_valid = FALSE,
           error_reason = COALESCE(error_reason,'') ||
                CASE
                    WHEN position('missing_away_team' IN COALESCE(error_reason,'')) = 0
                    THEN 'missing_away_team; '
                    ELSE ''
                END
     WHERE is_valid
       AND NOT EXISTS (SELECT 1 FROM teams t WHERE t.team_id = s.away_team_id);
    GET DIAGNOSTICS v_away_missing = ROW_COUNT;
    RAISE NOTICE 'Rule-3b (missing AWAY team FK) → % row(s)', v_away_missing;

    -- combined total for easy reconciliation
    RAISE NOTICE 'Rule-3 total (missing team FK) → % row(s)',
                 v_home_missing + v_away_missing;

    -------------------------------------------------------------------------
    -- 4) duplicate fixture_id in staging table
    -------------------------------------------------------------------------
    UPDATE stg_fixtures sf
       SET is_valid     = FALSE,
           error_reason = COALESCE(error_reason,'') || 'dup_fixture_id; '
      FROM (
            SELECT fixture_id
              FROM stg_fixtures
             WHERE is_valid
               AND fixture_id IS NOT NULL
             GROUP BY fixture_id
            HAVING COUNT(*) > 1
           ) d
     WHERE sf.fixture_id = d.fixture_id
       AND sf.is_valid;
    GET DIAGNOSTICS v_rule_rows = ROW_COUNT;
    RAISE NOTICE 'Rule-4 (duplicate fixture_id)  → % row(s)', v_rule_rows;

    -------------------------------------------------------------------------
    -- 5) insert new error rows
    -------------------------------------------------------------------------
    INSERT INTO data_load_errors (staging_table, record_key, error_reason)
    SELECT DISTINCT
           'stg_fixtures',
           COALESCE(fixture_id::TEXT, '<NULL>'),
           trim(both '; ' FROM error_reason)
      FROM stg_fixtures s
     WHERE s.is_valid = FALSE
       AND s.error_reason IS NOT NULL
       AND NOT EXISTS (
             SELECT 1
               FROM data_load_errors d
              WHERE d.staging_table = 'stg_fixtures'
                AND d.record_key    = COALESCE(s.fixture_id::TEXT, '<NULL>')
                AND d.error_reason  = trim(both '; ' FROM s.error_reason)
           );
    GET DIAGNOSTICS v_rows_ins = ROW_COUNT;
    RAISE NOTICE 'Logged % new row(s) in data_load_errors', v_rows_ins;
END;$procedure$
