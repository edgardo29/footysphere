CREATE OR REPLACE PROCEDURE public.check_stg_standings()
 LANGUAGE plpgsql
AS $procedure$DECLARE
    -- Run identity for optional logging
    v_run_ts   timestamptz := now();
    v_run_id   text        := to_char(v_run_ts, 'YYYYMMDDHH24MISSMS');

    -- totals
    v_total_rows     INT := 0;
    v_total_invalid  INT := 0;

    -- per-rule counters
    r_missing_keys   INT := 0;  -- R1
    r_neg_points     INT := 0;  -- R2
    r_rank_lt1       INT := 0;  -- R3
    r_neg_fields     INT := 0;  -- R4
    r_played_mis     INT := 0;  -- R5
    r_goalsdiff_mis  INT := 0;  -- R6
    r_points_mis     INT := 0;  -- R7
    r_dups           INT := 0;  -- R8
    r_missing_fk     INT := 0;  -- R9

    v_tmp            INT := 0;
    has_run_ts       BOOLEAN := FALSE;
    has_run_id       BOOLEAN := FALSE;
BEGIN
    SELECT COUNT(*) INTO v_total_rows FROM stg_standings;
    RAISE NOTICE 'stg_standings: % row(s) before validation.', v_total_rows;

    -- Reset so each run is independent
    UPDATE stg_standings SET is_valid = TRUE, error_reason = NULL;

    --------------------------------------------------------------------
    -- Precompute counts for R1–R7
    --------------------------------------------------------------------
    SELECT COUNT(*) INTO r_missing_keys
      FROM stg_standings
     WHERE (league_id IS NULL OR season_year IS NULL OR team_id IS NULL);

    SELECT COUNT(*) INTO r_neg_points
      FROM stg_standings
     WHERE points < 0;

    SELECT COUNT(*) INTO r_rank_lt1
      FROM stg_standings
     WHERE rank IS NOT NULL AND rank < 1;

    SELECT COUNT(*) INTO r_neg_fields
      FROM stg_standings
     WHERE (played < 0 OR win < 0 OR draw < 0 OR lose < 0 OR goals_for < 0 OR goals_against < 0);

    SELECT COUNT(*) INTO r_played_mis
      FROM stg_standings
     WHERE played IS NOT NULL AND win IS NOT NULL AND draw IS NOT NULL AND lose IS NOT NULL
       AND played <> (win + draw + lose);

    SELECT COUNT(*) INTO r_goalsdiff_mis
      FROM stg_standings
     WHERE goals_diff IS NOT NULL AND goals_for IS NOT NULL AND goals_against IS NOT NULL
       AND goals_diff <> (goals_for - goals_against);

    SELECT COUNT(*) INTO r_points_mis
      FROM stg_standings
     WHERE points IS NOT NULL AND win IS NOT NULL AND draw IS NOT NULL
       AND points <> (3*win + draw);

    --------------------------------------------------------------------
    -- Aggregate reasons for R1–R7 in one pass (multi-reason per row)
    --------------------------------------------------------------------
    WITH eval AS (
      SELECT
        ctid,
        (league_id IS NULL OR season_year IS NULL OR team_id IS NULL)                                                    AS r1,
        (points < 0)                                                                                                     AS r2,
        (rank IS NOT NULL AND rank < 1)                                                                                  AS r3,
        (played < 0 OR win < 0 OR draw < 0 OR lose < 0 OR goals_for < 0 OR goals_against < 0)                            AS r4,
        (played IS NOT NULL AND win IS NOT NULL AND draw IS NOT NULL AND lose IS NOT NULL
             AND played <> (win + draw + lose))                                                                          AS r5,
        (goals_diff IS NOT NULL AND goals_for IS NOT NULL AND goals_against IS NOT NULL
             AND goals_diff <> (goals_for - goals_against))                                                              AS r6,
        (points IS NOT NULL AND win IS NOT NULL AND draw IS NOT NULL
             AND points <> (3*win + draw))                                                                               AS r7
      FROM stg_standings
    )
    UPDATE stg_standings s
       SET error_reason = NULLIF(
               TRIM(BOTH ' ' FROM CONCAT(
                 CASE WHEN e.r1 THEN 'Missing league/season/team; ' ELSE '' END,
                 CASE WHEN e.r2 THEN 'Negative points; '             ELSE '' END,
                 CASE WHEN e.r3 THEN 'Invalid rank (<1); '           ELSE '' END,
                 CASE WHEN e.r4 THEN 'Negative numeric; '            ELSE '' END,
                 CASE WHEN e.r5 THEN 'Played != W+D+L; '             ELSE '' END,
                 CASE WHEN e.r6 THEN 'GoalsDiff != GF-GA; '          ELSE '' END,
                 CASE WHEN e.r7 THEN 'Points != 3*W + D; '           ELSE '' END
               )),
               ''
           ),
           is_valid = CASE WHEN (e.r1 OR e.r2 OR e.r3 OR e.r4 OR e.r5 OR e.r6 OR e.r7) THEN FALSE ELSE TRUE END
      FROM eval e
     WHERE s.ctid = e.ctid;

    --------------------------------------------------------------------
    -- Rule-8: duplicates within same group_label → invalidate extras
    --------------------------------------------------------------------
    WITH d AS (
      SELECT league_id, season_year, COALESCE(group_label,'') AS g, team_id, MIN(ctid) AS keep_ctid
        FROM stg_standings
       GROUP BY 1,2,3,4
      HAVING COUNT(*) > 1
    )
    UPDATE stg_standings s
       SET is_valid = FALSE,
           error_reason = CONCAT_WS(' ', s.error_reason, 'Duplicate team row in group;')
      FROM d
     WHERE s.league_id   = d.league_id
       AND s.season_year = d.season_year
       AND COALESCE(s.group_label,'') = d.g
       AND s.team_id     = d.team_id
       AND s.ctid        <> d.keep_ctid;
    GET DIAGNOSTICS r_dups = ROW_COUNT;

    --------------------------------------------------------------------
    -- Rule-9: missing team FK (if teams table exists)
    --------------------------------------------------------------------
    IF to_regclass('public.teams') IS NOT NULL THEN
      UPDATE stg_standings s
         SET is_valid = FALSE,
             error_reason = CONCAT_WS(' ', s.error_reason, 'Missing team FK;')
        WHERE NOT EXISTS (SELECT 1 FROM teams t WHERE t.team_id = s.team_id);
      GET DIAGNOSTICS r_missing_fk = ROW_COUNT;
    ELSE
      r_missing_fk := 0;
      RAISE NOTICE 'Rule-9 (missing team FK)         → skipped (no teams table).';
    END IF;

    --------------------------------------------------------------------
    -- Totals & log invalid rows to data_load_errors (columns optional)
    --------------------------------------------------------------------
    SELECT COUNT(*) INTO v_total_invalid FROM stg_standings WHERE is_valid = FALSE;

    -- detect optional columns in data_load_errors
    IF to_regclass('public.data_load_errors') IS NOT NULL THEN
      SELECT COUNT(*) INTO v_tmp
        FROM information_schema.columns
       WHERE table_schema='public' AND table_name='data_load_errors' AND column_name='run_ts';
      has_run_ts := v_tmp > 0;

      SELECT COUNT(*) INTO v_tmp
        FROM information_schema.columns
       WHERE table_schema='public' AND table_name='data_load_errors' AND column_name='run_id';
      has_run_id := v_tmp > 0;

      IF has_run_ts AND has_run_id THEN
        INSERT INTO data_load_errors (staging_table, record_key, error_reason, run_ts, run_id)
        SELECT 'stg_standings',
               (league_id||'-'||season_year||'-'||COALESCE(group_label,'')||'-'||team_id)::text,
               error_reason, v_run_ts, v_run_id
          FROM stg_standings
         WHERE is_valid = FALSE AND error_reason IS NOT NULL;

      ELSIF has_run_ts THEN
        INSERT INTO data_load_errors (staging_table, record_key, error_reason, run_ts)
        SELECT 'stg_standings',
               (league_id||'-'||season_year||'-'||COALESCE(group_label,'')||'-'||team_id)::text,
               error_reason, v_run_ts
          FROM stg_standings
         WHERE is_valid = FALSE AND error_reason IS NOT NULL;

      ELSIF has_run_id THEN
        INSERT INTO data_load_errors (staging_table, record_key, error_reason, run_id)
        SELECT 'stg_standings',
               (league_id||'-'||season_year||'-'||COALESCE(group_label,'')||'-'||team_id)::text,
               error_reason, v_run_id
          FROM stg_standings
         WHERE is_valid = FALSE AND error_reason IS NOT NULL;

      ELSE
        INSERT INTO data_load_errors (staging_table, record_key, error_reason)
        SELECT 'stg_standings',
               (league_id||'-'||season_year||'-'||COALESCE(group_label,'')||'-'||team_id)::text,
               error_reason
          FROM stg_standings
         WHERE is_valid = FALSE AND error_reason IS NOT NULL;
      END IF;
    ELSE
      RAISE NOTICE 'data_load_errors table not found; skipping error log.';
    END IF;

    --------------------------------------------------------------------
    -- Final summary
    --------------------------------------------------------------------
    RAISE NOTICE 'Summary → total rows: %, invalid: %', v_total_rows, v_total_invalid;
    RAISE NOTICE 'Breakdown: R1=% R2=% R3=% R4=% R5=% R6=% R7=% R8=% R9=%',
                  r_missing_keys, r_neg_points, r_rank_lt1, r_neg_fields,
                  r_played_mis, r_goalsdiff_mis, r_points_mis, r_dups, r_missing_fk;
END;$procedure$
