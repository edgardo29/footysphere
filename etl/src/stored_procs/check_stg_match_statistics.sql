CREATE OR REPLACE PROCEDURE public.check_stg_match_statistics()
 LANGUAGE plpgsql
AS $procedure$DECLARE
  -- scratch
  cnt                      int := 0;

  -- rule counters (aggregated)
  missing_keys             int := 0;
  negatives_total          int := 0;
  ranges_total             int := 0;
  shot_sums_total          int := 0;
  components_total         int := 0;
  duplicates_marked        int := 0;

  -- table totals
  tot_rows                 int := 0;
  tot_invalid              int := 0;
  tot_valid                int := 0;

  -- fixture coverage (finished fixtures limited to leagues+seasons present in staging)
  fixtures_complete        int := 0;  -- 2 valid rows with team_id present
  fixtures_missing         int := 0;  -- 0 rows in staging
  fixtures_partial         int := 0;  -- 1 valid row

  -- extra DQ signal
  dq_saves_gt_opponent_sog int := 0;

  -- optional per-league coverage reporting
  rec_per_league RECORD;
BEGIN
  --------------------------------------------------------------------------
  -- Missing keys
  --------------------------------------------------------------------------
  UPDATE stg_match_statistics
     SET is_valid = false,
         error_reason = COALESCE(error_reason,'') || 'Missing fixture_id or team_id; '
   WHERE (fixture_id IS NULL OR team_id IS NULL)
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Missing fixture_id or team_id;%');
  GET DIAGNOSTICS cnt = ROW_COUNT;
  missing_keys := cnt;

  --------------------------------------------------------------------------
  -- Negatives (accumulate into one total)
  --------------------------------------------------------------------------
  -- shots_on_goal
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative shots_on_goal; '
   WHERE shots_on_goal < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative shots_on_goal;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- shots_off_goal
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative shots_off_goal; '
   WHERE shots_off_goal < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative shots_off_goal;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- shots_inside_box
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative shots_inside_box; '
   WHERE shots_inside_box < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative shots_inside_box;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- shots_outside_box
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative shots_outside_box; '
   WHERE shots_outside_box < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative shots_outside_box;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- blocked_shots
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative blocked_shots; '
   WHERE blocked_shots < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative blocked_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- goalkeeper_saves
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative goalkeeper_saves; '
   WHERE goalkeeper_saves < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative goalkeeper_saves;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- total_shots
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative total_shots; '
   WHERE total_shots < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative total_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- corners
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative corners; '
   WHERE corners < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative corners;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- offsides
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative offsides; '
   WHERE offsides < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative offsides;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- fouls
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative fouls; '
   WHERE fouls < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative fouls;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- yellow_cards
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative yellow_cards; '
   WHERE yellow_cards < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative yellow_cards;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- red_cards
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative red_cards; '
   WHERE red_cards < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative red_cards;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- total_passes
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative total_passes; '
   WHERE total_passes < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative total_passes;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  -- passes_accurate
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Negative passes_accurate; '
   WHERE passes_accurate < 0
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Negative passes_accurate;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; negatives_total := negatives_total + cnt;

  --------------------------------------------------------------------------
  -- Ranges (0..100)
  --------------------------------------------------------------------------
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Invalid possession (0..100); '
   WHERE possession IS NOT NULL
     AND (possession < 0 OR possession > 100)
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Invalid possession (0..100);%');
  GET DIAGNOSTICS cnt = ROW_COUNT; ranges_total := ranges_total + cnt;

  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Invalid pass_accuracy (0..100); '
   WHERE pass_accuracy IS NOT NULL
     AND (pass_accuracy < 0 OR pass_accuracy > 100)
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Invalid pass_accuracy (0..100);%');
  GET DIAGNOSTICS cnt = ROW_COUNT; ranges_total := ranges_total + cnt;

  --------------------------------------------------------------------------
  -- Shot sums vs totals
  --------------------------------------------------------------------------
  -- on+off > total_shots
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Shots mismatch (on+off > total_shots); '
   WHERE shots_on_goal IS NOT NULL
     AND shots_off_goal IS NOT NULL
     AND total_shots IS NOT NULL
     AND (shots_on_goal + shots_off_goal) > total_shots
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Shots mismatch (on+off > total_shots);%');
  GET DIAGNOSTICS cnt = ROW_COUNT; shot_sums_total := shot_sums_total + cnt;

  -- inside+outside > total_shots
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'Inside+Outside > total_shots; '
   WHERE shots_inside_box IS NOT NULL
     AND shots_outside_box IS NOT NULL
     AND total_shots IS NOT NULL
     AND (shots_inside_box + shots_outside_box) > total_shots
     AND (error_reason IS NULL OR error_reason NOT LIKE '%Inside+Outside > total_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; shot_sums_total := shot_sums_total + cnt;

  --------------------------------------------------------------------------
  -- Each component ≤ total_shots (when both present)
  --------------------------------------------------------------------------
  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'shots_on_goal > total_shots; '
   WHERE shots_on_goal IS NOT NULL
     AND total_shots   IS NOT NULL
     AND shots_on_goal > total_shots
     AND (error_reason IS NULL OR error_reason NOT LIKE '%shots_on_goal > total_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; components_total := components_total + cnt;

  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'shots_off_goal > total_shots; '
   WHERE shots_off_goal IS NOT NULL
     AND total_shots   IS NOT NULL
     AND shots_off_goal > total_shots
     AND (error_reason IS NULL OR error_reason NOT LIKE '%shots_off_goal > total_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; components_total := components_total + cnt;

  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'shots_inside_box > total_shots; '
   WHERE shots_inside_box IS NOT NULL
     AND total_shots    IS NOT NULL
     AND shots_inside_box > total_shots
     AND (error_reason IS NULL OR error_reason NOT LIKE '%shots_inside_box > total_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; components_total := components_total + cnt;

  UPDATE stg_match_statistics
     SET is_valid=false,
         error_reason=COALESCE(error_reason,'') || 'shots_outside_box > total_shots; '
   WHERE shots_outside_box IS NOT NULL
     AND total_shots     IS NOT NULL
     AND shots_outside_box > total_shots
     AND (error_reason IS NULL OR error_reason NOT LIKE '%shots_outside_box > total_shots;%');
  GET DIAGNOSTICS cnt = ROW_COUNT; components_total := components_total + cnt;

  --------------------------------------------------------------------------
  -- Duplicates in staging: keep latest by load_timestamp
  --------------------------------------------------------------------------
  WITH ranked AS (
    SELECT ctid, fixture_id, team_id, load_timestamp,
           ROW_NUMBER() OVER (PARTITION BY fixture_id, team_id ORDER BY load_timestamp DESC) AS rn
    FROM stg_match_statistics
    WHERE fixture_id IS NOT NULL AND team_id IS NOT NULL
  )
  UPDATE stg_match_statistics s
     SET is_valid=false,
         error_reason=COALESCE(s.error_reason,'') || 'Duplicate staging row; '
  FROM ranked r
  WHERE s.ctid = r.ctid
    AND r.rn > 1
    AND (s.error_reason IS NULL OR s.error_reason NOT LIKE '%Duplicate staging row;%');
  GET DIAGNOSTICS duplicates_marked = ROW_COUNT;

  --------------------------------------------------------------------------
  -- Log invalid rows to data_load_errors (append-only)
  --------------------------------------------------------------------------
  INSERT INTO data_load_errors (staging_table, record_key, error_reason)
  SELECT 'stg_match_statistics',
         (COALESCE(fixture_id,-1)::text || '-' || COALESCE(team_id,-1)::text) AS record_key,
         error_reason
  FROM stg_match_statistics
  WHERE is_valid = false
    AND error_reason IS NOT NULL;

  --------------------------------------------------------------------------
  -- Totals
  --------------------------------------------------------------------------
  SELECT COUNT(*) INTO tot_rows FROM stg_match_statistics;
  SELECT COUNT(*) INTO tot_invalid FROM stg_match_statistics WHERE is_valid = false;
  SELECT COUNT(*) INTO tot_valid   FROM stg_match_statistics WHERE is_valid = true;

  --------------------------------------------------------------------------
  -- Coverage by fixture (finished fixtures, scoped by leagues+seasons in staging)
  --------------------------------------------------------------------------
  WITH leagues_in_stg AS (
    SELECT DISTINCT f.league_id, f.season_year
    FROM stg_match_statistics s
    JOIN fixtures f USING (fixture_id)
  ),
  fixture_rollup AS (
    SELECT f.fixture_id,
           COUNT(s.*) FILTER (WHERE s.is_valid AND s.team_id IS NOT NULL) AS valid_rows
    FROM fixtures f
    LEFT JOIN stg_match_statistics s ON s.fixture_id = f.fixture_id
    WHERE f.status IN ('FT','AET','PEN')
      AND (f.league_id, f.season_year) IN (SELECT league_id, season_year FROM leagues_in_stg)
    GROUP BY f.fixture_id
  )
  SELECT
    COUNT(*) FILTER (WHERE valid_rows = 2),
    COUNT(*) FILTER (WHERE valid_rows = 0),
    COUNT(*) FILTER (WHERE valid_rows = 1)
  INTO fixtures_complete, fixtures_missing, fixtures_partial
  FROM fixture_rollup;

  --------------------------------------------------------------------------
  -- Extra DQ: GK saves > opponent shots_on_goal
  --------------------------------------------------------------------------
  SELECT COUNT(*) INTO dq_saves_gt_opponent_sog
  FROM (
    SELECT s1.fixture_id
    FROM stg_match_statistics s1
    JOIN stg_match_statistics s2
      ON s1.fixture_id = s2.fixture_id
     AND s1.team_id    <> s2.team_id
    WHERE s1.is_valid AND s2.is_valid
      AND s1.goalkeeper_saves IS NOT NULL
      AND s2.shots_on_goal   IS NOT NULL
      AND s1.goalkeeper_saves > s2.shots_on_goal
    GROUP BY s1.fixture_id
  ) t;

  --------------------------------------------------------------------------
  -- Minimal, clean summary
  --------------------------------------------------------------------------
  RAISE NOTICE '=== MATCH STATS • STAGING VALIDATION ===';
  RAISE NOTICE 'Rows — total:%  valid:%  invalid:%', tot_rows, tot_valid, tot_invalid;
  RAISE NOTICE 'Fixtures — complete(2):%  missing(0):%  partial(1):%', fixtures_complete, fixtures_missing, fixtures_partial;
  RAISE NOTICE 'Rules — negatives:%  ranges:%  shot_sums:%  components>total:%  duplicates:%',
               negatives_total, ranges_total, shot_sums_total, components_total, duplicates_marked;
  RAISE NOTICE 'DQ — saves>opp_SoG:%', dq_saves_gt_opponent_sog;
  RAISE NOTICE '========================================';

  --------------------------------------------------------------------------
  -- Optional: per-league coverage snapshot (comment out if too chatty)
  --------------------------------------------------------------------------
  FOR rec_per_league IN
    WITH leagues_in_stg AS (
      SELECT DISTINCT f.league_id, f.season_year
      FROM stg_match_statistics s
      JOIN fixtures f USING (fixture_id)
    ),
    roll AS (
      SELECT f.fixture_id, f.league_id,
             COUNT(s.*) FILTER (WHERE s.is_valid AND s.team_id IS NOT NULL) AS valid_rows
      FROM fixtures f
      LEFT JOIN stg_match_statistics s ON s.fixture_id = f.fixture_id
      WHERE f.status IN ('FT','AET','PEN')
        AND (f.league_id, f.season_year) IN (SELECT league_id, season_year FROM leagues_in_stg)
      GROUP BY f.fixture_id, f.league_id
    )
    SELECT league_id,
           COUNT(*) FILTER (WHERE valid_rows = 2) AS complete2,
           COUNT(*) FILTER (WHERE valid_rows = 0) AS missing0,
           COUNT(*) FILTER (WHERE valid_rows = 1) AS partial1
    FROM roll
    GROUP BY league_id
    ORDER BY league_id
  LOOP
    RAISE NOTICE 'League:%  complete:%  missing:%  partial:%',
                 rec_per_league.league_id, rec_per_league.complete2, rec_per_league.missing0, rec_per_league.partial1;
  END LOOP;
END;$procedure$
