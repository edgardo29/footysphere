CREATE OR REPLACE PROCEDURE public.update_match_statistics()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  off_rows    INT := 0;       -- chunk offset
  lim_rows    INT := 2000;    -- chunk size
  rowcount    INT;            -- rows affected in last upsert
  total_rows  INT := 0;       -- cumulative rows affected
BEGIN
  RAISE NOTICE 'Starting chunked upsert from stg_match_statistics to match_statistics...';

  LOOP
    WITH eligible AS (
      -- Fixtures complete in staging: exactly 2 valid rows with team_id present
      SELECT s.fixture_id
      FROM stg_match_statistics s
      WHERE s.is_valid = true
        AND s.team_id IS NOT NULL
      GROUP BY s.fixture_id
      HAVING COUNT(*) = 2
    ),
    chunk AS (
      SELECT
        stg.fixture_id,
        stg.team_id,
        f.league_id,
        f.season_year,
        stg.shots_on_goal,
        stg.shots_off_goal,
        stg.shots_inside_box,
        stg.shots_outside_box,
        stg.blocked_shots,
        stg.goalkeeper_saves,
        stg.total_shots,
        stg.corners,
        stg.offsides,
        stg.fouls,
        stg.possession,
        stg.yellow_cards,
        stg.red_cards,
        stg.total_passes,
        stg.passes_accurate,
        stg.pass_accuracy,
        stg.expected_goals,
        stg.goals_prevented
      FROM stg_match_statistics stg
      JOIN eligible e ON e.fixture_id = stg.fixture_id
      JOIN fixtures f ON f.fixture_id = stg.fixture_id
      WHERE stg.is_valid = true
        AND stg.team_id IS NOT NULL
        AND f.status IN ('FT','AET','PEN')     -- finished fixtures only
      ORDER BY stg.fixture_id, stg.team_id
      LIMIT lim_rows OFFSET off_rows
    )
    INSERT INTO match_statistics (
      fixture_id, team_id,
      league_id, season_year,
      shots_on_goal, shots_off_goal, shots_inside_box, shots_outside_box,
      blocked_shots, goalkeeper_saves, total_shots,
      corners, offsides, fouls, possession,
      yellow_cards, red_cards,
      total_passes, passes_accurate, pass_accuracy,
      expected_goals, goals_prevented
    )
    SELECT
      fixture_id, team_id,
      league_id, season_year,
      shots_on_goal, shots_off_goal, shots_inside_box, shots_outside_box,
      blocked_shots, goalkeeper_saves, total_shots,
      corners, offsides, fouls, possession,
      yellow_cards, red_cards,
      total_passes, passes_accurate, pass_accuracy,
      expected_goals, goals_prevented
    FROM chunk
    ON CONFLICT (fixture_id, team_id)
    DO UPDATE SET
      league_id         = EXCLUDED.league_id,
      season_year       = EXCLUDED.season_year,
      shots_on_goal     = EXCLUDED.shots_on_goal,
      shots_off_goal    = EXCLUDED.shots_off_goal,
      shots_inside_box  = EXCLUDED.shots_inside_box,
      shots_outside_box = EXCLUDED.shots_outside_box,
      blocked_shots     = EXCLUDED.blocked_shots,
      goalkeeper_saves  = EXCLUDED.goalkeeper_saves,
      total_shots       = EXCLUDED.total_shots,
      corners           = EXCLUDED.corners,
      offsides          = EXCLUDED.offsides,
      fouls             = EXCLUDED.fouls,
      possession        = EXCLUDED.possession,
      yellow_cards      = EXCLUDED.yellow_cards,
      red_cards         = EXCLUDED.red_cards,
      total_passes      = EXCLUDED.total_passes,
      passes_accurate   = EXCLUDED.passes_accurate,
      pass_accuracy     = EXCLUDED.pass_accuracy,
      expected_goals    = EXCLUDED.expected_goals,
      goals_prevented   = EXCLUDED.goals_prevented,
      upd_date          = NOW();

    GET DIAGNOSTICS rowcount = ROW_COUNT;
    total_rows := total_rows + rowcount;
    RAISE NOTICE 'Offset % => upserted % rows in chunk.', off_rows, rowcount;

    IF rowcount < lim_rows THEN
      EXIT;
    END IF;

    off_rows := off_rows + lim_rows;
  END LOOP;

  RAISE NOTICE 'Upsert completed. Total rows processed=%', total_rows;
END;
$procedure$
