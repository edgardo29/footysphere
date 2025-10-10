CREATE OR REPLACE PROCEDURE public.update_standings()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    rows_ins           INT := 0;
    rows_upd           INT := 0;
    rows_skipped_nulls INT := 0;
    rows_no_form       INT := 0;

    -- Adjust if you use other “finished” status codes in fixtures.status
    FINISHED_STATUSES  TEXT[] := ARRAY['FT','AET','PEN'];

    -- Persisted window; FE can still render 5
    v_form_window      SMALLINT := 5;
BEGIN
    /*
      Schemas used (matching your definitions):
        stg_standings(league_id, season_year, team_id, rank, points, played, win, draw, lose,
                      goals_for, goals_against, goals_diff, group_label, is_valid, ...)
        fixtures(fixture_id, league_id, season_year, home_team_id, away_team_id, fixture_date,
                 status, round, home_score, away_score, venue_id, ...)
    */

    WITH
    -- 1) Candidates from staging
    s_raw AS (
        SELECT
            s.league_id, s.season_year, s.team_id, COALESCE(s.group_label,'') AS group_label,
            s.rank, s.points, s.played, s.win, s.draw, s.lose,
            s.goals_for, s.goals_against,
            COALESCE(s.goals_diff, s.goals_for - s.goals_against) AS goals_diff
        FROM stg_standings s
        WHERE s.is_valid = TRUE
    ),

    -- 2) Split invalid vs valid based on required NOT NULLs in target
    s_invalid AS (
        SELECT *
        FROM s_raw
        WHERE rank IS NULL
           OR points IS NULL
           OR played IS NULL
           OR win IS NULL
           OR draw IS NULL
           OR lose IS NULL
           OR goals_for IS NULL
           OR goals_against IS NULL
           OR goals_diff IS NULL
    ),
    s_valid AS (
        SELECT *
        FROM s_raw
        WHERE (league_id, season_year, team_id, group_label) NOT IN
              (SELECT league_id, season_year, team_id, group_label FROM s_invalid)
    ),

    -- 3) Pull the last N finished fixtures per team and compute W/D/L using home_score/away_score
    last_n_results AS (
        SELECT
            sv.league_id,
            sv.season_year,
            sv.team_id,
            f.fixture_date,
            CASE
                WHEN f.home_team_id = sv.team_id THEN
                    CASE
                        WHEN f.home_score >  f.away_score THEN 'W'
                        WHEN f.home_score =  f.away_score THEN 'D'
                        ELSE 'L'
                    END
                ELSE
                    CASE
                        WHEN f.away_score >  f.home_score THEN 'W'
                        WHEN f.away_score =  f.home_score THEN 'D'
                        ELSE 'L'
                    END
            END AS wdl
        FROM s_valid sv
        JOIN LATERAL (
            SELECT
                fx.fixture_date,
                fx.home_team_id,
                fx.away_team_id,
                fx.home_score,
                fx.away_score
            FROM fixtures fx
            WHERE fx.league_id   = sv.league_id
              AND fx.season_year = sv.season_year
              AND (fx.home_team_id = sv.team_id OR fx.away_team_id = sv.team_id)
              AND fx.status = ANY (FINISHED_STATUSES)
              AND fx.home_score IS NOT NULL
              AND fx.away_score IS NOT NULL
            ORDER BY fx.fixture_date DESC
            LIMIT v_form_window
        ) f ON TRUE
    ),

    -- 4) Build compact string, oldest→newest so newest ends on the right
    form_calc AS (
        SELECT
            league_id,
            season_year,
            team_id,
            STRING_AGG(wdl, '' ORDER BY fixture_date ASC) AS form_compact
        FROM last_n_results
        GROUP BY league_id, season_year, team_id
    ),

    invalid_count AS (
        SELECT COUNT(*) AS n FROM s_invalid
    ),

    -- 5) Upsert into target; update only when something actually changes
    upsert AS (
        INSERT INTO standings AS tgt (
            league_id, season_year, team_id, group_label,
            rank, points, played, win, draw, lose,
            goals_for, goals_against, goals_diff,
            form_window, form_compact,
            load_date, upd_date
        )
        SELECT
            sv.league_id, sv.season_year, sv.team_id, sv.group_label,
            sv.rank, sv.points, sv.played, sv.win, sv.draw, sv.lose,
            sv.goals_for, sv.goals_against, sv.goals_diff,
            v_form_window,
            COALESCE(fc.form_compact, ''),    -- empty if no finished fixtures yet
            NOW(), NOW()
        FROM s_valid sv
        LEFT JOIN form_calc fc
               ON fc.league_id   = sv.league_id
              AND fc.season_year = sv.season_year
              AND fc.team_id     = sv.team_id

        ON CONFLICT (league_id, season_year, team_id, group_label)
        DO UPDATE SET
            rank          = EXCLUDED.rank,
            points        = EXCLUDED.points,
            played        = EXCLUDED.played,
            win           = EXCLUDED.win,
            draw          = EXCLUDED.draw,
            lose          = EXCLUDED.lose,
            goals_for     = EXCLUDED.goals_for,
            goals_against = EXCLUDED.goals_against,
            goals_diff    = EXCLUDED.goals_diff,
            form_window   = EXCLUDED.form_window,
            form_compact  = EXCLUDED.form_compact,
            upd_date      = NOW()
        WHERE (tgt.rank,  tgt.points,  tgt.played,
               tgt.win,   tgt.draw,    tgt.lose,
               tgt.goals_for, tgt.goals_against, tgt.goals_diff,
               tgt.form_window, tgt.form_compact)
              IS DISTINCT FROM
              (EXCLUDED.rank, EXCLUDED.points, EXCLUDED.played,
               EXCLUDED.win,  EXCLUDED.draw,   EXCLUDED.lose,
               EXCLUDED.goals_for, EXCLUDED.goals_against, EXCLUDED.goals_diff,
               EXCLUDED.form_window, EXCLUDED.form_compact)
        RETURNING (xmax = 0) AS was_insert
    )

    SELECT
        (SELECT COUNT(*) FILTER (WHERE was_insert)     FROM upsert),
        (SELECT COUNT(*) FILTER (WHERE NOT was_insert) FROM upsert),
        (SELECT n FROM invalid_count),
        (SELECT COUNT(*) FROM s_valid sv
            LEFT JOIN form_calc fc
                   ON fc.league_id   = sv.league_id
                  AND fc.season_year = sv.season_year
                  AND fc.team_id     = sv.team_id
           WHERE COALESCE(fc.form_compact, '') = '')
    INTO rows_ins, rows_upd, rows_skipped_nulls, rows_no_form;

    RAISE NOTICE 'update_standings(): % inserted, % updated, % skipped(nulls), % with no finished matches (empty form)',
                 rows_ins, rows_upd, rows_skipped_nulls, rows_no_form;
END;
$procedure$
