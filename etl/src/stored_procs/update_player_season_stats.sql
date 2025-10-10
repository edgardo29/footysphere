CREATE OR REPLACE PROCEDURE public.update_player_season_stats()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    rows_selected integer := 0;   -- distinct rows taken from staging
    rows_inserted integer := 0;   -- new keys added
    rows_updated  integer := 0;   -- existing keys refreshed
BEGIN
    RAISE NOTICE '► update_player_season_stats – starting merge';

    WITH
    latest AS (
        -- newest row per (player,team,league,season)
        SELECT DISTINCT ON (player_id, team_id, league_id, season)
               player_id, team_id, league_id, season,
               position, number,
               appearances, lineups, minutes_played,
               goals, assists, saves,
               dribbles_attempts, dribbles_success,
               fouls_committed, tackles,
               passes_total, passes_key, pass_accuracy,
               yellow_cards, red_cards
        FROM   stg_player_season_stats
        WHERE  is_valid = TRUE
        ORDER  BY player_id, team_id, league_id, season, stg_stats_id DESC
    ),
    merge AS (
        INSERT INTO player_season_stats (
            player_id, team_id, league_id, season,
            position, number,
            appearances, lineups, minutes_played,
            goals, assists, saves,
            dribbles_attempts, dribbles_success,
            fouls_committed, tackles,
            passes_total, passes_key, pass_accuracy,
            yellow_cards, red_cards,
            load_date, upd_date
        )
        SELECT  l.*,
                NOW() AS load_date,
                NOW() AS upd_date
        FROM    latest l
        ON CONFLICT (player_id, team_id, league_id, season)
        DO UPDATE
           SET position          = EXCLUDED.position,
               number            = EXCLUDED.number,
               appearances       = EXCLUDED.appearances,
               lineups           = EXCLUDED.lineups,
               minutes_played    = EXCLUDED.minutes_played,
               goals             = EXCLUDED.goals,
               assists           = EXCLUDED.assists,
               saves             = EXCLUDED.saves,
               dribbles_attempts = EXCLUDED.dribbles_attempts,
               dribbles_success  = EXCLUDED.dribbles_success,
               fouls_committed   = EXCLUDED.fouls_committed,
               tackles           = EXCLUDED.tackles,
               passes_total      = EXCLUDED.passes_total,
               passes_key        = EXCLUDED.passes_key,
               pass_accuracy     = EXCLUDED.pass_accuracy,
               yellow_cards      = EXCLUDED.yellow_cards,
               red_cards         = EXCLUDED.red_cards,
               upd_date          = NOW()
        RETURNING (xmax = 0)  AS was_insert,
                  (xmax <> 0) AS was_update
    ),
    counts AS (
        SELECT
            COUNT(*) FILTER (WHERE was_insert) AS inserted,
            COUNT(*) FILTER (WHERE was_update) AS updated
        FROM merge
    )
    SELECT
        (SELECT COUNT(*) FROM latest),   -- rows_selected
        inserted,                        -- rows_inserted
        updated                          -- rows_updated
    INTO
        rows_selected,
        rows_inserted,
        rows_updated
    FROM counts;

    -- Vertical summary
    RAISE NOTICE 'SUMMARY:';
    RAISE NOTICE '  Rows selected from staging : %', rows_selected;
    RAISE NOTICE '  Rows inserted              : %', rows_inserted;
    RAISE NOTICE '  Rows updated               : %', rows_updated;

    RAISE NOTICE '► update_player_season_stats – completed';
END;
$procedure$
