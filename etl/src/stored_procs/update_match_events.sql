CREATE OR REPLACE PROCEDURE public.update_match_events()
 LANGUAGE plpgsql
AS $procedure$DECLARE
    -- tunable
    slice_size          CONSTANT INT := 30000;

    -- loop state
    offset_rows         INT := 0;
    last_slice          BOOLEAN := FALSE;

    -- metrics
    slice_events        INT;
    slice_placeholders  INT;
    raw_slice_count     INT;   -- rows pulled before de-dupe
    total_events        INT := 0;
    total_placeholders  INT := 0;
BEGIN
    -- single-run mutex
    PERFORM pg_advisory_xact_lock(42);

    WHILE NOT last_slice LOOP
        WITH base AS (
            SELECT  stg_event_id, fixture_id, minute, minute_extra,
                    team_id, player_id, assist_player_id,
                    event_type, event_detail, comments
            FROM    stg_match_events
            WHERE   is_valid = TRUE
            ORDER BY stg_event_id
            LIMIT   slice_size
            OFFSET  offset_rows
        ),
        -- de-dupe within THIS slice by the exact ON CONFLICT key
        next_slice AS (
            SELECT DISTINCT ON (
                fixture_id,
                minute,
                COALESCE(minute_extra,0),
                team_id,
                COALESCE(player_id,0),
                event_type
            )
                stg_event_id, fixture_id, minute, minute_extra,
                team_id, player_id, assist_player_id,
                event_type, event_detail, comments
            FROM base
            ORDER BY
                fixture_id,
                minute,
                COALESCE(minute_extra,0),
                team_id,
                COALESCE(player_id,0),
                event_type,
                stg_event_id DESC      -- keep the latest row for that key
        ),
        player_gaps AS (
            INSERT INTO players (player_id, firstname, lastname, full_name)
            SELECT DISTINCT
                   ids.player_id, 'Unknown', 'Player', 'Unknown Player'
            FROM (
                SELECT player_id        FROM next_slice WHERE player_id        IS NOT NULL
                UNION
                SELECT assist_player_id FROM next_slice WHERE assist_player_id IS NOT NULL
            ) AS ids
            LEFT JOIN players p ON p.player_id = ids.player_id
            WHERE p.player_id IS NULL
            ON CONFLICT DO NOTHING
            RETURNING 1
        ),
        upsert AS (
            INSERT INTO match_events (
                fixture_id, minute, minute_extra,
                team_id, player_id, assist_player_id,
                event_type, event_detail, comments
            )
            SELECT  fixture_id, minute, minute_extra,
                    team_id, player_id, assist_player_id,
                    event_type, event_detail, comments
            FROM    next_slice
            ON CONFLICT (
                fixture_id,
                minute,
                COALESCE(minute_extra, 0),
                team_id,
                COALESCE(player_id, 0),
                event_type
            )
            DO UPDATE SET
                event_detail = EXCLUDED.event_detail,
                comments     = EXCLUDED.comments,
                upd_date     = now()
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM upsert),
            (SELECT COUNT(*) FROM player_gaps),
            (SELECT COUNT(*) FROM base)
        INTO slice_events, slice_placeholders, raw_slice_count;

        total_events       := total_events       + slice_events;
        total_placeholders := total_placeholders + slice_placeholders;

        RAISE NOTICE 'Slice % processed:', offset_rows / slice_size + 1;
        RAISE NOTICE '   • events upserted          : %', slice_events;
        RAISE NOTICE '   • placeholders created     : %', slice_placeholders;

        -- advance window; decide if we're done based on RAW slice size
        offset_rows := offset_rows + slice_size;
        last_slice  := raw_slice_count < slice_size;
    END LOOP;

    RAISE NOTICE '---------------------------------------------------------';
    RAISE NOTICE 'FINISHED update_match_events() run';
    RAISE NOTICE '  • TOTAL events upserted      : %', total_events;
    RAISE NOTICE '  • TOTAL placeholders created : %', total_placeholders;
END;$procedure$
