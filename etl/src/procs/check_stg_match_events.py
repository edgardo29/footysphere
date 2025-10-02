#!/usr/bin/env python3
"""
Run the PL/pgSQL procedure that flags invalid rows in stg_match_events
and print a readable per-rule summary.
"""
import os, sys, psycopg2
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../test_scripts")))
from get_db_conn import get_db_connection

def main():
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        # Run validation
        cur.execute("CALL check_stg_match_events();")
        conn.commit()

        # Summarize per rule (match on reason text set by the proc)
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE is_valid = FALSE AND error_reason ILIKE '%Missing fixture/team%') AS fk_count,
              COUNT(*) FILTER (WHERE is_valid = FALSE AND error_reason ILIKE '%Bad minute%')           AS minute_count,
              COUNT(*) FILTER (WHERE is_valid = FALSE AND error_reason ILIKE '%Unknown type%')         AS type_count,
              COUNT(*) FILTER (WHERE is_valid = FALSE AND error_reason ILIKE '%Duplicate%')            AS dup_count,
              COUNT(*)                                                                                AS total_rows,
              COUNT(*) FILTER (WHERE is_valid IS TRUE)                                                AS valid_rows,
              COUNT(*) FILTER (WHERE is_valid IS FALSE)                                               AS invalid_rows
            FROM stg_match_events;
        """)
        fk_count, minute_count, type_count, dup_count, total_rows, valid_rows, invalid_rows = cur.fetchone()

        # Pretty print
        print("\nValidation summary — stg_match_events")
        print("--------------------------------------")
        print(f"Foreign key failures (missing fixture/team): {fk_count}")
        print(f"Minute out of range / invalid extra      : {minute_count}")
        print(f"Unknown event type                        : {type_count}")
        print(f"Duplicate events (fixture+minute(+extra)+"
              f"team+player+type+detail)                 : {dup_count}")
        print("--------------------------------------")
        print(f"Valid rows                                : {valid_rows}")
        print(f"Invalid rows                              : {invalid_rows}")
        print(f"Total rows                                : {total_rows}\n")

    except Exception as exc:
        conn.rollback()
        print("Validation error:", exc)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
