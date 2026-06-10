# create_user_favorite_teams_table.py
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../test_scripts")))

from get_db_conn import get_db_connection

def create_user_favorite_teams_table():
    """
    Creates the `user_favorite_teams` join table.

    Purpose:
      Stores which teams a user has favorited.

    Columns:
      user_id   (BIGINT) -> references `users.user_id`
      team_id   (INT)    -> references `teams.team_id`
      load_date (TIMESTAMP) default now()
      upd_date  (TIMESTAMP) default now()

    Constraints:
      PRIMARY KEY (user_id, team_id) prevents duplicates.
      ON DELETE CASCADE so favorites are removed if the user/team is deleted.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS user_favorite_teams (
        user_id BIGINT NOT NULL,
        team_id INT NOT NULL,
        load_date TIMESTAMP(0) NOT NULL DEFAULT now(),
        upd_date  TIMESTAMP(0) NOT NULL DEFAULT now(),

        PRIMARY KEY (user_id, team_id),

        CONSTRAINT fk_uft_user
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,

        CONSTRAINT fk_uft_team
            FOREIGN KEY (team_id) REFERENCES teams(team_id) ON DELETE CASCADE
    );
    """

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("`user_favorite_teams` table created successfully.")
    except Exception as e:
        print(f"Error creating `user_favorite_teams` table: {e}")
        raise

if __name__ == "__main__":
    create_user_favorite_teams_table()