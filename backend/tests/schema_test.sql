
BEGIN;

-- Drop in dependency order so reruns are clean
DROP TABLE IF EXISTS fixtures;
DROP TABLE IF EXISTS teams;
DROP TABLE IF EXISTS leagues;

CREATE TABLE leagues (
    league_id        INTEGER PRIMARY KEY,
    league_name      TEXT NOT NULL,
    league_logo_url  TEXT,
    league_country   TEXT,
    is_popular       BOOLEAN NOT NULL DEFAULT FALSE,
    display_order    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE teams (
    team_id        INTEGER PRIMARY KEY,
    team_name      TEXT NOT NULL,
    team_logo_url  TEXT
);

CREATE TABLE fixtures (
    fixture_id     INTEGER PRIMARY KEY,
    league_id      INTEGER NOT NULL REFERENCES leagues(league_id),
    home_team_id   INTEGER NOT NULL REFERENCES teams(team_id),
    away_team_id   INTEGER NOT NULL REFERENCES teams(team_id),
    fixture_date   TIMESTAMPTZ NOT NULL,
    status         TEXT NOT NULL
);

COMMIT;
