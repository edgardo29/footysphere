-- Finished home-side lookups
CREATE INDEX IF NOT EXISTS ix_fx_finished_home
ON fixtures (league_id, season_year, home_team_id, fixture_date DESC)
WHERE status IN ('FT','AET','PEN')
  AND home_score IS NOT NULL
  AND away_score IS NOT NULL;

-- Finished away-side lookups
CREATE INDEX IF NOT EXISTS ix_fx_finished_away
ON fixtures (league_id, season_year, away_team_id, fixture_date DESC)
WHERE status IN ('FT','AET','PEN')
  AND home_score IS NOT NULL
  AND away_score IS NOT NULL;
