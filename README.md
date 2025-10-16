# Footysphere

Personal football analytics web app. Shows recent results, league tables, team fixtures, and match details with key stats. Non-real-time; refreshed weekly from API-Football via Python ETL, stored in Postgres, served by FastAPI, rendered with a React/Vite UI. Snapshots archived in Azure Blob.

## Website Link 
- https://zealous-coast-018496d10.1.azurestaticapps.net/ 

## Features
- Results & featured matches
- League standings
- Team fixtures (past/upcoming)
- Match details (goals, cards, possession, shots, etc.)

## Tech Stack
React/Vite (frontend) · FastAPI (API) · Postgres (DB) · Airflow + Python (ETL) · Azure Blob (snapshots) · **Azure Static Web Apps (frontend hosting)** · **Azure Virtual Machine (API & Airflow)** · API-Football (data)

## Data & Refresh
Weekly incremental updates (plus manual backfills when needed). 

## Airflow DAG Pipelines (names only)
- discover_league_directory_etl
- fixtures_backfill_etl
- ingest_leagues_etl
- fixtures_etl
- match_events_etl
- match_stats_etl
- standings_etl
- players_etl
- player_season_stats_etl
- teams_venues_etl

## Deployment 
- **Frontend**: Azure Static Web Apps 
- **API**: Azure Virtual Machine (Ubuntu) behind Caddy
- **ETL**: Azure Virtual Machine running Airflow 

