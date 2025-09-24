// src/pages/leaguePage/LeaguesPage.jsx
import React, { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import "./styles/leaguesPage.css";
import StandingsTab from "./StandingsTab";
import WeeklyMatchesTab from "./WeeklyMatchesTab";

function formatSeason(year) {
  if (!year || Number.isNaN(Number(year))) return "";
  const nextYY = String((Number(year) + 1) % 100).padStart(2, "0");
  return `${year}/${nextYY}`;
}

export default function LeaguesPage() {
  const { leagueId } = useParams();
  const leagueIdNum = Number(leagueId);

  const [league, setLeague] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [activeTab, setActiveTab] = useState("standings"); // "standings" | "weekly"

  useEffect(() => {
    if (!leagueIdNum) {
      setErr("Invalid league id.");
      setLoading(false);
      return;
    }
    const ctrl = new AbortController();
    async function fetchLeagueDetail() {
      try {
        setLoading(true);
        setErr(null);
        const res = await fetch(`/api/leaguesPage/${leagueIdNum}`, { signal: ctrl.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setLeague(data);
      } catch (e) {
        if (e.name !== "AbortError") {
          console.error("Failed to load league detail:", e);
          setErr("Failed to load league.");
          setLeague(null);
        }
      } finally {
        setLoading(false);
      }
    }
    fetchLeagueDetail();
    return () => ctrl.abort();
  }, [leagueIdNum]);

  if (loading) return <p className="loading">Loading…</p>;
  if (err) return <p className="error">{err}</p>;
  if (!league) return <p className="error">League not found</p>;

  const seasonLabel = league.current_season ? `${formatSeason(league.current_season)} Season` : "Season";

  return (
    <div>
      <NavBar />
      <div className="leagues-page">
        {/* Header / Back button */}
        <div className="header-container">
          <Link to="/" className="back-button">
            <span className="back-arrow">←</span>
            Back to Home
          </Link>

          <div className="league-header">
            <img
              src={league.league_logo_url}
              alt={`${league.name} logo`}
              className="league-logo"
            />
            <div className="header-text">
              <h1 className="league-title">{league.name}</h1>
              <p className="season-text">{seasonLabel}</p>
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div className="league-tabs">
          <button
            className={`league-tab ${activeTab === "standings" ? "active" : ""}`}
            onClick={() => setActiveTab("standings")}
          >
            Standings
          </button>
          <button
            className={`league-tab ${activeTab === "weekly" ? "active" : ""}`}
            onClick={() => setActiveTab("weekly")}
          >
            Weekly Matches
          </button>
        </div>

        {/* Content */}
        <div className="league-content">
          {activeTab === "standings" && (
            <StandingsTab leagueId={leagueIdNum} season={league.current_season} />
          )}
          {activeTab === "weekly" && (
            <WeeklyMatchesTab leagueId={leagueIdNum} />
          )}
        </div>
      </div>
    </div>
  );
}
