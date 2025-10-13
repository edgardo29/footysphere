// src/pages/leaguePage/LeaguesPage.jsx
import React, { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import Footer from "../homePage/Footer";

// pull in the shared theme tokens & core classes (tile, section header, etc.)
import "../homePage/styles/homePage.css";
import "./styles/leaguesPage.css";

import StandingsTab from "./StandingsTab";
import WeeklyMatchesTab from "./WeeklyMatchesTab";

const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");

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
        const res = await fetch(`${API}/leaguesPage/${leagueIdNum}`, { signal: ctrl.signal });
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

  if (loading) return <p className="muted main-content">Loading…</p>;
  if (err) return <p className="muted main-content">{err}</p>;
  if (!league) return <p className="muted main-content">League not found</p>;

  const seasonLabel = league.current_season
    ? `${formatSeason(league.current_season)} Season`
    : "Season";

  return (
    <div className="app">
      <NavBar />

      <main className="main-content leagues-page">
        {/* Back button */}
        <div className="header-actions">
          <Link to="/" className="back-button" aria-label="Back to Home">
            <span className="back-arrow">←</span>
            Back to Home
          </Link>
        </div>

        {/* League header tile */}
        <section className="section-block">
          <div className="league-header tile">
            <div className="league-logo-wrap">
              <img
                src={league.league_logo_url}
                alt={`${league.name} logo`}
                className="league-logo"
                loading="lazy"
              />
            </div>

            <div className="header-text">
              <h1 className="league-title">{league.name}</h1>
              <p className="season-text">{seasonLabel}</p>
            </div>
          </div>
        </section>

        {/* Tabs */}
        <section className="section-block">
          <div className="league-tabs">
            <button
              className={`league-tab ${activeTab === "standings" ? "active" : ""}`}
              onClick={() => setActiveTab("standings")}
              type="button"
            >
              Standings
            </button>
            <button
              className={`league-tab ${activeTab === "weekly" ? "active" : ""}`}
              onClick={() => setActiveTab("weekly")}
              type="button"
            >
              Weekly Matches
            </button>
          </div>

          <div className="league-content">
            {activeTab === "standings" && (
              <StandingsTab leagueId={leagueIdNum} season={league.current_season} />
            )}
            {activeTab === "weekly" && <WeeklyMatchesTab leagueId={leagueIdNum} />}
          </div>
        </section>
      </main>
            <Footer />

    </div>
  );
}
