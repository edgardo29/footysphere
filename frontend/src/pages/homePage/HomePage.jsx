// src/pages/homePage/HomePage.jsx
import React, { useState, useEffect, useMemo } from "react";
import "../../App.css";
import "./styles/homePage.css";
import "./styles/footer.css";
import Footer from "./Footer";

import { Link, useNavigate } from "react-router-dom";
import NavBar from "./NavBar";
import TopBanner from "./TopBanner";
import LeagueModal from "./LeagueModal";

import { FaTrophy, FaCalendarAlt } from "react-icons/fa";

// Helper to try multiple endpoints safely
async function firstPopularThatWorks(urls) {
  for (const u of urls) {
    try {
      const res = await fetch(u);
      if (!res.ok) continue;
      const data = await res.json();
      if (Array.isArray(data) && data.length) return data;
    } catch {}
  }
  return [];
}

export default function HomePage() {
  const [leagues, setLeagues] = useState([]);
  const [matchesByLeague, setMatchesByLeague] = useState({});
  const [loadingLeagues, setLoadingLeagues] = useState(true);
  const [loadingMatches, setLoadingMatches] = useState(true);
  const [showLeagueModal, setShowLeagueModal] = useState(false);

  const navigate = useNavigate();

  const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");


  // Popular leagues (DB-ordered endpoint first)
  useEffect(() => {
    let mounted = true;
    (async () => {
      setLoadingLeagues(true);
      const data = await firstPopularThatWorks([
        `${API}/leagues/popular`,
        `${API}/leagues/popular?limit=5`,
        `${API}/leagueModal/popular?limit=5`,
      ]);
      if (mounted) {
        setLeagues((data || []).slice(0, 5));
        setLoadingLeagues(false);
      }
    })();
    return () => { mounted = false; };
  }, []);

  // Today's matches
  useEffect(() => {
    fetch(`${API}/matches/today`)
      .then((res) => res.json())
      .then((data) => {
        const grouped = (Array.isArray(data) ? data : []).reduce((acc, m) => {
          (acc[m.league_name] ??= []).push(m);
          return acc;
        }, {});
        setMatchesByLeague(grouped);
      })
      .catch(console.error)
      .finally(() => setLoadingMatches(false));
  }, []);

  const allMatches = useMemo(
    () => Object.values(matchesByLeague).flat(),
    [matchesByLeague]
  );

  const tickerMatches = useMemo(() => {
    const arr = (allMatches || []).slice().sort(
      (a, b) => new Date(a.kickoff_utc) - new Date(b.kickoff_utc)
    );
    return arr.slice(0, 10);
  }, [allMatches]);

  const handleSelectLeague = (lg) => {
    setShowLeagueModal(false);
    navigate(`/league/${lg.id}`);
  };

  return (
    <div className="app">
      <NavBar />
      <TopBanner tickerMatches={tickerMatches} visual="orbs" />

      <main className="main-content">
        {/* Popular Leagues */}
        <section id="leagues" className="section-block">
          <div className="section-header">
            <div className="section-icon-box">
              <FaTrophy className="section-icon" />
            </div>
            <h2 className="section-title">Popular Leagues</h2>
          </div>

          {loadingLeagues && <p className="muted">Loading leagues…</p>}

          <div className="leagues-grid">
            {leagues.map((lg) => (
              <Link
                key={lg.id}
                to={`/league/${lg.id}`}
                className="league-card tile tile--interactive"
              >
                <div className="league-logo-wrap">
                  <img
                    src={lg.league_logo_url}
                    alt={`${lg.name} logo`}
                    className="league-logo"
                    loading="lazy"
                  />
                </div>
                <div className="league-text">
                  <h3>{lg.name}</h3>
                  <p>{lg.country}</p>
                </div>
              </Link>
            ))}
          </div>

          <div className="view-more-container">
            <button
              type="button"
              className="view-more-button"
              onClick={() => setShowLeagueModal(true)}
              aria-label="View more leagues"
              title="View more leagues"
            >
              View More
            </button>
          </div>
        </section>

        {/* Today's Matches */}
        <section id="today" className="section-block">
          <div className="section-header">
            <div className="section-icon-box matches-icon-box">
              <FaCalendarAlt className="section-icon matches-icon" />
            </div>
            <h2 className="section-title">Today's Matches</h2>
          </div>

          {loadingMatches && <p className="muted">Loading matches…</p>}

          <div className="todays-leagues-grid">
            {Object.entries(matchesByLeague)
              .filter(([, arr]) => arr.length)
              .map(([leagueName, leagueMatches]) => (
                <div key={leagueName} className="league-block tile">
                  <div className="group-header">
                    <span className="group-dot" />
                    <span className="group-title">{leagueName}</span>
                  </div>

                  {leagueMatches.map((m) => (
                    <div key={m.id} className="match-card">
                      <div className="team-block">
                        <div className="team-logo-wrap">
                          <img
                            src={m.home_logo}
                            alt={m.home_name}
                            className="team-logo"
                            loading="lazy"
                          />
                        </div>
                        <div className="team-info">
                          <span className="team-name">{m.home_name}</span>
                          <span className="team-place">Home</span>
                        </div>
                      </div>

                      <div className="match-details">
                        <span className="match-time">
                          {new Date(m.kickoff_utc).toLocaleTimeString([], {
                            hour: "2-digit",
                            minute: "2-digit",
                          })}
                        </span>
                        <span className={`match-status ${m.status === "Live" ? "live" : ""}`}>
                          {m.status}
                        </span>
                      </div>

                      <div className="team-block right">
                        <div className="team-info right-info">
                          <span className="team-name">{m.away_name}</span>
                          <span className="team-place">Away</span>
                        </div>
                        <div className="team-logo-wrap">
                          <img
                            src={m.away_logo}
                            alt={m.away_name}
                            className="team-logo"
                            loading="lazy"
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              ))}
          </div>
        </section>
      </main>

      <Footer />

      <LeagueModal
        open={showLeagueModal}
        onClose={() => setShowLeagueModal(false)}
        onSelect={handleSelectLeague}
      />
    </div>
  );
}
