// src/pages/homePage/HomePage.jsx
import React, { useState, useEffect } from "react";
import "../../App.css";
import { Link, useNavigate } from "react-router-dom";
import NavBar from "./NavBar";

import { FaTrophy, FaCalendarAlt } from "react-icons/fa";
import { MdOutlineSportsSoccer } from "react-icons/md"; // keeps eslint happy

export default function HomePage() {
  /* ─────────────── STATE ─────────────── */
  const [activeTab, setActiveTab] = useState("leagues");

  // data fetched from the API
  const [leagues, setLeagues] = useState([]);          // popular leagues cards
  const [matchesByLeague, setMatchesByLeague] = useState({});

  // simple loading flags
  const [loadingLeagues, setLoadingLeagues] = useState(true);
  const [loadingMatches, setLoadingMatches] = useState(true);

  const navigate = useNavigate();

  /* ─────────────── EFFECTS ───────────── */

  // fetch “Popular Leagues” once on mount
  useEffect(() => {
    fetch("/api/leagues/popular")
      .then((res) => res.json())
      .then((data) => setLeagues(data))
      .catch(console.error)
      .finally(() => setLoadingLeagues(false));
  }, []);

  // fetch today’s fixtures once on mount
  useEffect(() => {
    fetch("/api/matches/today")
      .then((res) => res.json())
      .then((data) => {
        // group fixtures by league for easier rendering
        const grouped = data.reduce((acc, m) => {
          acc[m.league_name] ??= [];
          acc[m.league_name].push(m);
          return acc;
        }, {});
        setMatchesByLeague(grouped);
      })
      .catch(console.error)
      .finally(() => setLoadingMatches(false));
  }, []);

  /* ─────────────── RENDER ────────────── */
  return (
    <div className="app">
      <NavBar />

      {/* ────────────── Banner Section ────────────── */}
      <section className="banner-container">
        <div className="banner-wrapper">
          <div className="banner-grid">
            {/* LEFT: angled card */}
            <div className="banner-left-card">
              <h1 className="banner-heading">
                <span className="banner-heading-gradient">
                  Live Football Updates
                </span>
                <br />
                <span className="heading-subtle">Right at Your Fingertips</span>
              </h1>
              <p className="banner-subtitle">
                Track live scores, explore leagues, and stay updated with the
                latest football news from around the globe.
              </p>

              <div className="stats-grid">
                <div className="stat-box">
                  <div className="stat-box-icon" />
                  <div className="stat-box-number">20+</div>
                  <div className="stat-box-label">Leagues</div>
                </div>
                <div className="stat-box">
                  <div className="stat-box-icon" />
                  <div className="stat-box-number">500+</div>
                  <div className="stat-box-label">Teams</div>
                </div>
                <div className="stat-box">
                  <div className="stat-box-icon" />
                  <div className="stat-box-number">100+</div>
                  <div className="stat-box-label">Live Matches</div>
                </div>
              </div>
            </div>

            {/* RIGHT: hero image container (CSS handles background) */}
            <div className="banner-right" />
          </div>
        </div>
      </section>

      {/* ────────────── Main Content ────────────── */}
      <main className="main-content">
        {activeTab === "leagues" && (
          <>
            {/* ───── Popular Leagues ───── */}
            <section className="section-block">
              <div className="section-header">
                <div className="section-icon-box">
                  <FaTrophy className="section-icon" />
                </div>
                <h2 className="section-title">Popular Leagues</h2>
              </div>

              {loadingLeagues && (
                <p style={{ color: "#aaa" }}>Loading leagues…</p>
              )}

              <div className="leagues-grid">
                {leagues.map((lg) => (
                  <Link
                    key={lg.id}
                    to={`/league/${lg.id}`}
                    className="league-card"
                  >
                    <div className="league-logo-wrap">
                      <img
                        src={lg.league_logo_url}  
                        alt={`${lg.name} logo`}
                        className="league-logo"
                      />
                    </div>
                    <div>
                      <h3>{lg.name}</h3>
                      <p>{lg.country}</p>
                    </div>
                  </Link>
                ))}
              </div>

              <div className="view-more-container">
                <button className="view-more-button">View More</button>
              </div>
            </section>

            {/* ───── Today's Matches ───── */}
            <section className="section-block">
              <div className="section-header">
                <div className="section-icon-box matches-icon-box">
                  <FaCalendarAlt className="section-icon matches-icon" />
                </div>
                <h2>Today's Matches</h2>
              </div>

              {loadingMatches && (
                <p style={{ color: "#aaa" }}>Loading matches…</p>
              )}

              <div className="todays-leagues-grid">
                {Object.entries(matchesByLeague)
                  .filter(([, arr]) => arr.length)
                  .map(([leagueName, leagueMatches]) => (
                    <div key={leagueName} className="league-block">
                      <div className="league-info-pill">
                        <span>{leagueName}</span>
                      </div>

                      {leagueMatches.map((m) => (
                        <div key={m.id} className="match-card">
                          {/* Home team */}
                          <div className="team-block">
                            <div className="team-logo-wrap">
                              <img
                                src={m.home_logo}
                                alt={m.home_name}
                                className="team-logo"
                              />
                            </div>
                            <div className="team-info">
                              <span className="team-name">{m.home_name}</span>
                              <span className="team-place">Home</span>
                            </div>
                          </div>

                          {/* kick-off & status */}
                          <div className="match-details">
                            <span className="match-time">
                              {new Date(
                                m.kickoff_utc
                              ).toLocaleTimeString([], {
                                hour: "2-digit",
                                minute: "2-digit",
                              })}
                            </span>
                            <span
                              className={`match-status ${
                                m.status === "Live" ? "live" : ""
                              }`}
                            >
                              {m.status}
                            </span>
                          </div>

                          {/* Away team */}
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
                              />
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ))}
              </div>
            </section>
          </>
        )}

        {/* Placeholder for future News tab */}
        {activeTab === "news" && (
          <section className="section-block">
            <div className="section-header">
              <div className="section-square" />
              <h2>Latest News</h2>
            </div>
            <p style={{ color: "#aaa" }}>News content coming soon…</p>
          </section>
        )}
      </main>
    </div>
  );
}
