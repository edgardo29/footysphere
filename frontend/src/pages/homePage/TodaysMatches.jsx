import React from "react";
import { FaCalendarAlt } from "react-icons/fa";
import "./styles/todaysMatches.css";

export default function TodaysMatches({ matchesByLeague, loading }) {
  return (
    <section id="today" className="section-block">
      <div className="section-header">
        <div className="section-icon-box matches-icon-box">
          <FaCalendarAlt className="section-icon matches-icon" />
        </div>
        <h2 className="section-title">Today's Matches</h2>
      </div>

      {loading && <p className="muted">Loading matches…</p>}

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
                    <span
                      className={`match-status ${
                        m.status === "Live" ? "live" : ""
                      }`}
                    >
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
  );
}
