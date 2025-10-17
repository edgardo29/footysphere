import React from "react";
import { Link } from "react-router-dom";
import { FaTrophy } from "react-icons/fa";
import "./styles/popularLeagues.css";

export default function PopularLeagues({ leagues, loading, onViewMore }) {
  return (
    <section id="leagues" className="section-block">
      <div className="section-header">
        <div className="section-icon-box">
          <FaTrophy className="section-icon" />
        </div>
        <h2 className="section-title">Popular Leagues</h2>
      </div>

      {loading && <p className="muted">Loading leagues…</p>}

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
          onClick={onViewMore}
          aria-label="View more leagues"
          title="View more leagues"
        >
          View More
        </button>
      </div>
    </section>
  );
}
