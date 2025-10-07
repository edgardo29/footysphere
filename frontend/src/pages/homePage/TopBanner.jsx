import React from "react";
import "./styles/topBanner.css";
import { FaTrophy, FaCalendarAlt } from "react-icons/fa";
import { MdOutlineSportsSoccer } from "react-icons/md";

export default function TopBanner({ tickerMatches = [] }) {
  const fmtTime = (iso) =>
    new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

  return (
    <section className="tb" aria-label="Intro">
      {/* Background */}
      <div className="tb-bg" aria-hidden>
        <span className="bg-mesh" />
        <span className="bg-grid" />
        <span className="bg-vignette" />
        <span className="bg-noise" />
      </div>

      <div className="tb-wrap">
        <div className="tb-left">
          <h1 className="tb-title">
            <span className="tb-strong">All&nbsp;Football.</span>
            <br />
            <span className="tb-soft">One&nbsp;Place.</span>
          </h1>

          <p className="tb-sub">
            Fixtures, results, standings and news — minimal, modern, fast.
          </p>

          <div className="tb-cta">
            <a href="#leagues" className="cta-solid">Explore Leagues</a>
            <a href="#today" className="cta-ghost">Today’s Matches</a>
          </div>
        </div>

        {/* RIGHT — diagonal glass slab + floating stat cards (no matches shown here) */}
        <aside className="hero-right" aria-hidden="true">
          <div className="hero-slab" />
          <span className="beam b1" />
          <span className="beam b2" />
          <div className="panels">
            <div className="panel">
              <div className="pi"><FaTrophy /></div>
              <div className="pt">Leagues</div>
              <div className="pn">20+</div>
              <div className="pd">Domestic & international</div>
            </div>
            <div className="panel">
              <div className="pi"><MdOutlineSportsSoccer /></div>
              <div className="pt">Teams</div>
              <div className="pn">500+</div>
              <div className="pd">Clubs covered</div>
            </div>
            <div className="panel">
              <div className="pi"><FaCalendarAlt /></div>
              <div className="pt">Fixtures</div>
              <div className="pn">100+</div>
              <div className="pd">Fresh every week</div>
            </div>
          </div>
        </aside>
      </div>

      {/* Ticker (only surface for matches in banner) */}
      <div className="tb-ticker" role="marquee" aria-label="Today’s fixtures">
        <div className="fade l" aria-hidden />
        <div className="fade r" aria-hidden />
        <div className="track">
          {tickerMatches.map((m) => (
            <div key={m.id} className="tick">
              <span className="t-time">{fmtTime(m.kickoff_utc)}</span>
              <span className="t-teams">{m.home_name} vs {m.away_name}</span>
              <span className="t-league">{m.league_name}</span>
            </div>
          ))}
          {tickerMatches.map((m, i) => (
            <div key={`dup-${m.id}-${i}`} className="tick">
              <span className="t-time">{fmtTime(m.kickoff_utc)}</span>
              <span className="t-teams">{m.home_name} vs {m.away_name}</span>
              <span className="t-league">{m.league_name}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
