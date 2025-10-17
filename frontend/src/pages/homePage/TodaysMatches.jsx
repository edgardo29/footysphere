import React, { useEffect, useMemo, useRef, useState } from "react";
import { FaCalendarAlt } from "react-icons/fa";
import "./styles/todaysMatches.css";

export default function TodaysMatches({ matchesByLeague, loading }) {
  // ── Right-rail Quick Find (client-side filter) ──────────────────
  const [q, setQ] = useState("");
  const [qDebounced, setQDebounced] = useState("");
  const inputRef = useRef(null);

  useEffect(() => {
    const t = setTimeout(() => setQDebounced(q.trim()), 120);
    return () => clearTimeout(t);
  }, [q]);

  // "/" to focus search
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "/") {
        e.preventDefault();
        inputRef.current?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Helpers
  const norm = (s) => String(s || "").toLowerCase().trim();
  const isTimeLike = (s) => /^\d{1,2}[:.]?\d{0,2}$/i.test(s);

  const matchRow = (m, query, leagueName) => {
    if (!query) return true;
    const qn = norm(query);

    // time search like "1:30" / "1330"
    if (isTimeLike(qn)) {
      const t = new Date(m.kickoff_utc)
        .toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
        .toLowerCase()
        .replace(/\s+/g, "");
      const qTime = qn.replace(".", ":");
      return t.includes(qTime);
    }

    return (
      norm(m.home_name).includes(qn) ||
      norm(m.away_name).includes(qn) ||
      norm(leagueName).includes(qn)
    );
  };

  // Any matches today? (controls whether the right rail appears)
  const hasAnyToday = useMemo(
    () => Object.values(matchesByLeague || {}).some((arr) => Array.isArray(arr) && arr.length > 0),
    [matchesByLeague]
  );

  // Filtered view (client-side only)
  const filtered = useMemo(() => {
    if (!qDebounced) return matchesByLeague || {};
    const out = {};
    for (const [leagueName, arr] of Object.entries(matchesByLeague || {})) {
      const next = (arr || []).filter((m) => matchRow(m, qDebounced, leagueName));
      if (next.length) out[leagueName] = next;
    }
    return out;
  }, [matchesByLeague, qDebounced]);

  const leaguesToRender = qDebounced ? filtered : (matchesByLeague || {});
  const isFiltering = hasAnyToday && qDebounced.length > 0;

  return (
    <section id="today" className="section-block">
      <div className="section-header">
        <div className="section-icon-box matches-icon-box">
          <FaCalendarAlt className="section-icon matches-icon" />
        </div>
        <h2 className="section-title">Today's Matches</h2>
      </div>

      {loading && <p className="muted">Loading matches…</p>}

      {hasAnyToday ? (
        <div className="today-layout">
          {/* ===== Two content rails (unchanged visuals) ===== */}
          <div className="content-zone">
            <div className={`todays-leagues-grid ${isFiltering ? "is-filtering" : ""}`}>
              {Object.entries(leaguesToRender)
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
          </div>

          {/* ===== Right rail: Search only (hidden when no matches) ===== */}
          <aside className="today-rail">
            <div className="rail-sticky">
              <div className="qf-wrap">
                <input
                  ref={inputRef}
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  className="qf-input"
                  type="text"
                  inputMode="search"
                  placeholder="Quick find: team, league, or time…  (press /)"
                  aria-label="Quick find: filter today's matches"
                />
                {q && (
                  <button
                    type="button"
                    className="qf-clear"
                    aria-label="Clear search"
                    onClick={() => setQ("")}
                  >
                    <svg viewBox="0 0 14 14" aria-hidden="true">
                      <path d="M3 3l8 8M11 3l-8 8" />
                    </svg>
                  </button>
                )}
              </div>
            </div>
          </aside>
        </div>
      ) : (
        // No matches today → rail disappears; your existing empty state stays as-is
        <div className="todays-leagues-grid">{/* intentionally empty */}</div>
      )}
    </section>
  );
}
