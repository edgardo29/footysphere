// src/pages/matchPage/MatchDetails.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import "./styles/matchDetails.css";

/**
 * Mocks live NEXT TO this file:
 *   ./match_<MATCHID>_details_<SEASON>.json
 */

const MOCKS = import.meta.glob("./match_*_details_*.json", {
  eager: true,
  import: "default",
});

function loadMockOrThrow(localPath) {
  const data = MOCKS[localPath];
  if (!data) throw new Error(`Missing mock file: ${localPath}`);
  return data;
}

function fmtWhen(iso) {
  if (!iso) return "TBD";
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return "TBD";
  const date = dt.toLocaleDateString();
  const time = dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return `${date} ${time}`;
}
const minuteLabel = (m) => (typeof m === "number" ? `${m}′` : `${m}′`);

const STAT_ROWS = [
  { key: "possession_pct", label: "Possession", isPct: true, suffix: "%" },
  { key: "shots_total", label: "Total Shots" },
  { key: "shots_on_target", label: "Shots on Target" },
  { key: "shots_inside_box", label: "Shots Inside Box" },
  { key: "shots_outside_box", label: "Shots Outside Box" },
  { key: "corners", label: "Corners" },
  { key: "offsides", label: "Offsides" },
  { key: "passes_total", label: "Total Passes" },
  { key: "pass_accuracy_pct", label: "Pass Accuracy", isPct: true, suffix: "%" },
  { key: "fouls", label: "Fouls" },
  { key: "yellow", label: "Yellow Cards" },
  { key: "red", label: "Red Cards" }
];

export default function MatchDetails() {
  const { matchId } = useParams();
  const [searchParams] = useSearchParams();
  const season = searchParams.get("season") || String(new Date().getFullYear());
  const teamIdFromQuery = searchParams.get("teamId"); // for "Back to Team"

  const [match, setMatch] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    let canceled = false;
    setLoading(true);
    setErr("");
    const file = `./match_${matchId}_details_${season}.json`;
    try {
      const json = loadMockOrThrow(file);
      if (!canceled) setMatch(json);
    } catch (e) {
      console.error(e);
      if (!canceled) setErr(`Could not load match details.\nExpected: ${file}`);
    } finally {
      if (!canceled) setLoading(false);
    }
    return () => { canceled = true; };
  }, [matchId, season]);

  // Derived – goal lists
  const goals = useMemo(() => {
    if (!match?.events) return { home: [], away: [] };
    const homeId = match?.home?.id;
    const awayId = match?.away?.id;
    const isGoalType = (t) => ["goal", "penalty", "own_goal"].includes(String(t).toLowerCase());
    const all = (match.events || []).filter((e) => isGoalType(e.type));
    return {
      home: all.filter((e) => e.team_id === homeId).sort((a, b) => (a.minute || 0) - (b.minute || 0)),
      away: all.filter((e) => e.team_id === awayId).sort((a, b) => (a.minute || 0) - (b.minute || 0)),
    };
  }, [match]);

  // Derived – event timeline
  const timeline = useMemo(() => {
    const events = Array.isArray(match?.events) ? match.events.slice() : [];
    events.sort((a, b) => {
      const toNum = (x) => (typeof x === "number" ? x : Number(String(x).split("+")[0]) || 0);
      return toNum(a.minute) - toNum(b.minute);
    });
    return events;
  }, [match]);

  const scoreFT = match?.score?.fulltime;
  const status = match?.status || "—";
  const showScore = ["full-time", "live"].includes(status.toLowerCase());

  if (loading) {
    return (
      <div>
        <NavBar />
        <div className="match-page"><p className="loading">Loading match…</p></div>
      </div>
    );
  }
  if (err) {
    return (
      <div>
        <NavBar />
        <div className="match-page"><pre className="error">{err}</pre></div>
      </div>
    );
  }
  if (!match) {
    return (
      <div>
        <NavBar />
        <div className="match-page"><p className="error">No data.</p></div>
      </div>
    );
  }

  // helper to render goal-type chip
  const GoalChip = ({ type }) => {
    const t = String(type || "").toLowerCase();
    const label = t === "penalty" ? "Penalty" : t === "own_goal" ? "Own Goal" : "Goal";
    return (
      <span
        className={`chip ${t === "penalty" ? "penalty" : t === "own_goal" ? "og" : "goal"}`}
        role="img"
        aria-label={label}
        title={label}
      >
        {t === "penalty" ? "P" : t === "own_goal" ? "OG" : "⚽"}
      </span>
    );
  };

  return (
    <div>
      <NavBar />
      <div className="match-page">
        {/* Header actions */}
        <div className="header-container">
          <div className="header-actions">
            <Link to="/" className="back-button">
              <span className="back-arrow">←</span>
              Back to Home
            </Link>

            <div className="right-actions">
              {teamIdFromQuery ? (
                <Link
                  className="ghost-button"
                  to={`/team/${teamIdFromQuery}?season=${season}`}
                  title="Back to Team"
                >
                  ← Back to Team
                </Link>
              ) : null}
              {match?.competition?.name ? (
                <span className="comp-pill" aria-label={`Competition ${match.competition.name}`}>
                  {match.competition.name}
                </span>
              ) : null}
            </div>
          </div>

          {/* Overview card */}
          <div className="match-header">
            <div className="teams-vs">
              <div className="team">
                {match.home?.logo ? (
                  <img className="logo" src={match.home.logo} alt={`${match.home?.name} logo`} />
                ) : null}
                <div className="name">{match.home?.name ?? "—"}</div>
              </div>

              <div className="score-block">
                {showScore && typeof scoreFT?.home === "number" && typeof scoreFT?.away === "number" ? (
                  <div className="score" aria-label={`Final score ${scoreFT.home} to ${scoreFT.away}`}>
                    <span>{scoreFT.home}</span>
                    <span className="dash">–</span>
                    <span>{scoreFT.away}</span>
                  </div>
                ) : (
                  <div className="score placeholder" aria-label="Match not finished">vs</div>
                )}
                <div
                  className={`status-chip ${status === "Live" ? "live" : ""}`}
                  aria-label={`Status: ${status}`}
                  title={status}
                >
                  {status === "Live" ? <span className="pulse" /> : null}
                  {status}
                </div>
              </div>

              <div className="team right">
                {match.away?.logo ? (
                  <img className="logo" src={match.away.logo} alt={`${match.away?.name} logo`} />
                ) : null}
                <div className="name">{match.away?.name ?? "—"}</div>
              </div>
            </div>

            <div className="meta-row">
              <span className="meta">{fmtWhen(match.date_utc)}</span>
              {match.round ? (
                <>
                  <span className="dot">·</span>
                  <span className="meta">{match.round}</span>
                </>
              ) : null}
              {match.venue?.name ? (
                <>
                  <span className="dot">·</span>
                  <span className="meta">
                    {match.venue.name}
                    {match.venue.city ? ` · ${match.venue.city}` : ""}
                  </span>
                </>
              ) : null}
            </div>
          </div>
        </div>

        {/* Content grid */}
        <div className="grid">
          {/* Left column: stacked cards */}
          <div className="left-stack">
            <div className="card">
              <div className="card-title">Goal Scorers</div>
              {goals.home.length === 0 && goals.away.length === 0 ? (
                <div className="card-body">No goals recorded.</div>
              ) : (
                <div className="goals">
                  <div className="goals-col">
                    <div className="side-label">
                      {match.home?.logo ? <img src={match.home.logo} alt="" className="side-badge" /> : null}
                      {match.home?.name ?? "Home"}
                    </div>
                    <ul className="goals-list">
                      {goals.home.map((g, idx) => (
                        <li key={`h-${idx}`} className="goal-row">
                          <span className="minute right">{minuteLabel(g.minute)}</span>
                          <span className="player">{g.player}</span>
                          <GoalChip type={g.type} />
                          {g.assist ? <span className="assist">· {g.assist}</span> : null}
                        </li>
                      ))}
                    </ul>
                  </div>

                  <div className="divider" />

                  <div className="goals-col">
                    <div className="side-label">
                      {match.away?.logo ? <img src={match.away.logo} alt="" className="side-badge" /> : null}
                      {match.away?.name ?? "Away"}
                    </div>
                    <ul className="goals-list">
                      {goals.away.map((g, idx) => (
                        <li key={`a-${idx}`} className="goal-row">
                          <span className="minute right">{minuteLabel(g.minute)}</span>
                          <span className="player">{g.player}</span>
                          <GoalChip type={g.type} />
                          {g.assist ? <span className="assist">· {g.assist}</span> : null}
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}
            </div>

            <div className="card">
              <div className="card-title">Match Timeline</div>
              {!timeline.length ? (
                <div className="card-body">No events recorded.</div>
              ) : (
                <ul className="timeline">
                  {timeline.map((e, i) => {
                    const t = String(e.type || "").toLowerCase();
                    const icon =
                      t === "goal" ? "⚽"
                      : t === "penalty" ? "🅿️"
                      : t === "own_goal" ? "🥅"
                      : t === "yellow" ? "🟨"
                      : t === "red" ? "🟥"
                      : "•";
                    const isHome = e.team_id === match?.home?.id;
                    return (
                      <li key={i} className={`timeline-row ${isHome ? "home" : "away"}`}>
                        <span className="t-minute">{minuteLabel(e.minute)}</span>
                        <span className="t-icon" aria-hidden>{icon}</span>
                        <span className="t-text">
                          <strong>{e.player || "—"}</strong>
                          {e.detail ? ` · ${e.detail}` : ""}
                          {e.assist ? ` (assist ${e.assist})` : ""}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          </div>

          {/* Right column: stats */}
          <div className="card">
            <div className="card-title">Match Statistics</div>
            <div className="stats">
              {STAT_ROWS.map(({ key, label, isPct, suffix }) => {
                const hv = match?.stats?.home?.[key] ?? 0;
                const av = match?.stats?.away?.[key] ?? 0;

                let homePct = 0;
                let awayPct = 0;
                if (isPct) {
                  homePct = Math.max(0, Math.min(100, Number(hv)));
                  awayPct = Math.max(0, Math.min(100, Number(av)));
                } else {
                  const total = Number(hv) + Number(av);
                  if (total > 0) {
                    homePct = (Number(hv) / total) * 100;
                    awayPct = (Number(av) / total) * 100;
                  }
                }

                return (
                  <div className="stat-row" key={key}>
                    <div className="stat-values">
                      <span className="val left">{hv}{suffix || ""}</span>
                      <span className="stat-label">{label}</span>
                      <span className="val right">{av}{suffix || ""}</span>
                    </div>
                    <div className="stat-bar">
                      <div className="bar home" style={{ width: `${homePct}%` }} />
                      <div className="bar away" style={{ width: `${awayPct}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}
