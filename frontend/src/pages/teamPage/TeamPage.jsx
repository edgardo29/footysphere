import React, { useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams, useNavigate } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import Footer from "../homePage/Footer";


// Pull shared theme tokens & core classes (tile, section-block, etc.)
import "../homePage/styles/homePage.css";
import "./styles/teamPage.css";

// Safe date parser
function parseDate(iso) {
  try { return iso ? new Date(iso) : null; } catch { return null; }
}

export default function TeamPage() {
  const navigate = useNavigate();
  const { teamId } = useParams();
  const teamIdNum = Number(teamId);

  // allow deep-link ?season=YYYY; fallback to current year
  const [searchParams] = useSearchParams();
  const season = searchParams.get("season") || String(new Date().getFullYear());

  // UI state
  const [tab, setTab] = useState("all");           // "all" | "upcoming" | "played"
  const [search, setSearch] = useState("");

  // Data
  const [summary, setSummary] = useState(null);
  const [fixtures, setFixtures] = useState([]);
  const [countsMeta, setCountsMeta] = useState({ count_total: 0, count_upcoming: 0, count_played: 0 });

  const [formFixturesPlayed, setFormFixturesPlayed] = useState([]); // for Team Form
  const [upcomingFixtures, setUpcomingFixtures]   = useState([]);   // for Next Match

  // Fetch: team summary (header)
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        const res = await fetch(`/api/teamPage/${teamId}/summary?season=${encodeURIComponent(season)}`);
        if (!res.ok) throw new Error(`Summary HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) setSummary(j);
      } catch (e) {
        if (!canceled) console.error(e);
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season]);

  // Fetch: fixtures for CURRENT TAB (main list)
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        const params = new URLSearchParams({
          season, status: tab, limit: "200", offset: "0", q: search.trim(),
        });
        const res = await fetch(`/api/teamPage/${teamId}/fixtures?${params.toString()}`);
        if (!res.ok) throw new Error(`Fixtures HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) {
          setFixtures(Array.isArray(j.fixtures) ? j.fixtures : []);
          setCountsMeta(
            j?.meta
              ? {
                  count_total: Number(j.meta.count_total || 0),
                  count_upcoming: Number(j.meta.count_upcoming || 0),
                  count_played: Number(j.meta.count_played || 0),
                }
              : { count_total: 0, count_upcoming: 0, count_played: 0 }
          );
        }
      } catch (e) {
        if (!canceled) console.error(e);
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season, tab, search]);

  // Fetch: played fixtures JUST for Team Form
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        const params = new URLSearchParams({ season, status: "played", limit: "50", offset: "0", q: "" });
        const res = await fetch(`/api/teamPage/${teamId}/fixtures?${params.toString()}`);
        if (!res.ok) throw new Error(`Form fixtures HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) setFormFixturesPlayed(Array.isArray(j.fixtures) ? j.fixtures : []);
      } catch {
        if (!canceled) setFormFixturesPlayed([]);
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season]);

  // Fetch: upcoming fixtures JUST for Next Match
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        const params = new URLSearchParams({ season, status: "upcoming", limit: "50", offset: "0", q: "" });
        const res = await fetch(`/api/teamPage/${teamId}/fixtures?${params.toString()}`);
        if (!res.ok) throw new Error(`Upcoming fixtures HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) setUpcomingFixtures(Array.isArray(j.fixtures) ? j.fixtures : []);
      } catch {
        if (!canceled) setUpcomingFixtures([]);
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season]);

  // Derived: next match
  const nextMatch = useMemo(() => {
    if (!upcomingFixtures.length) return null;
    const sorted = [...upcomingFixtures].sort((a, b) => {
      const da = parseDate(a.date_utc)?.getTime() ?? Number.MAX_SAFE_INTEGER;
      const db = parseDate(b.date_utc)?.getTime() ?? Number.MAX_SAFE_INTEGER;
      return da - db;
    });
    return sorted[0] || null;
  }, [upcomingFixtures]);

  // Derived: last 5 results for form
  const formLast5 = useMemo(() => {
    const played = formFixturesPlayed.filter((m) => m.status === "Full-Time");
    if (!played.length) return [];
    const sorted = [...played].sort((a, b) => {
      const da = parseDate(a.date_utc)?.getTime() ?? 0;
      const db = parseDate(b.date_utc)?.getTime() ?? 0;
      return db - da; // newest first
    });
    return sorted.slice(0, 5).map((m) => {
      const isHome = m?.home?.id === teamIdNum;
      const my = isHome ? m?.score?.home : m?.score?.away;
      const opp = isHome ? m?.score?.away : m?.score?.home;
      const result = (typeof my === "number" && typeof opp === "number")
        ? (my > opp ? "W" : my < opp ? "L" : "D")
        : "D";
      return {
        ...m,
        result,
        scoreText:
          typeof m?.score?.home === "number" && typeof m?.score?.away === "number"
            ? `${m.score.home}–${m.score.away}`
            : "",
        opponent: isHome ? m?.away : m?.home,
      };
    });
  }, [formFixturesPlayed, teamIdNum]);

  const counts = useMemo(() => ({
    total: countsMeta.count_total || 0,
    upcoming: countsMeta.count_upcoming || 0,
    played: countsMeta.count_played || 0,
  }), [countsMeta]);

  const toMatchUrl = (id) =>
    `/match/${id}?season=${encodeURIComponent(season)}&teamId=${encodeURIComponent(teamId)}`;

  const formatWhen = (iso) => {
    const dt = parseDate(iso);
    if (!dt) return "TBD";
    return `${dt.toLocaleDateString()} ${dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
    };

  return (
    <div className="app">
      <NavBar />

      <main className="main-content team-page">
        {/* Back + 'View table' */}
        <div className="header-actions">
          <Link to="/" className="back-button" aria-label="Back to Home">
            <span className="back-arrow">←</span>
            Back to Home
          </Link>

          {summary?.league?.id ? (
            <Link className="subtle-link" to={`/league/${summary.league.id}`} title="View league standings">
              View table →
            </Link>
          ) : null}
        </div>

        {/* Team header tile (matches league header tile) */}
        <section className="section-block">
          <div className="team-header tile">
            <div className="crest-wrap">
              {summary?.team_logo_url ? (
                <img
                  src={summary.team_logo_url}
                  alt={`${summary?.team_name || "Team"} logo`}
                  className="crest-logo"
                  loading="lazy"
                />
              ) : (
                <div className="crest-logo placeholder" aria-hidden="true" />
              )}
            </div>

            <div className="header-text">
              <h1 className="team-title">{summary?.team_name || "—"}</h1>
              <p className="season-text">
                Season {season}
                {summary?.league?.name ? <> · {summary.league.name}</> : null}
              </p>
              <p className="team-meta">
                <span>Pos {summary?.position ?? "—"}</span>
                <span className="dot">·</span>
                <span>Pts {summary?.points ?? "—"}</span>
                <span className="dot">·</span>
                <span>
                  {summary?.record
                    ? `${summary.record.wins}-${summary.record.draws}-${summary.record.losses} (GD ${summary.record.goal_diff})`
                    : "—"}
                </span>
              </p>
            </div>
          </div>
        </section>

        {/* Summary widgets row (two tiles) */}
        <section className="section-block">
          <div className="summary-grid">
            {/* Team form */}
            <div className="tile form-card">
              <div className="tile-title">Team form</div>

              {formLast5.length ? (
                <div className="form-row">
                  {formLast5.map((m) => (
                    <div className="form-col" key={m.match_id}>
                      <span
                        className={`form-chip ${m.result === "W" ? "win" : m.result === "L" ? "loss" : "draw"}`}
                        title={`${m.home?.name ?? ""} ${m.scoreText || ""} ${m.away?.name ?? ""}`}
                      >
                        {m.scoreText || m.result}
                      </span>

                      {m.opponent?.logo
                        ? <img src={m.opponent.logo} alt="" className="form-logo" />
                        : <div className="badge-placeholder form-logo" />}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="tile-body">No recent matches.</div>
              )}
            </div>

            {/* Next match */}
            <div className="tile next-card">
              <div className="tile-title">
                Next match
                {nextMatch?.competition?.name ? <span className="right-faint">{nextMatch.competition.name}</span> : null}
              </div>

              {nextMatch ? (
                <div
                  role="button"
                  tabIndex={0}
                  className="next-match"
                  onClick={() => navigate(toMatchUrl(nextMatch.match_id))}
                  onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && navigate(toMatchUrl(nextMatch.match_id))}
                  title="Open match details"
                >
                  <div className="kickoff">{formatWhen(nextMatch.date_utc)}</div>

                  <div className="next-row">
                    <div className="next-team home">
                      {nextMatch.home?.logo ? <img src={nextMatch.home.logo} alt="" /> : <div className="badge-placeholder" />}
                      <span className="team-name-ellip">{nextMatch.home?.name ?? "—"}</span>
                    </div>
                    <div className="center-cell">vs</div>
                    <div className="next-team away">
                      {nextMatch.away?.logo ? <img src={nextMatch.away.logo} alt="" /> : <div className="badge-placeholder" />}
                      <span className="team-name-ellip">{nextMatch.away?.name ?? "—"}</span>
                    </div>
                  </div>

                  <div className="next-venue">{nextMatch.venue || "Venue TBA"}</div>
                </div>
              ) : (
                <div className="tile-body">No upcoming fixtures.</div>
              )}
            </div>
          </div>
        </section>

        {/* Tabs + search (styled to match league tabs) */}
        <section className="section-block">
          <div className="filterbar">
            <div className="team-tabs">
              <button className={`team-tab ${tab === "all" ? "active" : ""}`} type="button" onClick={() => setTab("all")}>
                All <span className="count-badge">{counts.total}</span>
              </button>
              <button className={`team-tab ${tab === "upcoming" ? "active" : ""}`} type="button" onClick={() => setTab("upcoming")}>
                Upcoming <span className="count-badge">{counts.upcoming}</span>
              </button>
              <button className={`team-tab ${tab === "played" ? "active" : ""}`} type="button" onClick={() => setTab("played")}>
                Completed <span className="count-badge">{counts.played}</span>
              </button>
            </div>

            <div className="filter-right">
              <span className="times-note">All times local</span>
              <input
                type="text"
                className="search"
                placeholder="Search opponent or round…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
          </div>
        </section>

        {/* Fixtures list */}
        <section className="section-block">
          <div className="team-card tile">
            <h3 className="team-card-title">
              {tab === "upcoming" ? "Upcoming Fixtures" : tab === "played" ? "Completed Matches" : "All Matches"}
            </h3>

            {!fixtures.length ? (
              <div className="tile-body">
                {tab === "upcoming" ? "No upcoming fixtures." : tab === "played" ? "No completed matches." : "No matches found."}
              </div>
            ) : (
              <ul className="fixtures-list">
                {fixtures.map((m) => {
                  const when = formatWhen(m.date_utc);
                  const score =
                    m?.score && typeof m.score.home === "number" && typeof m.score.away === "number"
                      ? `${m.score.home}–${m.score.away}`
                      : "";
                  const isHome = m?.home?.id === teamIdNum;

                  return (
                    <li
                      key={m.match_id}
                      className="fixture-item linkish"
                      onClick={() => navigate(toMatchUrl(m.match_id))}
                      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && navigate(toMatchUrl(m.match_id))}
                      role="button"
                      tabIndex={0}
                      title="Open match details"
                    >
                      {/* Top meta */}
                      <div className="item-row-top">
                        <span className="when">{when}</span>
                        <span className="status">{m.status}</span>
                      </div>

                      {/* Main row */}
                      <div className="teams-row">
                        <span className="team-side home" title={m.home?.name}>
                          {m.home?.logo ? <img src={m.home.logo} alt="" className="badge" loading="lazy" /> : null}
                          <span className="team-name-ellip">{m.home?.name ?? "—"}</span>
                        </span>

                        <span className="center-cell">{score || "vs"}</span>

                        <span className="team-side away" title={m.away?.name}>
                          {m.away?.logo ? <img src={m.away.logo} alt="" className="badge" loading="lazy" /> : null}
                          <span className="team-name-ellip">{m.away?.name ?? "—"}</span>
                        </span>
                      </div>

                      {/* Bottom meta */}
                      <div className="item-row-bottom">
                        <div className="bottom-left">
                          <span className={`ha-pill ${isHome ? "home" : "away"}`}>{isHome ? "Home" : "Away"}</span>
                          <span className="dot">·</span>
                          <span className="meta-sm">{m.venue || "Venue TBA"}</span>
                        </div>
                        <div className="bottom-right">
                          <span className="meta-sm">{m.competition?.name ?? summary?.league?.name ?? ""}</span>
                        </div>
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </section>
      </main>
      <Footer />

    </div>
  );
}
