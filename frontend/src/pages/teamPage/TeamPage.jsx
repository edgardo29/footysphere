// src/pages/teamPage/TeamPage.jsx
// -----------------------------------------------------------------------------
// PURPOSE
//  - Keep layout clean and aligned across cards/rows.
//  - Team Form: compact 5-column strip; each crest directly under its score chip.
//  - Next Match: uses a dedicated upcoming fetch so it doesn't disappear on tabs.
//  - Per-row meta: league/competition on the right; venue shows a placeholder.
//  - Lots of inline comments for future maintenance.
// -----------------------------------------------------------------------------

import React, { useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams, useNavigate } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import "./styles/teamPage.css";

// Guarded date parser
function parseDate(iso) {
  try { return iso ? new Date(iso) : null; } catch { return null; }
}

export default function TeamPage() {
  // ---------- routing / params ----------
  const navigate = useNavigate();
  const { teamId } = useParams();
  const teamIdNum = Number(teamId);

  // allow deep-link ?season=YYYY; fallback to current year
  const [searchParams] = useSearchParams();
  const season = searchParams.get("season") || String(new Date().getFullYear());

  // ---------- local UI state ----------
  const [tab, setTab] = useState("all");           // "all" | "upcoming" | "played"
  const [search, setSearch] = useState("");

  // ---------- server data powers ----------
  const [summary, setSummary] = useState(null);    // team header
  const [fixtures, setFixtures] = useState([]);    // main list for current tab
  const [countsMeta, setCountsMeta] = useState({ count_total: 0, count_upcoming: 0, count_played: 0 });

  // Extra (decoupled) sources to keep summary cards stable across tabs:
  const [formFixturesPlayed, setFormFixturesPlayed] = useState([]); // last results for Team Form
  const [upcomingFixtures, setUpcomingFixtures]   = useState([]);   // provides Next Match

  // ---------- fetch state / errors ----------
  const [loadingSummary, setLoadingSummary] = useState(true);
  const [loadingFixtures, setLoadingFixtures] = useState(true);
  const [errSummary, setErrSummary] = useState("");
  const [errFixtures, setErrFixtures] = useState("");

  // URL builder for match details
  const toMatchUrl = (id) =>
    `/match/${id}?season=${encodeURIComponent(season)}&teamId=${encodeURIComponent(teamId)}`;

  // ────────────────────────────────────────────────────────────────────────────
  // FETCH: team summary (header box)
  // ────────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        setLoadingSummary(true); setErrSummary("");
        const res = await fetch(`/api/teamPage/${teamId}/summary?season=${encodeURIComponent(season)}`);
        if (!res.ok) throw new Error(`Summary HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) setSummary(j);
      } catch (e) {
        if (!canceled) setErrSummary(e.message || "Failed to load team summary.");
        console.error(e);
      } finally {
        if (!canceled) setLoadingSummary(false);
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season]);

  // ────────────────────────────────────────────────────────────────────────────
  // FETCH: fixtures for CURRENT TAB (powers the big list)
  // ────────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        setLoadingFixtures(true); setErrFixtures("");
        const params = new URLSearchParams({
          season,
          status: tab,          // "all" | "upcoming" | "played"
          limit: "200",
          offset: "0",
          q: search.trim(),
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
        if (!canceled) setErrFixtures(e.message || "Failed to load fixtures.");
        console.error(e);
      } finally {
        if (!canceled) setLoadingFixtures(false);
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season, tab, search]);

  // ────────────────────────────────────────────────────────────────────────────
  // FETCH: played fixtures JUST for Team Form (decoupled from tab)
  // ────────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        const params = new URLSearchParams({
          season, status: "played", limit: "50", offset: "0", q: ""
        });
        const res = await fetch(`/api/teamPage/${teamId}/fixtures?${params.toString()}`);
        if (!res.ok) throw new Error(`Form fixtures HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) setFormFixturesPlayed(Array.isArray(j.fixtures) ? j.fixtures : []);
      } catch {
        if (!canceled) setFormFixturesPlayed([]); // non-fatal: form will just show "No recent matches."
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season]);

  // ────────────────────────────────────────────────────────────────────────────
  // FETCH: upcoming fixtures JUST for Next Match (decoupled from tab)
  // ────────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    let canceled = false;
    (async () => {
      try {
        const params = new URLSearchParams({
          season, status: "upcoming", limit: "50", offset: "0", q: ""
        });
        const res = await fetch(`/api/teamPage/${teamId}/fixtures?${params.toString()}`);
        if (!res.ok) throw new Error(`Upcoming fixtures HTTP ${res.status}`);
        const j = await res.json();
        if (!canceled) setUpcomingFixtures(Array.isArray(j.fixtures) ? j.fixtures : []);
      } catch {
        if (!canceled) setUpcomingFixtures([]); // non-fatal
      }
    })();
    return () => { canceled = true; };
  }, [teamId, season]);

  // ────────────────────────────────────────────────────────────────────────────
  // MEMO: Next match from the dedicated upcoming batch
  // ────────────────────────────────────────────────────────────────────────────
  const nextMatch = useMemo(() => {
    if (!upcomingFixtures.length) return null;
    const sorted = [...upcomingFixtures].sort((a, b) => {
      const da = parseDate(a.date_utc)?.getTime() ?? Number.MAX_SAFE_INTEGER;
      const db = parseDate(b.date_utc)?.getTime() ?? Number.MAX_SAFE_INTEGER;
      return da - db;
    });
    return sorted[0] || null;
  }, [upcomingFixtures]);

  // ────────────────────────────────────────────────────────────────────────────
  // MEMO: Last 5 results (for Team Form) from the dedicated played batch
  // ────────────────────────────────────────────────────────────────────────────
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
        opponent: isHome ? m?.away : m?.home, // used for the crest
      };
    });
  }, [formFixturesPlayed, teamIdNum]);

  // Tab counts compacted
  const counts = useMemo(() => ({
    total: countsMeta.count_total || 0,
    upcoming: countsMeta.count_upcoming || 0,
    played: countsMeta.count_played || 0,
  }), [countsMeta]);

  // Localized date/time
  const formatWhen = (iso) => {
    const dt = parseDate(iso);
    if (!dt) return "TBD";
    return `${dt.toLocaleDateString()} ${dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
  };

  // First-paint fallbacks — once we have anything, keep rendering
  const anyLoading = loadingSummary || loadingFixtures;
  const anyError = errSummary || errFixtures;

  if (anyLoading && !summary && fixtures.length === 0) {
    return (
      <div>
        <NavBar />
        <div className="team-page"><p className="loading">Loading…</p></div>
      </div>
    );
  }
  if (anyError && !summary && fixtures.length === 0) {
    return (
      <div>
        <NavBar />
        <div className="team-page"><p className="error">{anyError}</p></div>
      </div>
    );
  }

  // ────────────────────────────────────────────────────────────────────────────
  // RENDER
  // ────────────────────────────────────────────────────────────────────────────
  return (
    <div>
      <NavBar />
      <div className="team-page">
        {/* ── Header: back + "View table" + hero info ── */}
        <div className="header-container">
          <div className="header-actions">
            <Link to="/" className="back-button">
              <span className="back-arrow">←</span>
              Back to Home
            </Link>

            {summary?.league?.id ? (
              <Link className="subtle-link" to={`/league/${summary.league.id}`} title="View league standings">
                View table →
              </Link>
            ) : null}
          </div>

          <div className="team-header">
            {summary?.team_logo_url ? (
              <img
                src={summary.team_logo_url}
                alt={`${summary.team_name} logo`}
                className="team-logo"
                loading="lazy"
              />
            ) : null}

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
        </div>

        {/* ── Summary widgets row ── */}
        <div className="summary-grid">
          {/* TEAM FORM — compact, centered 5-column strip */}
          <div className="card form-card">
            <div className="card-title">Team form</div>

            {formLast5.length ? (
              <div className="form-row">
                {formLast5.map((m) => (
                  <div className="form-col" key={m.match_id}>
                    {/* Score chip (fixed width for uniformity) */}
                    <span
                      className={`form-chip ${m.result === "W" ? "win" : m.result === "L" ? "loss" : "draw"}`}
                      title={`${m.home?.name ?? ""} ${m.scoreText || ""} ${m.away?.name ?? ""}`}
                    >
                      {m.scoreText || m.result}
                    </span>

                    {/* Opponent crest directly under the chip */}
                    {m.opponent?.logo
                      ? <img src={m.opponent.logo} alt="" className="form-logo" />
                      : <div className="badge-placeholder form-logo" />}
                  </div>
                ))}
              </div>
            ) : (
              <div className="card-body">No recent matches.</div>
            )}
          </div>

          {/* NEXT MATCH — driven by dedicated upcoming fetch so it persists on tabs */}
          <div className="card next-card">
            <div className="card-title">
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

                {/* Venue placeholder keeps line from looking broken */}
                <div className="next-venue">{nextMatch.venue || "Venue TBA"}</div>
              </div>
            ) : (
              <div className="card-body">No upcoming fixtures.</div>
            )}
          </div>
        </div>

        {/* ── Sticky filter bar ── */}
        <div className="filterbar sticky">
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

        {/* ── Fixtures list ── */}
        <div className="team-card">
          <h3 className="team-card-title">
            {tab === "upcoming" ? "Upcoming Fixtures" : tab === "played" ? "Completed Matches" : "All Matches"}
          </h3>

          {!fixtures.length ? (
            <div className="section-body">
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
                    role="button" tabIndex={0} title="Open match details"
                  >
                    {/* Top meta: date left, status right */}
                    <div className="item-row-top">
                      <span className="when">{when}</span>
                      <span className="status">{m.status}</span>
                    </div>

                    {/* Main row: HOME | SCORE/VS | AWAY (logos near center) */}
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

                    {/* Bottom meta: left = HA + venue, right = competition */}
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
      </div>
    </div>
  );
}
