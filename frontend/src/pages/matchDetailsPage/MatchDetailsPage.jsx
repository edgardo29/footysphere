import React, { useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import Footer from "../homePage/Footer";


// Pull the shared theme tokens & core classes (tile, section-block, etc.)
import "../homePage/styles/homePage.css";
import "./styles/matchDetailsPage.css";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "/api";
const USER_TZ = Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;

function fmtWhen(iso) {
  if (!iso) return "TBD";
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return "TBD";
  const date = dt.toLocaleDateString(undefined, { year: "numeric", month: "numeric", day: "numeric" });
  const time = dt.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit", timeZoneName: "short" });
  return `${date} ${time}`;
}

// Tidy API "round" strings.
function fmtRound(round) {
  const raw = String(round || "").trim();
  if (!raw) return "";
  const mRS = raw.match(/regular season\s*-\s*(\d+)/i);
  if (mRS) return ""; // hide "Regular Season - N"
  const mGroup = raw.match(/group\s*-\s*([A-Za-z])/i);
  if (mGroup) return `Group ${mGroup[1].toUpperCase()}`;
  const mRound = raw.match(/round\s*-\s*(\d+)/i);
  if (mRound) return `Round ${mRound[1]}`;
  return raw.replace(/\s*-\s*$/, "");
}

function fmtMinute(min, extra = 0) {
  const base = typeof min === "number" ? String(min) : String(Number(min) || "0");
  const add = Number(extra) > 0 ? `+${Number(extra)}` : "";
  return `${base}${add}′`;
}

const STAT_ROWS = [
  { key: "possession_pct", label: "Possession", kind: "possession", suffix: "%" },
  { key: "shots_total", label: "Total Shots", kind: "value" },
  { key: "shots_on_target", label: "Shots on Target", kind: "value" },
  { key: "shots_inside_box", label: "Shots Inside Box", kind: "value" },
  { key: "shots_outside_box", label: "Shots Outside Box", kind: "value" },
  { key: "corners", label: "Corners", kind: "value" },
  { key: "offsides", label: "Offsides", kind: "value" },
  { key: "passes_total", label: "Total Passes", kind: "value" },
  { key: "pass_accuracy_pct", label: "Pass Accuracy", kind: "value", suffix: "%" },
  { key: "fouls", label: "Fouls", kind: "value" },
  { key: "yellow", label: "Yellow Cards", kind: "value" },
  { key: "red", label: "Red Cards", kind: "value" },
];

export default function MatchDetailsPage() {
  const { matchId } = useParams();
  const [searchParams] = useSearchParams();
  const season = searchParams.get("season") || String(new Date().getFullYear());
  const teamIdFromQuery = searchParams.get("teamId");

  const [match, setMatch] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  // Always land at top on route change (fixes the "middle of page" issue)
  useEffect(() => {
    // rAF avoids fighting the browser's scroll restoration
    requestAnimationFrame(() => window.scrollTo(0, 0));
  }, [matchId]);

  useEffect(() => {
    const ctrl = new AbortController();
    (async () => {
      try {
        setLoading(true); setErr("");
        const url = `${API_BASE}/matchDetailsPage/${encodeURIComponent(matchId)}/details`;
        const res = await fetch(url, { signal: ctrl.signal, headers: { Accept: "application/json" } });
        const ct = res.headers.get("content-type") || "";
        if (!res.ok) {
          const text = await res.text().catch(() => "");
          throw new Error(`HTTP ${res.status} ${res.statusText}${text ? ` – ${text.slice(0,180)}…` : ""}`);
        }
        if (!ct.includes("application/json")) {
          const text = await res.text().catch(() => "");
          throw new Error(`Expected JSON but got ${ct || "unknown"} – ${text.slice(0,180)}…`);
        }
        setMatch(await res.json());
      } catch (e) {
        if (e.name !== "AbortError") {
          console.error(e);
          setErr(`Could not load match ${matchId}. ${e.message || e}`);
        }
      } finally {
        setLoading(false);
      }
    })();
    return () => ctrl.abort();
  }, [matchId]);

  const toNum = (x) => (typeof x === "number" ? x : Number(String(x).split("+")[0]) || 0);

  // Sort events chronologically and number substitutions per team
  const timeline = useMemo(() => {
    const events = Array.isArray(match?.events) ? match.events.slice() : [];
    events.sort((a, b) => {
      if (toNum(a.minute) !== toNum(b.minute)) return toNum(a.minute) - toNum(b.minute);
      return Number(a.minute_extra || 0) - Number(b.minute_extra || 0);
    });
    const subCountByTeam = new Map();
    for (const e of events) {
      if (String(e.type || "").toLowerCase() === "substitution") {
        const key = e.team_id ?? "unknown";
        const next = (subCountByTeam.get(key) || 0) + 1;
        subCountByTeam.set(key, next);
        e._subNo = next;
      }
    }
    return events;
  }, [match]);

  // Build halves, group by minute (+extra), stack multiple events per minute per side
  const timelineGroups = useMemo(() => {
    const parts = { first: [], second: [], extra: [] };
    for (const e of timeline) {
      const m = Number(e.minute) || 0;
      if (m < 46) parts.first.push(e);
      else if (m < 91) parts.second.push(e);
      else parts.extra.push(e);
    }

    const sortWithinRow = (arr) =>
      arr.slice().sort((a, b) => {
        const ta = String(a.type || "");
        const tb = String(b.type || "");
        if (ta === "substitution" && tb === "substitution") {
          return (a._subNo || 0) - (b._subNo || 0);
        }
        return 0;
      });

    const build = (arr) => {
      const map = new Map();
      for (const e of arr) {
        const key = `${toNum(e.minute)}|${Number(e.minute_extra || 0)}`;
        if (!map.has(key)) {
          map.set(key, { minute: toNum(e.minute), extra: Number(e.minute_extra || 0), left: [], right: [] });
        }
        const isHome = e.team_id === match?.home?.id;
        (isHome ? map.get(key).left : map.get(key).right).push(e);
      }
      const rows = Array.from(map.values()).sort((a, b) => a.minute - b.minute || a.extra - b.extra);
      for (const r of rows) {
        r.left = sortWithinRow(r.left);
        r.right = sortWithinRow(r.right);
      }
      return rows;
    };

    const out = [];
    if (parts.first.length) out.push({ id: "first", label: "FIRST HALF", rows: build(parts.first) });
    if (parts.second.length) out.push({ id: "second", label: "SECOND HALF", rows: build(parts.second) });
    if (parts.extra.length) out.push({ id: "extra", label: "EXTRA TIME", rows: build(parts.extra) });
    return out;
  }, [timeline, match?.home?.id]);

  const status = match?.status || "—";
  const scoreFT = match?.score?.fulltime;
  const showScore = ["full-time", "live"].includes(String(status).toLowerCase());

  if (loading) {
    return (
      <div className="app">
        <NavBar />
        <main className="main-content match-page"><p className="muted">Loading match…</p></main>
      </div>
    );
  }
  if (err) {
    return (
      <div className="app">
        <NavBar />
        <main className="main-content match-page"><pre className="error">{err}</pre></main>
      </div>
    );
  }
  if (!match) {
    return (
      <div className="app">
        <NavBar />
        <main className="main-content match-page"><p className="error">No data.</p></main>
      </div>
    );
  }

  const chipFor = (t, detail) => {
    const d = String(detail || "").toLowerCase();
    if (t === "goal") return d.includes("penalty") ? ["PEN", "goal"] : d.includes("own goal") ? ["OWN", "goal"] : ["GOAL", "goal"];
    if (t === "yellow") return ["YC", "yellow"];
    if (t === "red") return ["RC", "red"];
    if (t === "substitution") return ["SUB", "sub"];
    if (t === "var") return ["VAR", "var"];
    return ["EVT", "evt"];
  };

  const clampPct = (n) => Math.max(0, Math.min(100, Number(n) || 0));

  return (
    <div className="app">
      <NavBar />

      <main className="main-content match-page">
        {/* Actions row */}
        <div className="header-actions">
          <Link to="/" className="back-button" aria-label="Back to Home">
            <span className="back-arrow">←</span>
            Back to Home
          </Link>

          <div className="right-actions">
            {teamIdFromQuery ? (
              <Link className="ghost-button" to={`/teams/${teamIdFromQuery}?season=${season}`} title="Back to Team">
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

        {/* Overview tile */}
        <section className="section-block">
          <div className="match-header tile">
            <div className="teams-vs">
              <div className="team">
                {match.home?.logo ? <img className="logo" src={match.home.logo} alt={`${match.home?.name} logo`} /> : null}
                <div className="name">{match.home?.name ?? "—"}</div>
              </div>

              <div className="score-block">
                {showScore && typeof scoreFT?.home === "number" && typeof scoreFT?.away === "number" ? (
                  <div className="score" aria-label={`Final score ${scoreFT.home} to ${scoreFT.away}`}>
                    <span>{scoreFT.home}</span><span className="dash">–</span><span>{scoreFT.away}</span>
                  </div>
                ) : (
                  <div className="score placeholder" aria-label="Match not finished">vs</div>
                )}
                <div className={`status-chip ${status === "Live" ? "live" : ""}`} title={status}>
                  {status === "Live" ? <span className="pulse" /> : null}
                  {status}
                </div>
              </div>

              <div className="team right">
                {match.away?.logo ? <img className="logo" src={match.away.logo} alt={`${match.away?.name} logo`} /> : null}
                <div className="name">{match.away?.name ?? "—"}</div>
              </div>
            </div>

            <div className="meta-row">
              <span className="meta" title={`Times shown in your local timezone: ${USER_TZ || "local"}`}>
                {fmtWhen(match.date_utc)}
              </span>
              {(() => {
                const pretty = fmtRound(match.round);
                return pretty ? (<><span className="dot">·</span><span className="meta">{pretty}</span></>) : null;
              })()}
              {match.venue?.name ? (
                <>
                  <span className="dot">·</span>
                  <span className="meta">
                    {match.venue.name}{match.venue.city ? ` · ${match.venue.city}` : ""}
                  </span>
                </>
              ) : null}
            </div>
          </div>
        </section>

        {/* Content grid */}
        <section className="section-block">
          <div className="grid grid-5050">
            {/* Left: Timeline */}
            <div className="tile">
              <div className="tile-title">Match Timeline</div>

              {!timelineGroups.length ? (
                <div className="tile-body">No events recorded.</div>
              ) : (
                <div className="timeline-wrap">
                  {timelineGroups.map((g) => (
                    <div key={g.id} className="phase-block">
                      <div className="phase-sep">{g.label}</div>

                      <ul className="timeline timeline--split">
                        {g.rows.map((row, idx) => (
                          <li key={`${g.id}-${row.minute}-${row.extra}-${idx}`} className="minute-row">
                            {/* left (home) */}
                            <div className="side left">
                              {row.left.map((e, i) => {
                                const t = String(e.type || "").toLowerCase();
                                const [label, cls] = chipFor(t, e.detail);
                                const primary =
                                  t === "substitution"
                                    ? `${e.player || "—"} · substitution ${e._subNo ?? ""}`.trim()
                                    : t === "goal"
                                    ? `${e.player || "—"}${
                                        String(e.detail || "").toLowerCase().includes("penalty")
                                          ? " · penalty"
                                          : String(e.detail || "").toLowerCase().includes("own goal")
                                          ? " · own goal"
                                          : ""
                                      }`
                                    : `${e.player || "—"}${e.detail ? ` · ${e.detail}` : ""}`;
                                const secondary =
                                  t === "goal" && e.assist
                                    ? `assist ${e.assist}`
                                    : t === "substitution" && e.assist
                                    ? `for ${e.assist}`
                                    : "";
                                return (
                                  <div key={`L-${i}`} className="t-pill">
                                    <span className={`etype ${cls}`}>{label}</span>
                                    <div className="t-text">
                                      <div className="t-primary"><strong>{primary}</strong></div>
                                      {secondary ? <div className="t-secondary">{secondary}</div> : null}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>

                            {/* center rail */}
                            <div className="center-rail">
                              <span className="t-minute mono">{fmtMinute(row.minute, row.extra)}</span>
                            </div>

                            {/* right (away) */}
                            <div className="side right">
                              {row.right.map((e, i) => {
                                const t = String(e.type || "").toLowerCase();
                                const [label, cls] = chipFor(t, e.detail);
                                const primary =
                                  t === "substitution"
                                    ? `${e.player || "—"} · substitution ${e._subNo ?? ""}`.trim()
                                    : t === "goal"
                                    ? `${e.player || "—"}${
                                        String(e.detail || "").toLowerCase().includes("penalty")
                                          ? " · penalty"
                                          : String(e.detail || "").toLowerCase().includes("own goal")
                                          ? " · own goal"
                                          : ""
                                      }`
                                    : `${e.player || "—"}${e.detail ? ` · ${e.detail}` : ""}`;
                                const secondary =
                                  t === "goal" && e.assist
                                    ? `assist ${e.assist}`
                                    : t === "substitution" && e.assist
                                    ? `for ${e.assist}`
                                    : "";
                                return (
                                  <div key={`R-${i}`} className="t-pill">
                                    <span className={`etype ${cls}`}>{label}</span>
                                    <div className="t-text">
                                      <div className="t-primary"><strong>{primary}</strong></div>
                                      {secondary ? <div className="t-secondary">{secondary}</div> : null}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          </li>
                        ))}
                      </ul>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Right: Stats */}
            <div className="tile stats-card">
              <div className="tile-title">Match Statistics</div>

              <div className="stats">
                {STAT_ROWS.map(({ key, label, kind = "value", suffix = "" }) => {
                  const hvRaw = match?.stats?.home?.[key] ?? 0;
                  const avRaw = match?.stats?.away?.[key] ?? 0;
                  const hvNum = Number(hvRaw) || 0;
                  const avNum = Number(avRaw) || 0;

                  const leftWins = hvNum > avNum;
                  const rightWins = avNum > hvNum;

                  const renderVal = (val, side) =>
                    (side === "left" && leftWins) ? <span className="val-badge home">{val}{suffix}</span> :
                    (side === "right" && rightWins) ? <span className="val-badge away">{val}{suffix}</span> :
                    <>{val}{suffix}</>;

                  return (
                    <div className="stat-row" key={key}>
                      <div className="stat-values">
                        <span className="val left">{renderVal(hvRaw, "left")}</span>
                        <span className="stat-label">{label}</span>
                        <span className="val right">{renderVal(avRaw, "right")}</span>
                      </div>

                      {kind === "possession" ? (() => {
                        let left = clampPct(hvRaw), right = clampPct(avRaw);
                        const sum = left + right;
                        if (sum > 0) { left = (left / sum) * 100; right = (right / sum) * 100; }
                        return (
                          <div className="stat-bar">
                            <div className="bar home" style={{ width: `${left}%` }} />
                            <div className="bar away" style={{ width: `${right}%` }} />
                          </div>
                        );
                      })() : null}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </section>
      </main>
      <Footer />

    </div>
  );
}
