// src/pages/matchDetailsPage/MatchDetailsPage.jsx
import React, { useEffect, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import Footer from "../homePage/Footer";

// Global + page styles
import "../homePage/styles/homePage.css";
import "./styles/matchDetailsPage.css";

// Eager-load component styles here ⬇️
import "./styles/matchHeader.css";
import "./styles/matchTimeline.css";
import "./styles/matchStats.css";

// Components
import MatchHeader from "./MatchHeader";
import MatchTimeline from "./MatchTimeline";
import MatchStats from "./MatchStats";

const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");
const USER_TZ = Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;

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

  // Always land at top on route change
  useEffect(() => {
    requestAnimationFrame(() => window.scrollTo(0, 0));
  }, [matchId]);

  // Fetch match details
  useEffect(() => {
    const ctrl = new AbortController();
    (async () => {
      try {
        setLoading(true); setErr("");
        const url = `${API}/matchDetailsPage/${encodeURIComponent(matchId)}/details`;
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

  const status = match?.status || "—";
  const scoreFT = match?.score?.fulltime;

  return (
    <div className="app">
      <NavBar />
      <main className="main-content match-page">
        <MatchHeader
          home={match.home}
          away={match.away}
          status={status}
          scoreFT={scoreFT}
          dateUtc={match.date_utc}
          round={match.round}
          venue={match.venue}
          competitionName={match?.competition?.name || null}
          teamIdFromQuery={teamIdFromQuery}
          season={season}
          userTZ={USER_TZ}
        />

        <section className="section-block">
          <div className="grid grid-5050">
            <MatchTimeline
              events={Array.isArray(match?.events) ? match.events : []}
              homeTeamId={match?.home?.id}
            />
            <MatchStats
              statsHome={match?.stats?.home || {}}
              statsAway={match?.stats?.away || {}}
              rowsConfig={STAT_ROWS}
            />
          </div>
        </section>
      </main>
      <Footer />
    </div>
  );
}
