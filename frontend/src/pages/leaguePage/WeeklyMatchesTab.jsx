import React, { useEffect, useMemo, useState } from "react";
import "./styles/weeklyMatchesTab.css";

/** Monday of the current week (local) as YYYY-MM-DD */
function getDefaultWeekStartISO() {
  const d = new Date();
  const day = d.getDay(); // 0=Sun..6=Sat
  const diff = day === 0 ? -6 : 1 - day; // move to Monday
  const monday = new Date(d);
  monday.setDate(d.getDate() + diff);
  monday.setHours(0, 0, 0, 0);
  return monday.toISOString().slice(0, 10);
}

/** Helpers for rendering dates */
function dayKey(dateStr) {
  const d = new Date(dateStr);
  // yyyy-mm-dd key for grouping (kept same behavior as before)
  return d.toISOString().slice(0, 10);
}
function dayLabel(dateStr) {
  const d = new Date(dateStr);
  return d.toLocaleDateString(undefined, {
    weekday: "long",
    month: "short",
    day: "numeric",
  });
}

export default function WeeklyMatchesTab({ leagueId }) {
  const [weekStartISO, setWeekStartISO] = useState(getDefaultWeekStartISO());
  const [days, setDays] = useState(7);

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);

  useEffect(() => {
    if (!leagueId) return;
    const ctrl = new AbortController();

    async function fetchWeekly() {
      try {
        setLoading(true);
        setErr(null);
        const res = await fetch(
          `/api/leaguesPage/${leagueId}/weekly-matches?start=${weekStartISO}&days=${days}`,
          { signal: ctrl.signal }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (e.name !== "AbortError") {
          console.error("Failed to load weekly matches:", e);
          setErr("Failed to load weekly matches.");
          setRows([]);
        }
      } finally {
        setLoading(false);
      }
    }

    fetchWeekly();
    return () => ctrl.abort();
  }, [leagueId, weekStartISO, days]);

  // Group by day
  const grouped = useMemo(() => {
    const map = new Map();
    for (const m of rows) {
      const k = dayKey(m.kickoff_utc);
      if (!map.has(k)) map.set(k, []);
      map.get(k).push(m);
    }
    return Array.from(map.entries()).sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0
    );
  }, [rows]);

  const weekEnd = useMemo(() => {
    const d = new Date(weekStartISO + "T00:00:00");
    d.setDate(d.getDate() + (days - 1));
    return d.toISOString().slice(0, 10);
  }, [weekStartISO, days]);

  if (loading) return <p className="loading">Loading weekly matches…</p>;
  if (err) return <p className="error">{err}</p>;

  return (
    <div className="weekly-tab">
      <div className="weekly-inner">
        <div className="week-controls">
          <button
            className="week-nav"
            onClick={() => {
              const d = new Date(weekStartISO + "T00:00:00");
              d.setDate(d.getDate() - 7);
              setWeekStartISO(d.toISOString().slice(0, 10));
            }}
          >
            ← Prev
          </button>

          <div className="week-range">
            {new Date(weekStartISO).toLocaleDateString()} —{" "}
            {new Date(weekEnd).toLocaleDateString()}
          </div>

          <button
            className="week-nav"
            onClick={() => {
              const d = new Date(weekStartISO + "T00:00:00");
              d.setDate(d.getDate() + 7);
              setWeekStartISO(d.toISOString().slice(0, 10));
            }}
          >
            Next →
          </button>
        </div>

        {grouped.length === 0 ? (
          <p className="empty">No matches scheduled in this range.</p>
        ) : (
          grouped.map(([dayIso, matches]) => (
            <section key={dayIso} className="day-block">
              <div className="day-header">
                <span className="day-pill">{dayLabel(dayIso)}</span>
                <span className="day-count">{matches.length} matches</span>
              </div>

              {matches.map((m) => {
                const isFinished = ["FT", "AET", "PEN"].includes(m.status);
                const centerText = isFinished
                  ? `${m.home_score ?? 0} - ${m.away_score ?? 0}`
                  : new Date(m.kickoff_utc).toLocaleTimeString([], {
                      hour: "2-digit",
                      minute: "2-digit",
                    });

                return (
                  <div key={m.id} className="match-card">
                    {/* Home */}
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

                    {/* Center (time or score) + status */}
                    <div className="match-details">
                      <span className="match-time">{centerText}</span>
                      <span
                        className={`match-status ${
                          m.status === "Live" ? "live" : ""
                        }`}
                      >
                        {m.status}
                      </span>
                    </div>

                    {/* Away */}
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
                );
              })}
            </section>
          ))
        )}
      </div>
    </div>
  );
}
