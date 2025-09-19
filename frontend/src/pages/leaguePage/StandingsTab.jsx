// src/pages/leaguePage/StandingsTab.jsx
import React, { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import "./styles/standingsTab.css";

export default function StandingsTab({ leagueId, season }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);

  useEffect(() => {
    if (!leagueId || !season) {
      setRows([]);
      setLoading(false);
      setErr(!leagueId ? "Invalid league id." : "No season available.");
      return;
    }

    const ctrl = new AbortController();
    (async () => {
      try {
        setLoading(true);
        setErr(null);
        const res = await fetch(
          `/api/leaguesPage/${leagueId}/standings?season=${season}`,
          { signal: ctrl.signal }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (e.name !== "AbortError") {
          console.error("Failed to load standings:", e);
          setErr("Failed to load standings.");
          setRows([]);
        }
      } finally {
        setLoading(false);
      }
    })();

    return () => ctrl.abort();
  }, [leagueId, season]);

  // Build groups map
  const groups = useMemo(() => {
    const map = new Map();
    for (const r of rows) {
      const key = r.group_label || "";
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(r);
    }
    return Array.from(map.entries()).sort(([a], [b]) =>
      a === b ? 0 : a === "" ? -1 : b === "" ? 1 : a.localeCompare(b)
    );
  }, [rows]);

  // If there is any non-empty label OR more than one group, treat as multi-group
  const isMultiGroup = useMemo(
    () => groups.length > 1 || groups.some(([label]) => label !== ""),
    [groups]
  );

  // Keep only the useful part of the label (e.g., "Eastern Conference")
  function cleanGroupLabel(label = "") {
    let t = String(label).trim();
    if (!t) return t;

    // Prefer last segment after comma or dash (often where "Eastern/Western" lives)
    if (t.includes(",")) t = t.split(",").pop().trim();
    if (t.includes(" - ")) t = t.split(" - ").pop().trim();

    // Drop season patterns like 2025 or 2025/26 or 2025-26
    t = t.replace(/\b20\d{2}(?:[\/\-]\d{2})?\b/g, "").trim();

    // Drop leftover league name prefixes if present
    t = t.replace(/^\s*(MLS|La Liga|Serie A|Premier League)\s*/i, "").trim();

    // Clean stray punctuation
    t = t.replace(/^[,–-]\s*/, "").replace(/\s{2,}/g, " ").trim();
    return t || label; // fallback to original if we stripped everything
  }

  if (loading) return <p className="loading">Loading standings…</p>;
  if (err) return <p className="error">{err}</p>;
  if (!rows.length) return <p className="empty">No standings available.</p>;

  return (
    <div className="standings-tab">
      {groups.map(([label, teams]) => (
        <section key={label || "main"} className="table-block">
          {/* For multi-group leagues only, show a small right-aligned chip */}
          {isMultiGroup ? (
            <div className="table-header compact">
              <span className="group-chip">{cleanGroupLabel(label)}</span>
            </div>
          ) : null}

          <div className="table-wrap">
            <table className="standings-table">
              <colgroup>
                <col className="col-pos" />
                <col className="col-team" />
                <col className="col-stat" />
                <col className="col-stat" />
                <col className="col-stat" />
                <col className="col-stat" />
                <col className="col-stat" />
                <col className="col-stat" />
                <col className="col-form" />
              </colgroup>

              <thead>
                <tr>
                  <th>POS</th>
                  <th>TEAM</th>
                  <th className="stat-th">PL</th>
                  <th className="stat-th">W</th>
                  <th className="stat-th">D</th>
                  <th className="stat-th">L</th>
                  <th className="stat-th">GD</th>
                  <th className="stat-th">PTS</th>
                  <th>FORM</th>
                </tr>
              </thead>

              <tbody>
                {teams.map((team) => (
                  <tr key={`${team.team_id}-${team.position}`}>
                    <td>{team.position ?? ""}</td>

                    {/* TEAM cell is the only navigation affordance */}
                    <td className="team-cell">
                      {team.team_id ? (
                        <Link
                          to={`/teams/${team.team_id}?season=${encodeURIComponent(
                            season
                          )}&league=${encodeURIComponent(leagueId)}`}
                          className="team-link"
                          aria-label={`Open ${team.team_name} team page`}
                        >
                          <div className="team-wrap">
                            {team.team_logo_url ? (
                              <img
                                src={team.team_logo_url}
                                alt={`${team.team_name} logo`}
                                className="team-logo"
                                loading="lazy"
                              />
                            ) : (
                              <div
                                className="team-logo placeholder"
                                aria-hidden="true"
                              />
                            )}
                            <span className="team-name">{team.team_name}</span>
                          </div>
                        </Link>
                      ) : (
                        <div className="team-wrap">
                          {team.team_logo_url ? (
                            <img
                              src={team.team_logo_url}
                              alt={`${team.team_name} logo`}
                              className="team-logo"
                              loading="lazy"
                            />
                          ) : (
                            <div
                              className="team-logo placeholder"
                              aria-hidden="true"
                            />
                          )}
                          <span className="team-name">{team.team_name}</span>
                        </div>
                      )}
                    </td>

                    <td className="stat-td">{team.played}</td>
                    <td className="stat-td">{team.wins}</td>
                    <td className="stat-td">{team.draws}</td>
                    <td className="stat-td">{team.losses}</td>
                    <td className="stat-td">{team.goal_diff}</td>
                    <td className="stat-td">{team.points}</td>

                    <td className="form-td">
                      <div className="form-wrap">
                        {(team.form || []).map((r, i) => (
                          <span
                            key={i}
                            className={`form-dot ${
                              r === "W" ? "win" : r === "D" ? "draw" : "loss"
                            }`}
                            title={r}
                          >
                            {r}
                          </span>
                        ))}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ))}
    </div>
  );
}
