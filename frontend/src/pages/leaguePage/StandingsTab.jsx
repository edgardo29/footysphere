// src/pages/leaguePage/StandingsTab.jsx
import React, { useEffect, useMemo, useState } from "react";
import "./styles/standingsTab.css";

export default function StandingsTab({ leagueId, season }) {
  const [rows, setRows] = useState([]);     // API rows
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

    async function fetchStandings() {
      try {
        setLoading(true);
        setErr(null);
        const res = await fetch(`/api/leaguesPage/${leagueId}/standings?season=${season}`, { signal: ctrl.signal });
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
    }

    fetchStandings();
    return () => ctrl.abort();
  }, [leagueId, season]);

  // Group by group_label so leagues with phases/groups render cleanly
  const groups = useMemo(() => {
    const map = new Map();
    for (const r of rows) {
      const key = r.group_label || "";
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(r);
    }
    // Keep stable order: empty label (regular table) first, then others by label
    return Array.from(map.entries()).sort(([a], [b]) => (a === b ? 0 : a === "" ? -1 : b === "" ? 1 : a.localeCompare(b)));
  }, [rows]);

  if (loading) return <p className="loading">Loading standings…</p>;
  if (err)      return <p className="error">{err}</p>;
  if (!rows.length) return <p className="empty">No standings available.</p>;

  return (
    <div className="standings-tab">
      {groups.map(([label, teams]) => (
        <div key={label || "main"}>
          {label ? <h3 className="group-title">{label}</h3> : null}

          <table className="standings-table">
            <thead>
              <tr>
                <th>POS</th>
                <th>TEAM</th>
                <th>PL</th>
                <th>W</th>
                <th>D</th>
                <th>L</th>
                <th>GD</th>
                <th>PTS</th>
                <th>FORM</th>
              </tr>
            </thead>
            <tbody>
              {teams.map((team) => (
                <tr key={`${team.team_id}-${team.position}`}>
                  <td>{team.position ?? ""}</td>
                  <td className="team-cell">
                    <div className="team-wrap">
                      {team.team_logo_url ? (
                        <img
                          src={team.team_logo_url}
                          alt={`${team.team_name} logo`}
                          className="team-logo"
                          loading="lazy"
                        />
                      ) : (
                        <div className="team-logo placeholder" aria-hidden="true" />
                      )}
                      <span className="team-name">{team.team_name}</span>
                    </div>
                  </td>

                  <td>{team.played}</td>
                  <td>{team.wins}</td>
                  <td>{team.draws}</td>
                  <td>{team.losses}</td>
                  <td>{team.goal_diff}</td>
                  <td>{team.points}</td>
                  <td className="form-td">
                    {(team.form || []).map((r, i) => (
                      <span
                        key={i}
                        className={`form-dot ${r === "W" ? "win" : r === "D" ? "draw" : "loss"}`}
                        title={r}
                      >
                        {r}
                      </span>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}
