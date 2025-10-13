import React, { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import "./styles/standingsTab.css";

const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");

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
          `${API}/leaguesPage/${leagueId}/standings?season=${season}`,
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

  const isMultiGroup = useMemo(
    () => groups.length > 1 || groups.some(([label]) => label !== ""),
    [groups]
  );

  function cleanGroupLabel(label = "") {
    let t = String(label).trim();
    if (!t) return t;
    if (t.includes(",")) t = t.split(",").pop().trim();
    if (t.includes(" - ")) t = t.split(" - ").pop().trim();
    t = t.replace(/\b20\d{2}(?:[\/\-]\d{2})?\b/g, "").trim();
    t = t.replace(/^\s*(MLS|La Liga|Serie A|Premier League)\s*/i, "").trim();
    t = t.replace(/^[,–-]\s*/, "").replace(/\s{2,}/g, " ").trim();
    return t || label;
  }

  if (loading) return <p className="muted">Loading standings…</p>;
  if (err) return <p className="muted">{err}</p>;
  if (!rows.length) return <p className="muted">No standings available.</p>;

  return (
    <div className="standings-tab">
      {groups.map(([label, teams]) => (
        <section key={label || "main"} className="table-block">
          {isMultiGroup ? (
            <div className="table-header compact">
              <span className="group-chip">{cleanGroupLabel(label)}</span>
            </div>
          ) : null}

          <div className="table-wrap tile">
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

                    {/* TEAM cell (only clickable cell) */}
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
                            {/* Crest chip wrapper — same as Weekly tab */}
                            <span className="crest-chip">
                              {team.team_logo_url ? (
                                <img
                                  src={team.team_logo_url}
                                  alt={`${team.team_name} logo`}
                                  className="team-logo"
                                  loading="lazy"
                                />
                              ) : (
                                <span className="team-logo placeholder" aria-hidden="true" />
                              )}
                            </span>
                            <span className="team-name">{team.team_name}</span>
                          </div>
                        </Link>
                      ) : (
                        <div className="team-wrap">
                          <span className="crest-chip">
                            {team.team_logo_url ? (
                              <img
                                src={team.team_logo_url}
                                alt={`${team.team_name} logo`}
                                className="team-logo"
                                loading="lazy"
                              />
                            ) : (
                              <span className="team-logo placeholder" aria-hidden="true" />
                            )}
                          </span>
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
