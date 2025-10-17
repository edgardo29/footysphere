import React, { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import "./styles/standingsTab.css";

const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");

/* Canonicalize group labels so small text differences collapse */
function normalizeGroupKey(label = "") {
  return String(label)
    .replace(/\b20\d{2}(?:[\/\-]\d{2})?\b/g, "")           // drop season bits
    .replace(/\b(MLS|La Liga|Serie A|Premier League)\b/gi, "") // drop league prefixes
    .replace(/[,–-]+/g, " ")                               // unify separators
    .replace(/\s+/g, " ")                                  // collapse spaces
    .trim()
    .toLowerCase();
}

/* Your pretty display formatter (unchanged from behavior) */
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

  // De-dupe defensively in the UI (one row per team_id)
  const uniqueRows = useMemo(() => {
    const seen = new Set();
    const out = [];
    for (const r of rows) {
      if (seen.has(r.team_id)) continue;
      seen.add(r.team_id);
      out.push(r);
    }
    return out;
  }, [rows]);

  // Group by normalized key; keep first raw label for display
  const groups = useMemo(() => {
    const map = new Map(); // key -> { label, teams }
    for (const r of uniqueRows) {
      const key = normalizeGroupKey(r.group_label || "");
      if (!map.has(key)) map.set(key, { label: r.group_label || "", teams: [] });
      map.get(key).teams.push(r);
    }
    return Array.from(map.values()).sort((a, b) =>
      a.label === b.label ? 0 : a.label === "" ? -1 : b.label === "" ? 1 : a.label.localeCompare(b.label)
    );
  }, [uniqueRows]);

  const isMultiGroup = useMemo(
    () => groups.length > 1 || groups.some((g) => (g.label || "") !== ""),
    [groups]
  );

  if (loading) return <p className="muted">Loading standings…</p>;
  if (err) return <p className="muted">{err}</p>;
  if (!uniqueRows.length) return <p className="muted">No standings available.</p>;

  return (
    <div className="standings-tab">
      {groups.map(({ label, teams }) => (
        <section key={normalizeGroupKey(label) || "main"} className="table-block">
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

                    {/* TEAM cell (clickable) */}
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
                            className={`form-dot ${r === "W" ? "win" : r === "D" ? "draw" : "loss"}`}
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
