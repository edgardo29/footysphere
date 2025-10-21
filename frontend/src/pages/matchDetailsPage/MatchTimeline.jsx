import React, { useMemo } from "react";

export default function MatchTimeline({ events, homeTeamId }) {
  const toNum = (x) => (typeof x === "number" ? x : Number(String(x).split("+")[0]) || 0);

  const sortedEvents = useMemo(() => {
    const arr = Array.isArray(events) ? events.slice() : [];
    arr.sort((a, b) => {
      if (toNum(a.minute) !== toNum(b.minute)) return toNum(a.minute) - toNum(b.minute);
      return Number(a.minute_extra || 0) - Number(b.minute_extra || 0);
    });
    const subCountByTeam = new Map();
    for (const e of arr) {
      if (String(e.type || "").toLowerCase() === "substitution") {
        const key = e.team_id ?? "unknown";
        const next = (subCountByTeam.get(key) || 0) + 1;
        subCountByTeam.set(key, next);
        e._subNo = next;
      }
    }
    return arr;
  }, [events]);

  const timelineGroups = useMemo(() => {
    const parts = { first: [], second: [], extra: [] };
    for (const e of sortedEvents) {
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
        const isHome = e.team_id === homeTeamId;
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
  }, [sortedEvents, homeTeamId]);

  const fmtMinute = (min, extra = 0) => {
    const base = typeof min === "number" ? String(min) : String(Number(min) || "0");
    const add = Number(extra) > 0 ? `+${Number(extra)}` : "";
    return `${base}${add}′`;
  };

  const chipFor = (t, detail) => {
    const d = String(detail || "").toLowerCase();
    if (t === "goal") return d.includes("penalty") ? ["PEN", "goal"] : d.includes("own goal") ? ["OWN", "goal"] : ["GOAL", "goal"];
    if (t === "yellow") return ["YC", "yellow"];
    if (t === "red") return ["RC", "red"];
    if (t === "substitution") return ["SUB", "sub"];
    if (t === "var") return ["VAR", "var"];
    return ["EVT", "evt"];
  };

  return (
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

                    <div className="center-rail">
                      <span className="t-minute mono">{fmtMinute(row.minute, row.extra)}</span>
                    </div>

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
  );
}
