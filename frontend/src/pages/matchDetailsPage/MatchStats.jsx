import React from "react";

export default function MatchStats({ statsHome, statsAway, rowsConfig }) {
  const clampPct = (n) => Math.max(0, Math.min(100, Number(n) || 0));

  return (
    <div className="tile stats-card">
      <div className="tile-title">Match Statistics</div>

      <div className="stats">
        {rowsConfig.map(({ key, label, kind = "value", suffix = "" }) => {
          const hvRaw = statsHome?.[key] ?? 0;
          const avRaw = statsAway?.[key] ?? 0;

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
  );
}
