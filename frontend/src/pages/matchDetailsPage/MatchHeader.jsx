// src/pages/matchDetailsPage/MatchHeader.jsx
import React from "react";
import { Link } from "react-router-dom";

function fmtWhen(iso, userTZ) {
  if (!iso) return "TBD";
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return "TBD";
  const date = dt.toLocaleDateString(undefined, { year: "numeric", month: "numeric", day: "numeric" });
  const time = dt.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit", timeZoneName: "short" });
  return `${date} ${time}`;
}

function fmtRound(round) {
  const raw = String(round || "").trim();
  if (!raw) return "";
  const mRS = raw.match(/regular season\s*-\s*(\d+)/i);
  if (mRS) return "";
  const mGroup = raw.match(/group\s*-\s*([A-Za-z])/i);
  if (mGroup) return `Group ${mGroup[1].toUpperCase()}`;
  const mRound = raw.match(/round\s*-\s*(\d+)/i);
  if (mRound) return `Round ${mRound[1]}`;
  return raw.replace(/\s*-\s*$/, "");
}

export default function MatchHeader({
  home,
  away,
  status,
  scoreFT,
  dateUtc,
  round,
  venue,
  competitionName,
  teamIdFromQuery,
  season,
  userTZ,
}) {
  const showScore = ["full-time", "live"].includes(String(status).toLowerCase());

  return (
    <>
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
          {competitionName ? (
            <span className="comp-pill" aria-label={`Competition ${competitionName}`}>
              {competitionName}
            </span>
          ) : null}
        </div>
      </div>

      {/* Overview tile */}
      <section className="section-block">
        <div className="match-header tile">
          <div className="teams-vs">
            <div className="team">
              {home?.logo ? (
                <img
                  className="logo"
                  src={home.logo}
                  alt={`${home?.name} logo`}
                  width={48}
                  height={48}
                  loading="eager"
                  fetchpriority="high"
                  decoding="async"
                />
              ) : null}
              <div className="name">{home?.name ?? "—"}</div>
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
              {away?.logo ? (
                <img
                  className="logo"
                  src={away.logo}
                  alt={`${away?.name} logo`}
                  width={48}
                  height={48}
                  loading="eager"
                  fetchpriority="high"
                  decoding="async"
                />
              ) : null}
              <div className="name">{away?.name ?? "—"}</div>
            </div>
          </div>

          <div className="meta-row">
            <span className="meta" title={`Times shown in your local timezone: ${userTZ || "local"}`}>
              {fmtWhen(dateUtc, userTZ)}
            </span>
            {(() => {
              const pretty = fmtRound(round);
              return pretty ? (<><span className="dot">·</span><span className="meta">{pretty}</span></>) : null;
            })()}
            {venue?.name ? (
              <>
                <span className="dot">·</span>
                <span className="meta">
                  {venue.name}{venue.city ? ` · ${venue.city}` : ""}
                </span>
              </>
            ) : null}
          </div>
        </div>
      </section>
    </>
  );
}
