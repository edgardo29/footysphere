import React, { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { FaNewspaper } from "react-icons/fa";
import "./styles/news.css";

const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");
const PREVIEW_COUNT = 3;

// tiny helpers
function timeAgo(isoUtc) {
  try {
    const d = new Date(isoUtc);
    const s = Math.max(0, (Date.now() - d.getTime()) / 1000);
    if (s < 60) return "just now";
    const m = s / 60, h = m / 60, d2 = h / 24;
    if (m < 60) return `${Math.floor(m)}m ago`;
    if (h < 24) return `${Math.floor(h)}h ago`;
    return `${Math.floor(d2)}d ago`;
  } catch { return ""; }
}
function stripTags(html = "") {
  const tmp = document.createElement("div");
  tmp.innerHTML = html;
  return (tmp.textContent || tmp.innerText || "").trim();
}
// deterministic daily selection (stable for a given UTC day)
function pickDaily(items, count) {
  if (!Array.isArray(items) || items.length <= count) return items || [];
  const key = new Date().toISOString().slice(0, 10); // YYYY-MM-DD (UTC)
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
  const step = 7; // small coprime-ish step
  const out = [];
  let idx = hash % items.length;
  const used = new Set();
  while (out.length < count) {
    if (!used.has(idx)) {
      out.push(items[idx]);
      used.add(idx);
    }
    idx = (idx + step) % items.length;
  }
  return out;
}

export default function News() {
  const [items, setItems] = useState([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState("");
  const abortRef = useRef(null);

  useEffect(() => {
    if (abortRef.current) abortRef.current.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    (async () => {
      try {
        setError("");
        // grab a small pool (20 newest across all tabs), then pick 3 deterministically
        const res = await fetch(`${API}/news?limit=20`, { signal: ac.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const pool = Array.isArray(data.items) ? data.items : [];
        setItems(pickDaily(pool, PREVIEW_COUNT));
      } catch (e) {
        if (e.name !== "AbortError") setError("Failed to load news preview.");
      } finally {
        if (abortRef.current === ac) abortRef.current = null;
        setLoaded(true);
      }
    })();

    return () => ac.abort();
  }, [API]);

  const hasAny = useMemo(() => (items || []).length > 0, [items]);

  return (
    <section id="news-preview" className="section-block">
      <div className="section-header">
        <div className="section-icon-box news-icon-box">
          <FaNewspaper className="section-icon news-icon" />
        </div>
        <h2 className="section-title">News</h2>
      </div>

      {!loaded && !error && <p className="muted">Loading news…</p>}
      {error && <p className="muted">{error}</p>}

      {hasAny && (
        <div className="news-preview-grid">
          {items.map((it) => (
            <article key={it.article_id} className="news-card tile">
              {it.image_url ? (
                <a
                  href={it.url}
                  className="news-thumb"
                  target="_blank"
                  rel="noreferrer"
                  aria-label={it.title}
                >
                  <img src={it.image_url} alt="" loading="lazy" />
                </a>
              ) : (
                <div className="news-thumb news-thumb--empty" />
              )}

              <div className="news-body">
                <h3 className="news-title">
                  <a href={it.url} target="_blank" rel="noreferrer">
                    {it.title}
                  </a>
                </h3>

                <div className="news-meta">
                  <span className="news-source">{it.source || "Source"}</span>
                  <span className="news-dot">•</span>
                  <time dateTime={it.published_at_utc}>{timeAgo(it.published_at_utc)}</time>
                  {it.tab && (
                    <>
                      <span className="news-dot">•</span>
                      <span className="news-tabchip">{it.tab}</span>
                    </>
                  )}
                </div>

                {it.summary && (
                  <p className="news-summary clamp-3">{stripTags(it.summary)}</p>
                )}
              </div>
            </article>
          ))}
        </div>
      )}

      <div className="view-more-container">
        <Link to="/news" className="view-more-button">
          View all news
        </Link>
      </div>
    </section>
  );
}
