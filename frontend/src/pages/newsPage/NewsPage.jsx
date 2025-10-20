import React, { useEffect, useMemo, useRef, useState } from "react";
import "../../App.css";
import "./styles/newsPage.css";
  // reuse the News styles
import { FaNewspaper } from "react-icons/fa";
import { useSearchParams } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import Footer from "../homePage/Footer";

// ───────────────────────────────────────────────
// Constants & helpers
// ───────────────────────────────────────────────
const TABS = [
  { key: "all", label: "All" },
  { key: "transfers", label: "Transfers" },
  { key: "injuries", label: "Injuries" },
  { key: "matchreports", label: "Match reports" },
];

const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");

function timeAgo(isoUtc) {
  try {
    const d = new Date(isoUtc);
    const s = Math.max(0, (Date.now() - d.getTime()) / 1000);
    if (s < 60) return "just now";
    const m = s / 60, h = m / 60, d2 = h / 24;
    if (m < 60) return `${Math.floor(m)}m ago`;
    if (h < 24) return `${Math.floor(h)}h ago`;
    return `${Math.floor(d2)}d ago`;
  } catch {
    return "";
  }
}

function stripTags(html = "") {
  const tmp = document.createElement("div");
  tmp.innerHTML = html;
  return (tmp.textContent || tmp.innerText || "").trim();
}

// ───────────────────────────────────────────────
// Page component (full News experience)
// ───────────────────────────────────────────────
export default function NewsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialTab = (() => {
    const t = (searchParams.get("tab") || "all").toLowerCase();
    return TABS.some(x => x.key === t) ? t : "all";
  })();

  const [tab, setTab] = useState(initialTab);
  const [items, setItems] = useState([]);
  const [nextCursor, setNextCursor] = useState(null);
  const [loading, setLoading] = useState(false);
  const [initialLoaded, setInitialLoaded] = useState(false);
  const [error, setError] = useState("");
  const abortRef = useRef(null);

  // Keep URL in sync with current tab
  useEffect(() => {
    const params = new URLSearchParams(searchParams);
    if (tab === "all") params.delete("tab");
    else params.set("tab", tab);
    setSearchParams(params, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  const ids = useMemo(() => new Set(items.map(i => i.article_id)), [items]);

  async function fetchPage({ reset = false } = {}) {
    if (abortRef.current) abortRef.current.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    setLoading(true);
    setError("");
    try {
      const qs = new URLSearchParams();
      qs.set("limit", "20");
      if (tab !== "all") qs.set("tab", tab);
      if (!reset && nextCursor) qs.set("cursor", nextCursor);

      const res = await fetch(`${API}/news?${qs.toString()}`, { signal: ac.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const fresh = (data.items || []).filter(it => !ids.has(it.article_id));
      setItems(prev => (reset ? fresh : prev.concat(fresh)));
      setNextCursor(data.next_cursor || null);
      setInitialLoaded(true);
    } catch (e) {
      if (e.name !== "AbortError") setError("Failed to load news.");
    } finally {
      if (abortRef.current === ac) abortRef.current = null;
      setLoading(false);
    }
  }

  // Initial load + when tab changes
  useEffect(() => {
    setItems([]);
    setNextCursor(null);
    setInitialLoaded(false);
    fetchPage({ reset: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  return (
    <div className="app">
      <NavBar />

      <main className="main-content">
        <section id="news" className="section-block">
          <div className="section-header">
            <div className="section-icon-box news-icon-box">
              <FaNewspaper className="section-icon news-icon" />
            </div>
            <h2 className="section-title">News</h2>
          </div>

          {/* Tabs */}
          <div className="news-tabs" role="tablist" aria-label="News tabs">
            {TABS.map(t => (
              <button
                key={t.key}
                role="tab"
                aria-selected={tab === t.key}
                className={`news-tab ${tab === t.key ? "is-active" : ""}`}
                onClick={() => setTab(t.key)}
                disabled={loading && tab === t.key}
              >
                {t.label}
              </button>
            ))}
          </div>

          {/* States */}
          {error && <p className="muted">{error}</p>}
          {!initialLoaded && loading && <p className="muted">Loading news…</p>}
          {initialLoaded && items.length === 0 && !loading && (
            <p className="muted">No news yet.</p>
          )}

          {/* Grid */}
          <div className="news-grid">
            {items.map(it => (
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

                  <div className="news-footer">
                    <a className="news-read" href={it.url} target="_blank" rel="noreferrer">
                      Read article →
                    </a>
                  </div>
                </div>
              </article>
            ))}
          </div>

          {/* Load more */}
          {nextCursor && (
            <div className="load-more-wrap">
              <button
                className="load-more-btn"
                onClick={() => fetchPage()}
                disabled={loading}
              >
                {loading ? "Loading…" : "Load more"}
              </button>
            </div>
          )}
        </section>
      </main>

      <Footer />
    </div>
  );
}
