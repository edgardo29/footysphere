import React, { useEffect, useRef, useState } from "react";
import { FiSearch } from "react-icons/fi";
import "./styles/leagueModal.css";


const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");

// JSON fetch helper with better error messages
async function fetchJSON(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const j = await res.json();
      if (j?.detail) msg = j.detail;
    } catch {}
    const err = new Error(msg);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

export default function LeagueModal({ open, onClose, onSelect }) {
  const [q, setQ] = useState("");

  // Default list (paged)
  const [allItems, setAllItems] = useState([]);
  const [listPage, setListPage] = useState(1);
  const [listHasMore, setListHasMore] = useState(false);
  const [loadingList, setLoadingList] = useState(false);

  // Search list
  const [items, setItems] = useState([]);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [searchError, setSearchError] = useState("");

  const [active, setActive] = useState(0);
  const inputRef = useRef(null);
  const abortRef = useRef(null);
  const debounceRef = useRef();

  // Reset on open
  useEffect(() => {
    if (!open) return;
    setQ("");
    setItems([]);
    setSearchError("");
    setActive(0);
    setAllItems([]);
    setListPage(1);
    setListHasMore(false);
    setLoadingList(false);
    const t = setTimeout(() => inputRef.current?.focus(), 10);
    return () => clearTimeout(t);
  }, [open]);

  // Load initial list
  useEffect(() => {
    if (!open) return;
    (async () => { await loadMoreList(true); })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // Esc to close
  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        onClose?.();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  // Debounced search (≥2 chars). If it fails, we keep the ALL list visible.
  useEffect(() => {
    if (!open) return;

    if (q.trim().length < 2) {
      setItems([]);
      setSearchError("");
      setActive(0);
      setLoadingSearch(false);
      return;
    }

    setLoadingSearch(true);
    setSearchError("");
    if (debounceRef.current) clearTimeout(debounceRef.current);

    debounceRef.current = setTimeout(async () => {
      if (abortRef.current) abortRef.current.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const data = await fetchJSON(
              `${API}/leagueModal/search?q=${encodeURIComponent(q)}&limit=20`,
                { signal: controller.signal }
        );
        setItems(Array.isArray(data?.items) ? data.items : []);
        setActive(0);
      } catch (e) {
        if (e.name !== "AbortError") {
          setSearchError(typeof e.message === "string" ? e.message : "Search failed.");
          setItems([]); // keep list view via fallback below
        }
      } finally {
        setLoadingSearch(false);
      }
    }, 250);

    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [q, open]);

  // Load/paginate the ALL list
  const loadMoreList = async (reset = false) => {
    if (loadingList) return;
    setLoadingList(true);
    try {
      const page = reset ? 1 : listPage;
      const data = await fetchJSON(`${API}/leagueModal/list?page=${page}&page_size=50`);
      const next = Array.isArray(data?.items) ? data.items : [];
      setAllItems((prev) => (reset ? next : [...prev, ...next]));
      const nextPage = data?.next_cursor ? parseInt(data.next_cursor, 10) : null;
      setListHasMore(!!nextPage);
      setListPage(nextPage || page + (next.length ? 1 : 0));
    } catch (e) {
      console.error(e);
      setListHasMore(false);
    } finally {
      setLoadingList(false);
    }
  };

  // Active dataset with graceful fallback:
  // - If typing (q>=2) and search succeeded → show search results
  // - If search errored → show ALL list and surface the error inline
  const typing = q.trim().length >= 2;
  const usingSearch = typing && !searchError;
  const list = usingSearch ? items : allItems;
  const loading = usingSearch ? loadingSearch : loadingList;

  // Keyboard nav
  const onListKeyDown = (e) => {
    if (!open) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((i) => Math.min(i + 1, Math.max(list.length - 1, 0)));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      const target = list[active];
      if (target) onSelect?.(target);
    }
  };

  if (!open) return null;

  return (
    <div className="lm-overlay" onMouseDown={onClose}>
      <div className="lm-dialog" onMouseDown={(e) => e.stopPropagation()}>
        {/* Header + input */}
        <div className="lm-header">
          <div className="lm-input-wrap">
            <FiSearch className="lm-icon-left" aria-hidden />
            <input
              ref={inputRef}
              className="lm-input"
              placeholder="Search leagues (optional)…"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onKeyDown={onListKeyDown}
              autoCorrect="off"
              autoCapitalize="none"
              spellCheck="false"
            />
            <span className="lm-icon-right">Esc</span>
          </div>
        </div>

        {/* Section label */}
        <div className="lm-section">
          {usingSearch
            ? (loading ? "Searching…" : `Results (${list.length})`)
            : "All Leagues"}
          {searchError && (
            <span className="lm-section-error"> — {searchError}. Showing all leagues.</span>
          )}
        </div>

        {/* Results */}
        <div className="lm-results" onKeyDown={onListKeyDown} tabIndex={-1}>
          {!loading && list.length === 0 ? (
            <div className="lm-empty">
              {usingSearch ? "No leagues found." : "No leagues available."}
            </div>
          ) : (
            <>
              {list.map((lg, idx) => (
                <div
                  key={`${lg.id}-${idx}`}
                  className={`lm-row ${idx === active ? "is-active" : ""}`}
                  onMouseEnter={() => setActive(idx)}
                  onClick={() => onSelect?.(lg)}
                >
                  <div className="lm-logo-wrap">
                    <img className="lm-logo" src={lg.league_logo_url} alt={`${lg.name} logo`} loading="lazy" />
                  </div>
                  <div>
                    <div className="lm-name">{lg.name}</div>
                    <div className="lm-meta">{lg.country || "—"}</div>
                  </div>
                  <div className="lm-badge">{lg.country || "League"}</div>
                </div>
              ))}

              {/* Load more for the ALL list */}
              {!usingSearch && listHasMore && (
                <button
                  type="button"
                  className="lm-loadmore"
                  onClick={() => loadMoreList(false)}
                  disabled={loadingList}
                >
                  {loadingList ? "Loading…" : "Load more"}
                </button>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="lm-footer">
          <div>
            Use <span className="lm-kbd">↑</span><span className="lm-kbd">↓</span> to navigate,
            <span className="lm-kbd">Enter</span> to open
          </div>
          <div>Press <span className="lm-kbd">Esc</span> to close</div>
        </div>
      </div>
    </div>
  );
}
