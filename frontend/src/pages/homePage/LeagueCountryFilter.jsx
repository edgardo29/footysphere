import React, { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { FaGlobeAmericas } from "react-icons/fa";

import "./styles/popularLeagues.css";
import "./styles/leagueCountryFilter.css";

export default function LeagueCountryFilter({ apiBase }) {
  const API = (apiBase || "").replace(/\/+$/, "");

  const [countries, setCountries] = useState([]);
  const [selectedCountry, setSelectedCountry] = useState(null);

  const [leagues, setLeagues] = useState([]);
  const [loadingCountries, setLoadingCountries] = useState(true);
  const [loadingLeagues, setLoadingLeagues] = useState(false);

  const [pickerOpen, setPickerOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [letter, setLetter] = useState("");
  const inputRef = useRef(null);

  // Keep leagues from getting cluttered if a country has many
  const LEAGUE_PAGE_SIZE = 10;
  const [leagueLimit, setLeagueLimit] = useState(LEAGUE_PAGE_SIZE);

  // Fetch countries once (backend already returns alphabetical)
  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        setLoadingCountries(true);
        const res = await fetch(`${API}/countries`);
        if (!res.ok) throw new Error("Failed to load countries");
        const data = await res.json();

        const list = (Array.isArray(data) ? data : [])
          .map((x) => (x?.country ?? "").trim())
          .filter(Boolean);

        if (!cancelled) setCountries(list);
      } catch (e) {
        console.error(e);
        if (!cancelled) setCountries([]);
      } finally {
        if (!cancelled) setLoadingCountries(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [API]);

  // Default selected = first country (alphabetical)
  useEffect(() => {
    if (selectedCountry) return;
    if (!countries.length) return;
    setSelectedCountry(countries[0]);
  }, [countries, selectedCountry]);

  // Fetch leagues for selected country
  useEffect(() => {
    let cancelled = false;

    (async () => {
      if (!selectedCountry) {
        setLeagues([]);
        return;
      }

      try {
        setLoadingLeagues(true);
        const url = `${API}/leagues_by_country?country=${encodeURIComponent(
          selectedCountry
        )}`;
        const res = await fetch(url);
        if (!res.ok) throw new Error("Failed to load leagues");
        const data = await res.json();

        if (!cancelled) setLeagues(Array.isArray(data) ? data : []);
      } catch (e) {
        console.error(e);
        if (!cancelled) setLeagues([]);
      } finally {
        if (!cancelled) setLoadingLeagues(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [API, selectedCountry]);

  // Reset leagues pagination when changing country
  useEffect(() => {
    setLeagueLimit(LEAGUE_PAGE_SIZE);
  }, [selectedCountry]);

  // Top chips: first 9 (alphabetical from API)
  const chipCountries = useMemo(() => countries.slice(0, 9), [countries]);

  // More: everything after first 9
  const moreCountries = useMemo(() => countries.slice(9), [countries]);

  // Big list behavior (100+ countries)
  const isLargeMore = moreCountries.length > 24;

  const availableLetters = useMemo(() => {
    const set = new Set();
    for (const c of moreCountries) {
      const ch = (c[0] || "").toUpperCase();
      if (ch) set.add(ch);
    }
    return Array.from(set).sort();
  }, [moreCountries]);

  const filteredCountries = useMemo(() => {
    let base = moreCountries;

    if (letter) {
      base = base.filter((c) => (c[0] || "").toUpperCase() === letter);
    }

    const q = query.trim().toLowerCase();
    if (q) {
      base = base.filter((c) => c.toLowerCase().includes(q));
    }

    // Don’t dump 100+ countries unless user filters
    if (isLargeMore && !letter && !q) return [];

    return base.slice(0, 120);
  }, [moreCountries, letter, query, isLargeMore]);

  const visibleLeagues = useMemo(
    () => leagues.slice(0, leagueLimit),
    [leagues, leagueLimit]
  );

  const canLoadMoreLeagues = leagues.length > leagueLimit;

  const selectCountry = (c) => {
    setSelectedCountry(c);
    setPickerOpen(false);
    setQuery("");
    setLetter("");
  };

  const togglePicker = () => {
    if (!moreCountries.length) return;

    setPickerOpen((v) => {
      const next = !v;

      if (next) {
        if (isLargeMore) setTimeout(() => inputRef.current?.focus(), 0);
      } else {
        setQuery("");
        setLetter("");
      }

      return next;
    });
  };

  // Keep “More” highlighted if selected country isn’t in top chips
  const moreIsActive =
    pickerOpen || (selectedCountry && !chipCountries.includes(selectedCountry));

  return (
    <section className="section-block league-country-filter">
      <div className="section-header">
        <div className="section-icon-box">
          <FaGlobeAmericas className="section-icon" />
        </div>
        <h2 className="section-title">Browse Leagues</h2>
      </div>

      {/* Chips row */}
      <div className="country-bar country-bar--wrap">
        {loadingCountries ? (
          <span className="muted">Loading…</span>
        ) : (
          <>
            {chipCountries.map((c) => (
              <button
                key={c}
                type="button"
                className={`country-chip ${
                  selectedCountry === c ? "country-chip--active" : ""
                }`}
                onClick={() => selectCountry(c)}
              >
                {c}
              </button>
            ))}

            {/* Anchor so dropdown is positioned under the More button */}
            <div className="more-anchor">
              <button
                type="button"
                className={`country-chip country-chip--more ${
                  moreIsActive ? "country-chip--active" : ""
                }`}
                onClick={togglePicker}
                disabled={!moreCountries.length}
              >
                More
              </button>

              {pickerOpen && (
                <div
                  className={`more-dropdown ${isLargeMore ? "is-large" : "is-small"}`}
                >
                  {isLargeMore && (
                    <>
                      <input
                        ref={inputRef}
                        className="more-search"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Search countries…"
                        aria-label="Search countries"
                      />

                      <div className="more-alpha">
                        <button
                          type="button"
                          className={`alpha-btn ${letter === "" ? "is-active" : ""}`}
                          onClick={() => setLetter("")}
                        >
                          All
                        </button>

                        {availableLetters.map((ch) => (
                          <button
                            key={ch}
                            type="button"
                            className={`alpha-btn ${letter === ch ? "is-active" : ""}`}
                            onClick={() => setLetter(ch)}
                          >
                            {ch}
                          </button>
                        ))}
                      </div>
                    </>
                  )}

                  <div className={`more-list ${isLargeMore ? "dense" : "compact"}`}>
                    {filteredCountries.map((c) => (
                      <button
                        key={c}
                        type="button"
                        className={`more-pill ${
                          selectedCountry === c ? "is-active" : ""
                        }`}
                        onClick={() => selectCountry(c)}
                        title={c}
                      >
                        {c}
                      </button>
                    ))}

                    {!filteredCountries.length && (
                      <div className="more-empty muted">
                        {isLargeMore
                          ? "Type to search or pick a letter."
                          : "No matches."}
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </>
        )}
      </div>

      {/* Leagues results */}
      {loadingLeagues && <p className="muted">Loading leagues…</p>}

      {!loadingLeagues && selectedCountry && (
        <>
          <div className="leagues-grid leagues-grid--compact">
            {visibleLeagues.map((lg) => {
              const id = lg.id ?? lg.league_id;
              const name = lg.name ?? lg.league_name;
              const country = lg.country ?? lg.league_country;
              const logo = lg.logo_url ?? lg.league_logo_url;

              return (
                <Link
                  key={id}
                  to={`/league/${id}`}
                  className="league-card tile tile--interactive"
                >
                  <div className="league-logo-wrap">
                    <img
                      src={logo}
                      alt={`${name} logo`}
                      className="league-logo"
                      loading="lazy"
                    />
                  </div>

                  <div className="league-text">
                    <h3>{name}</h3>
                    <p>{country}</p>
                  </div>
                </Link>
              );
            })}
          </div>

          {canLoadMoreLeagues && (
            <button
              type="button"
              className="leagues-load-more"
              onClick={() => setLeagueLimit((v) => v + LEAGUE_PAGE_SIZE)}
            >
              Show more leagues
            </button>
          )}

          {!visibleLeagues.length && (
            <p className="muted">No leagues found for {selectedCountry}.</p>
          )}
        </>
      )}
    </section>
  );
}
