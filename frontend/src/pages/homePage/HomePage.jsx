import React, { useState, useEffect, useMemo } from "react";
import "../../App.css";
import "./styles/homePage.css";

import { useNavigate } from "react-router-dom";
import NavBar from "./NavBar";
import TopBanner from "./TopBanner";
import Footer from "./Footer";
import LeagueModal from "./LeagueModal";

import PopularLeagues from "./PopularLeagues";
import TodaysMatches from "./TodaysMatches";

// Helper: try multiple endpoints safely
async function firstPopularThatWorks(urls) {
  for (const u of urls) {
    try {
      const res = await fetch(u);
      if (!res.ok) continue;
      const data = await res.json();
      if (Array.isArray(data) && data.length) return data;
    } catch {}
  }
  return [];
}

export default function HomePage() {
  const [leagues, setLeagues] = useState([]);
  const [matchesByLeague, setMatchesByLeague] = useState({});
  const [loadingLeagues, setLoadingLeagues] = useState(true);
  const [loadingMatches, setLoadingMatches] = useState(true);
  const [showLeagueModal, setShowLeagueModal] = useState(false);

  const navigate = useNavigate();
  const API = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");

  // Popular Leagues
  useEffect(() => {
    let mounted = true;
    (async () => {
      setLoadingLeagues(true);
      const data = await firstPopularThatWorks([
        `${API}/leagues/popular`,
        `${API}/leagues/popular?limit=10`,
        `${API}/leagueModal/popular?limit=10`,
      ]);
      if (mounted) {
        setLeagues(data || []); // no slicing; show whatever API returns
        setLoadingLeagues(false);
      }
    })();
    return () => {
      mounted = false;
    };
  }, [API]);

  // Today's Matches (TZ-aware)
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoadingMatches(true);
        const tz =
          Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
        const res = await fetch(
          `${API}/matches/today?tz=${encodeURIComponent(tz)}`
        );
        const data = await res.json();
        if (cancelled) return;
        const grouped = (Array.isArray(data) ? data : []).reduce((acc, m) => {
          (acc[m.league_name] ??= []).push(m);
          return acc;
        }, {});
        setMatchesByLeague(grouped);
      } catch (e) {
        console.error(e);
      } finally {
        if (!cancelled) setLoadingMatches(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [API]);

  const allMatches = useMemo(
    () => Object.values(matchesByLeague).flat(),
    [matchesByLeague]
  );

  const tickerMatches = useMemo(() => {
    const arr = (allMatches || [])
      .slice()
      .sort((a, b) => new Date(a.kickoff_utc) - new Date(b.kickoff_utc));
    return arr.slice(0, 10);
  }, [allMatches]);

  const handleSelectLeague = (lg) => {
    setShowLeagueModal(false);
    navigate(`/league/${lg.id}`);
  };

  return (
    <div className="app">
      <NavBar />
      <TopBanner tickerMatches={tickerMatches} visual="orbs" />

      <main className="main-content">
        <PopularLeagues
          leagues={leagues}
          loading={loadingLeagues}
          onViewMore={() => setShowLeagueModal(true)}
        />

        <TodaysMatches
          matchesByLeague={matchesByLeague}
          loading={loadingMatches}
        />
      </main>

      <Footer />

      <LeagueModal
        open={showLeagueModal}
        onClose={() => setShowLeagueModal(false)}
        onSelect={handleSelectLeague}
      />
    </div>
  );
}
