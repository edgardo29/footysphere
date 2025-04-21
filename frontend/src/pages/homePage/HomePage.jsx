import React, { useState } from 'react';
import '../../App.css';
import { useNavigate } from "react-router-dom";
import NavBar from "./NavBar"; // Adjust path if needed

import { Link } from "react-router-dom";


import { FaTrophy, FaCalendarAlt } from "react-icons/fa";  // FontAwesome Trophy
import { MdOutlineSportsSoccer } from "react-icons/md"; // Material Soccer Icon
// Example data
const leagues = [
  {
    id: 1,
    name: 'Premier League',
    country: 'England',
    logo: 'https://upload.wikimedia.org/wikipedia/en/thumb/f/f2/Premier_League_Logo.svg/1200px-Premier_League_Logo.svg.png',
  },
  {
    id: 2,
    name: 'La Liga',
    country: 'Spain',
    logo: '/public/la-liga.png',
  },
  {
    id: 3,
    name: 'Bundesliga',
    country: 'Germany',
    logo: 'https://upload.wikimedia.org/wikipedia/en/thumb/d/df/Bundesliga_logo_%282017%29.svg/1200px-Bundesliga_logo_%282017%29.svg.png',
  },
  {
    id: 4,
    name: 'Serie A',
    country: 'Italy',
    logo: '/public/serie-a-logo.png',
  },

  {
    id: 5,
    name: 'Ligue 1',
    country: 'France',
    logo: '/public/ligue-1-logo.png',
  },
];




// We now have two leagues: "Premier League" and "La Liga"
// Example array with 3 leagues that have matches, and we’ll handle the 4th (Serie A) as empty:
const matches = [
  // Premier League
  {
    id: 1,
    homeTeam: {
      name: 'Arsenal',
      logo: 'https://upload.wikimedia.org/wikipedia/en/5/53/Arsenal_FC.svg',
    },
    awayTeam: {
      name: 'Chelsea',
      logo: 'https://upload.wikimedia.org/wikipedia/en/c/cc/Chelsea_FC.svg',
    },
    time: '15:00',
    status: 'Upcoming',
    league: {
      name: 'Premier League',
      logo: 'https://upload.wikimedia.org/wikipedia/en/thumb/f/f2/Premier_League_Logo.svg/1200px-Premier_League_Logo.svg.png',
    },
  },
  {
    id: 2,
    homeTeam: {
      name: 'Liverpool',
      logo: 'https://upload.wikimedia.org/wikipedia/en/0/0c/Liverpool_FC.svg',
    },
    awayTeam: {
      name: 'Manchester City',
      logo: 'https://upload.wikimedia.org/wikipedia/en/e/eb/Manchester_City_FC_badge.svg',
    },
    time: '17:30',
    status: 'Live',
    league: {
      name: 'Premier League',
      logo: 'https://upload.wikimedia.org/wikipedia/en/thumb/f/f2/Premier_League_Logo.svg/1200px-Premier_League_Logo.svg.png',
    },
  },

  // La Liga
  {
    id: 3,
    homeTeam: {
      name: 'Barcelona',
      logo: 'https://upload.wikimedia.org/wikipedia/en/4/47/FC_Barcelona_%28crest%29.svg',
    },
    awayTeam: {
      name: 'Real Madrid',
      logo: 'https://upload.wikimedia.org/wikipedia/en/5/56/Real_Madrid_CF.svg',
    },
    time: '20:00',
    status: 'Upcoming',
    league: {
      name: 'La Liga',
      logo: 'https://upload.wikimedia.org/wikipedia/en/9/9f/La_Liga_Santander.svg',
    },
  },
  {
    id: 4,
    homeTeam: {
      name: 'Atletico Madrid',
      logo: 'https://upload.wikimedia.org/wikipedia/en/f/f4/Atletico_Madrid_2017_logo.svg',
    },
    awayTeam: {
      name: 'Valencia',
      logo: 'https://upload.wikimedia.org/wikipedia/en/9/9f/Valenciacf.svg',
    },
    time: '22:00',
    status: 'Finished',
    league: {
      name: 'La Liga',
      logo: 'https://upload.wikimedia.org/wikipedia/en/9/9f/La_Liga_Santander.svg',
    },
  },

  // Bundesliga
  {
    id: 5,
    homeTeam: {
      name: 'Bayern Munich',
      logo: 'https://upload.wikimedia.org/wikipedia/en/1/1f/FC_Bayern_Munich_logo_%282017%29.svg',
    },
    awayTeam: {
      name: 'Borussia Dortmund',
      logo: 'https://upload.wikimedia.org/wikipedia/commons/6/67/Borussia_Dortmund_logo.svg',
    },
    time: '18:00',
    status: 'Upcoming',
    league: {
      name: 'Bundesliga',
      logo: 'https://upload.wikimedia.org/wikipedia/en/d/df/Bundesliga_logo_%282017%29.svg',
    },
  },
  {
    id: 6,
    homeTeam: {
      name: 'RB Leipzig',
      logo: 'https://upload.wikimedia.org/wikipedia/en/0/04/RB_Leipzig_2014_logo.svg',
    },
    awayTeam: {
      name: 'Bayer Leverkusen',
      logo: 'https://upload.wikimedia.org/wikipedia/en/2/2f/Bayer_04_Leverkusen_logo.svg',
    },
    time: '20:30',
    status: 'Live',
    league: {
      name: 'Bundesliga',
      logo: 'https://upload.wikimedia.org/wikipedia/en/d/df/Bundesliga_logo_%282017%29.svg',
    },
  },
];

// We want four leagues in total: "Premier League", "La Liga", "Bundesliga", and "Serie A"
// 3) We want five leagues total, including "Serie A" and "Ligue 1" which have no matches
const leagueNames = [
  'Premier League',
  'La Liga',
  'Bundesliga',
  'Serie A',
  'Ligue 1'
];

// 4) Build a single matchesByLeague object so we get empty arrays for leagues with no matches
// Build a single matchesByLeague object so we get empty arrays for leagues with no matches
const matchesByLeague = leagueNames.reduce((acc, league) => {
  acc[league] = [];
  return acc;
}, {});

// Fill in the matches for the leagues that actually have them
matches.forEach((match) => {
  const { name } = match.league;
  matchesByLeague[name].push(match);
});

export default function HomePage() {
    const [activeTab, setActiveTab] = useState('leagues');
    const navigate = useNavigate();


  return (
    <div className="app">
    <NavBar /> {/* ✅ Navigation Bar is now imported */}


      
    {/* Banner Section */}
    <section className="banner-container">
        <div className="banner-wrapper">
          <div className="banner-grid">
            {/* Left: Angled Glass Card */}
            <div className="banner-left-card">
              <h1 className="banner-heading">
                <span className="banner-heading-gradient">Live Football Updates</span>
                <br />
                <span className="heading-subtle">Right at Your Fingertips</span>
              </h1>
              <p className="banner-subtitle">
                Track live scores, explore leagues, and stay updated with the latest
                football news from around the globe.
              </p>
              <div className="stats-grid">
                <div className="stat-box">
                  <div className="stat-box-icon" />
                  <div className="stat-box-number">20+</div>
                  <div className="stat-box-label">Leagues</div>
                </div>
                <div className="stat-box">
                  <div className="stat-box-icon" />
                  <div className="stat-box-number">500+</div>
                  <div className="stat-box-label">Teams</div>
                </div>
                <div className="stat-box">
                  <div className="stat-box-icon" />
                  <div className="stat-box-number">100+</div>
                  <div className="stat-box-label">Live Matches</div>
                </div>
              </div>
            </div>
            {/* Right: Big diagonal/wave-masked image */}
            <div className="banner-right" />
          </div>
        </div>
      </section>



  {/* Main Content */}
  <main className="main-content">
        {activeTab === 'leagues' && (
          <>
            {/* Popular Leagues */}
            <section className="section-block">
        <div className="section-header">
          <div className="section-icon-box">
            <FaTrophy className="section-icon" />
          </div>
          <h2 className="section-title">Popular Leagues</h2>
        </div>


{/* League Cards */}
<div className="leagues-grid">
  {leagues.map((league) => (
    <Link key={league.id} to={`/league/${league.id}`} className="league-card">
      <div className="league-logo-wrap">
        <img
          src={league.logo}
          alt={`${league.name} logo`}
          className="league-logo"
        />
      </div>
      <div>
        <h3>{league.name}</h3>
        <p>{league.country}</p>
      </div>
    </Link>
  ))}
</div>



                {/* View More Button */}
            <div className="view-more-container">
              <button className="view-more-button">View More</button>
            </div>
            </section>




            {/* Today's Matches */}
            <section className="section-block">
              <div className="section-header">
                <div className="section-icon-box matches-icon-box">
                  <FaCalendarAlt className="section-icon matches-icon" />
                </div>
                <h2>Today's Matches</h2>
              </div>

              <div className="todays-leagues-grid">
  {Object.entries(matchesByLeague)
    .filter(([_, leagueMatches]) => leagueMatches.length > 0) // Only show leagues with matches
    .map(([leagueName, leagueMatches]) => (
      <div key={leagueName} className="league-block">
        {/* League header */}
        <div className="league-info-pill">
          <span>{leagueName}</span>
        </div>

        {/* Match Cards */}
        {leagueMatches.map((match) => (
          <div key={match.id} className="match-card">
            <div className="team-block">
              <div className="team-logo-wrap">
                <img src={match.homeTeam.logo} alt={match.homeTeam.name} className="team-logo" />
              </div>
              <div className="team-info">
                <span className="team-name">{match.homeTeam.name}</span>
                <span className="team-place">Home</span>
              </div>
            </div>

            <div className="match-details">
              <span className="match-time">{match.time}</span>
              <span className={`match-status ${match.status === "Live" ? "live" : ""}`}>
                {match.status}
              </span>
            </div>

            <div className="team-block right">
              <div className="team-info right-info">
                <span className="team-name">{match.awayTeam.name}</span>
                <span className="team-place">Away</span>
              </div>
              <div className="team-logo-wrap">
                <img src={match.awayTeam.logo} alt={match.awayTeam.name} className="team-logo" />
              </div>
            </div>
          </div>
        ))}
      </div>
    ))}
</div>

            </section>




          </>
        )}







        {activeTab === 'news' && (
          <section className="section-block">
            <div className="section-header">
              <div className="section-square" />
              <h2>Latest News</h2>
            </div>
            <p style={{ color: '#aaa' }}>News content coming soon...</p>
          </section>
        )}
      </main>
    </div>
  );
}
