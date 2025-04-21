import React from "react";
import { useParams, Link } from "react-router-dom";
import NavBar from "../homePage/NavBar";
import "./styles/leaguesPage.css";
import StandingsTab from "./StandingsTab"; // adjust path as needed


// Mock Data
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
    logo: 'https://upload.wikimedia.org/wikipedia/en/9/9f/La_Liga_Santander.svg',
  },
  {
    id: 3,
    name: 'Bundesliga',
    country: 'Germany',
    logo: 'https://upload.wikimedia.org/wikipedia/en/d/df/Bundesliga_logo_%282017%29.svg',
  },
  {
    id: 4,
    name: 'Serie A',
    country: 'Italy',
    logo: 'https://upload.wikimedia.org/wikipedia/en/e/e1/Serie_A_logo.png',
  },
  {
    id: 5,
    name: 'Ligue 1',
    country: 'France',
    logo: 'https://upload.wikimedia.org/wikipedia/en/b/ba/Ligue_1_Uber_Eats_logo.png',
  },
];

export default function LeaguesPage() {
  const { leagueId } = useParams();
  const selectedLeague = leagues.find((league) => league.id === Number(leagueId));

  if (!selectedLeague) {
    return <div className="error">League not found</div>;
  }

  return (
    <div>
      <NavBar />

      <div className="leagues-page">
        <div className="header-container">
          <Link to="/" className="back-button">
            <span className="back-arrow">←</span>
            Back to Home
          </Link>
          
          <div className="league-header">
            <img 
              src={selectedLeague.logo} 
              alt={`${selectedLeague.name} logo`} 
              className="league-logo" 
            />
            <div className="header-text">
              <h1 className="league-title">{selectedLeague.name}</h1>
              <p className="season-text">2023/24 Season</p>
            </div>
          </div>
        </div>

        <div className="league-tabs">
          <button className="league-tab active">Standings</button>
          <button className="league-tab">Weekly Matches</button>
        </div>

        <div className="league-content">
        <StandingsTab />
        </div>
      </div>
    </div>
  );
}