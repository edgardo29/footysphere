// src/components/leagues/StandingsTab.jsx

import React from "react";
import "./styles/standingsTab.css";

const mockStandings = [
  {
    position: 1,
    teamName: "Arsenal",
    played: 20,
    wins: 15,
    draws: 3,
    losses: 2,
    goalDifference: 25,
    points: 48,
    form: ["W", "W", "D", "W", "W"],
  },
  {
    position: 2,
    teamName: "Manchester City",
    played: 20,
    wins: 14,
    draws: 4,
    losses: 2,
    goalDifference: 23,
    points: 46,
    form: ["W", "W", "W", "L", "W"],
  },
  // ... more teams can go here
];

export default function StandingsTab() {
  return (
    <div className="standings-tab">
      <table className="standings-table">
        <thead>
          <tr>
            <th>POS</th>
            <th>TEAM</th>
            <th>PL</th>
            <th>W</th>
            <th>D</th>
            <th>L</th>
            <th>GD</th>
            <th>PTS</th>
            <th>FORM</th>
          </tr>
        </thead>
        <tbody>
          {mockStandings.map((team, index) => (
            <tr key={index}>
              <td>{team.position}</td>
              <td>{team.teamName}</td>
              <td>{team.played}</td>
              <td>{team.wins}</td>
              <td>{team.draws}</td>
              <td>{team.losses}</td>
              <td>{team.goalDifference}</td>
              <td>{team.points}</td>
              <td className="form-td">
                {team.form.map((result, i) => (
                  <span 
                    key={i} 
                    className={`form-dot ${result === "W" ? "win" : result === "D" ? "draw" : "loss"}`}
                  >
                    {result}
                  </span>
                ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
