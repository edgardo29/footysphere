import "./styles/matchesTab.css";  // ✅ Keeping styles separate

const mockMatches = [
  { home: "Arsenal", away: "Chelsea", score: "2-1", date: "Feb 15" },
  { home: "Liverpool", away: "Man City", score: "1-1", date: "Feb 16" },
];

export default function MatchesTab() {
  return (
    <div className="matches-tab">
      {mockMatches.map((match, index) => (
        <div key={index} className="match-card">
          <span>{match.date}</span>
          <strong>{match.home} vs {match.away}</strong>
          <span>Score: {match.score}</span>
        </div>
      ))}
    </div>
  );
}
