import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import HomePage from "./pages/homePage/HomePage";
import LeaguePage from "./pages/leaguePage/LeaguePage";
import HealthTest from "./pages/HealthTest";
import TeamPage from "./pages/teamPage/TeamPage"; // NEW
import MatchDetails from "./pages/matchDetails/MatchDetails";


export default function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/league/:leagueId" element={<LeaguePage />} />
        <Route path="/teams/:teamId" element={<TeamPage />} /> {/* NEW */}
        <Route path="/match/:matchId" element={<MatchDetails />} />
        <Route path="/health-test" element={<HealthTest />} />
      </Routes>
    </Router>
  );
}
