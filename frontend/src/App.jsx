import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import HomePage from "./pages/homePage/HomePage";
import LeaguePage from "./pages/leaguePage/LeaguePage";
import HealthTest from "./pages/HealthTest";
import TeamPage from "./pages/teamPage/TeamPage";
import MatchDetailsPage from "./pages/matchDetailsPage/MatchDetailsPage";
import NewsPage from "./pages/newsPage/NewsPage"; 

export default function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/news" element={<NewsPage />} /> 
        <Route path="/league/:leagueId" element={<LeaguePage />} />
        <Route path="/teams/:teamId" element={<TeamPage />} />
        <Route path="/match/:matchId" element={<MatchDetailsPage />} />
        <Route path="/health-test" element={<HealthTest />} />
      </Routes>
    </Router>
  );
}
