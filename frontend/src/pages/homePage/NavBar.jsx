import React, { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import "./styles/navBar.css"; // Ensure styles are applied globally

export default function NavBar() {
  const location = useLocation();
  const [activeTab, setActiveTab] = useState(
    location.pathname.includes("/news") ? "news" : "leagues"
  );

  return (
    <header className="header">
      <div className="header-content">
        {/* Left Side - Logo */}
        <div className="logo-area">
          <div className="logo-placeholder"></div>
          <span className="logo-text">FootySphere</span>
        </div>

        {/* Center - Navigation Links */}
        <nav className="nav-links">
          <Link
            to="/"
            className={activeTab === "leagues" ? "nav-button active" : "nav-button"}
            onClick={() => setActiveTab("leagues")}
          >
            <svg className="icon" viewBox="0 0 20 20" fill="currentColor">
              <path d="M10 0l2.09 6.26h6.09l-4.63 3.88 1.59 6.26L10 12.52l-5.14 3.88 1.59-6.26L1 6.26h6.09L10 0z" />
            </svg>
            Leagues
          </Link>

          <Link
            to="/news"
            className={activeTab === "news" ? "nav-button active" : "nav-button"}
            onClick={() => setActiveTab("news")}
          >
            <svg className="icon" viewBox="0 0 20 20" fill="currentColor">
              <path d="M4 2h12v2H4V2zm-2 3h16v11H2V5zm2 2v7h2V7H4zm4 0v2h8V7H8zm0 4v2h8v-2H8zm0 4v1h8v-1H8z" />
            </svg>
            News
          </Link>
        </nav>
      </div>
    </header>
  );
}
