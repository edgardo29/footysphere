import React from "react";
import { Link, useLocation } from "react-router-dom";
import "./styles/navBar.css";

export default function NavBar() {
  const { pathname } = useLocation();
  const isHome = pathname === "/";
  const isNews = pathname.startsWith("/news"); // ← NEW

  return (
    <header className="nb">
      <div className="nb__inner">
        {/* Brand */}
        <Link to="/" className="nb__brand" aria-label="FootySphere home">
          <span className="nb__logo-wrap">
            <img src="/logo.png" alt="" className="nb__logo-img" width={20} height={20} />
          </span>
          <span className="nb__name">FootySphere</span>
        </Link>

        {/* Primary nav */}
        <nav className="nb__nav" aria-label="Primary">
          <Link to="/" className={`nb__btn ${isHome ? "is-active" : ""}`}>
            <svg className="nb__icon" viewBox="0 0 20 20" fill="currentColor" aria-hidden>
              <path d="M10 1.25l2.18 5.38 5.82.18-4.6 3.68 1.64 5.61L10 13.7 4.96 16.1l1.64-5.61-4.6-3.68 5.82-.18L10 1.25z"/>
            </svg>
            <span>Leagues</span>
          </Link>

          {/* NEW: News */}
          <Link to="/news" className={`nb__btn ${isNews ? "is-active" : ""}`}>
            <svg className="nb__icon" viewBox="0 0 24 24" fill="currentColor" aria-hidden>
              <path d="M4 5h12a2 2 0 0 1 2 2v10h1a1 1 0 0 0 1-1V8h-2V7a3 3 0 0 0-3-3H4a1 1 0 1 0 0 2Zm0 3h12v9a2 2 0 0 1-2 2H5a3 3 0 0 1-3-3V8h2v9a1 1 0 0 0 1 1h9a1 1 0 0 0 1-1V8H4Zm1 3h8v2H5v-2Zm0 3h8v2H5v-2Z"/>
            </svg>
            <span>News</span>
          </Link>
        </nav>
      </div>
    </header>
  );
}
