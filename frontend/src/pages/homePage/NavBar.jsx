import React from "react";
import { Link, useLocation } from "react-router-dom";
import "./styles/navBar.css";

export default function NavBar() {
  const { pathname } = useLocation();
  const isHome = pathname === "/";

  return (
    <header className="nb">
      <div className="nb__inner">
        {/* Brand */}
        <Link to="/" className="nb__brand" aria-label="FootySphere home">
          <span className="nb__logo-wrap">
            {/* transparent PNG sits on a colored chip so it’s visible */}
            <img
              src="/logo.png"   // /public/logo.png
              alt=""
              className="nb__logo-img"
              width={20}
              height={20}
            />
          </span>
          <span className="nb__name">FootySphere</span>
        </Link>

        {/* Primary nav (News removed) */}
        <nav className="nb__nav" aria-label="Primary">
          <Link to="/" className={`nb__btn ${isHome ? "is-active" : ""}`}>
            <svg className="nb__icon" viewBox="0 0 20 20" fill="currentColor" aria-hidden>
              <path d="M10 1.25l2.18 5.38 5.82.18-4.6 3.68 1.64 5.61L10 13.7 4.96 16.1l1.64-5.61-4.6-3.68 5.82-.18L10 1.25z"/>
            </svg>
            <span>Leagues</span>
          </Link>
        </nav>
      </div>
    </header>
  );
}
