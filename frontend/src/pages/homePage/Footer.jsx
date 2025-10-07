import React from "react";
import { FaLinkedin } from "react-icons/fa";
import "./styles/footer.css";

const LINKEDIN_URL = "https://www.linkedin.com/feed/";

export default function Footer() {
  const year = new Date().getFullYear();

  return (
    <footer className="ft" role="contentinfo">
      <div className="ft-bar">
        <div className="ft-left">© {year} Footysphere</div>

        <div className="ft-center" aria-hidden="true">
          <span>Terms of use</span>
          <span className="ft-sep">•</span>
          <span>Cookie policy</span>
          <span className="ft-sep">•</span>
          <span>Privacy policy</span>
        </div>

        <div className="ft-right">
          <span className="ft-follow">Follow us</span>
          <a
            href={LINKEDIN_URL}
            className="ft-icon"
            target="_blank"
            rel="noreferrer"
            aria-label="LinkedIn"
            title="LinkedIn"
          >
            <FaLinkedin />
          </a>
        </div>
      </div>
    </footer>
  );
}
