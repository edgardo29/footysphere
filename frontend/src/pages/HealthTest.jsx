import React, { useState, useEffect } from 'react';

export default function HealthTest() {
  const [status, setStatus] = useState('loading…');

  useEffect(() => {
    // hits your backend via the Vite proxy /api/health
    fetch('/api/health')
      .then(res => res.json())
      .then(data => setStatus(data.status))
      .catch(() => setStatus('error'));
  }, []);

  return (
    <div style={{ padding: 20, fontFamily: 'sans-serif' }}>
      <h2>API Health Test</h2>
      <p>Status: <strong>{status}</strong></p>
    </div>
  );
}
