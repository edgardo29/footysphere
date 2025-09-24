import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    // Any request starting with /api will be forwarded to your FastAPI backend
    proxy: {
      '/api': {
        target: 'http://172.202.48.252:8000',
        changeOrigin: true,
        // strip off the /api prefix so your backend routes stay at /teams, /health, etc.
        rewrite: (path) => path.replace(/^\/api/, '')
      }
    }
  }
})
