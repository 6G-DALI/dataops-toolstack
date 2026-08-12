import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  resolve: {
    // Vite's default order lists .jsx before .tsx, so for the leftover
    // HomePage/TaskCreator .jsx + .tsx pairs the untyped legacy file won.
    // The app was silently running the old JS versions. Preferring .tsx makes
    // the TypeScript components the ones that ship.
    // TODO: delete components/HomePage.jsx and components/TaskCreator.jsx once
    // the .tsx versions are confirmed good, then drop this override.
    extensions: ['.mjs', '.js', '.mts', '.ts', '.tsx', '.jsx', '.json'],
  },
  server: {
    port: 3000,
    // Uncomment to proxy API calls to Airflow in dev (avoids CORS):
    // proxy: {
    //   '/api': {
    //     target: 'http://localhost:8080',
    //     changeOrigin: true,
    //   },
    // },
  },
})