import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const projectRoot = fileURLToPath(new URL('.', import.meta.url))

/**
 * Short commit SHA for the footer's build marker.
 *
 * GIT_SHA first, because the image build cannot ask git: .dockerignore excludes
 * .git (correctly — it would bloat the context and bust the layer cache), so CI
 * passes the SHA in as a build arg. `git rev-parse` is the local-development
 * path. Neither working means an unmarked build rather than a failed one — a
 * missing build number must never be the reason a release cannot be cut.
 */
function buildSha(): string {
  const fromEnv = process.env.GIT_SHA?.trim()
  if (fromEnv) return fromEnv.slice(0, 7)

  try {
    return execFileSync('git', ['rev-parse', '--short=7', 'HEAD'], {
      cwd: projectRoot,
      stdio: ['ignore', 'pipe', 'ignore'],
    }).toString().trim()
  } catch {
    return 'unknown'
  }
}

export default defineConfig({
  plugins: [react()],
  define: {
    // Serialised, not interpolated: define does a raw textual substitution, so
    // the value has to arrive as a JS literal.
    __BUILD_SHA__: JSON.stringify(buildSha()),
  },
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