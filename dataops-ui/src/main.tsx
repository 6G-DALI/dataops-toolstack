import React from 'react'
import ReactDOM from 'react-dom/client'
import 'bootstrap/dist/css/bootstrap.min.css'
import 'admin-lte/dist/css/adminlte.min.css'
import 'admin-lte/dist/js/adminlte.min.js'
import App from './App'
import keycloak, { redirectUri } from './auth/keycloak'
import './index.css'

const RETURN_KEY = 'kc_post_login_hash'

function renderFatal(message: string) {
  const root = document.getElementById('root')
  if (root) {
    root.innerHTML =
      `<div style="max-width:520px;margin:15vh auto;font-family:system-ui,sans-serif;text-align:center">
        <h2 style="color:#dc3545">Authentication error</h2>
        <p style="color:#6c757d">${message}</p>
        <button onclick="location.reload()" style="padding:.4rem 1rem;cursor:pointer">Retry</button>
      </div>`
  }
}

async function bootstrap() {
  // Preserve the route the user tried to open across the login round-trip:
  // the OAuth redirect goes back to the clean base URL (no hash), so we stash
  // the intended hash and restore it once we're authenticated.
  if (window.location.hash && !/(?:^|&)(state|access_token|code)=/.test(window.location.hash.slice(1))) {
    sessionStorage.setItem(RETURN_KEY, window.location.hash)
  }

  let authenticated = false
  try {
    // Authorization Code + PKCE. `responseMode: 'query'` keeps the `?code=…`
    // callback in the query string so it never collides with the hash router.
    authenticated = await keycloak.init({
      onLoad: 'login-required',
      pkceMethod: 'S256',
      responseMode: 'query',
      checkLoginIframe: false,
      redirectUri: redirectUri(),
      enableLogging: import.meta.env.DEV,
    })
  } catch (err) {
    renderFatal((err as Error)?.message || 'Could not reach the identity provider.')
    return
  }

  if (!authenticated) {
    keycloak.login({ redirectUri: redirectUri() })
    return
  }

  // Restore the originally requested route.
  const target = sessionStorage.getItem(RETURN_KEY)
  if (target) {
    sessionStorage.removeItem(RETURN_KEY)
    if (window.location.hash !== target) window.location.hash = target
  }

  // Best-effort silent token renewal; fall back to a fresh login on failure.
  keycloak.onTokenExpired = () => {
    keycloak.updateToken(30).catch(() => keycloak.login({ redirectUri: redirectUri() }))
  }

  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  )
}

bootstrap()
