import Keycloak from 'keycloak-js'
import { config } from '../config'

/**
 * Single Keycloak adapter instance for the whole app.
 *
 * Defaults point at the DSpace Keycloak (realm `dspace`, client `dataops-ui`);
 * every value can be overridden through Vite env vars so the same build can be
 * pointed at a different IdP per deployment.
 */
const keycloak = new Keycloak({
  url: config.keycloakUrl,
  realm: config.keycloakRealm,
  clientId: config.keycloakClientId,
})

/** Clean base URL (no hash) used as the OAuth redirect target. */
export function redirectUri(): string {
  return window.location.origin + window.location.pathname
}

export default keycloak
