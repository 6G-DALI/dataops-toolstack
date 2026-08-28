import { createKeycloak, redirectUri } from '@6g-dali/ui-shell'
import { config } from '../config'

/**
 * Single Keycloak adapter instance for the whole app.
 *
 * Realm and IdP host are shared with every other DALI front end so that one
 * sign-in carries across the suite; they come from the shared DaliBaseConfig
 * keys rather than being written out per application. The *client* is this
 * app's own, since each needs its own redirect URIs.
 *
 * The adapter is created here, not in the package: each app owns its own
 * `init()` policy. main.tsx uses `login-required` — this app has no anonymous
 * state — where the portal uses `check-sso` to keep its landing page public.
 */
export { redirectUri }

export default createKeycloak(config)
