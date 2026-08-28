/**
 * Resolves configuration at RUNTIME as well as build time.
 *
 * Vite inlines `import.meta.env.VITE_*` into the bundle, so a container built by
 * CI — where no .env exists — would ship with an undefined orchestrator URL and
 * no navbar links. Guidelines §27 warns about exactly this and recommends a
 * generated runtime config; that is what `public/config.js` is.
 *
 * The resolver itself now lives in @6g-dali/ui-shell, so this app and the portal
 * cannot drift apart on precedence, on the VITE_* names for the shared keys, or
 * on URL normalisation. Resolution order per key:
 *
 *   1. window.__DALI_CONFIG__     (public/config.js — replaceable in the image)
 *   2. import.meta.env.VITE_*     (build-time, used by `npm run dev`)
 *   3. the fallback below
 *
 * window.__DATAOPS_CONFIG__ is still honoured as a legacy fallback, so an
 * already-deployed container keeps working with the config.js it has.
 */
import { resolveConfig, DALI_ENV_KEYS, type DaliBaseConfig } from '@6g-dali/ui-shell'

/**
 * The shared keys — the 6G-DALI tool suite and single sign-on — come from
 * DaliBaseConfig, which is what guarantees the navbar links and the SSO realm
 * are described identically in every front end. Below are this app's own.
 */
export interface DataopsConfig extends DaliBaseConfig {
  /** Base URL of the DataOps Orchestrator API. */
  orchestratorUrl: string
  /** piveau catalogue front end; links are built as <base>/datasets/<id>. */
  catalogueBaseUrl: string
  /**
   * Deprecated alias for `authUrl`.
   *
   * This app named the IdP `keycloakUrl`/`VITE_KEYCLOAK_URL` before the shared
   * config settled on `authUrl`/`VITE_AUTH_URL`, and deployed containers still
   * emit the old name from docker-entrypoint.d/40-dataops-config.sh. Keeping it
   * as a fallback means upgrading the image does not silently repoint the app
   * at the compiled-in default IdP. Remove once every deployment sets
   * VITE_AUTH_URL.
   */
  keycloakUrl: string
}

const resolved = resolveConfig<DataopsConfig>({
  defaults: {
    // Empty by default: an unset URL leaves the username as plain text and
    // drops the navbar link rather than pointing somewhere that does not exist.
    portalUrl: '',
    daliUrl: '',
    dataspaceUrl: '',
    dataopsUrl: '',
    mlopsUrl: '',

    // Deliberately empty — see the merge below. The real default lives on
    // keycloakUrl so that the legacy key still wins over it.
    authUrl: '',
    keycloakUrl: 'https://auth.dspace.sparkworks.net/auth',
    keycloakRealm: 'dspace',
    keycloakClientId: 'dataops-ui',

    orchestratorUrl: '',
    catalogueBaseUrl: '',
  },
  envKeys: {
    // The shared names (VITE_DALI_URL, VITE_AUTH_URL, …) come from the package,
    // so renaming one is a single change that reaches every front end.
    ...DALI_ENV_KEYS,

    keycloakUrl: 'VITE_KEYCLOAK_URL',
    orchestratorUrl: 'VITE_ORCHESTRATOR_URL',
    catalogueBaseUrl: 'VITE_CATALOGUE_BASE_URL',
  },
  // Passed in rather than read inside the package: `import.meta.env` is
  // substituted by Vite in *this* file, and a pre-bundled dependency cannot
  // count on that substitution reaching it.
  env: import.meta.env,
})

export const config: DataopsConfig = {
  ...resolved,
  // VITE_AUTH_URL when a deployment sets it, the legacy VITE_KEYCLOAK_URL
  // otherwise, and only then the compiled-in default.
  authUrl: resolved.authUrl || resolved.keycloakUrl,
}
