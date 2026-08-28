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
 * Only window.__DALI_CONFIG__ is read. A config.js still using the retired
 * __DATAOPS_CONFIG__ is reported on the console by the shared resolver rather
 * than silently ignored.
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

    authUrl: 'https://auth.dspace.sparkworks.net/auth',
    keycloakRealm: 'dspace',
    keycloakClientId: 'dataops-ui',

    orchestratorUrl: '',
    catalogueBaseUrl: '',
  },
  envKeys: {
    // The shared names (VITE_DALI_URL, VITE_AUTH_URL, …) come from the package,
    // so renaming one is a single change that reaches every front end.
    ...DALI_ENV_KEYS,

    orchestratorUrl: 'VITE_ORCHESTRATOR_URL',
    catalogueBaseUrl: 'VITE_CATALOGUE_BASE_URL',
  },
  // Passed in rather than read inside the package: `import.meta.env` is
  // substituted by Vite in *this* file, and a pre-bundled dependency cannot
  // count on that substitution reaching it.
  env: import.meta.env,
})

export const config: DataopsConfig = resolved
