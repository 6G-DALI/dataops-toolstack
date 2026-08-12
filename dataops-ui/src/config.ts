/**
 * Resolves configuration at RUNTIME as well as build time.
 *
 * Vite inlines `import.meta.env.VITE_*` into the bundle, so a container built by
 * CI — where no .env exists — would ship with an undefined orchestrator URL and
 * no navbar links. Guidelines §27 warns about exactly this and recommends a
 * generated runtime config; that is what `public/config.js` is.
 *
 * Resolution order per key:
 *   1. window.__DATAOPS_CONFIG__  (public/config.js — replaceable in the image)
 *   2. import.meta.env.VITE_*     (build-time, used by `npm run dev`)
 *   3. the fallback below
 *
 * Same variable names as the portal and the same precedence, so one deployment
 * configures both front ends identically.
 */

export interface DataopsConfig {
  /** Base URL of the DataOps Orchestrator API. */
  orchestratorUrl: string
  /** piveau catalogue front end; links are built as <base>/datasets/<id>. */
  catalogueBaseUrl: string
  /** Portal base URL; the navbar username links to <base>/#/account. */
  portalUrl: string

  /* Shared 6G-DALI tool suite — the navbar links. */
  daliUrl: string
  dataspaceUrl: string
  dataopsUrl: string
  mlopsUrl: string

  /* Single sign-on. Realm and IdP host must match the other DALI front ends. */
  keycloakUrl: string
  keycloakRealm: string
  keycloakClientId: string
}

declare global {
  interface Window {
    __DATAOPS_CONFIG__?: Partial<Record<keyof DataopsConfig, string>>
  }
}

const DEFAULTS: DataopsConfig = {
  orchestratorUrl: '',
  catalogueBaseUrl: '',
  // Empty by default: an unset URL leaves the username as plain text and drops
  // the navbar link rather than pointing somewhere that does not exist.
  portalUrl: '',
  daliUrl: '',
  dataspaceUrl: '',
  dataopsUrl: '',
  mlopsUrl: '',
  keycloakUrl: 'https://auth.dspace.sparkworks.net/auth',
  keycloakRealm: 'dspace',
  keycloakClientId: 'dataops-ui',
}

const ENV_KEYS: Record<keyof DataopsConfig, string> = {
  orchestratorUrl: 'VITE_ORCHESTRATOR_URL',
  catalogueBaseUrl: 'VITE_CATALOGUE_BASE_URL',
  portalUrl: 'VITE_PORTAL_URL',
  daliUrl: 'VITE_DALI_URL',
  dataspaceUrl: 'VITE_DATASPACE_URL',
  dataopsUrl: 'VITE_DATAOPS_URL',
  mlopsUrl: 'VITE_MLOPS_URL',
  keycloakUrl: 'VITE_KEYCLOAK_URL',
  keycloakRealm: 'VITE_KEYCLOAK_REALM',
  keycloakClientId: 'VITE_KEYCLOAK_CLIENT_ID',
}

function stripTrailingSlash(url: string): string {
  return url.endsWith('/') && url.length > 1 ? url.slice(0, -1) : url
}

function resolve(key: keyof DataopsConfig): string {
  const runtime = window.__DATAOPS_CONFIG__?.[key]
  if (runtime) return stripTrailingSlash(runtime)

  const buildTime = import.meta.env[ENV_KEYS[key]] as string | undefined
  if (buildTime) return stripTrailingSlash(buildTime)

  return DEFAULTS[key]
}

export const config: DataopsConfig = {
  orchestratorUrl: resolve('orchestratorUrl'),
  catalogueBaseUrl: resolve('catalogueBaseUrl'),
  portalUrl: resolve('portalUrl'),
  daliUrl: resolve('daliUrl'),
  dataspaceUrl: resolve('dataspaceUrl'),
  dataopsUrl: resolve('dataopsUrl'),
  mlopsUrl: resolve('mlopsUrl'),
  keycloakUrl: resolve('keycloakUrl'),
  keycloakRealm: resolve('keycloakRealm'),
  keycloakClientId: resolve('keycloakClientId'),
}
