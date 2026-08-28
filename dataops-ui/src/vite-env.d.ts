/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_ORCHESTRATOR_URL?: string
  readonly VITE_AUTH_URL?: string
  readonly VITE_KEYCLOAK_REALM?: string
  readonly VITE_KEYCLOAK_CLIENT_ID?: string
  readonly VITE_CATALOGUE_BASE_URL?: string
  // The 6G-DALI tool suite, linked from the navbar. Each link is rendered only
  // when its variable is set, so an unconfigured environment shows no dead links.
  readonly VITE_DALI_URL?: string
  readonly VITE_DATASPACE_URL?: string
  readonly VITE_DATAOPS_URL?: string
  readonly VITE_MLOPS_URL?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

/** Short commit SHA the bundle was built from; injected by vite.config.ts. */
declare const __BUILD_SHA__: string
