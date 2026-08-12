/*
 * Runtime configuration for the 6G-DALI DataOps UI.
 *
 * Loaded before the bundle, so a deployment can repoint every URL by replacing
 * this one file — no rebuild. Overridden at container start by
 * docker-entrypoint.d/40-dataops-config.sh when the matching VITE_* variables
 * are set in the environment.
 *
 * Leave a value empty to fall back to the build-time value, then to the default
 * in src/config.ts. An empty tool URL drops its navbar link rather than pointing
 * at nothing.
 */
window.__DATAOPS_CONFIG__ = {
  // Required for the app to function: the DataOps Orchestrator API.
  orchestratorUrl: '',

  // piveau catalogue front end; dataset links are <base>/datasets/<id>.
  catalogueBaseUrl: '',

  // Portal base URL — the navbar username links to <base>/#/account.
  portalUrl: '',

  // Shared 6G-DALI tool suite — the navbar links. Same names as the portal.
  daliUrl: '',
  dataspaceUrl: '',
  dataopsUrl: '',
  mlopsUrl: '',

  // Single sign-on. Realm and IdP host MUST match the other DALI front ends;
  // the client is specific to this application.
  keycloakUrl: 'https://auth.dspace.sparkworks.net/auth',
  keycloakRealm: 'dspace',
  keycloakClientId: 'dataops-ui',
}
