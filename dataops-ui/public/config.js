/*
 * Runtime configuration, read through the shared __DALI_CONFIG__ key.
 *
 * Every 6G-DALI front end reads that one key, so a single generated file can
 * configure the whole suite and the navbar links cannot disagree between apps.
 * Each app takes the keys it knows and ignores the rest.
 *
 * Served as a static file, so it can be replaced at deploy time by a Docker
 * volume mount, a ConfigMap, or an entrypoint script — no image rebuild needed.
 */
window.__DALI_CONFIG__ = {
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
