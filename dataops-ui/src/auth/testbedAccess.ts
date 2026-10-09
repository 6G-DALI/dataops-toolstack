import keycloak from './keycloak'

/** Realm role that manages every testbed and the registry itself (the orchestrator's TESTBED_ADMIN_ROLE). */
const TESTBED_ADMIN_ROLE = 'testbed-admin'

/** Owners of a testbed are members of the Keycloak group /<prefix>/<slug> (the orchestrator's TESTBED_GROUP_PREFIX). */
const TESTBED_GROUP_PREFIX = 'testbeds'

export function isTestbedAdmin(): boolean {
  return keycloak.hasRealmRole(TESTBED_ADMIN_ROLE)
}

/** True for a member of at least one testbed group. The orchestrator decides which testbeds they actually get. */
export function ownsTestbeds(): boolean {
  const groups = (keycloak.tokenParsed as { groups?: string[] } | undefined)?.groups ?? []
  return groups.some(g => g.replace(/^\/+/, '').startsWith(`${TESTBED_GROUP_PREFIX}/`))
}

/** Whether the Testbeds menu is shown. This only hides the UI; the orchestrator enforces access. */
export function canSeeTestbeds(): boolean {
  return isTestbedAdmin() || ownsTestbeds()
}
