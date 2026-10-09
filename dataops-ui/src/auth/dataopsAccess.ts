import keycloak from './keycloak'
import type { View } from '../types'

/**
 * Keycloak group whose members run DataOps: the DAGs, tasks, datasets and services pages. Members of the
 * testbed groups (/testbeds/<slug>) and testbed admins do not get these pages unless they are in this group too.
 * Accepts the group as a path ('/dataops-operators') or a bare name, like the testbed groups.
 */
const DATAOPS_OPERATOR_GROUP = 'dataops-operators'

export function isDataopsOperator(): boolean {
  const groups = (keycloak.tokenParsed as { groups?: string[] } | undefined)?.groups ?? []
  return groups.some(g => g.replace(/^\/+/, '') === DATAOPS_OPERATOR_GROUP)
}

/** The pages that need the operator group: everything except the home page and the testbed pages. The home
 *  page is for everyone and gates its own blocks. */
const OPEN_VIEWS: View[] = ['home', 'testbeds', 'testbed-register', 'testbed-detail']

export function isOperationsView(view: View): boolean {
  return !OPEN_VIEWS.includes(view)
}
