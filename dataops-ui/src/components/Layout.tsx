import type { ReactNode } from 'react'
import {
  FiHome, FiGrid, FiList, FiDatabase, FiPlusCircle, FiSettings,
} from 'react-icons/fi'
import {
  AppShell,
  daliTools,
  portalAccountUrl,
  usernameOf,
  type AccountAction,
  type Crumb,
  type NavItem,
} from '@6g-dali/ui-shell'
import keycloak, { redirectUri } from '../auth/keycloak'
import { config } from '../config'
import type { NavigateFn, NavParams, View } from '../types'

/**
 * The DataOps application shell.
 *
 * The frame itself — navbar, sidebar, content header, breadcrumb, footer — is
 * @6g-dali/ui-shell's AppShell, shared with portal-ui. What stays here is what
 * is genuinely this app's: its sidebar, the mapping from a sub-view to the
 * top-level entry that should light up, and the breadcrumb trail, which is the
 * only really involved part — DAG → run → task, built from the nav params.
 */

const NAV_ITEMS: NavItem<View>[] = [
  { label: 'Home',        view: 'home',            icon: FiHome },
  { label: 'DAGs',        view: 'dags',            icon: FiGrid },
  { label: 'Tasks',       view: 'all-tasks',       icon: FiList },
  { label: 'Datasets',    view: 'datasets',        icon: FiDatabase },
  { label: 'Add Dataset', view: 'dataset-creator', icon: FiPlusCircle },
  { label: 'Services',    view: 'services',        icon: FiSettings },
]

/** The portal owns the single account page for the whole DALI SSO environment,
 *  so the username links there rather than being inert. An unset portal URL
 *  leaves it as plain text instead of a broken link — same rule as the navbar
 *  tool links. */
function accountAction(): AccountAction | undefined {
  const url = portalAccountUrl(config.portalUrl)
  // A separate tab, consistent with the other cross-app links: this app's
  // wizards may be holding unsaved form state.
  return url ? { href: url, newTab: true, title: 'Account settings — opens the 6G-DALI Portal' } : undefined
}

function shortenRunId(runId: string): string {
  if (!runId) return ''
  return runId.length > 30 ? runId.slice(0, 30) + '…' : runId
}

/**
 * The breadcrumb trail.
 *
 * Each crumb carries its own click handler, which is what lets one shared shell
 * serve this five-level trail and the portal's two: `NavParams` never crosses
 * the package boundary. AppShell renders the last crumb as the inactive current
 * page (§7.3), so nothing here has to special-case the tail.
 */
function buildCrumbs(
  view: View,
  dagId: string | null,
  runId: string | null,
  taskId: string | null,
  onNavigate: NavigateFn,
): Crumb[] {
  const crumb = (label: string, target: View, params: NavParams = {}): Crumb => ({
    label,
    onSelect: () => onNavigate(target, params),
  })

  if (view === 'home') return [crumb('Home', 'home')]
  if (view === 'datasets') return [crumb('Datasets', 'datasets')]
  if (view === 'dataset-creator') {
    return [crumb('Datasets', 'datasets'), crumb('Add Dataset', 'dataset-creator')]
  }
  if (view === 'services') return [crumb('Services', 'services')]
  if (view === 'all-tasks') return [crumb('Tasks', 'all-tasks')]
  if (view === 'dag-builder') {
    return [crumb('Tasks', 'all-tasks'), crumb('Build DAG', 'dag-builder')]
  }
  if (view === 'task-creator') {
    return [
      crumb('Tasks', 'all-tasks'),
      crumb(dagId ? 'Edit Task' : 'Create Task', 'task-creator'),
    ]
  }

  const crumbs: Crumb[] = [crumb('DAGs', 'dags')]
  if (dagId) crumbs.push(crumb(dagId, 'runs', { dagId }))
  if (view === 'dag-tasks') crumbs.push(crumb('Tasks', 'dag-tasks', { dagId: dagId ?? '' }))
  if (runId) {
    crumbs.push(crumb(shortenRunId(runId), 'tasks', { dagId: dagId ?? '', runId }))
  }
  if (taskId) {
    crumbs.push(crumb(taskId, 'logs', { dagId: dagId ?? '', runId: runId ?? '', taskId }))
  }
  return crumbs
}

/** Which sidebar entry lights up for a view that is not itself in the sidebar. */
function topLevelView(view: View): View {
  if (view === 'home') return 'home'
  if (['all-tasks', 'dag-builder', 'task-creator'].includes(view)) return 'all-tasks'
  if (view === 'dataset-creator') return 'dataset-creator'
  if (view === 'datasets') return 'datasets'
  if (view === 'services') return 'services'
  return 'dags'
}

interface LayoutProps {
  view: View
  dagId: string | null
  runId: string | null
  taskId: string | null
  onNavigate: NavigateFn
  children: ReactNode
}

export default function Layout({ view, dagId, runId, taskId, onNavigate, children }: LayoutProps) {
  return (
    <AppShell<View>
      brand={<>6G-<span className="dali-accent">DALI</span> DataOps</>}
      homeView="home"
      nav={NAV_ITEMS}
      activeView={topLevelView(view)}
      onNavigate={next => onNavigate(next, {})}
      tools={daliTools(config)}
      breadcrumbs={buildCrumbs(view, dagId, runId, taskId, onNavigate)}
      username={usernameOf(keycloak)}
      account={accountAction()}
      onLogout={() => keycloak.logout({ redirectUri: redirectUri() })}
      footer={<strong>DataOps &mdash; Apache Airflow control plane.</strong>}
      // Injected by vite.config.ts: the commit this bundle was built from, so a
      // deployed page can be traced back to a revision without guessing.
      build={__BUILD_SHA__}
    >
      {children}
    </AppShell>
  )
}
