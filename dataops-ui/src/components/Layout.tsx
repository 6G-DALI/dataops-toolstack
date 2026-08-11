import type { ReactNode } from 'react'
import {
  FiMenu, FiHome, FiGrid, FiList, FiDatabase, FiPlusCircle, FiSettings, FiUser, FiLogOut,
} from 'react-icons/fi'
import type { IconType } from 'react-icons'
import type { NavigateFn, NavParams, View } from '../types'
import keycloak, { redirectUri } from '../auth/keycloak'
import '../styles/Layout.css'

interface NavItem {
  label: string
  view: View
  icon: IconType
}

const NAV_ITEMS: NavItem[] = [
  { label: 'Home',     view: 'home',      icon: FiHome },
  { label: 'DAGs',     view: 'dags',      icon: FiGrid },
  { label: 'Tasks',    view: 'all-tasks', icon: FiList },
  { label: 'Datasets', view: 'datasets',  icon: FiDatabase },
  { label: 'Add Dataset', view: 'dataset-creator', icon: FiPlusCircle },
  { label: 'Services', view: 'services',  icon: FiSettings },
]

// AdminLTE's own sidebar state, reimplemented here because its JS binds the
// toggle with a one-shot `document.querySelectorAll('[data-lte-toggle="sidebar"]')`
// on DOMContentLoaded. React renders this navbar *after* that runs, so the
// element never receives AdminLTE's listener — and since that listener is also
// what calls preventDefault(), an <a href="#"> toggle silently cleared the hash
// instead, which App.tsx resolves to its default view ('dags'). Hence a real
// <button> plus these class manipulations, which mirror AdminLTE's PushMenu
// (classes X/Y and the 992px sidebarBreakpoint in adminlte.min.js).
const SIDEBAR_COLLAPSE_CLASS = 'sidebar-collapse'
const SIDEBAR_OPEN_CLASS = 'sidebar-open'
const SIDEBAR_BREAKPOINT = 992

function toggleSidebar() {
  const body = document.body
  if (body.classList.contains(SIDEBAR_COLLAPSE_CLASS)) {
    // expand
    body.classList.remove(SIDEBAR_COLLAPSE_CLASS)
    // On small screens the sidebar is an overlay that needs opening explicitly.
    if (globalThis.innerWidth <= SIDEBAR_BREAKPOINT) body.classList.add(SIDEBAR_OPEN_CLASS)
  } else {
    // collapse
    body.classList.remove(SIDEBAR_OPEN_CLASS)
    body.classList.add(SIDEBAR_COLLAPSE_CLASS)
  }
}

interface ExternalTool {
  label: string
  url: string | undefined
  title: string
}

/** The 6G-DALI tool suite, linked from the navbar so the same set is reachable
 *  from every app. Each entry is dropped when its URL is unset rather than
 *  pointing somewhere that doesn't exist, so a partially configured deployment
 *  simply shows fewer links. */
const EXTERNAL_TOOLS: ExternalTool[] = [
  {
    label: '6G-DALI',
    url: import.meta.env.VITE_DALI_URL,
    title: 'The 6G-DALI project site',
  },
  {
    label: 'Data Space',
    url: import.meta.env.VITE_DATASPACE_URL,
    title: 'Browse and search the 6G-DALI Data Space catalogue',
  },
  {
    label: 'Data Ops',
    url: import.meta.env.VITE_DATAOPS_URL,
    title: 'Data Ops — pipelines, datasets and data quality',
  },
  {
    label: 'ML Ops',
    url: import.meta.env.VITE_MLOPS_URL,
    title: 'ML Ops — model training and serving',
  },
]

function ExternalToolLinks() {
  const tools = EXTERNAL_TOOLS.filter((tool): tool is ExternalTool & { url: string } =>
    Boolean(tool.url && tool.url.trim()))
  if (tools.length === 0) return null

  return (
    <>
      {tools.map(tool => (
        <li className="nav-item" key={tool.label}>
          {/* Text only. The label is always rendered — with no icon beside it,
              hiding it on narrow screens would leave an empty clickable box. */}
          <a
            className="nav-link external-tool-link"
            href={tool.url}
            // Separate tab: these are other applications, and the wizard may
            // hold unsaved form state. noreferrer alongside noopener so the
            // target can't reach back through window.opener.
            target="_blank"
            rel="noopener noreferrer"
            title={tool.title}
          >
            {tool.label}
          </a>
        </li>
      ))}
      <li className="nav-item external-tool-divider" aria-hidden="true" />
    </>
  )
}

interface Crumb {
  label: string
  view: View
  params: NavParams
}

function shortenRunId(runId: string): string {
  if (!runId) return ''
  return runId.length > 30 ? runId.slice(0, 30) + '…' : runId
}

function buildCrumbs(view: View, dagId: string | null, runId: string | null, taskId: string | null): Crumb[] {
  const crumbs: Crumb[] = []
  if (view === 'home') {
    crumbs.push({ label: 'Home', view: 'home', params: {} })
  } else if (view === 'datasets') {
    crumbs.push({ label: 'Datasets', view: 'datasets', params: {} })
  } else if (view === 'dataset-creator') {
    crumbs.push({ label: 'Datasets', view: 'datasets', params: {} })
    crumbs.push({ label: 'Add Dataset', view: 'dataset-creator', params: {} })
  } else if (view === 'services') {
    crumbs.push({ label: 'Services', view: 'services', params: {} })
  } else if (view === 'all-tasks') {
    crumbs.push({ label: 'Tasks', view: 'all-tasks', params: {} })
  } else if (view === 'dag-builder') {
    crumbs.push({ label: 'Tasks', view: 'all-tasks', params: {} })
    crumbs.push({ label: 'Build DAG', view: 'dag-builder', params: {} })
  } else if (view === 'task-creator') {
    crumbs.push({ label: 'Tasks', view: 'all-tasks', params: {} })
    crumbs.push({ label: dagId ? 'Edit Task' : 'Create Task', view: 'task-creator', params: {} })
  } else {
    crumbs.push({ label: 'DAGs', view: 'dags', params: {} })
    if (dagId) {
      crumbs.push({ label: dagId, view: 'runs', params: { dagId } })
    }
    if (view === 'dag-tasks') {
      crumbs.push({ label: 'Tasks', view: 'dag-tasks', params: { dagId: dagId ?? '' } })
    }
    if (runId) {
      crumbs.push({ label: shortenRunId(runId), view: 'tasks', params: { dagId: dagId ?? '', runId } })
    }
    if (taskId) {
      crumbs.push({ label: taskId, view: 'logs', params: { dagId: dagId ?? '', runId: runId ?? '', taskId } })
    }
  }
  return crumbs
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
  const activeTopView: View =
    view === 'home' ? 'home'
    : ['all-tasks', 'dag-builder', 'task-creator'].includes(view) ? 'all-tasks'
    : view === 'dataset-creator' ? 'dataset-creator'
    : view === 'datasets' ? 'datasets'
    : view === 'services' ? 'services'
    : 'dags'

  const crumbs = buildCrumbs(view, dagId, runId, taskId)
  const pageLabel = crumbs.length > 0 ? crumbs[crumbs.length - 1].label : ''

  return (
    <div className="app-wrapper">
      {/* Header / navbar */}
      <nav className="app-header navbar navbar-expand bg-body">
        <div className="container-fluid">
          <ul className="navbar-nav">
            <li className="nav-item">
              {/* A <button>, not an <a href="#">: the anchor's default action
                  cleared the location hash and navigated away (see
                  toggleSidebar). data-lte-toggle is deliberately omitted so
                  AdminLTE cannot also bind here and double-toggle. */}
              <button
                type="button"
                className="nav-link border-0 bg-transparent"
                onClick={toggleSidebar}
                aria-label="Toggle sidebar"
              >
                <FiMenu />
              </button>
            </li>
          </ul>
          <ul className="navbar-nav ms-auto align-items-center">
            <ExternalToolLinks />
            <li className="nav-item">
              <span className="navbar-text text-muted small d-inline-flex align-items-center gap-1 me-2">
                <FiUser />
                {keycloak.tokenParsed?.preferred_username ?? keycloak.tokenParsed?.name ?? 'user'}
              </span>
            </li>
            <li className="nav-item">
              <button
                type="button"
                className="btn btn-sm btn-outline-secondary d-inline-flex align-items-center gap-1"
                onClick={() => keycloak.logout({ redirectUri: redirectUri() })}
              >
                <FiLogOut />
                Logout
              </button>
            </li>
          </ul>
        </div>
      </nav>

      {/* Sidebar */}
      <aside className="app-sidebar bg-body-secondary shadow" data-bs-theme="dark">
        <div className="sidebar-brand">
          <a
            href="#/home"
            className="brand-link"
            onClick={e => { e.preventDefault(); onNavigate('home', {}) }}
          >
            <span className="brand-text fw-light">
              6G-<span className="dali-accent">DALI</span> DataOps
            </span>
          </a>
        </div>
        <div className="sidebar-wrapper">
          <nav className="mt-2">
            <ul className="nav sidebar-menu flex-column" role="menu">
              {NAV_ITEMS.map(item => {
                const Icon = item.icon
                const active = activeTopView === item.view
                return (
                  <li className="nav-item" key={item.view}>
                    <a
                      href={`#/${item.view}`}
                      className={`nav-link${active ? ' active' : ''}`}
                      onClick={e => { e.preventDefault(); onNavigate(item.view, {}) }}
                    >
                      <Icon className="nav-icon" />
                      <p>{item.label}</p>
                    </a>
                  </li>
                )
              })}
            </ul>
          </nav>
        </div>
      </aside>

      {/* Main content */}
      <main className="app-main">
        <div className="app-content-header">
          <div className="container-fluid">
            <div className="row align-items-center">
              <div className="col-sm-6">
                <h1 className="mb-0 h4">{pageLabel}</h1>
              </div>
              <div className="col-sm-6">
                <ol className="breadcrumb float-sm-end mb-0">
                  {crumbs.map((crumb, i) => {
                    const isLast = i === crumbs.length - 1
                    return (
                      <li key={crumb.view + i} className={`breadcrumb-item${isLast ? ' active' : ''}`}>
                        {isLast ? (
                          crumb.label
                        ) : (
                          <a
                            href="#"
                            onClick={e => { e.preventDefault(); onNavigate(crumb.view, crumb.params) }}
                          >
                            {crumb.label}
                          </a>
                        )}
                      </li>
                    )
                  })}
                </ol>
              </div>
            </div>
          </div>
        </div>

        <div className="app-content">
          <div className="container-fluid">
            {children}
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="app-footer">
        <div className="float-end d-none d-sm-inline">6G-DALI</div>
        <strong>DataOps &mdash; Apache Airflow control plane.</strong>
      </footer>
    </div>
  )
}
