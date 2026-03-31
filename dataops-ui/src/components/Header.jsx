import '../styles/Header.css'

const NAV_ITEMS = [
  { label: 'Tasks',    view: 'all-tasks', params: {} },
  { label: 'DAGs',     view: 'dags',      params: {} },
  { label: 'Datasets', view: 'datasets',  params: {} },
]

export default function Header({ view, dagId, runId, taskId, onNavigate }) {
  const crumbs = []

  if (view === 'datasets') {
    crumbs.push({ label: 'Datasets', view: 'datasets', params: {} })
  } else if (view === 'all-tasks') {
    crumbs.push({ label: 'Tasks', view: 'all-tasks', params: {} })
  } else if (view === 'dag-builder') {
    crumbs.push({ label: 'Tasks', view: 'all-tasks', params: {} })
    crumbs.push({ label: 'Build DAG', view: 'dag-builder', params: {} })
  } else {
    crumbs.push({ label: 'DAGs', view: 'dags', params: {} })
    if (dagId) {
      crumbs.push({ label: dagId, view: 'runs', params: { dagId } })
    }
    if (view === 'dag-tasks') {
      crumbs.push({ label: 'Tasks', view: 'dag-tasks', params: { dagId } })
    }
    if (runId) {
      crumbs.push({ label: shortenRunId(runId), view: 'tasks', params: { dagId, runId } })
    }
    if (taskId) {
      crumbs.push({ label: taskId, view: 'logs', params: { dagId, runId, taskId } })
    }
  }

  const activeTopView = ['all-tasks', 'dag-builder'].includes(view) ? 'all-tasks'
    : view === 'datasets' ? 'datasets'
    : 'dags'

  return (
    <>
      <header className="header">
        <span className="header-title">Airflow DataOps</span>
        <nav className="header-nav">
          {NAV_ITEMS.map(item => (
            <button
              key={item.view}
              className={`nav-item${activeTopView === item.view ? ' nav-item-active' : ''}`}
              onClick={() => onNavigate(item.view, item.params)}
            >
              {item.label}
            </button>
          ))}
        </nav>
      </header>

      {crumbs.length > 1 && (
        <div className="breadcrumb-bar">
          {crumbs.map((crumb, i) => {
            const isLast = i === crumbs.length - 1
            return (
              <span key={crumb.view + i} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                {i > 0 && <span className="breadcrumb-sep">/</span>}
                <button
                  className={`breadcrumb-item${isLast ? ' active' : ''}`}
                  onClick={() => !isLast && onNavigate(crumb.view, crumb.params)}
                >
                  {crumb.label}
                </button>
              </span>
            )
          })}
        </div>
      )}
    </>
  )
}

function shortenRunId(runId) {
  if (!runId) return ''
  return runId.length > 30 ? runId.slice(0, 30) + '…' : runId
}
