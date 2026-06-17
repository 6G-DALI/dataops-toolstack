import { useState, useEffect } from 'react'
import Header from './components/Header'
import DagList from './components/DagList'
import DagRunList from './components/DagRunList'
import TaskInstanceList from './components/TaskInstanceList'
import TaskLog from './components/TaskLog'
import DagTaskList from './components/DagTaskList'
import AllTaskList from './components/AllTaskList'
import DagBuilder from './components/DagBuilder'
import DatasetList from './components/DatasetList'
import ServiceList from './components/ServiceList'
import './styles/App.css'

function parseHash() {
  const hash = window.location.hash.replace(/^#\/?/, '')
  const parts = hash.split('/')
  const view = parts[0] || 'dags'
  return {
    view,
    dagId: decodeURIComponent(parts[1] || ''),
    runId: decodeURIComponent(parts[2] || ''),
    taskId: decodeURIComponent(parts[3] || ''),
    tryNumber: parseInt(parts[4] || '1', 10) || 1,
  }
}

function buildHash(view, { dagId = '', runId = '', taskId = '', tryNumber = 1 } = {}) {
  const parts = [view]
  if (dagId) parts.push(encodeURIComponent(dagId))
  if (runId) parts.push(encodeURIComponent(runId))
  if (taskId) parts.push(encodeURIComponent(taskId))
  if (taskId && tryNumber) parts.push(tryNumber)
  return '#/' + parts.join('/')
}

export default function App() {
  const [nav, setNav] = useState(parseHash)

  useEffect(() => {
    const onHashChange = () => setNav(parseHash())
    window.addEventListener('hashchange', onHashChange)
    return () => window.removeEventListener('hashchange', onHashChange)
  }, [])

  function navigate(view, params = {}) {
    window.location.hash = buildHash(view, params)
  }

  const { view, dagId, runId, taskId, tryNumber } = nav

  return (
    <div className="app-container">
      <Header
        view={view}
        dagId={dagId || null}
        runId={runId || null}
        taskId={taskId || null}
        onNavigate={navigate}
      />
      <main className="main-content">
        {view === 'dags' && (
          <DagList onNavigate={navigate} />
        )}
        {view === 'runs' && dagId && (
          <DagRunList dagId={dagId} onNavigate={navigate} />
        )}
        {view === 'tasks' && dagId && runId && (
          <TaskInstanceList dagId={dagId} runId={runId} onNavigate={navigate} />
        )}
        {view === 'logs' && dagId && runId && taskId && (
          <TaskLog dagId={dagId} runId={runId} taskId={taskId} tryNumber={tryNumber} />
        )}
        {view === 'dag-tasks' && dagId && (
          <DagTaskList dagId={dagId} />
        )}
        {view === 'all-tasks' && (
          <AllTaskList onNavigate={navigate} />
        )}
        {view === 'dag-builder' && (
          <DagBuilder onNavigate={navigate} />
        )}
        {view === 'datasets' && (
          <DatasetList />
        )}
        {view === 'services' && (
          <ServiceList />
        )}
      </main>
    </div>
  )
}
