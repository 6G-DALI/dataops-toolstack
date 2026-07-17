import { useState, useEffect } from 'react'
import Layout from './components/Layout'
import DagList from './components/DagList'
import DagRunList from './components/DagRunList'
import TaskInstanceList from './components/TaskInstanceList'
import TaskLog from './components/TaskLog'
import DagTaskList from './components/DagTaskList'
import AllTaskList from './components/AllTaskList'
import DagBuilder from './components/DagBuilder'
import DatasetList from './components/DatasetList'
import DatasetCreator from './components/DatasetCreator'
import ServiceList from './components/ServiceList'
import TaskCreator from './components/TaskCreator'
import HomePage from './components/HomePage'
import type { NavParams, View } from './types'
import './styles/App.css'

interface NavState {
  view: View
  dagId: string
  runId: string
  taskId: string
  tryNumber: number
}

function parseHash(): NavState {
  const hash = window.location.hash.replace(/^#\/?/, '')
  const parts = hash.split('/')
  const view = (parts[0] || 'dags') as View
  return {
    view,
    dagId: decodeURIComponent(parts[1] || ''),
    runId: decodeURIComponent(parts[2] || ''),
    taskId: decodeURIComponent(parts[3] || ''),
    tryNumber: parseInt(parts[4] || '1', 10) || 1,
  }
}

function buildHash(view: View, { dagId = '', runId = '', taskId = '', tryNumber = 1 }: NavParams = {}): string {
  const parts: string[] = [view]
  if (dagId) parts.push(encodeURIComponent(dagId))
  if (runId) parts.push(encodeURIComponent(runId))
  if (taskId) parts.push(encodeURIComponent(taskId))
  if (taskId && tryNumber) parts.push(String(tryNumber))
  return '#/' + parts.join('/')
}

export default function App() {
  const [nav, setNav] = useState<NavState>(parseHash)

  useEffect(() => {
    const onHashChange = () => setNav(parseHash())
    window.addEventListener('hashchange', onHashChange)
    return () => window.removeEventListener('hashchange', onHashChange)
  }, [])

  function navigate(view: View, params: NavParams = {}) {
    window.location.hash = buildHash(view, params)
  }

  const { view, dagId, runId, taskId, tryNumber } = nav

  return (
    <Layout
      view={view}
      dagId={dagId || null}
      runId={runId || null}
      taskId={taskId || null}
      onNavigate={navigate}
    >
      {view === 'home' && (
        <HomePage onNavigate={navigate} />
      )}
      {view === 'dags' && (
        <DagList onNavigate={navigate} />
      )}
      {view === 'runs' && dagId && (
        <DagRunList dagId={dagId} onNavigate={navigate} />
      )}
      {view === 'tasks' && dagId && runId && (
        <TaskInstanceList dagId={dagId} runId={runId} />
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
      {view === 'task-creator' && (
        <TaskCreator editTaskId={dagId || null} onNavigate={navigate} />
      )}
      {view === 'datasets' && (
        <DatasetList onNavigate={navigate} />
      )}
      {view === 'dataset-creator' && (
        <DatasetCreator onNavigate={navigate} />
      )}
      {view === 'services' && (
        <ServiceList />
      )}
    </Layout>
  )
}
