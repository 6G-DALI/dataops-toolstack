import { useEffect, useState } from 'react'
import { getDags, patchDag, triggerDag, deleteDag } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import StateBadge from './StateBadge'
import TriggerModal from './TriggerModal'
import type { Dag, NavigateFn, TriggerConf } from '../types'

type BusyMap = Record<string, boolean>

interface DagListProps {
  onNavigate: NavigateFn
}

export default function DagList({ onNavigate }: DagListProps) {
  const [dags, setDags] = useState<Dag[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [triggering, setTriggering] = useState<BusyMap>({})
  const [triggered, setTriggered] = useState<BusyMap>({})
  const [deleting, setDeleting] = useState<BusyMap>({})
  const [triggerModalDagId, setTriggerModalDagId] = useState<string | null>(null)

  useEffect(() => {
    getDags()
      .then(data => setDags(data.dags || []))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  async function handleTogglePause(dag: Dag) {
    const newPaused = !dag.is_paused
    setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: newPaused } : d))
    try {
      await patchDag(dag.dag_id, newPaused)
    } catch (err) {
      setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: dag.is_paused } : d))
      setError((err as Error).message)
    }
  }

  async function handleTrigger(dagId: string, conf: TriggerConf) {
    setTriggerModalDagId(null)
    setTriggering(prev => ({ ...prev, [dagId]: true }))
    try {
      const run = await triggerDag(dagId, conf)
      setTriggered(prev => ({ ...prev, [dagId]: true }))
      setTimeout(() => setTriggered(prev => ({ ...prev, [dagId]: false })), 3000)
      onNavigate('tasks', { dagId, runId: run.dag_run_id })
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setTriggering(prev => ({ ...prev, [dagId]: false }))
    }
  }

  async function handleDelete(dagId: string) {
    if (!window.confirm(`Delete DAG "${dagId}"? This cannot be undone.`)) return
    setDeleting(prev => ({ ...prev, [dagId]: true }))
    try {
      await deleteDag(dagId)
      setDags(prev => prev.filter(d => d.dag_id !== dagId))
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setDeleting(prev => ({ ...prev, [dagId]: false }))
    }
  }

  if (error) return <ErrorMessage message={error} />
  if (loading) return <LoadingSpinner />

  return (
    <div>
      {triggerModalDagId && (
        <TriggerModal
          dagId={triggerModalDagId}
          onConfirm={conf => handleTrigger(triggerModalDagId, conf)}
          onCancel={() => setTriggerModalDagId(null)}
        />
      )}

      <div className="card">
        <div className="card-header d-flex align-items-center justify-content-between">
          <span className="fw-semibold">DAGs</span>
          <span className="text-muted small">{dags.length} DAG{dags.length !== 1 ? 's' : ''} found</span>
        </div>
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th>DAG ID</th>
                  <th>Owner</th>
                  <th>Status</th>
                  <th className="text-end">Actions</th>
                </tr>
              </thead>
              <tbody>
                {dags.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="text-center text-muted py-4">No DAGs found</td>
                  </tr>
                ) : dags.map(dag => (
                  <tr key={dag.dag_id}>
                    <td>
                      <a href="#" onClick={e => { e.preventDefault(); onNavigate('runs', { dagId: dag.dag_id }) }}>
                        {dag.dag_id}
                      </a>
                    </td>
                    <td>{(dag.owners || []).join(', ') || '—'}</td>
                    <td><StateBadge state={dag.is_paused ? 'paused' : 'active'} /></td>
                    <td className="text-end">
                      <div className="btn-group btn-group-sm" role="group">
                        <button className="btn btn-outline-secondary" onClick={() => onNavigate('dag-tasks', { dagId: dag.dag_id })}>
                          Tasks
                        </button>
                        <button
                          className={`btn ${dag.is_paused ? 'btn-primary' : 'btn-outline-primary'}`}
                          onClick={() => handleTogglePause(dag)}
                        >
                          {dag.is_paused ? 'Unpause' : 'Pause'}
                        </button>
                        <button
                          className="btn btn-primary"
                          disabled={triggering[dag.dag_id]}
                          onClick={() => setTriggerModalDagId(dag.dag_id)}
                        >
                          {triggering[dag.dag_id] && <span className="spinner-border spinner-border-sm me-1" />}
                          {triggered[dag.dag_id] ? 'Triggered!' : 'Trigger'}
                        </button>
                        <button
                          className="btn btn-danger"
                          disabled={deleting[dag.dag_id]}
                          onClick={() => handleDelete(dag.dag_id)}
                        >
                          {deleting[dag.dag_id] && <span className="spinner-border spinner-border-sm me-1" />}
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
