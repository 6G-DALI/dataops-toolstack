import { useEffect, useState } from 'react'
import { getDags, patchDag, triggerDag } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import TriggerModal from './TriggerModal'
import '../styles/Table.css'
import '../styles/Button.css'

export default function DagList({ onNavigate }) {
  const [dags, setDags] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [triggering, setTriggering] = useState({})
  const [triggered, setTriggered] = useState({})
  const [triggerModalDagId, setTriggerModalDagId] = useState(null)

  useEffect(() => {
    getDags()
      .then(data => setDags(data.dags || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  async function handleTogglePause(dag) {
    const newPaused = !dag.is_paused
    setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: newPaused } : d))
    try {
      await patchDag(dag.dag_id, newPaused)
    } catch (err) {
      setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: dag.is_paused } : d))
      setError(err.message)
    }
  }

  async function handleTrigger(dagId, conf) {
    setTriggerModalDagId(null)
    setTriggering(prev => ({ ...prev, [dagId]: true }))
    try {
      await triggerDag(dagId, conf)
      setTriggered(prev => ({ ...prev, [dagId]: true }))
      setTimeout(() => setTriggered(prev => ({ ...prev, [dagId]: false })), 3000)
    } catch (err) {
      setError(err.message)
    } finally {
      setTriggering(prev => ({ ...prev, [dagId]: false }))
    }
  }

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      {triggerModalDagId && (
        <TriggerModal
          dagId={triggerModalDagId}
          onConfirm={conf => handleTrigger(triggerModalDagId, conf)}
          onCancel={() => setTriggerModalDagId(null)}
        />
      )}
      <h1 className="page-title">DAGs</h1>
      <p className="page-subtitle">{dags.length} DAG{dags.length !== 1 ? 's' : ''} found</p>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>DAG ID</th>
              <th>Owner</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {dags.map(dag => (
              <tr key={dag.dag_id}>
                <td>
                  <a onClick={() => onNavigate('runs', { dagId: dag.dag_id })}>
                    {dag.dag_id}
                  </a>
                </td>
                <td>{(dag.owners || []).join(', ') || '—'}</td>
                <td>
                  <StateBadge state={dag.is_paused ? 'paused' : 'active'} />
                </td>
                <td>
                  <div className="table-actions">
                    <button
                      className="btn btn-secondary"
                      onClick={() => onNavigate('dag-tasks', { dagId: dag.dag_id })}
                    >
                      Tasks
                    </button>
                    <button
                      className={`btn ${dag.is_paused ? 'btn-success' : 'btn-warning'}`}
                      onClick={() => handleTogglePause(dag)}
                    >
                      {dag.is_paused ? 'Unpause' : 'Pause'}
                    </button>
                    <button
                      className="btn btn-primary"
                      onClick={() => setTriggerModalDagId(dag.dag_id)}
                      disabled={triggering[dag.dag_id]}
                    >
                      {triggering[dag.dag_id] ? 'Triggering…' : triggered[dag.dag_id] ? 'Triggered!' : 'Trigger'}
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
