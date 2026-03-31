import { useEffect, useState } from 'react'
import { getDagRuns } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import '../styles/Table.css'

export default function DagRunList({ dagId, onNavigate }) {
  const [runs, setRuns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    getDagRuns(dagId)
      .then(data => setRuns(data.dag_runs || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">DAG Runs</h1>
      <p className="page-subtitle">
        DAG: <strong>{dagId}</strong> — {runs.length} run{runs.length !== 1 ? 's' : ''}
      </p>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Type</th>
              <th>Execution Date</th>
              <th>Start</th>
              <th>End</th>
              <th>State</th>
            </tr>
          </thead>
          <tbody>
            {runs.length === 0 ? (
              <tr><td colSpan={6} style={{ textAlign: 'center', color: '#888', padding: '24px' }}>No runs found</td></tr>
            ) : runs.map(run => (
              <tr
                key={run.dag_run_id}
                className="clickable-row"
                onClick={() => onNavigate('tasks', { dagId, runId: run.dag_run_id })}
              >
                <td style={{ fontFamily: 'monospace', fontSize: '12px' }}>{run.dag_run_id}</td>
                <td>{run.run_type}</td>
                <td>{formatDate(run.execution_date)}</td>
                <td>{formatDate(run.start_date)}</td>
                <td>{formatDate(run.end_date)}</td>
                <td><StateBadge state={run.state} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function formatDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}
