import { useEffect, useState } from 'react'
import { getTaskInstances } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import '../styles/Table.css'

export default function TaskInstanceList({ dagId, runId, onNavigate }) {
  const [tasks, setTasks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    getTaskInstances(dagId, runId)
      .then(data => setTasks(data.task_instances || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, runId])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">Task Instances</h1>
      <p className="page-subtitle">
        Run: <code style={{ fontSize: '12px' }}>{runId}</code>
      </p>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Task ID</th>
              <th>State</th>
              <th>Start</th>
              <th>End</th>
              <th>Duration (s)</th>
              <th>Try #</th>
            </tr>
          </thead>
          <tbody>
            {tasks.length === 0 ? (
              <tr><td colSpan={6} style={{ textAlign: 'center', color: '#888', padding: '24px' }}>No tasks found</td></tr>
            ) : tasks.map(task => (
              <tr
                key={task.task_id}
                className="clickable-row"
                onClick={() => onNavigate('logs', { dagId, runId, taskId: task.task_id, tryNumber: task.try_number || 1 })}
              >
                <td>{task.task_id}</td>
                <td><StateBadge state={task.state} /></td>
                <td>{formatDate(task.start_date)}</td>
                <td>{formatDate(task.end_date)}</td>
                <td>{task.duration != null ? task.duration.toFixed(2) : '—'}</td>
                <td>{task.try_number ?? '—'}</td>
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
