import { useEffect, useState } from 'react'
import { getDagTasks } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Table.css'

export default function DagTaskList({ dagId }) {
  const [tasks, setTasks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    getDagTasks(dagId)
      .then(data => setTasks(data.tasks || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">Tasks</h1>
      <p className="page-subtitle">
        DAG: <strong>{dagId}</strong> — {tasks.length} task{tasks.length !== 1 ? 's' : ''} defined
      </p>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Task ID</th>
              <th>Type</th>
              <th>Owner</th>
              <th>Depends on Past</th>
            </tr>
          </thead>
          <tbody>
            {tasks.length === 0 ? (
              <tr>
                <td colSpan={4} style={{ textAlign: 'center', color: '#888', padding: '24px' }}>
                  No tasks found
                </td>
              </tr>
            ) : tasks.map(task => (
              <tr key={task.task_id}>
                <td><code>{task.task_id}</code></td>
                <td>{task.task_type || '—'}</td>
                <td>{task.owner || '—'}</td>
                <td>{task.depends_on_past ? 'Yes' : 'No'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
