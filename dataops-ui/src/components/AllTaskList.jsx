import { useEffect, useState } from 'react'
import { getAllTasks } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Table.css'
import '../styles/Button.css'

export default function AllTaskList({ onNavigate }) {
  const [tasks, setTasks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [filter, setFilter] = useState('')

  useEffect(() => {
    getAllTasks()
      .then(data => setTasks(data.tasks || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  const visible = filter
    ? tasks.filter(t =>
        t.task_id.toLowerCase().includes(filter.toLowerCase()) ||
        t.dag_id.toLowerCase().includes(filter.toLowerCase())
      )
    : tasks

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px' }}>
        <div>
          <h1 className="page-title" style={{ marginBottom: 0 }}>All Tasks</h1>
          <p className="page-subtitle" style={{ marginTop: '4px' }}>
            {visible.length} of {tasks.length} task{tasks.length !== 1 ? 's' : ''}
          </p>
        </div>
        <button className="btn btn-primary" onClick={() => onNavigate('dag-builder', {})}>
          + Build DAG
        </button>
      </div>

      <input
        type="text"
        placeholder="Filter by task ID or DAG..."
        value={filter}
        onChange={e => setFilter(e.target.value)}
        style={{
          width: '100%', padding: '8px 12px', marginBottom: '12px',
          border: '1px solid #dee2e6', borderRadius: '4px', fontSize: '13px',
        }}
      />

      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Task ID</th>
              <th>DAG</th>
              <th>Type</th>
              <th>Owner</th>
              <th>Depends on Past</th>
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td colSpan={5} style={{ textAlign: 'center', color: '#888', padding: '24px' }}>
                  No tasks found
                </td>
              </tr>
            ) : visible.map((task, i) => (
              <tr key={`${task.dag_id}-${task.task_id}-${i}`}>
                <td><code>{task.task_id}</code></td>
                <td>
                  <a onClick={() => onNavigate('dag-tasks', { dagId: task.dag_id })}>
                    {task.dag_id}
                  </a>
                </td>
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
