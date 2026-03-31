import { useEffect, useState } from 'react'
import { getTaskLogs } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/TaskLog.css'

export default function TaskLog({ dagId, runId, taskId, tryNumber = 1 }) {
  const [log, setLog] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    getTaskLogs(dagId, runId, taskId, tryNumber)
      .then(text => setLog(text))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, runId, taskId, tryNumber])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">Task Log</h1>
      <p className="page-subtitle">
        Task: <strong>{taskId}</strong> &nbsp;|&nbsp; Try: {tryNumber}
      </p>
      <div className="log-container">
        <pre>{log || '(empty log)'}</pre>
      </div>
    </div>
  )
}
