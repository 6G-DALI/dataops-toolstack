import { useEffect, useState } from 'react'
import { getTaskInstances, getDagRun } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import TaskLog from './TaskLog'
import '../styles/TaskTimeline.css'

export default function TaskInstanceList({ dagId, runId }) {
  const [tasks, setTasks] = useState([])
  const [conf, setConf] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    getDagRun(dagId, runId)
      .then(run => {
        const c = run.conf
        setConf(c && Object.keys(c).length > 0 ? c : null)
      })
      .catch(() => {})

    getTaskInstances(dagId, runId)
      .then(data => {
        const sorted = (data.task_instances || []).slice().sort((a, b) => {
          if (!a.start_date) return 1
          if (!b.start_date) return -1
          return new Date(a.start_date) - new Date(b.start_date)
        })
        setTasks(sorted)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, runId])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  function handleSelect(task) {
    setSelected(prev =>
      prev?.taskId === task.task_id ? null : { taskId: task.task_id, tryNumber: task.try_number || 1 }
    )
  }

  return (
    <div>
      <div className="run-header">
        <h1 className="page-title">{runId}</h1>
        {conf && (
          <div className="run-conf">
            <span className="run-conf-title">Configuration</span>
            <ul className="run-conf-list">
              {Object.entries(conf).map(([k, v]) => (
                <li key={k} className="run-conf-row">
                  <span className="run-conf-key">{k}</span>
                  <span className="run-conf-val">{typeof v === 'object' ? JSON.stringify(v) : String(v)}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      <p className="page-subtitle">Task Instances</p>

      {tasks.length === 0 ? (
        <p className="timeline-empty">No tasks found.</p>
      ) : (
        <div className={`timeline-layout${selected ? ' timeline-layout--split' : ''}`}>
          <div className="timeline">
            {tasks.map((task, i) => {
              const isSelected = selected?.taskId === task.task_id
              return (
                <div key={task.task_id} className="timeline-item">
                  <div className="timeline-spine">
                    <div className={`timeline-dot state-${(task.state || 'default').toLowerCase()}`}>
                      {task.state === 'running' && <span className="timeline-dot-pulse" />}
                    </div>
                    {i < tasks.length - 1 && <div className="timeline-line" />}
                  </div>

                  <div
                    className={`timeline-card${isSelected ? ' timeline-card--selected' : ''}`}
                    onClick={() => handleSelect(task)}
                  >
                    <div className="timeline-card-header">
                      <span className="timeline-task-id">{task.task_id}</span>
                      <StateBadge state={task.state} />
                    </div>
                    <div className="timeline-card-meta">
                      <span className="timeline-meta-item">
                        <span className="timeline-meta-label">Start</span>
                        {formatDate(task.start_date)}
                      </span>
                      <span className="timeline-meta-item">
                        <span className="timeline-meta-label">Duration</span>
                        {task.duration != null ? `${task.duration.toFixed(2)}s` : '—'}
                      </span>
                      <span className="timeline-meta-item">
                        <span className="timeline-meta-label">End</span>
                            {formatDate(task.end_date)}
                      </span>
                      {task.try_number > 1 && (
                        <span className="timeline-meta-item">
                          <span className="timeline-meta-label">Try</span>
                          {task.try_number}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              )
            })}
          </div>

          {selected && (
            <div className="timeline-log-panel">
              <TaskLog
                dagId={dagId}
                runId={runId}
                taskId={selected.taskId}
                tryNumber={selected.tryNumber}
              />
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function formatDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}
