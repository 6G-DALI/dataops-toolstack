import { useEffect, useState } from 'react'
import { getTaskInstances, getDagRun } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import TaskLog from './TaskLog'
import type { TaskInstance } from '../types'
import '../styles/TaskTimeline.css'

interface TaskInstanceListProps {
  dagId: string
  runId: string
}

interface Selection {
  taskId: string
  tryNumber: number
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

export default function TaskInstanceList({ dagId, runId }: TaskInstanceListProps) {
  const [tasks, setTasks] = useState<TaskInstance[]>([])
  const [conf, setConf] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<Selection | null>(null)

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
          return new Date(a.start_date).getTime() - new Date(b.start_date).getTime()
        })
        setTasks(sorted)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, runId])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  function handleSelect(task: TaskInstance) {
    setSelected(prev =>
      prev?.taskId === task.task_id ? null : { taskId: task.task_id, tryNumber: task.try_number || 1 }
    )
  }

  return (
    <div>
      <div className="run-header">
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

      <p className="text-muted">Task Instances</p>

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
                      {(task.try_number ?? 0) > 1 && (
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
