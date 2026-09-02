import { useEffect, useState } from 'react'
import { FiBarChart2, FiChevronDown, FiChevronRight } from 'react-icons/fi'
import { getTaskInstances, getDagRun } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import TaskLog from './TaskLog'
import RunResults from './RunResults'
import type { TaskInstance } from '../types'
import '../styles/TaskTimeline.css'
import '../styles/RunResults.css'

interface TaskInstanceListProps {
  dagId: string
  runId: string
}

interface Selection {
  taskId: string
  tryNumber: number
}

/** The DAG whose runs publish a results view. Scoped deliberately: it is the
 *  one that uploads a remediated frame and a report to diff against, and since
 *  the processing DAG was merged into it, the merged quality report too. */
const RESULTS_DAG_ID = 'dali_dataspace_validate_dataset'

/**
 * The panel beside the task list has one slot and two occupants: a run's
 * results, shown by default, and a task's logs, shown while a task is
 * selected. They are alternatives rather than a stack because both want the
 * full height of a sticky pane, and reading a log is a different job from
 * reading the run's outcome — mixing them halves each.
 */
type Panel = 'results' | 'log'

const POLL_INTERVAL_MS = 3000
const TERMINAL_RUN_STATES = new Set(['success', 'failed'])

/**
 * Time only. Every task of a run happens within minutes of the others, so the
 * date repeated on each of eleven cards is noise that costs a line of height
 * per card. The full value is kept as the cell's title for anyone who wants it.
 *
 * Only the start is shown: with the duration beside it the end is arithmetic,
 * and a third timestamp pushed the meta row towards wrapping again.
 */
function formatTime(iso: string | null | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleTimeString()
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return ''
  return new Date(iso).toLocaleString()
}

export default function TaskInstanceList({ dagId, runId }: TaskInstanceListProps) {
  const [tasks, setTasks] = useState<TaskInstance[]>([])
  const [conf, setConf] = useState<Record<string, unknown> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<Selection | null>(null)
  const [confOpen, setConfOpen] = useState(false)
  // Results are fetched once on mount, so a run still in flight would keep
  // showing "no artifacts yet". Keying the panel on the run's state remounts it
  // when the run reaches a terminal state, which is when artifacts exist.
  const [runState, setRunState] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined

    async function load(isInitial: boolean) {
      let runState: string | null | undefined

      try {
        const run = await getDagRun(dagId, runId)
        if (cancelled) return
        const c = run.conf
        setConf(c && Object.keys(c).length > 0 ? c : null)
        runState = run.state
        setRunState(run.state ?? null)
      } catch {
        // conf/state is supplementary — don't let it block task polling
      }

      try {
        const data = await getTaskInstances(dagId, runId)
        if (cancelled) return
        const sorted = (data.task_instances || []).slice().sort((a, b) => {
          if (!a.start_date) return 1
          if (!b.start_date) return -1
          return new Date(a.start_date).getTime() - new Date(b.start_date).getTime()
        })
        setTasks(sorted)
        if (isInitial) setError(null)
      } catch (err) {
        if (isInitial) setError((err as Error).message)
        // on later polls, keep the last good view and just retry silently
      } finally {
        if (isInitial) setLoading(false)
      }

      if (cancelled) return
      if (runState && TERMINAL_RUN_STATES.has(runState)) return
      timer = setTimeout(() => load(false), POLL_INTERVAL_MS)
    }

    load(true)

    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
  }, [dagId, runId])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  function handleSelect(task: TaskInstance) {
    setSelected(prev =>
      prev?.taskId === task.task_id ? null : { taskId: task.task_id, tryNumber: task.try_number || 1 }
    )
  }

  const hasResults = dagId === RESULTS_DAG_ID
  const panel: Panel | null = selected ? 'log' : hasResults ? 'results' : null

  return (
    <div>
      <div className="run-header">
        {hasResults && (
          // Not a link to the standalone #/run-results page any more: results
          // live in the panel beside the tasks, so this only has to hand the
          // panel back from a log. The route still exists for deep links.
          <button
            type="button"
            className={`btn btn-sm run-results-link ${panel === 'results' ? 'btn-primary' : 'btn-outline-primary'}`}
            aria-pressed={panel === 'results'}
            onClick={() => setSelected(null)}
          >
            <FiBarChart2 aria-hidden="true" /> View results
          </button>
        )}
        {/* Only where there is no results panel to hold it. On the merged DAG
            the configuration is a tab beside the report's own, so showing it
            here as well would be the same content in two places. */}
        {conf && !hasResults && (
          <div className="run-conf">
            <button
              type="button"
              className="run-conf-toggle"
              aria-expanded={confOpen}
              onClick={() => setConfOpen(o => !o)}
            >
              {confOpen ? <FiChevronDown aria-hidden="true" /> : <FiChevronRight aria-hidden="true" />}
              <span className="run-conf-title">Configuration</span>
              <span className="run-conf-count">{Object.keys(conf).length}</span>
            </button>
            {confOpen && (
              <ul className="run-conf-list">
                {Object.entries(conf).map(([k, v]) => (
                  <li key={k} className="run-conf-row">
                    <span className="run-conf-key">{k}</span>
                    <span className="run-conf-val">{typeof v === 'object' ? JSON.stringify(v) : String(v)}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </div>

      <div className={`timeline-layout${panel ? ' timeline-layout--split' : ''}`}>
        <div className="timeline-column">
          <p className="text-muted">Task Instances</p>
          {tasks.length === 0 ? (
            <p className="timeline-empty">No tasks found.</p>
          ) : (
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
                        <span className="timeline-meta-item" title={formatDate(task.start_date)}>
                          <span className="timeline-meta-label">Start</span>
                          {formatTime(task.start_date)}
                        </span>
                        <span className="timeline-meta-item">
                          <span className="timeline-meta-label">Duration</span>
                          {task.duration != null ? `${task.duration.toFixed(2)}s` : '—'}
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
          )}
        </div>

        {panel === 'log' && selected && (
          <div className="timeline-log-panel">
            <TaskLog
              dagId={dagId}
              runId={runId}
              taskId={selected.taskId}
              tryNumber={selected.tryNumber}
              state={tasks.find(t => t.task_id === selected.taskId)?.state}
            />
          </div>
        )}

        {panel === 'results' && (
          <div className="timeline-results-panel">
            <RunResults key={runState ?? 'pending'} dagId={dagId} runId={runId} />
          </div>
        )}
      </div>
    </div>
  )
}
