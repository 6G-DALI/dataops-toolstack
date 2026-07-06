import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { getAllTasks, getCustomTasks, createDag } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import { FiRefreshCw } from 'react-icons/fi'
import type { AllTask, NavigateFn } from '../types'
import '../styles/DagBuilder.css'
import '../styles/Button.css'


const LOCKED_FIRST = 'download_dataset_edc'

const LOCKED_TASKS: AllTask[] = [
  { task_id: LOCKED_FIRST, dag_id: 'csv_pipeline', task_type: 'PythonOperator', locked: true },
]

interface DagBuilderProps {
  onNavigate: NavigateFn
}

export default function DagBuilder({ onNavigate }: DagBuilderProps) {
  const [availableTasks, setAvailableTasks] = useState<AllTask[]>([])
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [filter, setFilter] = useState('')
  // Pipeline always starts with the locked first task
  const [pipeline, setPipeline] = useState<AllTask[]>(LOCKED_TASKS)

  const [dagId, setDagId] = useState('')
  const [description, setDescription] = useState('')
  const [owner, setOwner] = useState('')

  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  const loadTasks = useCallback((isInitial: boolean) => {
    if (isInitial) setLoading(true)
    else setRefreshing(true)
    return Promise.all([getAllTasks(), getCustomTasks()])
      .then(([airflow, custom]) => {
        const airflowTasks = airflow.tasks || []
        const customTasks: AllTask[] = (custom.tasks || []).map(t => ({
          task_id: t.task_id,
          dag_id: 'custom',
          task_type: 'PythonOperator',
        }))
        // merge, custom tasks take precedence (deduplicate by task_id)
        const seen = new Set(customTasks.map(t => t.task_id))
        const merged = [...customTasks, ...airflowTasks.filter(t => !seen.has(t.task_id))]
        setAvailableTasks(merged)
        setError(null)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => {
        if (isInitial) setLoading(false)
        else setRefreshing(false)
      })
  }, [])

  useEffect(() => {
    loadTasks(true)
  }, [loadTasks])

  function addTask(task: AllTask) {
    setPipeline(prev => [...prev, { ...task, locked: false }])
  }

  function removeTask(index: number) {
    if (pipeline[index].locked) return
    setPipeline(prev => prev.filter((_, i) => i !== index))
  }

  function moveTask(index: number, direction: number) {
    if (pipeline[index].locked) return
    const swapWith = index + direction
    if (pipeline[swapWith]?.locked) return
    setPipeline(prev => {
      const next = [...prev]
      ;[next[index], next[swapWith]] = [next[swapWith], next[index]]
      return next
    })
  }

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setSubmitError(null)
    if (!dagId.trim()) return setSubmitError('DAG ID is required.')
    if (pipeline.length === 0) return setSubmitError('Add at least one task to the pipeline.')

    setSubmitting(true)
    try {
      await createDag({
        dag_id: dagId.trim(),
        description,
        schedule: 'None',
        task_ids: pipeline.map(t => t.task_id),
        owner: owner.trim() || 'airflow',
      })
      onNavigate('dags', {})
    } catch (err) {
      setSubmitError((err as Error).message)
    } finally {
      setSubmitting(false)
    }
  }

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  const libraryTasks = availableTasks.filter(t => t.task_id !== LOCKED_FIRST)

  const visible = filter
    ? libraryTasks.filter(t =>
        t.task_id.toLowerCase().includes(filter.toLowerCase()) ||
        t.dag_id.toLowerCase().includes(filter.toLowerCase())
      )
    : libraryTasks

  return (
    <div>
      <p className="text-muted">Pick tasks from the library and arrange them into a pipeline.</p>

      <div className="builder-layout">
        {/* Left: task library */}
        <div className="builder-panel">
          <div className="builder-panel-header d-flex align-items-center justify-content-between">
            <span>Task Library ({availableTasks.length})</span>
            <button
              type="button"
              className="icon-btn"
              disabled={refreshing}
              onClick={() => loadTasks(false)}
              title="Refresh task library"
            >
              <FiRefreshCw className={refreshing ? 'spin' : ''} />
            </button>
          </div>
          <div className="builder-task-filter">
            <input
              type="text"
              placeholder="Filter tasks..."
              value={filter}
              onChange={e => setFilter(e.target.value)}
            />
          </div>
          <div className="builder-task-list">
            {visible.length === 0 ? (
              <div style={{ padding: '24px', textAlign: 'center', color: '#aaa', fontSize: '13px' }}>
                No tasks match
              </div>
            ) : visible.map((task, i) => (
              <div className="builder-task-item" key={`${task.dag_id}-${task.task_id}-${i}`}>
                <div>
                  <code>{task.task_id}</code>
                  <div className="task-meta">{task.dag_id} · {task.task_type || 'PythonOperator'}</div>
                </div>
                <button className="btn btn-primary" style={{ padding: '3px 10px', fontSize: '12px' }} onClick={() => addTask(task)}>
                  + Add
                </button>
              </div>
            ))}
          </div>
        </div>

        {/* Right: pipeline */}
        <div className="builder-panel">
          <div className="builder-panel-header">Pipeline ({pipeline.length} tasks)</div>
          <div className="builder-task-list">
            {pipeline.map((task, i) => (
              <div className={`pipeline-item${task.locked ? ' pipeline-item-locked' : ''}`} key={i}>
                <div className="step-num">{i + 1}</div>
                <div style={{ flex: 1 }}>
                  <span className="task-label">{task.task_id}</span>
                  {task.locked && <span className="lock-badge">locked</span>}
                  <div className="task-source">{task.dag_id}</div>
                </div>
                <div className="pipeline-controls">
                  <button className="icon-btn" disabled={task.locked || i === 0 || pipeline[i - 1]?.locked} onClick={() => moveTask(i, -1)} title="Move up">↑</button>
                  <button className="icon-btn" disabled={task.locked || i === pipeline.length - 1 || pipeline[i + 1]?.locked} onClick={() => moveTask(i, 1)} title="Move down">↓</button>
                  <button className="icon-btn remove" disabled={task.locked} onClick={() => removeTask(i)} title="Remove">✕</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* DAG config form */}
      <form className="builder-form" onSubmit={handleSubmit}>
        <div className="builder-panel-header" style={{ margin: '-20px -20px 16px', padding: '10px 20px', borderRadius: '6px 6px 0 0' }}>
          DAG Configuration
        </div>
        <div className="form-group" style={{ marginBottom: '16px' }}>
          <label>DAG ID *</label>
          <input
            type="text"
            placeholder="my_new_dag"
            value={dagId}
            onChange={e => setDagId(e.target.value.replace(/\s+/g, '_'))}
            required
          />
        </div>
        <div className="form-group" style={{ marginBottom: '16px' }}>
          <label>Description</label>
          <input
            type="text"
            placeholder="What does this DAG do?"
            value={description}
            onChange={e => setDescription(e.target.value)}
          />
        </div>
        <div className="form-group" style={{ marginBottom: '16px' }}>
          <label>Owner</label>
          <input
            type="text"
            placeholder="airflow"
            value={owner}
            onChange={e => setOwner(e.target.value)}
          />
        </div>

        {submitError && <ErrorMessage message={submitError} />}

        <div className="builder-submit">
          <button type="button" className="btn btn-secondary" onClick={() => onNavigate('dags', {})}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={submitting}>
            {submitting ? 'Creating…' : 'Create DAG'}
          </button>
        </div>
      </form>
    </div>
  )
}
