import { useEffect, useRef, useState } from 'react'
import { getCustomTask, createTask } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import { EditorView, basicSetup } from 'codemirror'
import { python } from '@codemirror/lang-python'
import type { NavigateFn } from '../types'

const DEFAULT_CODE = `def my_task(**context):
    """
    Implement your task logic here.
    The 'context' dict contains Airflow task instance metadata.
    """
    print("Hello from my_task!")
`

const TASK_ID_PATTERN = /^[a-z0-9_]+$/

interface TaskCreatorProps {
  editTaskId: string | null
  onNavigate: NavigateFn
}

export default function TaskCreator({ editTaskId, onNavigate }: TaskCreatorProps) {
  const [taskId, setTaskId] = useState('')
  const [description, setDescription] = useState('')
  const [taskIdError, setTaskIdError] = useState<string | null>(null)
  const [loading, setLoading] = useState(!!editTaskId)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState(false)
  const editorRef = useRef<HTMLDivElement>(null)
  const viewRef = useRef<EditorView | null>(null)

  useEffect(() => {
    const view = new EditorView({
      doc: DEFAULT_CODE,
      extensions: [basicSetup, python()],
      parent: editorRef.current ?? undefined,
    })
    viewRef.current = view
    return () => view.destroy()
  }, [])

  useEffect(() => {
    if (!editTaskId) return
    getCustomTask(editTaskId)
      .then(data => {
        setTaskId(data.task_id)
        setDescription('')
        const view = viewRef.current
        if (view) {
          view.dispatch({
            changes: { from: 0, to: view.state.doc.length, insert: data.code || '' },
          })
        }
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [editTaskId])

  function validateTaskId(): boolean {
    if (!taskId.trim()) { setTaskIdError('Task ID is required'); return false }
    if (!TASK_ID_PATTERN.test(taskId)) { setTaskIdError('Only lowercase letters, numbers and underscores'); return false }
    setTaskIdError(null)
    return true
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!validateTaskId()) return
    const code = viewRef.current?.state.doc.toString() ?? ''
    if (!code.trim()) return setError('Code cannot be empty.')
    setSaving(true)
    setError(null)
    try {
      await createTask({ task_id: taskId, description, code })
      setSuccess(true)
      setTimeout(() => onNavigate('all-tasks', {}), 1200)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setSaving(false)
    }
  }

  if (loading) return null
  if (error) return <ErrorMessage message={error} />

  return (
    <div style={{ maxWidth: 860 }}>
      <p className="text-muted">
        {editTaskId ? `Editing task: ${editTaskId}` : 'Define a new reusable Python task for use in DAG pipelines.'}
      </p>

      {success && (
        <div className="alert alert-success" role="alert">Task saved successfully. Redirecting…</div>
      )}

      <form onSubmit={handleSubmit}>
        <div className="mb-3">
          <label className="form-label" htmlFor="task_id">Task ID <span className="text-danger">*</span></label>
          <input
            id="task_id"
            type="text"
            className={`form-control${taskIdError ? ' is-invalid' : ''}`}
            placeholder="e.g. normalise_csv"
            value={taskId}
            disabled={!!editTaskId}
            onChange={e => { setTaskId(e.target.value); if (taskIdError) setTaskIdError(null) }}
            onBlur={validateTaskId}
          />
          {taskIdError && <div className="invalid-feedback">{taskIdError}</div>}
        </div>

        <div className="mb-3">
          <label className="form-label" htmlFor="description">Description</label>
          <input
            id="description"
            type="text"
            className="form-control"
            placeholder="What does this task do?"
            value={description}
            onChange={e => setDescription(e.target.value)}
          />
        </div>

        <div className="mb-3">
          <label className="form-label">Python Code <span className="text-danger">*</span></label>
          <div ref={editorRef} className="border rounded overflow-hidden" style={{ fontSize: 13 }} />
        </div>

        <div className="d-flex gap-2">
          <button type="submit" className="btn btn-primary" disabled={saving}>
            {saving && <span className="spinner-border spinner-border-sm me-1" />}
            {editTaskId ? 'Save Changes' : 'Create Task'}
          </button>
          <button type="button" className="btn btn-outline-secondary" onClick={() => onNavigate('all-tasks', {})}>
            Cancel
          </button>
        </div>
      </form>
    </div>
  )
}
