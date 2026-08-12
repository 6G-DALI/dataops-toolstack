import { useState, useEffect } from 'react'
import CodeEditor from './CodeEditor'
import { createTask, getCustomTask } from '../api/airflow'
import '../styles/Button.css'

const DEFAULT_CODE = `def run(**context):
    """Describe what this task does."""
    # Access DAG run config via context['dag_run'].conf
    # e.g. dataset_uri = context['dag_run'].conf.get('dataset_uri')
    pass
`

export default function TaskCreator({ editTaskId = null, onNavigate }) {
  const isEdit = !!editTaskId

  const [taskId, setTaskId] = useState(editTaskId || '')
  const [description, setDescription] = useState('')
  const [code, setCode] = useState(DEFAULT_CODE)
  const [loading, setLoading] = useState(isEdit)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    if (!isEdit) return
    getCustomTask(editTaskId)
      .then(task => {
        setCode(task.code || DEFAULT_CODE)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [editTaskId])

  async function handleSave() {
    if (!taskId.trim()) {
      setError('Task ID is required.')
      return
    }
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(taskId.trim())) {
      setError('Task ID must be a valid Python identifier (letters, digits, underscores).')
      return
    }
    setSaving(true)
    setError(null)
    setSaved(false)
    try {
      await createTask({ task_id: taskId.trim(), description, code })
      setSaved(true)
    } catch (e) {
      setError(e.message)
    } finally {
      setSaving(false)
    }
  }

  const fieldStyle = {
    width: '100%', padding: '8px 12px', border: '1px solid #dee2e6',
    borderRadius: '4px', fontSize: '13px', boxSizing: 'border-box',
  }

  if (loading) return <div style={{ padding: '40px', textAlign: 'center', color: '#888' }}>Loading task…</div>

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
        <div>
          <h1 className="page-title" style={{ marginBottom: 0 }}>{isEdit ? 'Edit Task' : 'Create Task'}</h1>
          <p className="page-subtitle" style={{ marginTop: '4px' }}>
            Write a Python callable that will be used as an Airflow PythonOperator task.
          </p>
        </div>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button className="btn btn-secondary" onClick={() => onNavigate('all-tasks', {})}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={handleSave} disabled={saving}>
            {saving ? 'Saving…' : 'Save Task'}
          </button>
        </div>
      </div>

      {error && (
        <div style={{ background: '#fff3f3', border: '1px solid #f5c6cb', borderRadius: '4px', padding: '10px 14px', marginBottom: '16px', color: '#721c24', fontSize: '13px' }}>
          {error}
        </div>
      )}

      {saved && (
        <div style={{ background: '#f0fff4', border: '1px solid #b7ebc8', borderRadius: '4px', padding: '10px 14px', marginBottom: '16px', color: '#155724', fontSize: '13px' }}>
          Task <strong>{taskId}</strong> saved. You can now use it in the DAG builder.
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '12px', marginBottom: '16px' }}>
        <div>
          <label style={{ display: 'block', fontSize: '12px', fontWeight: 600, marginBottom: '4px', color: '#555' }}>
            Task ID <span style={{ color: '#e53e3e' }}>*</span>
          </label>
          <input
            style={{ ...fieldStyle, background: isEdit ? '#f8f9fa' : undefined }}
            placeholder="e.g. validate_schema"
            value={taskId}
            onChange={e => { setTaskId(e.target.value); setSaved(false) }}
            readOnly={isEdit}
          />
        </div>
        <div>
          <label style={{ display: 'block', fontSize: '12px', fontWeight: 600, marginBottom: '4px', color: '#555' }}>
            Description
          </label>
          <input
            style={fieldStyle}
            placeholder="Short description of what this task does"
            value={description}
            onChange={e => { setDescription(e.target.value); setSaved(false) }}
          />
        </div>
      </div>

      <label style={{ display: 'block', fontSize: '12px', fontWeight: 600, marginBottom: '6px', color: '#555' }}>
        Python Code
      </label>
      <CodeEditor value={code} onChange={v => { setCode(v); setSaved(false) }} height="480px" />

      <p style={{ fontSize: '12px', color: '#888', marginTop: '8px' }}>
        The function name must match the Task ID. Use <code>**context</code> to access Airflow context variables.
      </p>
    </div>
  )
}
