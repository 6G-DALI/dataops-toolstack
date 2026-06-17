import { useEffect, useRef, useState } from 'react'
import { getCustomTask, createTask } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import { Button, Form, Input, Space, Alert } from 'antd'
import { EditorView, basicSetup } from 'codemirror'
import { python } from '@codemirror/lang-python'
import 'antd/dist/reset.css'

const DEFAULT_CODE = `def my_task(**context):
    """
    Implement your task logic here.
    The 'context' dict contains Airflow task instance metadata.
    """
    print("Hello from my_task!")
`

export default function TaskCreator({ editTaskId, onNavigate }) {
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(!!editTaskId)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(false)
  const editorRef = useRef(null)
  const viewRef = useRef(null)

  useEffect(() => {
    const view = new EditorView({
      doc: DEFAULT_CODE,
      extensions: [basicSetup, python()],
      parent: editorRef.current,
    })
    viewRef.current = view
    return () => view.destroy()
  }, [])

  useEffect(() => {
    if (!editTaskId) return
    getCustomTask(editTaskId)
      .then(data => {
        form.setFieldsValue({ task_id: data.task_id, description: '' })
        const view = viewRef.current
        if (view) {
          view.dispatch({
            changes: { from: 0, to: view.state.doc.length, insert: data.code || '' },
          })
        }
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [editTaskId])

  async function handleSubmit(values) {
    const code = viewRef.current?.state.doc.toString() ?? ''
    if (!code.trim()) return setError('Code cannot be empty.')
    setSaving(true)
    setError(null)
    try {
      await createTask({ task_id: values.task_id, description: values.description || '', code })
      setSuccess(true)
      setTimeout(() => onNavigate('all-tasks', {}), 1200)
    } catch (e) {
      setError(e.message)
    } finally {
      setSaving(false)
    }
  }

  if (loading) return null
  if (error) return <ErrorMessage message={error} />

  return (
    <div style={{ maxWidth: 860 }}>
      <h1 className="page-title">{editTaskId ? 'Edit Task' : 'Create Task'}</h1>
      <p className="page-subtitle">
        {editTaskId ? `Editing task: ${editTaskId}` : 'Define a new reusable Python task for use in DAG pipelines.'}
      </p>

      {success && <Alert type="success" message="Task saved successfully. Redirecting…" style={{ marginBottom: 16 }} />}

      <Form form={form} layout="vertical" onFinish={handleSubmit}>
        <Form.Item
          label="Task ID"
          name="task_id"
          rules={[{ required: true, message: 'Task ID is required' }, { pattern: /^[a-z0-9_]+$/, message: 'Only lowercase letters, numbers and underscores' }]}
        >
          <Input placeholder="e.g. normalise_csv" disabled={!!editTaskId} />
        </Form.Item>

        <Form.Item label="Description" name="description">
          <Input placeholder="What does this task do?" />
        </Form.Item>

        <Form.Item label="Python Code" required>
          <div
            ref={editorRef}
            style={{ border: '1px solid #d9d9d9', borderRadius: 6, overflow: 'hidden', fontSize: 13 }}
          />
        </Form.Item>

        <Form.Item>
          <Space>
            <Button type="primary" htmlType="submit" loading={saving}>
              {editTaskId ? 'Save Changes' : 'Create Task'}
            </Button>
            <Button onClick={() => onNavigate('all-tasks', {})}>Cancel</Button>
          </Space>
        </Form.Item>
      </Form>
    </div>
  )
}
