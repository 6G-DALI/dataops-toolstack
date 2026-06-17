import { useEffect, useState } from 'react'
import { getDags, patchDag, triggerDag, deleteDag } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import TriggerModal from './TriggerModal'
import { Table, Button, Space } from 'antd'
import 'antd/dist/reset.css'

export default function DagList({ onNavigate }) {
  const [dags, setDags] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [triggering, setTriggering] = useState({})
  const [triggered, setTriggered] = useState({})
  const [deleting, setDeleting] = useState({})
  const [triggerModalDagId, setTriggerModalDagId] = useState(null)

  useEffect(() => {
    getDags()
      .then(data => setDags(data.dags || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  async function handleTogglePause(dag) {
    const newPaused = !dag.is_paused
    setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: newPaused } : d))
    try {
      await patchDag(dag.dag_id, newPaused)
    } catch (err) {
      setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: dag.is_paused } : d))
      setError(err.message)
    }
  }

  async function handleTrigger(dagId, conf) {
    setTriggerModalDagId(null)
    setTriggering(prev => ({ ...prev, [dagId]: true }))
    try {
      await triggerDag(dagId, conf)
      setTriggered(prev => ({ ...prev, [dagId]: true }))
      setTimeout(() => setTriggered(prev => ({ ...prev, [dagId]: false })), 3000)
    } catch (err) {
      setError(err.message)
    } finally {
      setTriggering(prev => ({ ...prev, [dagId]: false }))
    }
  }

  async function handleDelete(dagId) {
    if (!window.confirm(`Delete DAG "${dagId}"? This cannot be undone.`)) return
    setDeleting(prev => ({ ...prev, [dagId]: true }))
    try {
      await deleteDag(dagId)
      setDags(prev => prev.filter(d => d.dag_id !== dagId))
    } catch (err) {
      setError(err.message)
    } finally {
      setDeleting(prev => ({ ...prev, [dagId]: false }))
    }
  }

  if (error) return <ErrorMessage message={error} />

  const columns = [
    {
      title: 'DAG ID',
      dataIndex: 'dag_id',
      key: 'dag_id',
      render: (id) => (
        <a onClick={() => onNavigate('runs', { dagId: id })} style={{ cursor: 'pointer' }}>{id}</a>
      ),
    },
    {
      title: 'Owner',
      dataIndex: 'owners',
      key: 'owners',
      render: (owners) => (owners || []).join(', ') || '—',
    },
    {
      title: 'Status',
      dataIndex: 'is_paused',
      key: 'status',
      render: (is_paused) => <StateBadge state={is_paused ? 'paused' : 'active'} />,
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_, dag) => (
        <Space>
          <Button size="small" onClick={() => onNavigate('dag-tasks', { dagId: dag.dag_id })}>
            Tasks
          </Button>
          <Button
            size="small"
            type={dag.is_paused ? 'primary' : 'default'}
            onClick={() => handleTogglePause(dag)}
          >
            {dag.is_paused ? 'Unpause' : 'Pause'}
          </Button>
          <Button
            size="small"
            type="primary"
            loading={triggering[dag.dag_id]}
            onClick={() => setTriggerModalDagId(dag.dag_id)}
          >
            {triggered[dag.dag_id] ? 'Triggered!' : 'Trigger'}
          </Button>
          <Button
            size="small"
            danger
            loading={deleting[dag.dag_id]}
            onClick={() => handleDelete(dag.dag_id)}
          >
            Delete
          </Button>
        </Space>
      ),
    },
  ]

  return (
    <div>
      {triggerModalDagId && (
        <TriggerModal
          dagId={triggerModalDagId}
          onConfirm={conf => handleTrigger(triggerModalDagId, conf)}
          onCancel={() => setTriggerModalDagId(null)}
        />
      )}
      <h1 className="page-title">DAGs</h1>
      <p className="page-subtitle">{dags.length} DAG{dags.length !== 1 ? 's' : ''} found</p>
      <Table
        rowKey="dag_id"
        columns={columns}
        dataSource={dags}
        loading={loading}
        pagination={false}
        size="small"
      />
    </div>
  )
}
