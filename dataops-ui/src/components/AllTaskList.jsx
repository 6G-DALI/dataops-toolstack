import { useEffect, useState } from 'react'
import { getAllTasks } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import { Table, Button, Tag, Input, Space } from 'antd'
import { SearchOutlined } from '@ant-design/icons'
import 'antd/dist/reset.css'

const COLUMNS = (onNavigate) => [
  {
    title: 'Task ID',
    dataIndex: 'task_id',
    key: 'task_id',
    render: v => <code style={{ fontSize: '12px' }}>{v}</code>,
    sorter: (a, b) => a.task_id.localeCompare(b.task_id),
  },
  {
    title: 'DAG',
    dataIndex: 'dag_id',
    key: 'dag_id',
    render: (id) => (
      <a onClick={() => onNavigate('dag-tasks', { dagId: id })} style={{ cursor: 'pointer' }}>{id}</a>
    ),
    sorter: (a, b) => a.dag_id.localeCompare(b.dag_id),
  },
  {
    title: 'Type',
    dataIndex: 'task_type',
    key: 'task_type',
    width: 180,
    render: v => v ? <Tag>{v}</Tag> : '—',
  },
  {
    title: 'Owner',
    dataIndex: 'owner',
    key: 'owner',
    width: 120,
    render: v => v || '—',
  },
  {
    title: 'Depends on Past',
    dataIndex: 'depends_on_past',
    key: 'depends_on_past',
    width: 140,
    render: v => v ? <Tag color="orange">Yes</Tag> : <Tag color="default">No</Tag>,
  },
]

export default function AllTaskList({ onNavigate }) {
  const [tasks, setTasks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [search, setSearch] = useState('')

  useEffect(() => {
    getAllTasks()
      .then(data => setTasks(data.tasks || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  if (error) return <ErrorMessage message={error} />

  const visible = search
    ? tasks.filter(t =>
        t.task_id.toLowerCase().includes(search.toLowerCase()) ||
        t.dag_id.toLowerCase().includes(search.toLowerCase())
      )
    : tasks

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h1 className="page-title" style={{ marginBottom: 0 }}>All Tasks</h1>
          <p className="page-subtitle" style={{ margin: '4px 0 0' }}>
            {visible.length}{visible.length !== tasks.length ? ` of ${tasks.length}` : ''} task{tasks.length !== 1 ? 's' : ''}
          </p>
        </div>
        <Space>
          <Input
            placeholder="Search by task ID or DAG…"
            prefix={<SearchOutlined />}
            value={search}
            onChange={e => setSearch(e.target.value)}
            allowClear
            style={{ width: 260 }}
          />
          <Button type="primary" onClick={() => onNavigate('dag-builder', {})}>
            + Build DAG
          </Button>
        </Space>
      </div>

      <Table
        rowKey={(t, i) => `${t.dag_id}-${t.task_id}-${i}`}
        columns={COLUMNS(onNavigate)}
        dataSource={visible}
        loading={loading}
        pagination={{ pageSize: 25, showSizeChanger: true, pageSizeOptions: [10, 25, 50, 100] }}
        size="small"
      />
    </div>
  )
}
