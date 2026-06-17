import { useEffect, useState } from 'react'
import { getDagRuns } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import { Table, Pagination } from 'antd'
import 'antd/dist/reset.css'

const COLUMNS = [
  {
    title: 'Run ID',
    dataIndex: 'dag_run_id',
    key: 'dag_run_id',
    render: v => <span style={{ fontFamily: 'monospace', fontSize: '12px' }}>{v}</span>,
  },
  {
    title: 'Type',
    dataIndex: 'run_type',
    key: 'run_type',
  },
  {
    title: 'Execution Date',
    dataIndex: 'execution_date',
    key: 'execution_date',
    render: formatDate,
  },
  {
    title: 'Start',
    dataIndex: 'start_date',
    key: 'start_date',
    render: formatDate,
  },
  {
    title: 'End',
    dataIndex: 'end_date',
    key: 'end_date',
    render: formatDate,
  },
  {
    title: 'State',
    dataIndex: 'state',
    key: 'state',
    render: state => <StateBadge state={state} />,
  },
]

export default function DagRunList({ dagId, onNavigate }) {
  const [runs, setRuns] = useState([])
  const [total, setTotal] = useState(0)
  const [limit, setLimit] = useState(10)
  const [page, setPage] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    getDagRuns(dagId, limit, page * limit)
      .then(data => {
        setRuns(data.dag_runs || [])
        setTotal(data.total_entries ?? 0)
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, limit, page])

  function handleLimitChange(newLimit) {
    setLimit(newLimit)
    setPage(0)
  }

  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">DAG Runs</h1>
      <p className="page-subtitle">DAG: <strong>{dagId}</strong></p>
      <Table
        rowKey="dag_run_id"
        columns={COLUMNS}
        dataSource={runs}
        loading={loading}
        pagination={false}
        onRow={run => ({ onClick: () => onNavigate('tasks', { dagId, runId: run.dag_run_id }), style: { cursor: 'pointer' } })}
        size="small"
        style={{ marginBottom: 16 }}
      />
      <Pagination
        current={page + 1}
        pageSize={limit}
        total={total}
        pageSizeOptions={[10, 25, 50, 100, 200]}
        showSizeChanger
        showTotal={(t, range) => `${range[0]}–${range[1]} of ${t} runs`}
        onChange={(p, size) => {
          if (size !== limit) handleLimitChange(size)
          else setPage(p - 1)
        }}
        size="small"
      />
    </div>
  )
}

function formatDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}
