import { useEffect, useState } from 'react'
import { getDagRuns } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import StateBadge from './StateBadge'
import Pagination from './Pagination'
import type { DagRun, NavigateFn } from '../types'

function formatDate(iso: string | null | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

interface DagRunListProps {
  dagId: string
  onNavigate: NavigateFn
}

export default function DagRunList({ dagId, onNavigate }: DagRunListProps) {
  const [runs, setRuns] = useState<DagRun[]>([])
  const [total, setTotal] = useState(0)
  const [limit, setLimit] = useState(10)
  const [page, setPage] = useState(0) // 0-based
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    getDagRuns(dagId, limit, page * limit)
      .then(data => {
        setRuns(data.dag_runs || [])
        setTotal(data.total_entries ?? 0)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, limit, page])

  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <p className="text-muted small mb-2">DAG: <strong>{dagId}</strong></p>
      <div className="card">
        <div className="card-body p-0">
          {loading ? (
            <LoadingSpinner />
          ) : (
            <div className="table-responsive">
              <table className="table table-hover align-middle mb-0">
                <thead>
                  <tr>
                    <th>Run ID</th>
                    <th>Type</th>
                    <th>Execution Date</th>
                    <th>Start</th>
                    <th>End</th>
                    <th>State</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.length === 0 ? (
                    <tr><td colSpan={6} className="text-center text-muted py-4">No runs found</td></tr>
                  ) : runs.map(run => (
                    <tr
                      key={run.dag_run_id}
                      style={{ cursor: 'pointer' }}
                      onClick={() => onNavigate('tasks', { dagId, runId: run.dag_run_id })}
                    >
                      <td><code className="small">{run.dag_run_id}</code></td>
                      <td>{run.run_type || '—'}</td>
                      <td>{formatDate(run.execution_date)}</td>
                      <td>{formatDate(run.start_date)}</td>
                      <td>{formatDate(run.end_date)}</td>
                      <td><StateBadge state={run.state} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
        <div className="card-footer">
          <Pagination
            page={page + 1}
            pageSize={limit}
            total={total}
            pageSizeOptions={[10, 25, 50, 100, 200]}
            unit="runs"
            onPageChange={p => setPage(p - 1)}
            onPageSizeChange={size => { setLimit(size); setPage(0) }}
          />
        </div>
      </div>
    </div>
  )
}
