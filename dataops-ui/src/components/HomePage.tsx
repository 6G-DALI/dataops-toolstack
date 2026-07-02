import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { getStats } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import StateBadge from './StateBadge'
import { FiPause } from 'react-icons/fi'
import type { NavigateFn, Stats } from '../types'

function fmt(iso: string | null | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

interface StatCardProps {
  title: string
  value: number
  valueClass?: string
  icon?: ReactNode
  onClick?: () => void
}

function StatCard({ title, value, valueClass, icon, onClick }: StatCardProps) {
  return (
    <div className="col-6 col-sm-3">
      <div
        className="card text-center h-100"
        role={onClick ? 'button' : undefined}
        onClick={onClick}
        style={onClick ? { cursor: 'pointer' } : undefined}
      >
        <div className="card-body">
          <div className="text-muted small text-uppercase mb-1" style={{ letterSpacing: '0.05em' }}>{title}</div>
          <div className={`fs-3 fw-semibold ${valueClass ?? ''}`}>
            {icon}{value}
          </div>
        </div>
      </div>
    </div>
  )
}

interface HomePageProps {
  onNavigate: NavigateFn
}

export default function HomePage({ onNavigate }: HomePageProps) {
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getStats()
      .then(setStats)
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <LoadingSpinner />
  if (!stats) return null

  return (
    <div>
      <div className="row g-3 mb-4">
        <StatCard title="Total DAGs" value={stats.dags?.total ?? 0} onClick={() => onNavigate('dags', {})} />
        <StatCard title="Active DAGs" value={stats.dags?.active ?? 0} valueClass="text-success" onClick={() => onNavigate('dags', {})} />
        <StatCard title="Paused DAGs" value={stats.dags?.paused ?? 0} valueClass="text-muted" icon={<FiPause className="me-1" />} />
        <StatCard title="Custom Tasks" value={stats.tasks?.custom ?? 0} onClick={() => onNavigate('all-tasks', {})} />
      </div>

      {(stats.recent_runs || []).length > 0 && (
        <div className="card">
          <div className="card-header fw-semibold">Recent Runs</div>
          <div className="card-body p-0">
            <div className="table-responsive">
              <table className="table table-hover align-middle mb-0">
                <thead>
                  <tr>
                    <th>DAG</th>
                    <th>Run ID</th>
                    <th>State</th>
                    <th>Started</th>
                  </tr>
                </thead>
                <tbody>
                  {(stats.recent_runs ?? []).map((run, i) => (
                    <tr
                      key={i}
                      style={{ cursor: 'pointer' }}
                      onClick={() => onNavigate('tasks', { dagId: run.dag_id, runId: run.dag_run_id })}
                    >
                      <td className="fw-medium">{run.dag_id}</td>
                      <td>
                        <code className="small text-muted">
                          {run.dag_run_id.length > 40 ? run.dag_run_id.slice(0, 40) + '…' : run.dag_run_id}
                        </code>
                      </td>
                      <td><StateBadge state={run.state} /></td>
                      <td className="small text-muted">{fmt(run.start_date)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
