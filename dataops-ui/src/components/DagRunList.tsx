import { useEffect, useState } from 'react'
import { formatDateTime } from '@6g-dali/ui-shell'
import { getDagRuns, getDatasets, getDistributions } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import StateBadge from './StateBadge'
import Pagination from './Pagination'
import type { DagRun, NavigateFn } from '../types'

function formatDuration(ms: number): string {
  const total = Math.max(0, Math.round(ms / 1000))
  if (total < 1) return '<1s'
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  const sec = total % 60
  if (h) return `${h}h ${m}m`
  if (m) return `${m}m ${sec}s`
  return `${sec}s`
}

// Finished runs show how long they took; a run still going shows its age so far.
function tookLabel(run: DagRun): string {
  if (!run.start_date) return 'not started'
  const start = Date.parse(run.start_date)
  if (run.end_date) return `took ${formatDuration(Date.parse(run.end_date) - start)}`
  return `running for ${formatDuration(Date.now() - start)}`
}

function confStr(run: DagRun, key: string): string | undefined {
  const v = run.conf?.[key]
  return typeof v === 'string' && v ? v : undefined
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

  // Readable names for the dataset / distribution each run was invoked with.
  // run.conf only holds ids; a failed lookup leaves the raw id showing.
  const [names, setNames] = useState<Record<string, string>>({})
  useEffect(() => {
    let cancelled = false
    const setName = (id: string, name?: string) => {
      if (name && !cancelled) setNames(prev => ({ ...prev, [id]: name }))
    }
    const catalogues = new Set<string | undefined>()
    const datasets = new Map<string, string | undefined>()
    for (const r of runs) {
      const ds = confStr(r, 'dataset_id')
      if (!ds) continue
      catalogues.add(confStr(r, 'catalogue_id'))
      if (confStr(r, 'asset_id')) datasets.set(ds, confStr(r, 'catalogue_id'))
    }
    catalogues.forEach(cat => {
      getDatasets(cat)
        .then(res => res.datasets.forEach(d => {
          setName(d.dataset_id ?? d.id, d.name)
          if (d.id) setName(d.id, d.name)
        }))
        .catch(() => { /* keep raw id */ })
    })
    datasets.forEach((cat, ds) => {
      getDistributions(ds, cat)
        .then(res => res.distributions.forEach(d => {
          const label = d.asset_title || d.name
          for (const id of [d.asset_id, d.id, d.distribution_id]) if (id) setName(id, label)
        }))
        .catch(() => { /* keep raw id */ })
    })
    return () => { cancelled = true }
  }, [runs])

  const showSubject = runs.some(r => confStr(r, 'dataset_id') || confStr(r, 'asset_id'))
  // conf.triggered_by is stamped by the orchestrator; runs from before that, or started
  // straight in Airflow, have none and show a dash.
  const showTrigger = runs.some(r => confStr(r, 'triggered_by'))
  const colCount = 3 + (showSubject ? 2 : 0) + (showTrigger ? 1 : 0)
  const label = (id?: string) => id ? (names[id] ?? id) : '—'

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
                    {showSubject && <th>Dataset</th>}
                    {showSubject && <th>Distribution</th>}
                    {showTrigger && <th>Triggered by</th>}
                    <th>Started</th>
                    <th>State</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.length === 0 ? (
                    <tr><td colSpan={colCount} className="text-center text-muted py-4">No runs found</td></tr>
                  ) : runs.map(run => (
                    <tr
                      key={run.dag_run_id}
                      style={{ cursor: 'pointer' }}
                      onClick={() => onNavigate('tasks', { dagId, runId: run.dag_run_id })}
                    >
                      <td><code className="small">{run.dag_run_id}</code></td>
                      {showSubject && (
                        <td title={confStr(run, 'dataset_id')}>{label(confStr(run, 'dataset_id'))}</td>
                      )}
                      {showSubject && (
                        <td title={confStr(run, 'asset_id')}>{label(confStr(run, 'asset_id'))}</td>
                      )}
                      {showTrigger && <td>{confStr(run, 'triggered_by') ?? '—'}</td>}
                      <td>
                        {formatDateTime(run.start_date ?? run.logical_date ?? run.execution_date)}
                        <div className="text-muted small">{tookLabel(run)}</div>
                      </td>
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
