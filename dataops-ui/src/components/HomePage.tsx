import { useCallback, useEffect, useRef, useState } from 'react'
import { getStats } from '../api/airflow'
import StateBadge from './StateBadge'
import MetricCard from './ui/MetricCard'
import CopyableId from './ui/CopyableId'
import { FiAlertTriangle, FiPause, FiRefreshCw } from 'react-icons/fi'
import type { NavigateFn, Stats } from '../types'

/** Dashboard summary refresh interval — §19.3 recommends 15–30 s. */
const POLL_INTERVAL_MS = 30_000

function fmtDateTime(iso: string | null | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

function fmtTime(date: Date | null): string {
  if (!date) return '—'
  return date.toLocaleTimeString()
}

/**
 * Loads the dashboard stats and keeps them fresh.
 *
 * §19.3 / §20 behaviour:
 * - polls every 30 s, but only while the tab is visible;
 * - on a failed refresh the previous data stays on screen and is flagged
 *   stale, rather than being replaced by an error page;
 * - the time of the last *successful* update is always reported.
 */
function useStats() {
  const [data, setData] = useState<Stats | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const hasData = useRef(false)

  const load = useCallback(async () => {
    try {
      const stats = await getStats()
      setData(stats)
      setLastUpdated(new Date())
      setError(null)
      hasData.current = true
    } catch (err) {
      // Keep whatever is already on screen; only surface a hard error when
      // there is nothing to show.
      setError((err as Error)?.message || 'Could not reach the orchestrator.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()

    const timer = window.setInterval(() => {
      if (!document.hidden) load()
    }, POLL_INTERVAL_MS)

    // Refresh immediately when the tab becomes visible again, so the user is
    // not looking at data frozen since they switched away.
    const onVisible = () => {
      if (!document.hidden) load()
    }
    document.addEventListener('visibilitychange', onVisible)

    return () => {
      window.clearInterval(timer)
      document.removeEventListener('visibilitychange', onVisible)
    }
  }, [load])

  return { data, error, loading, lastUpdated, refresh: load, isStale: Boolean(error && data) }
}

function MetricSkeleton() {
  return (
    <div className="col-6 col-sm-3">
      <div className="card metric-card h-100">
        <div className="card-body">
          <div className="skeleton skeleton-label" />
          <div className="skeleton skeleton-metric" />
        </div>
      </div>
    </div>
  )
}

interface HomePageProps {
  onNavigate: NavigateFn
}

export default function HomePage({ onNavigate }: HomePageProps) {
  const { data: stats, error, loading, lastUpdated, refresh, isStale } = useStats()

  // Loading: skeletons that approximate the final layout, not a page-wide
  // spinner (§20).
  if (loading && !stats) {
    return (
      <div className="row g-3 mb-4" aria-busy="true">
        <MetricSkeleton />
        <MetricSkeleton />
        <MetricSkeleton />
        <MetricSkeleton />
      </div>
    )
  }

  // Error with nothing to fall back on (§20) — the previous version swallowed
  // the failure and rendered a blank page.
  if (error && !stats) {
    return (
      <div className="card state-panel" role="alert">
        <div className="card-body text-center">
          <FiAlertTriangle className="state-panel-icon text-danger" aria-hidden="true" />
          <h2 className="state-panel-title">Dashboard unavailable</h2>
          <p className="state-panel-text">{error}</p>
          <button type="button" className="btn btn-primary btn-sm" onClick={refresh}>
            <FiRefreshCw className="me-1" aria-hidden="true" /> Retry
          </button>
        </div>
      </div>
    )
  }

  const recentRuns = stats?.recent_runs ?? []

  return (
    <div>
      {/* §20 stale data: keep the last good values visible, say so, offer retry. */}
      {isStale && (
        <div className="stale-banner" role="status">
          <FiAlertTriangle aria-hidden="true" />
          <span>Showing the last successful update — refresh failed.</span>
          <button type="button" className="btn btn-sm btn-outline-secondary ms-auto" onClick={refresh}>
            Retry
          </button>
        </div>
      )}

      {/* Metrics. Values are passed through untouched: `?? 0` would report an
          absent figure as a real zero, which §13.2 forbids.
          Note: NavParams carries no filter fields yet, so these drill down to
          the unfiltered list. True filtered views need query-param routing
          (§12.1) and are part of the router migration. */}
      <div className="row g-3 mb-4">
        <MetricCard
          label="Total DAGs"
          value={stats?.dags?.total}
          onNavigate={() => onNavigate('dags', {})}
          navigateLabel="Total DAGs: view all DAGs"
        />
        <MetricCard
          label="Active DAGs"
          value={stats?.dags?.active}
          tone="positive"
          onNavigate={() => onNavigate('dags', {})}
          navigateLabel="Active DAGs: view all DAGs"
        />
        <MetricCard
          label="Paused DAGs"
          value={stats?.dags?.paused}
          tone="neutral"
          icon={<FiPause />}
          onNavigate={() => onNavigate('dags', {})}
          navigateLabel="Paused DAGs: view all DAGs"
        />
        <MetricCard
          label="Custom tasks"
          value={stats?.tasks?.custom}
          onNavigate={() => onNavigate('all-tasks', {})}
          navigateLabel="Custom tasks: view all tasks"
        />
      </div>

      <div className="card">
        <div className="card-header d-flex align-items-center justify-content-between">
          <span>Recent runs</span>
          <button
            type="button"
            className="btn btn-sm btn-outline-secondary"
            onClick={refresh}
            aria-label="Refresh dashboard"
          >
            <FiRefreshCw aria-hidden="true" />
          </button>
        </div>
        <div className="card-body p-0">
          {recentRuns.length === 0 ? (
            // §20 empty state: what is missing, why, and what to do next.
            <div className="state-panel-inline">
              <p className="state-panel-title">No runs yet</p>
              <p className="state-panel-text">
                Runs appear here once a DAG has been triggered. Open the DAG list to start one.
              </p>
              <button
                type="button"
                className="btn btn-sm btn-outline-secondary"
                onClick={() => onNavigate('dags', {})}
              >
                Go to DAGs
              </button>
            </div>
          ) : (
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
                  {recentRuns.map(run => (
                    <tr
                      key={`${run.dag_id}:${run.dag_run_id}`}
                      style={{ cursor: 'pointer' }}
                      onClick={() => onNavigate('tasks', { dagId: run.dag_id, runId: run.dag_run_id })}
                    >
                      <td className="fw-medium">{run.dag_id}</td>
                      <td><CopyableId value={run.dag_run_id} /></td>
                      <td><StateBadge state={run.state} /></td>
                      <td className="small text-secondary">{fmtDateTime(run.start_date)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* §13.2: always show the data timestamp. */}
      <p className="data-timestamp">
        Last updated {fmtTime(lastUpdated)} · refreshes every {POLL_INTERVAL_MS / 1000}s while this tab is visible
      </p>
    </div>
  )
}
