import { useEffect, useMemo, useState } from 'react'
import { getDags, patchDag, triggerDag, deleteDag } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Pagination from './Pagination'
import StateBadge from './StateBadge'
import TriggerModal from './TriggerModal'
import { FiSearch } from 'react-icons/fi'
import type { Dag, NavigateFn, TriggerConf } from '../types'

type BusyMap = Record<string, boolean>
type SortKey = 'dag_id' | 'owners'
type SortDir = 'asc' | 'desc'

interface DagListProps {
  onNavigate: NavigateFn
}

export default function DagList({ onNavigate }: DagListProps) {
  const [dags, setDags] = useState<Dag[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [triggering, setTriggering] = useState<BusyMap>({})
  const [triggered, setTriggered] = useState<BusyMap>({})
  const [deleting, setDeleting] = useState<BusyMap>({})
  const [triggerModalDagId, setTriggerModalDagId] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('dag_id')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)

  useEffect(() => {
    getDags()
      .then(data => setDags(data.dags || []))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    const list = q
      ? dags.filter(d =>
          d.dag_id.toLowerCase().includes(q) ||
          (d.owners || []).some(o => o.toLowerCase().includes(q))
        )
      : dags
    const sorted = [...list].sort((a, b) => {
      const av = sortKey === 'owners' ? (a.owners || []).join(', ') : a.dag_id
      const bv = sortKey === 'owners' ? (b.owners || []).join(', ') : b.dag_id
      const cmp = av.localeCompare(bv)
      return sortDir === 'asc' ? cmp : -cmp
    })
    return sorted
  }, [dags, search, sortKey, sortDir])

  const pageItems = filtered.slice((page - 1) * pageSize, page * pageSize)

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(key); setSortDir('asc') }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return null
    return <span className="ms-1 text-muted">{sortDir === 'asc' ? '▲' : '▼'}</span>
  }

  async function handleTogglePause(dag: Dag) {
    const newPaused = !dag.is_paused
    setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: newPaused } : d))
    try {
      await patchDag(dag.dag_id, newPaused)
    } catch (err) {
      setDags(prev => prev.map(d => d.dag_id === dag.dag_id ? { ...d, is_paused: dag.is_paused } : d))
      setError((err as Error).message)
    }
  }

  async function handleTrigger(dagId: string, conf: TriggerConf) {
    setTriggerModalDagId(null)
    setTriggering(prev => ({ ...prev, [dagId]: true }))
    try {
      const run = await triggerDag(dagId, conf)
      setTriggered(prev => ({ ...prev, [dagId]: true }))
      setTimeout(() => setTriggered(prev => ({ ...prev, [dagId]: false })), 3000)
      onNavigate('tasks', { dagId, runId: run.dag_run_id })
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setTriggering(prev => ({ ...prev, [dagId]: false }))
    }
  }

  async function handleDelete(dagId: string) {
    if (!window.confirm(`Delete DAG "${dagId}"? This cannot be undone.`)) return
    setDeleting(prev => ({ ...prev, [dagId]: true }))
    try {
      await deleteDag(dagId)
      setDags(prev => prev.filter(d => d.dag_id !== dagId))
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setDeleting(prev => ({ ...prev, [dagId]: false }))
    }
  }

  if (error) return <ErrorMessage message={error} />
  if (loading) return <LoadingSpinner />

  return (
    <div>
      {triggerModalDagId && (
        <TriggerModal
          dagId={triggerModalDagId}
          onConfirm={conf => handleTrigger(triggerModalDagId, conf)}
          onCancel={() => setTriggerModalDagId(null)}
        />
      )}

      <div className="d-flex flex-wrap align-items-center justify-content-between gap-2 mb-3">
        <span className="text-muted small">
          {filtered.length}{filtered.length !== dags.length ? ` of ${dags.length}` : ''} DAG{dags.length !== 1 ? 's' : ''}
        </span>
        <div className="d-flex align-items-center gap-2">
          <div className="input-group input-group-sm" style={{ width: 260 }}>
            <span className="input-group-text"><FiSearch /></span>
            <input
              type="text"
              className="form-control"
              placeholder="Search by DAG ID or owner…"
              value={search}
              onChange={e => { setSearch(e.target.value); setPage(1) }}
            />
          </div>
          <button className="btn btn-sm btn-primary" onClick={() => onNavigate('dag-builder', {})}>
            + Build DAG
          </button>
        </div>
      </div>

      <div className="card">
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th style={{ cursor: 'pointer' }} onClick={() => toggleSort('dag_id')}>DAG ID{sortIndicator('dag_id')}</th>
                  <th style={{ cursor: 'pointer' }} onClick={() => toggleSort('owners')}>Owner{sortIndicator('owners')}</th>
                  <th>Status</th>
                  <th className="text-end">Actions</th>
                </tr>
              </thead>
              <tbody>
                {pageItems.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="text-center text-muted py-4">No DAGs found</td>
                  </tr>
                ) : pageItems.map(dag => (
                  <tr key={dag.dag_id}>
                    <td>
                      <a href="#" onClick={e => { e.preventDefault(); onNavigate('runs', { dagId: dag.dag_id }) }}>
                        {dag.dag_id}
                      </a>
                    </td>
                    <td>{(dag.owners || []).join(', ') || '—'}</td>
                    <td><StateBadge state={dag.is_paused ? 'paused' : 'active'} /></td>
                    <td className="text-end">
                      <div className="btn-group btn-group-sm" role="group">
                        <button className="btn btn-outline-secondary" onClick={() => onNavigate('dag-tasks', { dagId: dag.dag_id })}>
                          Tasks
                        </button>
                        <button
                          className={`btn ${dag.is_paused ? 'btn-primary' : 'btn-outline-primary'}`}
                          onClick={() => handleTogglePause(dag)}
                        >
                          {dag.is_paused ? 'Unpause' : 'Pause'}
                        </button>
                        <button
                          className="btn btn-primary"
                          disabled={triggering[dag.dag_id]}
                          onClick={() => setTriggerModalDagId(dag.dag_id)}
                        >
                          {triggering[dag.dag_id] && <span className="spinner-border spinner-border-sm me-1" />}
                          {triggered[dag.dag_id] ? 'Triggered!' : 'Trigger'}
                        </button>
                        <button
                          className="btn btn-danger"
                          disabled={deleting[dag.dag_id]}
                          onClick={() => handleDelete(dag.dag_id)}
                        >
                          {deleting[dag.dag_id] && <span className="spinner-border spinner-border-sm me-1" />}
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card-footer">
          <Pagination
            page={page}
            pageSize={pageSize}
            total={filtered.length}
            pageSizeOptions={[10, 25, 50, 100]}
            unit="DAGs"
            onPageChange={setPage}
            onPageSizeChange={size => { setPageSize(size); setPage(1) }}
          />
        </div>
      </div>
    </div>
  )
}