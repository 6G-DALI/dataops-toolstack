import { useEffect, useMemo, useState } from 'react'
import { getAllTasks, getCustomTasks } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Pagination from './Pagination'
import { FiSearch } from 'react-icons/fi'
import type { AllTask, NavigateFn } from '../types'

type SortKey = 'task_id' | 'dag_id'
type SortDir = 'asc' | 'desc'

interface AllTaskListProps {
  onNavigate: NavigateFn
}

export default function AllTaskList({ onNavigate }: AllTaskListProps) {
  const [tasks, setTasks] = useState<AllTask[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('task_id')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)

  useEffect(() => {
    Promise.all([getAllTasks(), getCustomTasks()])
      .then(([airflow, custom]) => {
        const customTasks: AllTask[] = (custom.tasks || []).map(t => ({
          task_id: t.task_id,
          dag_id: 'custom',
          task_type: 'PythonOperator',
          owner: '—',
          depends_on_past: false,
        }))
        const seen = new Set(customTasks.map(t => t.task_id))
        const merged = [...customTasks, ...(airflow.tasks || []).filter(t => !seen.has(t.task_id))]
        setTasks(merged)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    const list = q
      ? tasks.filter(t => t.task_id.toLowerCase().includes(q) || t.dag_id.toLowerCase().includes(q))
      : tasks
    const sorted = [...list].sort((a, b) => {
      const cmp = a[sortKey].localeCompare(b[sortKey])
      return sortDir === 'asc' ? cmp : -cmp
    })
    return sorted
  }, [tasks, search, sortKey, sortDir])

  const pageItems = filtered.slice((page - 1) * pageSize, page * pageSize)

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(key); setSortDir('asc') }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return null
    return <span className="ms-1 text-muted">{sortDir === 'asc' ? '▲' : '▼'}</span>
  }

  if (error) return <ErrorMessage message={error} />
  if (loading) return <LoadingSpinner />

  return (
    <div>
      <div className="d-flex flex-wrap align-items-center justify-content-between gap-2 mb-3">
        <span className="text-muted small">
          {filtered.length}{filtered.length !== tasks.length ? ` of ${tasks.length}` : ''} task{tasks.length !== 1 ? 's' : ''}
        </span>
        <div className="d-flex align-items-center gap-2">
          <div className="input-group input-group-sm" style={{ width: 260 }}>
            <span className="input-group-text"><FiSearch /></span>
            <input
              type="text"
              className="form-control"
              placeholder="Search by task ID or DAG…"
              value={search}
              onChange={e => { setSearch(e.target.value); setPage(1) }}
            />
          </div>
          <button className="btn btn-sm btn-outline-secondary" onClick={() => onNavigate('task-creator', {})}>
            + Create Task
          </button>
        </div>
      </div>

      <div className="card">
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th style={{ cursor: 'pointer' }} onClick={() => toggleSort('task_id')}>Task ID{sortIndicator('task_id')}</th>
                  <th style={{ cursor: 'pointer' }} onClick={() => toggleSort('dag_id')}>DAG{sortIndicator('dag_id')}</th>
                  <th style={{ width: 180 }}>Type</th>
                  <th style={{ width: 120 }}>Owner</th>
                  <th style={{ width: 140 }}>Depends on Past</th>
                  <th style={{ width: 80 }}></th>
                </tr>
              </thead>
              <tbody>
                {pageItems.length === 0 ? (
                  <tr><td colSpan={6} className="text-center text-muted py-4">No tasks found</td></tr>
                ) : pageItems.map((t, i) => (
                  <tr key={`${t.dag_id}-${t.task_id}-${i}`}>
                    <td><code className="small">{t.task_id}</code></td>
                    <td>
                      {t.dag_id === 'custom'
                        ? <span className="text-muted fst-italic">custom</span>
                        : <a href="#" onClick={e => { e.preventDefault(); onNavigate('dag-tasks', { dagId: t.dag_id }) }}>{t.dag_id}</a>}
                    </td>
                    <td>{t.task_type ? <span className="badge text-bg-light border">{t.task_type}</span> : '—'}</td>
                    <td>{t.owner || '—'}</td>
                    <td>
                      {t.depends_on_past
                        ? <span className="badge text-bg-warning">Yes</span>
                        : <span className="badge text-bg-light border">No</span>}
                    </td>
                    <td>
                      {t.dag_id === 'custom' && (
                        <button className="btn btn-sm btn-outline-secondary" onClick={() => onNavigate('task-creator', { taskId: t.task_id })}>
                          Edit
                        </button>
                      )}
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
            unit="tasks"
            onPageChange={setPage}
            onPageSizeChange={size => { setPageSize(size); setPage(1) }}
          />
        </div>
      </div>
    </div>
  )
}
