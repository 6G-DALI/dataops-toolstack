import { useEffect, useState, useMemo } from 'react'
import {
  useReactTable, getCoreRowModel, getFilteredRowModel,
  getPaginationRowModel, getSortedRowModel, flexRender,
} from '@tanstack/react-table'
import { getDags, patchDag, triggerDag, deleteDag } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import StateBadge from './StateBadge'
import TriggerModal from './TriggerModal'
import '../styles/Table.css'
import '../styles/Button.css'

export default function DagList({ onNavigate }) {
  const [dags, setDags] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [triggering, setTriggering] = useState({})
  const [triggered, setTriggered] = useState({})
  const [deleting, setDeleting] = useState({})
  const [triggerModalDagId, setTriggerModalDagId] = useState(null)
  const [globalFilter, setGlobalFilter] = useState('')
  const [sorting, setSorting] = useState([])

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

  const columns = useMemo(() => [
    {
      accessorKey: 'dag_id',
      header: 'DAG ID',
      cell: info => (
        <a onClick={() => onNavigate('runs', { dagId: info.getValue() })}>{info.getValue()}</a>
      ),
      size: 280,
    },
    {
      accessorFn: row => (row.owners || []).join(', '),
      id: 'owner',
      header: 'Owner',
      cell: info => info.getValue() || '—',
      size: 120,
    },
    {
      accessorKey: 'is_paused',
      header: 'Status',
      cell: info => <StateBadge state={info.getValue() ? 'paused' : 'active'} />,
      size: 100,
      enableGlobalFilter: false,
    },
    {
      id: 'actions',
      header: '',
      cell: info => {
        const dag = info.row.original
        return (
          <div className="table-actions">
            <button className="btn btn-secondary" style={{ padding: '3px 10px', fontSize: '12px' }}
              onClick={() => onNavigate('dag-tasks', { dagId: dag.dag_id })}>
              Tasks
            </button>
            <button
              className={`btn ${dag.is_paused ? 'btn-success' : 'btn-warning'}`}
              style={{ padding: '3px 10px', fontSize: '12px' }}
              onClick={() => handleTogglePause(dag)}>
              {dag.is_paused ? 'Unpause' : 'Pause'}
            </button>
            <button className="btn btn-primary" style={{ padding: '3px 10px', fontSize: '12px' }}
              onClick={() => setTriggerModalDagId(dag.dag_id)}
              disabled={triggering[dag.dag_id]}>
              {triggering[dag.dag_id] ? 'Triggering…' : triggered[dag.dag_id] ? 'Triggered!' : 'Trigger'}
            </button>
            <button className="btn btn-danger" style={{ padding: '3px 10px', fontSize: '12px' }}
              onClick={() => handleDelete(dag.dag_id)}
              disabled={deleting[dag.dag_id]}>
              {deleting[dag.dag_id] ? '…' : 'Delete'}
            </button>
          </div>
        )
      },
      size: 280,
      enableSorting: false,
    },
  ], [triggering, triggered, deleting, onNavigate])

  const table = useReactTable({
    data: dags,
    columns,
    state: { globalFilter, sorting },
    onGlobalFilterChange: setGlobalFilter,
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 10 } },
  })

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  const { pageIndex, pageSize } = table.getState().pagination
  const totalFiltered = table.getFilteredRowModel().rows.length

  return (
    <div>
      {triggerModalDagId && (
        <TriggerModal
          dagId={triggerModalDagId}
          onConfirm={conf => handleTrigger(triggerModalDagId, conf)}
          onCancel={() => setTriggerModalDagId(null)}
        />
      )}

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px' }}>
        <div>
          <h1 className="page-title" style={{ marginBottom: 0 }}>DAGs</h1>
          <p className="page-subtitle" style={{ marginTop: '4px' }}>
            {totalFiltered} of {dags.length} DAG{dags.length !== 1 ? 's' : ''}
          </p>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px' }}>
        <input
          type="text"
          placeholder="Search DAGs..."
          value={globalFilter}
          onChange={e => { setGlobalFilter(e.target.value); table.setPageIndex(0) }}
          style={{
            flex: 1, padding: '8px 12px',
            border: '1px solid var(--dali-gray)', borderRadius: '4px', fontSize: '13px',
          }}
        />
        <select
          value={pageSize}
          onChange={e => table.setPageSize(Number(e.target.value))}
          style={{ padding: '8px', border: '1px solid var(--dali-gray)', borderRadius: '4px', fontSize: '13px' }}
        >
          {[10, 25, 50].map(s => <option key={s} value={s}>Show {s}</option>)}
        </select>
      </div>

      <div className="table-wrapper">
        <table style={{ tableLayout: 'fixed' }}>
          <thead>
            {table.getHeaderGroups().map(hg => (
              <tr key={hg.id}>
                {hg.headers.map(header => (
                  <th
                    key={header.id}
                    style={{ width: header.getSize(), cursor: header.column.getCanSort() ? 'pointer' : 'default', userSelect: 'none' }}
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                    {header.column.getCanSort() && (
                      <span style={{ marginLeft: '4px', opacity: 0.6 }}>
                        {{ asc: '↑', desc: '↓' }[header.column.getIsSorted()] ?? '↕'}
                      </span>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.length === 0 ? (
              <tr>
                <td colSpan={columns.length} style={{ textAlign: 'center', color: '#888', padding: '24px' }}>
                  No DAGs found
                </td>
              </tr>
            ) : table.getRowModel().rows.map(row => (
              <tr key={row.id}>
                {row.getVisibleCells().map(cell => (
                  <td key={cell.id} style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {table.getPageCount() > 1 && (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: '12px', fontSize: '13px', color: 'var(--dali-muted)' }}>
          <span>Page {pageIndex + 1} of {table.getPageCount()} &mdash; {totalFiltered} DAGs</span>
          <div style={{ display: 'flex', gap: '6px' }}>
            <button className="btn btn-secondary" style={{ padding: '4px 10px' }} onClick={() => table.setPageIndex(0)} disabled={!table.getCanPreviousPage()}>«</button>
            <button className="btn btn-secondary" style={{ padding: '4px 10px' }} onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>‹ Prev</button>
            <button className="btn btn-secondary" style={{ padding: '4px 10px' }} onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>Next ›</button>
            <button className="btn btn-secondary" style={{ padding: '4px 10px' }} onClick={() => table.setPageIndex(table.getPageCount() - 1)} disabled={!table.getCanNextPage()}>»</button>
          </div>
        </div>
      )}
    </div>
  )
}
