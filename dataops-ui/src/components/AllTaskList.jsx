import { useEffect, useState, useMemo } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  flexRender,
} from '@tanstack/react-table'
import { getAllTasks, getCustomTasks } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Table.css'
import '../styles/Button.css'

export default function AllTaskList({ onNavigate }) {
  const [tasks, setTasks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [globalFilter, setGlobalFilter] = useState('')
  const [sorting, setSorting] = useState([])

  useEffect(() => {
    Promise.all([getAllTasks(), getCustomTasks()])
      .then(([airflow, custom]) => {
        const customTasks = (custom.tasks || []).map(t => ({
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
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const columns = useMemo(() => [
    {
      accessorKey: 'task_id',
      header: 'Task ID',
      cell: info => <code title={info.getValue()}>{info.getValue()}</code>,
      size: 280,
    },
    {
      accessorKey: 'dag_id',
      header: 'DAG',
      cell: info => {
        const val = info.getValue()
        return val === 'custom'
          ? <span style={{ color: '#888', fontStyle: 'italic' }}>custom</span>
          : <a title={val} onClick={() => onNavigate('dag-tasks', { dagId: val })}>{val}</a>
      },
      size: 280,
    },
    {
      accessorKey: 'owner',
      header: 'Owner',
      size: 120,
    },
    {
      accessorKey: 'depends_on_past',
      header: 'Depends',
      cell: info => info.getValue() ? 'Yes' : 'No',
      size: 90,
    },
    {
      id: 'actions',
      header: '',
      cell: info => info.row.original.dag_id === 'custom' ? (
        <button
          className="btn btn-secondary"
          style={{ padding: '3px 10px', fontSize: '12px' }}
          onClick={() => onNavigate('task-creator', { dagId: info.row.original.task_id })}
        >
          Edit
        </button>
      ) : null,
      size: 80,
      enableSorting: false,
    },
  ], [onNavigate])

  const table = useReactTable({
    data: tasks,
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
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px' }}>
        <div>
          <h1 className="page-title" style={{ marginBottom: 0 }}>All Tasks</h1>
          <p className="page-subtitle" style={{ marginTop: '4px' }}>
            {totalFiltered} of {tasks.length} task{tasks.length !== 1 ? 's' : ''}
          </p>
        </div>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button className="btn btn-secondary" onClick={() => onNavigate('task-creator', {})}>
            + Create Task
          </button>
          <button className="btn btn-primary" onClick={() => onNavigate('dag-builder', {})}>
            + Build DAG
          </button>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px' }}>
        <input
          type="text"
          placeholder="Search tasks..."
          value={globalFilter}
          onChange={e => { setGlobalFilter(e.target.value); table.setPageIndex(0) }}
          style={{
            flex: 1, padding: '8px 12px',
            border: '1px solid #dee2e6', borderRadius: '4px', fontSize: '13px',
          }}
        />
        <select
          value={pageSize}
          onChange={e => table.setPageSize(Number(e.target.value))}
          style={{ padding: '8px', border: '1px solid #dee2e6', borderRadius: '4px', fontSize: '13px' }}
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
                      <span style={{ marginLeft: '4px', color: '#aaa' }}>
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
                  No tasks found
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
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: '12px', fontSize: '13px', color: '#555' }}>
          <span>
            Page {pageIndex + 1} of {table.getPageCount()} &mdash; {totalFiltered} tasks
          </span>
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
