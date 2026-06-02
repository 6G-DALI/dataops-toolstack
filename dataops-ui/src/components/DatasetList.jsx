import { useEffect, useState, useMemo } from 'react'
import {
  useReactTable, getCoreRowModel, getFilteredRowModel,
  getPaginationRowModel, getSortedRowModel, flexRender,
} from '@tanstack/react-table'
import { getDatasets } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Table.css'
import '../styles/Button.css'

export default function DatasetList() {
  const [datasets, setDatasets] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [globalFilter, setGlobalFilter] = useState('')
  const [sorting, setSorting] = useState([])

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const columns = useMemo(() => [
    {
      accessorKey: 'name',
      header: 'Name',
      cell: info => info.getValue() || '—',
      size: 180,
    },
    {
      accessorKey: 'sns_project_name',
      header: 'Project',
      cell: info => info.getValue() || '—',
      size: 120,
    },
    {
      accessorKey: 'uri',
      header: 'URI',
      cell: info => <code style={{ wordBreak: 'break-all', fontSize: '11px' }}>{info.getValue()}</code>,
      size: 220,
    },
    {
      accessorFn: row => row.extra?.format,
      id: 'format',
      header: 'Format',
      cell: info => info.getValue() || '—',
      size: 80,
    },
    {
      accessorFn: row => row.extra?.license,
      id: 'license',
      header: 'License',
      cell: info => info.getValue() || '—',
      size: 100,
    },
    {
      accessorFn: row => (row.producing_dags || []).map(d => d.dag_id).join(', '),
      id: 'produced_by',
      header: 'Produced by',
      cell: info => info.getValue() || '—',
      size: 120,
    },
    {
      accessorFn: row => (row.consuming_dags || []).map(d => d.dag_id).join(', '),
      id: 'consumed_by',
      header: 'Consumed by',
      cell: info => info.getValue() || '—',
      size: 120,
    },
    {
      accessorKey: 'updated_at',
      header: 'Last Updated',
      cell: info => info.getValue() ? new Date(info.getValue()).toLocaleDateString() : '—',
      size: 110,
    },
  ], [])

  const table = useReactTable({
    data: datasets,
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
          <h1 className="page-title" style={{ marginBottom: 0 }}>Datasets</h1>
          <p className="page-subtitle" style={{ marginTop: '4px' }}>
            {totalFiltered} of {datasets.length} dataset{datasets.length !== 1 ? 's' : ''}
          </p>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px' }}>
        <input
          type="text"
          placeholder="Search datasets..."
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
                  No datasets found
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
          <span>Page {pageIndex + 1} of {table.getPageCount()} &mdash; {totalFiltered} datasets</span>
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
