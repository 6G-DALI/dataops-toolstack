import { useEffect, useState, useMemo, Fragment } from 'react'
import type { ReactNode } from 'react'
import { getDatasets } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Modal from './Modal'
import Pagination from './Pagination'
import { FiFile, FiSearch, FiChevronRight, FiChevronDown } from 'react-icons/fi'
import { BsFiletypeCsv, BsFiletypePdf, BsFiletypeJson, BsFiletypeXml, BsFiletypeTxt } from 'react-icons/bs'
import type { Dataset } from '../types'

const FORMAT_ICONS: Record<string, ReactNode> = {
  csv:  <BsFiletypeCsv size={14} />,
  pdf:  <BsFiletypePdf size={14} />,
  json: <BsFiletypeJson size={14} />,
  xml:  <BsFiletypeXml size={14} />,
  txt:  <BsFiletypeTxt size={14} />,
}

const FORMAT_COLORS: Record<string, string> = {
  csv: 'text-bg-success',
  pdf: 'text-bg-danger',
  json: 'text-bg-primary',
  xml: 'text-bg-warning',
  txt: 'text-bg-secondary',
}

interface FormatTagProps {
  format?: string
}

function FormatTag({ format }: FormatTagProps) {
  if (!format) return <>—</>
  const key = format.toLowerCase().trim()
  return (
    <span className={`badge d-inline-flex align-items-center gap-1 ${FORMAT_COLORS[key] ?? 'text-bg-secondary'}`}>
      {FORMAT_ICONS[key] ?? <FiFile size={14} />}
      {format.toUpperCase()}
    </span>
  )
}

interface TagRowProps {
  label: string
  children: ReactNode
}

function TagRow({ label, children }: TagRowProps) {
  return (
    <div className="d-flex flex-wrap align-items-baseline gap-1">
      <span className="text-uppercase text-muted fw-semibold small me-2" style={{ letterSpacing: '0.05em' }}>
        {label}
      </span>
      {children}
    </div>
  )
}

interface ExpandedRowProps {
  ds: Dataset
}

function ExpandedRow({ ds }: ExpandedRowProps) {
  return (
    <div className="d-flex flex-column gap-2 px-3 py-2">
      {(ds.variable_measured || []).length > 0 && (
        <TagRow label="Variables">
          {(ds.variable_measured ?? []).slice().sort().map(v => (
            <span key={v} className="badge text-bg-info">{v}</span>
          ))}
        </TagRow>
      )}
      {(ds.producing_dags || []).length > 0 && (
        <TagRow label="Produced by">
          {(ds.producing_dags ?? []).map(d => (
            <span key={d.dag_id} className="badge text-bg-primary">{d.dag_id}</span>
          ))}
        </TagRow>
      )}
      {(ds.consuming_dags || []).length > 0 && (
        <TagRow label="Consumed by">
          {(ds.consuming_dags ?? []).map(d => (
            <span key={d.dag_id} className="badge text-bg-danger">{d.dag_id}</span>
          ))}
        </TagRow>
      )}
    </div>
  )
}

function isExpandable(ds: Dataset): boolean {
  return (
    (ds.variable_measured?.length ?? 0) > 0 ||
    (ds.producing_dags?.length ?? 0) > 0 ||
    (ds.consuming_dags?.length ?? 0) > 0
  )
}

export default function DatasetList() {
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [inspecting, setInspecting] = useState<Dataset | null>(null)
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const filtered = useMemo(() => {
    const q = search.toLowerCase().trim()
    const list = q
      ? datasets.filter(ds =>
          [ds.name, ds.asset_title, ds.sns_project_name, ds.catalog_title, ds.extra?.format]
            .some(v => v?.toLowerCase().includes(q))
        )
      : datasets
    return [...list].sort(
      (a, b) => new Date(b.updated_at ?? 0).getTime() - new Date(a.updated_at ?? 0).getTime()
    )
  }, [datasets, search])

  const pageItems = filtered.slice((page - 1) * pageSize, page * pageSize)

  function toggleExpand(id: string) {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  if (error) return <ErrorMessage message={error} />
  if (loading) return <LoadingSpinner />

  return (
    <div>
      <div className="d-flex flex-wrap align-items-center justify-content-between gap-2 mb-3">
        <span className="text-muted small">
          {filtered.length}{filtered.length !== datasets.length ? ` of ${datasets.length}` : ''} dataset{datasets.length !== 1 ? 's' : ''}
        </span>
        <div className="input-group input-group-sm" style={{ width: 260 }}>
          <span className="input-group-text"><FiSearch /></span>
          <input
            type="text"
            className="form-control"
            placeholder="Search datasets…"
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(1) }}
          />
        </div>
      </div>

      <div className="card">
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th style={{ width: 32 }}></th>
                  <th>Dataset</th>
                  <th>Asset</th>
                  <th style={{ width: 90 }}>Format</th>
                  <th style={{ width: 200 }}>License</th>
                  <th style={{ width: 160 }}>Last Updated</th>
                  <th style={{ width: 90 }}></th>
                </tr>
              </thead>
              <tbody>
                {pageItems.length === 0 ? (
                  <tr><td colSpan={7} className="text-center text-muted py-4">No datasets found</td></tr>
                ) : pageItems.map(ds => {
                  const expandable = isExpandable(ds)
                  const isOpen = expanded.has(ds.id)
                  return (
                    <Fragment key={ds.id}>
                      <tr>
                        <td className="text-center">
                          {expandable && (
                            <button
                              className="btn btn-sm btn-link p-0 text-muted"
                              onClick={() => toggleExpand(ds.id)}
                              aria-label={isOpen ? 'Collapse' : 'Expand'}
                            >
                              {isOpen ? <FiChevronDown /> : <FiChevronRight />}
                            </button>
                          )}
                        </td>
                        <td>
                          <div className="fw-semibold">{ds.name || '—'}</div>
                          {ds.sns_project_name && <div className="small text-muted">{ds.sns_project_name}</div>}
                          {(ds.catalog_title || ds.catalog_id) && (
                            <div className="small text-body-tertiary">
                              {ds.catalog_url
                                ? <a href={ds.catalog_url} target="_blank" rel="noreferrer">{ds.catalog_title || ds.catalog_id}</a>
                                : ds.catalog_title || ds.catalog_id}
                            </div>
                          )}
                        </td>
                        <td>
                          <div className="fw-medium">{ds.asset_title || '—'}</div>
                          {ds.asset_id && <code className="small text-muted">{ds.asset_id}</code>}
                        </td>
                        <td><FormatTag format={ds.extra?.format} /></td>
                        <td>
                          {ds.extra?.license
                            ? <span className="badge text-bg-light border text-truncate d-inline-block" style={{ maxWidth: 180 }} title={ds.extra.license}>{ds.extra.license}</span>
                            : '—'}
                        </td>
                        <td className="small">{ds.updated_at ? new Date(ds.updated_at).toLocaleString() : '—'}</td>
                        <td>
                          <button className="btn btn-sm btn-outline-secondary" onClick={() => setInspecting(ds)}>Inspect</button>
                        </td>
                      </tr>
                      {expandable && isOpen && (
                        <tr className="table-light">
                          <td></td>
                          <td colSpan={6}><ExpandedRow ds={ds} /></td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card-footer">
          <Pagination
            page={page}
            pageSize={pageSize}
            total={filtered.length}
            pageSizeOptions={[10, 20, 50]}
            unit="datasets"
            onPageChange={setPage}
            onPageSizeChange={size => { setPageSize(size); setPage(1) }}
          />
        </div>
      </div>

      {inspecting && (
        <Modal
          title={`Raw record — ${inspecting.name || inspecting.id}`}
          onClose={() => setInspecting(null)}
          width={780}
          footer={<button className="btn btn-secondary" onClick={() => setInspecting(null)}>Close</button>}
        >
          <pre
            className="mb-0 p-3 rounded border bg-body-tertiary"
            style={{ fontSize: '12px', lineHeight: 1.5, whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: '60vh', overflowY: 'auto' }}
          >
            {JSON.stringify(inspecting.raw, null, 2)}
          </pre>
        </Modal>
      )}
    </div>
  )
}
