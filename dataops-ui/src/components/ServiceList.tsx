import { useEffect, useMemo, useState } from 'react'
import { getServices, registerAllServices, registerService, deregisterService } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Pagination from './Pagination'
import { FiSearch } from 'react-icons/fi'
import type { Service } from '../types'

type BusyMap = Record<string, boolean>
type ResultMap = Record<string, string>

const SERVICE_TYPE_COLORS: Record<string, string> = {
  QualityCheck:          'text-bg-success',
  Transformation:        'text-bg-primary',
  Augmentation:          'text-bg-info',
  Aggregation:           'text-bg-warning',
  Anonymisation:         'text-bg-danger',
  'Feature Engineering': 'text-bg-secondary',
}

function resultClass(status: string): string {
  if (status.startsWith('error')) return 'text-bg-danger'
  if (status === 'registered') return 'text-bg-success'
  return 'text-bg-secondary'
}

export default function ServiceList() {
  const [services, setServices] = useState<Service[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [registering, setRegistering] = useState<BusyMap>({})
  const [results, setResults] = useState<ResultMap>({})
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)

  useEffect(() => {
    getServices()
      .then(data => setServices(data.services || []))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  async function handleRegisterAll() {
    setRegistering(prev => ({ ...prev, _all: true }))
    try {
      const data = await registerAllServices()
      const map: ResultMap = {}
      for (const r of data.results || []) {
        map[r.service_id] = r.status === 'registered' ? 'registered' : `error: ${r.detail}`
      }
      setResults(map)
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setRegistering(prev => ({ ...prev, _all: false }))
    }
  }

  async function handleRegister(serviceId: string) {
    setRegistering(prev => ({ ...prev, [serviceId]: true }))
    try {
      await registerService(serviceId)
      setResults(prev => ({ ...prev, [serviceId]: 'registered' }))
    } catch (e) {
      setResults(prev => ({ ...prev, [serviceId]: `error: ${(e as Error).message}` }))
    } finally {
      setRegistering(prev => ({ ...prev, [serviceId]: false }))
    }
  }

  async function handleDeregister(serviceId: string) {
    setRegistering(prev => ({ ...prev, [serviceId]: true }))
    try {
      await deregisterService(serviceId)
      setResults(prev => ({ ...prev, [serviceId]: 'deregistered' }))
    } catch (e) {
      setResults(prev => ({ ...prev, [serviceId]: `error: ${(e as Error).message}` }))
    } finally {
      setRegistering(prev => ({ ...prev, [serviceId]: false }))
    }
  }

  const visible = useMemo(() => {
    const q = search.toLowerCase().trim()
    if (!q) return services
    return services.filter(s =>
      [s.service_id, s.title, s.service_type, s.framework].some(v => v?.toLowerCase().includes(q))
    )
  }, [services, search])

  const pageItems = visible.slice((page - 1) * pageSize, page * pageSize)

  if (error) return <ErrorMessage message={error} />
  if (loading) return <LoadingSpinner />

  return (
    <div>
      <div className="d-flex flex-wrap align-items-center justify-content-between gap-2 mb-3">
        <span className="text-muted small">
          {visible.length}{visible.length !== services.length ? ` of ${services.length}` : ''} service{services.length !== 1 ? 's' : ''} available
        </span>
        <div className="d-flex align-items-center gap-2">
          <div className="input-group input-group-sm" style={{ width: 240 }}>
            <span className="input-group-text"><FiSearch /></span>
            <input
              type="text"
              className="form-control"
              placeholder="Search services…"
              value={search}
              onChange={e => { setSearch(e.target.value); setPage(1) }}
            />
          </div>
          <button className="btn btn-sm btn-primary" disabled={registering._all} onClick={handleRegisterAll}>
            {registering._all && <span className="spinner-border spinner-border-sm me-1" />}
            Register All
          </button>
        </div>
      </div>

      <div className="card">
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th>Service</th>
                  <th style={{ width: 160 }}>Type</th>
                  <th style={{ width: 120 }}>Framework</th>
                  <th style={{ width: 90 }}>Input</th>
                  <th style={{ width: 90 }}>Output</th>
                  <th style={{ width: 160 }}>Module</th>
                  <th style={{ width: 170 }}>Catalogue</th>
                </tr>
              </thead>
              <tbody>
                {pageItems.length === 0 ? (
                  <tr><td colSpan={7} className="text-center text-muted py-4">No services found</td></tr>
                ) : pageItems.map(svc => {
                  const status = results[svc.service_id]
                  const busy = registering[svc.service_id]
                  return (
                    <tr key={svc.service_id}>
                      <td>
                        <div className="fw-semibold">{svc.title}</div>
                        <code className="small text-muted">{svc.service_id}</code>
                        {svc.description && (
                          <div
                            className="small text-muted mt-1"
                            style={{ display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}
                            title={svc.description}
                          >
                            {svc.description}
                          </div>
                        )}
                      </td>
                      <td>
                        {svc.service_type
                          ? <span className={`badge ${SERVICE_TYPE_COLORS[svc.service_type] ?? 'text-bg-secondary'}`}>{svc.service_type}</span>
                          : '—'}
                      </td>
                      <td>{svc.framework ? <span className="badge text-bg-light border">{svc.framework}</span> : '—'}</td>
                      <td>{svc.input_format ? <code className="small">{svc.input_format}</code> : '—'}</td>
                      <td>{svc.output_format ? <code className="small">{svc.output_format}</code> : '—'}</td>
                      <td className="small">
                        {svc.module && <div><code>{svc.module}</code></div>}
                        {svc.function && <div className="text-body-tertiary">{svc.function}()</div>}
                      </td>
                      <td>
                        {status && (
                          <span className={`badge d-block mb-1 ${resultClass(status)}`}>{status}</span>
                        )}
                        <div className="btn-group btn-group-sm" role="group">
                          <button className="btn btn-outline-secondary" disabled={busy} onClick={() => handleRegister(svc.service_id)}>
                            {busy && <span className="spinner-border spinner-border-sm me-1" />}
                            Register
                          </button>
                          <button className="btn btn-outline-danger" disabled={busy} onClick={() => handleDeregister(svc.service_id)}>
                            Remove
                          </button>
                        </div>
                      </td>
                    </tr>
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
            total={visible.length}
            pageSizeOptions={[10, 20, 50]}
            unit="services"
            onPageChange={setPage}
            onPageSizeChange={size => { setPageSize(size); setPage(1) }}
          />
        </div>
      </div>
    </div>
  )
}
