import { useCallback, useEffect, useState } from 'react'
import { FiEdit2, FiExternalLink, FiRefreshCw } from 'react-icons/fi'
import { getTestbedDatasets } from '../api/airflow'
import { catalogueDatasetUrl } from '../config'
import { LEVEL_LABEL } from './TestbedDatasetEdit'
import type { DatasetAccessLevel, NavigateFn, TestbedDatasets as Listing } from '../types'

function LevelBadge({ level }: { level?: DatasetAccessLevel | null }) {
  if (!level) return <span className="text-muted small">—</span>
  const cls = level === 'public' ? 'text-bg-light border' : level === 'restricted' ? 'text-bg-info' : 'text-bg-warning'
  return <span className={`badge ${cls}`}>{LEVEL_LABEL[level]}</span>
}

/**
 * The datasets of the testbed's catalogue, with their visibility. Edit opens the dataset's own page
 * (TestbedDatasetEdit). Edits change the catalogue only, never the testbed's metadata.json.
 */
export default function TestbedDatasets({ slug, onNavigate }: { slug: string; onNavigate: NavigateFn }) {
  const [data, setData] = useState<Listing | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(() => {
    setLoading(true)
    getTestbedDatasets(slug)
      .then(r => { setData(r); setError(null) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [slug])

  useEffect(load, [load])

  return (
    <div className="card"><div className="card-body">
      <div className="d-flex align-items-center mb-3">
        <div className="text-muted small me-3">
          The datasets of <code>{data?.catalogue_id ?? 'the testbed catalogue'}</code>, including the private ones.
          Edits change the catalogue only; the testbed's metadata.json stays as it was shared.
        </div>
        <button className="btn btn-sm btn-outline-secondary ms-auto text-nowrap" disabled={loading} onClick={load}>
          <FiRefreshCw className="me-1" />Refresh
        </button>
      </div>

      {error && <div className="alert alert-danger py-2 small">{error}</div>}

      {loading && !data ? (
        <span className="text-muted small">Loading…</span>
      ) : data && data.datasets.length === 0 ? (
        <span className="text-muted small">No datasets in this catalogue yet.</span>
      ) : data && (
        <div className="table-responsive">
          <table className="table table-sm table-hover align-middle mb-0">
            <thead>
              <tr>
                <th scope="col">Dataset</th>
                <th scope="col">Visibility</th>
                <th scope="col" className="text-end">Files</th>
                <th scope="col">Updated</th>
                <th scope="col" className="text-end"><span className="visually-hidden">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {data.datasets.map(d => {
                const link = catalogueDatasetUrl(d.id)
                return (
                  <tr key={d.id}>
                    <td className="text-break">
                      <div>{d.title}</div>
                      <code className="small text-muted">{d.id}</code>
                      {d.error && <div className="small text-danger">{d.error}</div>}
                    </td>
                    <td><LevelBadge level={d.access_level} /></td>
                    <td className="text-end">{d.distributions ?? '—'}</td>
                    <td className="small text-muted text-nowrap">{(d.modified ?? '').slice(0, 10) || '—'}</td>
                    <td className="text-end text-nowrap">
                      {link && (
                        <a href={link} target="_blank" rel="noreferrer" className="btn btn-sm btn-link py-0" title="Open in the catalogue">
                          <FiExternalLink />
                        </a>
                      )}
                      <button className="btn btn-sm btn-outline-primary py-0" disabled={!!d.error}
                        onClick={() => onNavigate('testbed-dataset', { dagId: slug, runId: d.id })}>
                        <FiEdit2 className="me-1" />Edit
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div></div>
  )
}
