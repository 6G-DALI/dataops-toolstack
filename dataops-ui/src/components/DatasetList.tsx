import { useEffect, useState, useMemo, Fragment } from 'react'
import type { ReactNode } from 'react'
import { deleteDataset, deleteDistribution, getCatalogues, getDatasets, getDistributions } from '../api/airflow'
import AddDistributionModal from './AddDistributionModal'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import ManageDistributionsModal from './ManageDistributionsModal'
import Modal from './Modal'
import Pagination from './Pagination'
import { FiExternalLink, FiList, FiSearch, FiChevronRight, FiChevronDown } from 'react-icons/fi'
import type { Catalogue, Dataset, Distribution, NavigateFn } from '../types'

const CATALOGUE_BASE_URL = import.meta.env.VITE_CATALOGUE_BASE_URL

function catalogueDatasetUrl(datasetId: string): string | null {
  return CATALOGUE_BASE_URL ? `${CATALOGUE_BASE_URL.replace(/\/$/, '')}/datasets/${encodeURIComponent(datasetId)}` : null
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
  distributions: Distribution[] | undefined
  loadingDistributions: boolean
  onDistributionDeleted: (assetId: string) => void
}

function ExpandedRow({ ds, distributions, loadingDistributions, onDistributionDeleted }: ExpandedRowProps) {
  const [confirmingId, setConfirmingId] = useState<string | null>(null)
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleDelete(dist: Distribution) {
    const datasetId = dist.dataset_id || ds.dataset_id || ds.id
    const catalogueId = dist.catalog_id || ds.catalog_id
    if (!dist.asset_id || !catalogueId) {
      setError('Missing asset_id or catalogue_id — cannot delete this distribution.')
      return
    }
    setConfirmingId(null)
    setDeletingId(dist.id)
    setError(null)
    try {
      await deleteDistribution(datasetId, catalogueId, dist.asset_id)
      onDistributionDeleted(dist.id)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setDeletingId(null)
    }
  }

  return (
    <div className="d-flex flex-column gap-2 px-3 py-2">
      {error && <ErrorMessage message={error} />}
      {loadingDistributions ? (
        <span className="text-muted small">Loading distributions…</span>
      ) : (distributions || []).length > 0 && (
        <table className="table table-sm table-borderless align-middle mb-0">
          <tbody>
            {(distributions ?? []).map(dist => (
              <tr key={dist.id}>
                <td>{dist.asset_title || dist.name || dist.id}</td>
                <td><code className="small">{dist.asset_id || '—'}</code></td>
                <td className="text-end" style={{ width: 140 }}>
                  {confirmingId === dist.id ? (
                    <div className="d-flex gap-1 justify-content-end">
                      <button
                        className="btn btn-sm btn-danger"
                        disabled={deletingId === dist.id}
                        onClick={() => handleDelete(dist)}
                      >
                        {deletingId === dist.id ? '…' : 'Confirm'}
                      </button>
                      <button className="btn btn-sm btn-outline-secondary" onClick={() => setConfirmingId(null)}>Cancel</button>
                    </div>
                  ) : (
                    <button
                      className="btn btn-sm btn-outline-danger"
                      disabled={!dist.asset_id || deletingId !== null}
                      onClick={() => setConfirmingId(dist.id)}
                    >
                      Delete
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
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
    (ds.distribution_count ?? 0) > 0 ||
    (ds.producing_dags?.length ?? 0) > 0 ||
    (ds.consuming_dags?.length ?? 0) > 0
  )
}

interface DatasetListProps {
  onNavigate: NavigateFn
}

const ALL_CATALOGUES = '__all__'

export default function DatasetList({ onNavigate }: DatasetListProps) {
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [catalogues, setCatalogues] = useState<Catalogue[]>([])
  // '' means "nothing picked yet" — no dataset fetch happens until the user
  // explicitly chooses a catalogue (or ALL_CATALOGUES) from the selector.
  const [selectedCatalogue, setSelectedCatalogue] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [inspecting, setInspecting] = useState<Dataset | null>(null)
  const [addingTo, setAddingTo] = useState<Dataset | null>(null)
  const [managingDistributionsOf, setManagingDistributionsOf] = useState<Dataset | null>(null)
  const [confirmDeleting, setConfirmDeleting] = useState<Dataset | null>(null)
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [distributionsByDataset, setDistributionsByDataset] = useState<Record<string, Distribution[]>>({})
  const [loadingDistIds, setLoadingDistIds] = useState<Set<string>>(new Set())
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)

  function loadDatasets(catalogueId: string) {
    setLoading(true)
    getDatasets(catalogueId === ALL_CATALOGUES ? undefined : catalogueId)
      .then(data => setDatasets(data.datasets || []))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    getCatalogues()
      .then(data => setCatalogues(data.catalogues || []))
      .catch(() => setCatalogues([]))
  }, [])

  useEffect(() => {
    if (!selectedCatalogue) { setDatasets([]); return }
    loadDatasets(selectedCatalogue)
    setPage(1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedCatalogue])

  const filtered = useMemo(() => {
    const q = search.toLowerCase().trim()
    const list = q
      ? datasets.filter(ds =>
          [ds.name, ds.sns_project_name, ds.catalog_title, ...(ds.extra?.formats ?? [])]
            .some(v => v?.toLowerCase().includes(q))
        )
      : datasets
    return [...list].sort(
      (a, b) => new Date(b.updated_at ?? 0).getTime() - new Date(a.updated_at ?? 0).getTime()
    )
  }, [datasets, search])

  const pageItems = filtered.slice((page - 1) * pageSize, page * pageSize)

  function toggleExpand(ds: Dataset) {
    const id = ds.id
    const opening = !expanded.has(id)
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
    if (opening && !distributionsByDataset[id]) {
      setLoadingDistIds(prev => new Set(prev).add(id))
      getDistributions(ds.dataset_id || ds.id, ds.catalog_id)
        .then(data => setDistributionsByDataset(prev => ({ ...prev, [id]: data.distributions || [] })))
        .catch(() => setDistributionsByDataset(prev => ({ ...prev, [id]: [] })))
        .finally(() => setLoadingDistIds(prev => { const next = new Set(prev); next.delete(id); return next }))
    }
  }

  function handleDistributionDeletedInline(datasetRowId: string, distId: string) {
    setDistributionsByDataset(prev => ({
      ...prev,
      [datasetRowId]: (prev[datasetRowId] || []).filter(d => d.id !== distId),
    }))
    loadDatasets(selectedCatalogue)
  }

  async function handleDeleteDataset(ds: Dataset) {
    const datasetId = ds.dataset_id || ds.id
    if (!ds.catalog_id) {
      setDeleteError('This dataset has no catalogue_id — cannot delete it.')
      return
    }
    setDeletingId(ds.id)
    setDeleteError(null)
    try {
      await deleteDataset(datasetId, ds.catalog_id)
      setConfirmDeleting(null)
      loadDatasets(selectedCatalogue)
    } catch (err) {
      setDeleteError((err as Error).message)
    } finally {
      setDeletingId(null)
    }
  }

  function renderRow(ds: Dataset, showCatalogue: boolean) {
    const expandable = isExpandable(ds)
    const isOpen = expanded.has(ds.id)
    return (
      <Fragment key={ds.id}>
        <tr>
          <td className="text-center">
            {expandable && (
              <button
                className="btn btn-sm btn-link p-0 text-muted"
                onClick={() => toggleExpand(ds)}
                aria-label={isOpen ? 'Collapse' : 'Expand'}
              >
                {isOpen ? <FiChevronDown /> : <FiChevronRight />}
              </button>
            )}
          </td>
          <td>
            <div className="fw-semibold">{ds.name || '—'}</div>
            {showCatalogue && (ds.catalog_title || ds.catalog_id) && (
              <div className="small text-body-tertiary">
                {ds.catalog_url
                  ? <a href={ds.catalog_url} target="_blank" rel="noreferrer">{ds.catalog_title || ds.catalog_id}</a>
                  : ds.catalog_title || ds.catalog_id}
              </div>
            )}
          </td>
          <td className="small">{ds.extra?.publisher || '—'}</td>
          <td className="small">{ds.sns_project_name || '—'}</td>
          <td className="small">{ds.updated_at ? new Date(ds.updated_at).toLocaleString() : '—'}</td>
          <td className="d-flex flex-wrap gap-1 justify-content-end">
            {catalogueDatasetUrl(ds.dataset_id || ds.id) && (
              <a
                className="btn btn-sm btn-outline-secondary"
                href={catalogueDatasetUrl(ds.dataset_id || ds.id)!}
                target="_blank"
                rel="noreferrer"
                title="View in catalogue"
                aria-label="View in catalogue"
              >
                <FiExternalLink />
              </a>
            )}
            <div className="btn-group btn-group-sm">
              <button
                className="btn btn-outline-secondary d-inline-flex align-items-center gap-1"
                onClick={() => setManagingDistributionsOf(ds)}
                title="Distributions"
                aria-label="Distributions"
              >
                <FiList /> {ds.distribution_count ?? 0}
              </button>
              <button className="btn btn-outline-primary" onClick={() => setAddingTo(ds)}>+ Distribution</button>
            </div>
            <button className="btn btn-sm btn-outline-secondary" onClick={() => setInspecting(ds)}>Inspect</button>
            <button
              className="btn btn-sm btn-outline-danger"
              disabled={deletingId === ds.id}
              onClick={() => setConfirmDeleting(ds)}
            >
              {deletingId === ds.id ? '…' : 'Delete'}
            </button>
          </td>
        </tr>
        {expandable && isOpen && (
          <tr className="table-light">
            <td></td>
            <td colSpan={5}>
              <ExpandedRow
                ds={ds}
                distributions={distributionsByDataset[ds.id]}
                loadingDistributions={loadingDistIds.has(ds.id)}
                onDistributionDeleted={distId => handleDistributionDeletedInline(ds.id, distId)}
              />
            </td>
          </tr>
        )}
      </Fragment>
    )
  }

  function datasetTable(items: Dataset[], showCatalogue: boolean) {
    return (
      <div className="table-responsive">
        <table className="table table-hover align-middle mb-0">
          <thead>
            <tr>
              <th style={{ width: 32 }}></th>
              <th>Dataset</th>
              <th style={{ width: 160 }}>Publisher</th>
              <th style={{ width: 160 }}>Project</th>
              <th style={{ width: 160 }}>Last Updated</th>
              <th style={{ width: 420 }}></th>
            </tr>
          </thead>
          <tbody>
            {items.length === 0 ? (
              <tr><td colSpan={6} className="text-center text-muted py-4">No datasets found</td></tr>
            ) : items.map(ds => renderRow(ds, showCatalogue))}
          </tbody>
        </table>
      </div>
    )
  }

  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <div className="d-flex flex-wrap align-items-center justify-content-between gap-2 mb-3">
        <span className="text-muted small">
          {filtered.length}{filtered.length !== datasets.length ? ` of ${datasets.length}` : ''} dataset{datasets.length !== 1 ? 's' : ''}
        </span>
        <div className="d-flex align-items-center gap-2">
          <select
            className="form-select form-select-sm"
            style={{ width: 200 }}
            value={selectedCatalogue}
            onChange={e => setSelectedCatalogue(e.target.value)}
          >
            <option value="" disabled>Select a catalogue…</option>
            <option value={ALL_CATALOGUES}>All catalogues</option>
            {catalogues.map(c => <option key={c.id} value={c.id}>{c.title || c.id}</option>)}
          </select>
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
          <button className="btn btn-sm btn-primary" onClick={() => onNavigate('dataset-creator', {})}>
            + Add Dataset
          </button>
        </div>
      </div>

      {!selectedCatalogue ? (
        <div className="card"><div className="card-body text-center text-muted py-4">Select a catalogue to view its datasets.</div></div>
      ) : loading ? (
        <LoadingSpinner />
      ) : (
        <div className="card">
          <div className="card-body p-0">{datasetTable(pageItems, selectedCatalogue === ALL_CATALOGUES)}</div>
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
      )}

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

      {addingTo && (
        <AddDistributionModal
          dataset={addingTo}
          onClose={() => setAddingTo(null)}
          onNavigate={onNavigate}
          onSubmitted={() => loadDatasets(selectedCatalogue)}
        />
      )}

      {managingDistributionsOf && (
        <ManageDistributionsModal
          dataset={managingDistributionsOf}
          onClose={() => setManagingDistributionsOf(null)}
          onChanged={() => loadDatasets(selectedCatalogue)}
        />
      )}

      {confirmDeleting && (
        <Modal
          title="Delete dataset?"
          onClose={() => setConfirmDeleting(null)}
          footer={
            <>
              <button className="btn btn-secondary" onClick={() => setConfirmDeleting(null)} disabled={deletingId === confirmDeleting.id}>
                Cancel
              </button>
              <button className="btn btn-danger" onClick={() => handleDeleteDataset(confirmDeleting)} disabled={deletingId === confirmDeleting.id}>
                {deletingId === confirmDeleting.id ? 'Deleting…' : 'Delete'}
              </button>
            </>
          }
        >
          {deleteError && <ErrorMessage message={deleteError} />}
          <p className="mb-0">
            This permanently deletes <strong>{confirmDeleting.name || confirmDeleting.id}</strong> and
            all {confirmDeleting.distribution_count ?? 0} of its distributions — piveau records, EDC assets,
            and S3 objects. This cannot be undone.
          </p>
        </Modal>
      )}
    </div>
  )
}
