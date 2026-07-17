import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { getCatalogues, getDatasets, getDistributions, getDagDetails } from '../api/airflow'
import Modal from './Modal'
import { FiDatabase, FiSettings } from 'react-icons/fi'
import type { Catalogue, DagParam, Dataset, Distribution, TriggerConf } from '../types'

const DATASET_FIELDS = new Set(['catalogue_id', 'dataset_id', 'asset_id'])

function renderValue(val: unknown): string {
  if (val === null || val === undefined) return ''
  if (typeof val === 'object') return JSON.stringify(val)
  return String(val)
}

/** Only include `key` in the patch if it's one of the DAG's declared conf fields. */
function ifPresent(prev: Record<string, string>, key: string, value: string): Record<string, string> {
  return Object.prototype.hasOwnProperty.call(prev, key) ? { [key]: value } : {}
}

interface SectionLabelProps {
  icon: ReactNode
  children: ReactNode
}

function SectionLabel({ icon, children }: SectionLabelProps) {
  return (
    <div className="d-flex align-items-center gap-2 text-muted text-uppercase fw-bold small mt-3 mb-2" style={{ letterSpacing: '0.06em' }}>
      {icon}
      {children}
    </div>
  )
}

interface TriggerModalProps {
  dagId: string
  onConfirm: (conf: TriggerConf) => void
  onCancel: () => void
}

export default function TriggerModal({ dagId, onConfirm, onCancel }: TriggerModalProps) {
  const [catalogues, setCatalogues] = useState<Catalogue[]>([])
  const [loadingCatalogues, setLoadingCatalogues] = useState(true)
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [loadingDatasets, setLoadingDatasets] = useState(false)
  const [distributions, setDistributions] = useState<Distribution[]>([])
  const [loadingDistributions, setLoadingDistributions] = useState(false)
  const [loadingDag, setLoadingDag] = useState(true)
  const [params, setParams] = useState<Record<string, DagParam> | null>(null)
  const [confValues, setConfValues] = useState<Record<string, string>>({})
  const [catalogueId, setCatalogueId] = useState<string | null>(null)
  const [datasetId, setDatasetId] = useState<string | null>(null)
  const [selected, setSelected] = useState<Distribution | null>(null)

  useEffect(() => {
    getCatalogues()
      .then(data => setCatalogues(data.catalogues || []))
      .catch(() => {})
      .finally(() => setLoadingCatalogues(false))

    getDagDetails(dagId)
      .then(dag => {
        const dagParams = dag.params ?? {}
        if (Object.keys(dagParams).length > 0) {
          setParams(dagParams)
          setConfValues(Object.fromEntries(
            Object.entries(dagParams).map(([k, p]) => [k, renderValue(p?.value ?? p)])
          ))
        }
      })
      .catch(() => {})
      .finally(() => setLoadingDag(false))
  }, [dagId])

  // Fetch datasets scoped to the selected catalogue only, once a catalogue is picked.
  useEffect(() => {
    if (!catalogueId) {
      setDatasets([])
      return
    }
    let cancelled = false
    setLoadingDatasets(true)
    getDatasets(catalogueId)
      .then(data => { if (!cancelled) setDatasets(data.datasets || []) })
      .catch(() => { if (!cancelled) setDatasets([]) })
      .finally(() => { if (!cancelled) setLoadingDatasets(false) })
    return () => { cancelled = true }
  }, [catalogueId])

  // Fetch the distributions of the selected dataset only, once a dataset is picked.
  useEffect(() => {
    if (!datasetId) {
      setDistributions([])
      return
    }
    let cancelled = false
    setLoadingDistributions(true)
    getDistributions(datasetId, catalogueId ?? undefined)
      .then(data => { if (!cancelled) setDistributions(data.distributions || []) })
      .catch(() => { if (!cancelled) setDistributions([]) })
      .finally(() => { if (!cancelled) setLoadingDistributions(false) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- catalogueId only narrows the search, re-fetching on it isn't needed
  }, [datasetId])

  function handleCatalogueChange(id: string) {
    setCatalogueId(id || null)
    setDatasetId(null)
    setSelected(null)
    setConfValues(prev => ({
      ...prev,
      ...ifPresent(prev, 'catalogue_id', id),
      ...ifPresent(prev, 'dataset_id', ''),
      ...ifPresent(prev, 'asset_id', ''),
    }))
  }

  function handleDatasetChange(dsId: string) {
    setDatasetId(dsId || null)
    setSelected(null)
    const ds = datasets.find(d => d.id === dsId) ?? null
    setConfValues(prev => ({
      ...prev,
      ...ifPresent(prev, 'dataset_id', ds?.dataset_id ?? ds?.id ?? ''),
      ...ifPresent(prev, 'asset_id', ''),
    }))
  }

  function handleDistributionChange(distId: string) {
    const dist = distributions.find(d => d.id === distId) ?? null
    setSelected(dist)
    if (dist) {
      setConfValues(prev => ({
        ...prev,
        ...ifPresent(prev, 'catalogue_id', dist.catalog_id ?? ''),
        ...ifPresent(prev, 'dataset_id', dist.dataset_id ?? ''),
        ...ifPresent(prev, 'asset_id', dist.asset_id ?? ''),
      }))
    }
  }

  function handleConfirm() {
    const conf: TriggerConf = {}
    for (const [k, v] of Object.entries(confValues)) {
      try { conf[k] = JSON.parse(v) } catch { conf[k] = v }
    }
    onConfirm(conf)
  }

  const loading = loadingCatalogues || loadingDag
  const hasParams = params !== null
  const needsDataset = params !== null && Object.keys(params).some(k => DATASET_FIELDS.has(k))
  const canTrigger = !loading && (!needsDataset || !!selected)

  const datasetParams: [string, DagParam][] = params !== null ? Object.entries(params).filter(([k]) => DATASET_FIELDS.has(k)) : []
  const otherParams: [string, DagParam][]   = params !== null ? Object.entries(params).filter(([k]) => !DATASET_FIELDS.has(k)) : []

  return (
    <Modal
      title={
        <span className="d-inline-flex align-items-center gap-2">
          Trigger DAG
          <span className="badge text-bg-primary font-monospace">{dagId}</span>
        </span>
      }
      onClose={onCancel}
      width={580}
      footer={
        <>
          <button className="btn btn-secondary" onClick={onCancel}>Cancel</button>
          <button className="btn btn-primary" disabled={!canTrigger} onClick={handleConfirm}>Trigger</button>
        </>
      }
    >
      {loading ? (
        <div className="text-center py-5">
          <div className="spinner-border text-primary" role="status">
            <span className="visually-hidden">Loading…</span>
          </div>
        </div>
      ) : (
        <form onSubmit={e => e.preventDefault()}>
          {needsDataset && (
            <>
              <SectionLabel icon={<FiDatabase size={13} />}>Source Dataset</SectionLabel>

              <div className="mb-3">
                <label className="form-label small">Catalogue</label>
                <select
                  className="form-select form-select-sm"
                  value={catalogueId ?? ''}
                  onChange={e => handleCatalogueChange(e.target.value)}
                >
                  <option value="">Select a catalogue…</option>
                  {catalogues.map(c => <option key={c.id} value={c.id}>{c.title}</option>)}
                </select>
              </div>

              <div className="mb-3">
                <label className="form-label small">Dataset</label>
                <select
                  className="form-select form-select-sm"
                  disabled={!catalogueId || loadingDatasets}
                  value={datasetId ?? ''}
                  onChange={e => handleDatasetChange(e.target.value)}
                >
                  <option value="">
                    {!catalogueId ? 'Select a catalogue first' : loadingDatasets ? 'Loading datasets…' : 'Select a dataset…'}
                  </option>
                  {datasets.map(ds => (
                    <option key={ds.id} value={ds.id}>
                      {ds.name || ds.id}{ds.distribution_count ? ` (${ds.distribution_count})` : ''}
                    </option>
                  ))}
                </select>
              </div>

              <div className="mb-3">
                <label className="form-label small">Distribution</label>
                <select
                  className="form-select form-select-sm"
                  disabled={!datasetId || loadingDistributions}
                  value={selected?.id ?? ''}
                  onChange={e => handleDistributionChange(e.target.value)}
                >
                  <option value="">
                    {!datasetId ? 'Select a dataset first' : loadingDistributions ? 'Loading distributions…' : 'Select a distribution…'}
                  </option>
                  {distributions.map(dist => (
                    <option key={dist.id} value={dist.id}>
                      {dist.asset_title || dist.extra?.format || dist.id}
                    </option>
                  ))}
                </select>
                {selected?.asset_id && (
                  <div className="form-text"><code className="small">asset_id: {selected.asset_id}</code></div>
                )}
              </div>
            </>
          )}

          {hasParams && (
            <>
              <SectionLabel icon={<FiSettings size={13} />}>DAG Parameters</SectionLabel>

              {selected && datasetParams.length > 0 && (
                <div className="rounded border bg-body-tertiary px-3 py-2 mb-3">
                  {datasetParams.map(([key]) => (
                    <div key={key} className="d-flex justify-content-between small mb-1">
                      <span className="text-muted font-monospace">{key}</span>
                      <span className="font-monospace text-truncate ms-2" style={{ maxWidth: 340 }}>{confValues[key]}</span>
                    </div>
                  ))}
                </div>
              )}

              {otherParams.map(([key, param]) => (
                <div key={key} className="mb-2">
                  <label className="form-label small font-monospace">{key}</label>
                  <input
                    type="text"
                    className="form-control form-control-sm"
                    value={confValues[key] ?? ''}
                    onChange={e => setConfValues(prev => ({ ...prev, [key]: e.target.value }))}
                  />
                  {param?.description && <div className="form-text">{param.description}</div>}
                </div>
              ))}
            </>
          )}

          {!hasParams && (
            <p className="text-muted mb-0">
              No parameters defined for this DAG. It will be triggered with an empty conf.
            </p>
          )}
        </form>
      )}
    </Modal>
  )
}
