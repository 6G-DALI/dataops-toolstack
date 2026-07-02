import { useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { getDatasets, getDagDetails } from '../api/airflow'
import Modal from './Modal'
import { FiDatabase, FiSettings } from 'react-icons/fi'
import type { DagParam, Dataset, TriggerConf } from '../types'

const DATASET_FIELDS = new Set(['input_key', 'catalogue_id', 'dataset_id', 'asset_title'])

function renderValue(val: unknown): string {
  if (val === null || val === undefined) return ''
  if (typeof val === 'object') return JSON.stringify(val)
  return String(val)
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
  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [loadingDatasets, setLoadingDatasets] = useState(true)
  const [loadingDag, setLoadingDag] = useState(true)
  const [params, setParams] = useState<Record<string, DagParam> | null>(null)
  const [confValues, setConfValues] = useState<Record<string, string>>({})
  const [catalogueId, setCatalogueId] = useState<string | null>(null)
  const [selected, setSelected] = useState<Dataset | null>(null)

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch(() => {})
      .finally(() => setLoadingDatasets(false))

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

  const catalogues = useMemo(() => {
    const seen = new Map<string, string>()
    for (const ds of datasets) {
      if (ds.catalog_id && !seen.has(ds.catalog_id))
        seen.set(ds.catalog_id, ds.catalog_title || ds.catalog_id)
    }
    return [...seen.entries()].map(([id, label]) => ({ value: id, label }))
  }, [datasets])

  const datasetsInCatalogue = useMemo(() =>
    datasets.filter(ds => ds.catalog_id === catalogueId),
    [datasets, catalogueId]
  )

  function handleCatalogueChange(id: string) {
    setCatalogueId(id || null)
    setSelected(null)
    setConfValues(prev => ({ ...prev, catalogue_id: id, input_key: '' }))
  }

  function handleDatasetChange(dsId: string) {
    const ds = datasets.find(d => d.id === dsId) ?? null
    setSelected(ds)
    if (ds) {
      setConfValues(prev => ({
        ...prev,
        ...(Object.prototype.hasOwnProperty.call(prev, 'input_key')    ? { input_key:    ds.input_key   ?? '' } : {}),
        ...(Object.prototype.hasOwnProperty.call(prev, 'catalogue_id') ? { catalogue_id: ds.catalog_id  ?? '' } : {}),
        ...(Object.prototype.hasOwnProperty.call(prev, 'dataset_id')   ? { dataset_id:   ds.dataset_id  ?? '' } : {}),
        ...(Object.prototype.hasOwnProperty.call(prev, 'asset_title')  ? { asset_title:  ds.asset_title ?? '' } : {}),
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

  const loading = loadingDatasets || loadingDag
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
                  {catalogues.map(c => <option key={c.value} value={c.value}>{c.label}</option>)}
                </select>
              </div>

              <div className="mb-3">
                <label className="form-label small">Dataset</label>
                <select
                  className="form-select form-select-sm"
                  disabled={!catalogueId}
                  value={selected?.id ?? ''}
                  onChange={e => handleDatasetChange(e.target.value)}
                >
                  <option value="">{catalogueId ? 'Select a dataset…' : 'Select a catalogue first'}</option>
                  {datasetsInCatalogue.map(ds => (
                    <option key={ds.id} value={ds.id}>{ds.name || ds.asset_title || ds.id}</option>
                  ))}
                </select>
                {selected?.input_key && (
                  <div className="form-text"><code className="small">{selected.input_key}</code></div>
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
