import { useState } from 'react'
import type { ChangeEvent } from 'react'
import { addDistribution } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import Modal from './Modal'
import type { Dataset, DistributionMetricsInput, DistributionSubmitResponse, GreatExpectation, NavigateFn } from '../types'

interface ColumnCheckState {
  exist: boolean
  notnull: boolean
}

interface AddDistributionModalProps {
  dataset: Dataset
  onClose: () => void
  onSubmitted: (result: DistributionSubmitResponse) => void
  onNavigate: NavigateFn
}

const emptyMetrics: DistributionMetricsInput = { variable_measured: [], measurement_technique: '' }

function splitList(value: string): string[] {
  return value.split(',').map(s => s.trim()).filter(Boolean)
}

export default function AddDistributionModal({ dataset, onClose, onSubmitted, onNavigate }: AddDistributionModalProps) {
  const [file, setFile] = useState<File | null>(null)
  const [previewRows, setPreviewRows] = useState<string[][] | null>(null)
  const [metrics, setMetrics] = useState(emptyMetrics)
  const [rowCountEnabled, setRowCountEnabled] = useState(true)
  const [rowCountMin, setRowCountMin] = useState('1')
  const [rowCountMax, setRowCountMax] = useState('')
  const [columnChecks, setColumnChecks] = useState<Record<string, ColumnCheckState>>({})
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<DistributionSubmitResponse | null>(null)

  const columns = metrics.variable_measured

  function ensureColumnCheck(col: string): ColumnCheckState {
    return columnChecks[col] ?? { exist: true, notnull: true }
  }

  function toggleColumnCheck(col: string, key: keyof ColumnCheckState, value: boolean) {
    setColumnChecks(prev => ({ ...prev, [col]: { ...ensureColumnCheck(col), [key]: value } }))
  }

  function handleFileChange(e: ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0] ?? null
    setFile(f)
    setPreviewRows(null)
    if (!f) return

    const ext = f.name.split('.').pop()?.toLowerCase()
    if (ext !== 'csv' && ext !== 'tsv') return

    const reader = new FileReader()
    reader.onload = ev => {
      const text = String(ev.target?.result ?? '')
      const lines = text.split('\n').filter(l => l.trim().length > 0)
      const sep = lines[0]?.includes('\t') ? '\t' : ','
      const rows = lines.slice(0, 6).map(l => l.split(sep).map(c => c.trim()))
      setPreviewRows(rows)
      if (rows.length > 0 && columns.length === 0) {
        setMetrics(m => ({ ...m, variable_measured: rows[0] }))
      }
    }
    reader.readAsText(f.slice(0, 65536))
  }

  function buildExpectations(): GreatExpectation[] {
    const exps: GreatExpectation[] = []
    if (rowCountEnabled) {
      exps.push({
        type: 'expect_table_row_count_to_be_between',
        min_value: Number(rowCountMin) || 1,
        ...(rowCountMax ? { max_value: Number(rowCountMax) } : {}),
      })
    }
    for (const col of columns) {
      const cs = ensureColumnCheck(col)
      if (cs.exist) exps.push({ type: 'expect_column_to_exist', column: col })
      if (cs.notnull) exps.push({ type: 'expect_column_values_to_not_be_null', column: col })
    }
    return exps
  }

  async function handleSubmit() {
    if (!file) return setError('A dataset file is required.')
    const datasetId = dataset.dataset_id || dataset.id
    const catalogueId = dataset.catalog_id
    if (!catalogueId) return setError('This dataset has no catalogue_id — cannot register a distribution.')

    setError(null)
    setSubmitting(true)
    try {
      const res = await addDistribution(datasetId, catalogueId, file, metrics, buildExpectations())
      setResult(res)
      onSubmitted(res)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setSubmitting(false)
    }
  }

  if (result) {
    return (
      <Modal title="Distribution added" onClose={onClose} width={640} footer={
        <button className="btn btn-primary" onClick={onClose}>Close</button>
      }>
        <div className="alert alert-success" role="alert">
          <strong>Distribution registered.</strong> The file was uploaded to the Data Lake, added to
          the dataset's catalogue record, and a validation run has been triggered.
        </div>
        <dl className="row small mb-0">
          <dt className="col-sm-4">Distribution ID</dt>
          <dd className="col-sm-8"><code>{result.distribution_id}</code></dd>
          <dt className="col-sm-4">Object key</dt>
          <dd className="col-sm-8"><code>{result.object_key}</code></dd>
          <dt className="col-sm-4">Catalogue record</dt>
          <dd className="col-sm-8"><code>{result.piveau.distribution_uri}</code></dd>
          {result.edc.status !== 'skipped' && (
            <>
              <dt className="col-sm-4">EDC registration</dt>
              <dd className="col-sm-8">
                {result.edc.status === 'registered' || result.edc.status === 'already_registered' ? (
                  <span className="badge text-bg-success">{result.edc.status.replace('_', ' ')}</span>
                ) : (
                  <span className="badge text-bg-danger" title={result.edc.error}>failed</span>
                )}
              </dd>
            </>
          )}
        </dl>
        <button
          className="btn btn-outline-secondary btn-sm mt-3"
          onClick={() => onNavigate('tasks', { dagId: 'dali_dataspace_validate_dataset', runId: result.validation_run.dag_run_id })}
        >
          View validation run
        </button>
      </Modal>
    )
  }

  return (
    <Modal
      title={`Add distribution — ${dataset.name || dataset.id}`}
      onClose={onClose}
      width={720}
      footer={
        <>
          <button className="btn btn-outline-secondary" onClick={onClose} disabled={submitting}>Cancel</button>
          <button className="btn btn-primary" onClick={handleSubmit} disabled={submitting}>
            {submitting && <span className="spinner-border spinner-border-sm me-1" />}
            {submitting ? 'Uploading…' : 'Add Distribution'}
          </button>
        </>
      }
    >
      {error && <ErrorMessage message={error} />}

      <div className="mb-3">
        <label className="form-label small">File <span className="text-danger">*</span></label>
        <input type="file" className="form-control" onChange={handleFileChange} />
        {file && (
          <div className="alert alert-light border small mt-2 mb-0">
            <strong>{file.name}</strong> ({(file.size / (1024 * 1024)).toFixed(2)} MB)
          </div>
        )}
      </div>

      <div className="mb-3">
        <label className="form-label small">Dataset columns / measured variables</label>
        <input className="form-control" value={metrics.variable_measured.join(', ')}
          onChange={e => setMetrics({ ...metrics, variable_measured: splitList(e.target.value) })} />
        <div className="form-text">Comma-separated. Auto-filled from the file above if left empty.</div>
      </div>
      <div className="mb-3">
        <label className="form-label small">Measurement technique</label>
        <input className="form-control" value={metrics.measurement_technique}
          onChange={e => setMetrics({ ...metrics, measurement_technique: e.target.value })} />
      </div>

      {previewRows && previewRows.length > 0 && (
        <div className="table-responsive mb-3">
          <table className="table table-sm table-bordered small mb-0">
            <thead><tr>{previewRows[0].map((h, i) => <th key={i}>{h}</th>)}</tr></thead>
            <tbody>
              {previewRows.slice(1).map((row, ri) => (
                <tr key={ri}>{row.map((cell, ci) => <td key={ci}>{cell}</td>)}</tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <hr />

      <div className="form-check mb-2">
        <input type="checkbox" className="form-check-input" id="rowcount" checked={rowCountEnabled}
          onChange={e => setRowCountEnabled(e.target.checked)} />
        <label className="form-check-label" htmlFor="rowcount">
          <code className="small">expect_table_row_count_to_be_between</code>
        </label>
      </div>
      {rowCountEnabled && (
        <div className="row mb-3">
          <div className="col-md-4">
            <label className="form-label small">Min rows</label>
            <input className="form-control" value={rowCountMin} onChange={e => setRowCountMin(e.target.value)} />
          </div>
          <div className="col-md-4">
            <label className="form-label small">Max rows</label>
            <input className="form-control" placeholder="unbounded" value={rowCountMax} onChange={e => setRowCountMax(e.target.value)} />
          </div>
        </div>
      )}

      {columns.length > 0 && (
        <div className="table-responsive">
          <table className="table table-sm align-middle mb-0">
            <thead>
              <tr><th>Column</th><th className="text-center">Exists</th><th className="text-center">Not Null</th></tr>
            </thead>
            <tbody>
              {columns.map(col => {
                const cs = ensureColumnCheck(col)
                return (
                  <tr key={col}>
                    <td><code className="small">{col}</code></td>
                    <td className="text-center">
                      <input type="checkbox" className="form-check-input" checked={cs.exist}
                        onChange={e => toggleColumnCheck(col, 'exist', e.target.checked)} />
                    </td>
                    <td className="text-center">
                      <input type="checkbox" className="form-check-input" checked={cs.notnull}
                        onChange={e => toggleColumnCheck(col, 'notnull', e.target.checked)} />
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </Modal>
  )
}
