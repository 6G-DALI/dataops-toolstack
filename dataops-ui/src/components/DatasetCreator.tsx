import { useState } from 'react'
import type { ChangeEvent, FormEvent, ReactNode } from 'react'
import { addDistribution, createDataset } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import type {
  DatasetIdentityInput,
  DatasetObjectInput,
  DistributionMetricsInput,
  DistributionSubmitResponse,
  GreatExpectation,
  NavigateFn,
  TestbedContextInput,
} from '../types'
import '../styles/DatasetCreator.css'

interface DatasetCreatorProps {
  onNavigate: NavigateFn
}

const emptyIdentity: DatasetIdentityInput = {
  title: '', description: '', sns_project_name: '6G-DALI', publisher_name: '', contact_email: '',
  contributors: [], keywords: [], related_publications: [], language: 'ENG', spatial: '',
  temporal_start: '', temporal_end: '', version: '1.0',
}

const emptyObject: DatasetObjectInput = {
  license: 'https://creativecommons.org/licenses/by/4.0/', access_rights: 'PUBLIC',
  gdpr_compliant: true, fair_compliant: true, contains_pii: false, produced_by: '',
}

const emptyTestbedContext: TestbedContextInput = {
  underlay_platform: '', environment: '', network_domain: '', ran_3gpp_release: '',
  ran_new_radio_type: '', ran_split: '', ran_focused_technology: '', ran_coverage_type: '',
  ran_frequency_band: '', ran_bandwidth_mhz: '', ran_max_end_devices: '1', ran_mobility_model: '',
  core_release: '', core_solution: '', transport_type: '', compute_orchestrator_type: '',
  compute_gpu_use: false, compute_virtualization_type: '', compute_infrastructure_type: '',
  traffic_origin: '', traffic_pattern: '', slice_type: '', reference_plane: '', related_vertical: '',
  observation_point_horizontal: '', observation_point_vertical: '', measurement_family: [], measurement_tool: [],
}

const emptyMetrics: DistributionMetricsInput = {
  variable_measured: [], measurement_technique: '',
}

function splitList(value: string): string[] {
  return value.split(',').map(s => s.trim()).filter(Boolean)
}

// ── Shared field/card building blocks ───────────────────────────────────────

interface FieldProps {
  label: string
  required?: boolean
  children: ReactNode
  help?: string
}

function Field({ label, required, children, help }: FieldProps) {
  return (
    <div className="mb-3">
      <label className="form-label small">
        {label} {required && <span className="text-danger">*</span>}
      </label>
      {children}
      {help && <div className="form-text">{help}</div>}
    </div>
  )
}

type Obligation = 'Mandatory' | 'Recommended' | 'Optional' | 'Auto-generated'

const OBLIGATION_CLASS: Record<Obligation, string> = {
  Mandatory: 'text-bg-danger',
  Recommended: 'text-bg-warning',
  Optional: 'text-bg-success',
  'Auto-generated': 'text-bg-danger',
}

interface CardProps {
  title: string
  obligation: Obligation
  children: ReactNode
}

function Card({ title, obligation, children }: CardProps) {
  return (
    <div className="card mb-3">
      <div className="card-header d-flex align-items-center justify-content-between">
        <span className="fw-semibold text-uppercase small">{title}</span>
        <span className={`badge ${OBLIGATION_CLASS[obligation]}`}>{obligation}</span>
      </div>
      <div className="card-body">{children}</div>
    </div>
  )
}

// ── Quality check state ──────────────────────────────────────────────────────

interface ColumnCheckState {
  exist: boolean
  notnull: boolean
}

type AddCheckType = 'between' | 'date' | 'inset' | 'notinset' | 'allnull'

const ADD_CHECK_LABELS: Record<AddCheckType, string> = {
  between: 'ExpectColumnValuesToBeBetween',
  date: 'ExpectColumnValuesToBeDateutilParseable',
  inset: 'ExpectColumnValuesToBeInSet',
  notinset: 'ExpectColumnValuesToNotBeInSet',
  allnull: 'ExpectColumnValuesToBeNull',
}

interface AddedCheck {
  id: number
  type: AddCheckType
  column: string
  minValue?: string
  maxValue?: string
  values?: string
}

function addedCheckToExpectation(c: AddedCheck): GreatExpectation {
  switch (c.type) {
    case 'between':
      return {
        type: 'expect_column_values_to_be_between', column: c.column,
        ...(c.minValue ? { min_value: Number(c.minValue) } : {}),
        ...(c.maxValue ? { max_value: Number(c.maxValue) } : {}),
      }
    case 'date':
      return { type: 'expect_column_values_to_be_dateutil_parseable', column: c.column }
    case 'inset':
      return { type: 'expect_column_values_to_be_in_set', column: c.column, value_set: splitList(c.values ?? '') }
    case 'notinset':
      return { type: 'expect_column_values_to_not_be_in_set', column: c.column, value_set: splitList(c.values ?? '') }
    case 'allnull':
      return { type: 'expect_column_values_to_be_null', column: c.column }
  }
}

const STEPS = ['Metadata', 'Data', 'Quality Checks', 'Review & Submit'] as const

export default function DatasetCreator({ onNavigate }: DatasetCreatorProps) {
  const [step, setStep] = useState(0)
  const [identity, setIdentity] = useState(emptyIdentity)
  const [object, setObject] = useState(emptyObject)
  const [testbedContext, setTestbedContext] = useState(emptyTestbedContext)
  const [metrics, setMetrics] = useState(emptyMetrics)
  const [file, setFile] = useState<File | null>(null)
  const [previewRows, setPreviewRows] = useState<string[][] | null>(null)

  const [rowCountEnabled, setRowCountEnabled] = useState(true)
  const [rowCountMin, setRowCountMin] = useState('1')
  const [rowCountMax, setRowCountMax] = useState('')
  const [columnChecks, setColumnChecks] = useState<Record<string, ColumnCheckState>>({})
  const [addType, setAddType] = useState<AddCheckType>('between')
  const [addColumn, setAddColumn] = useState('')
  const [addMin, setAddMin] = useState('')
  const [addMax, setAddMax] = useState('')
  const [addValues, setAddValues] = useState('')
  const [addedChecks, setAddedChecks] = useState<AddedCheck[]>([])
  const [confirmRights, setConfirmRights] = useState(false)

  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<DistributionSubmitResponse | null>(null)
  // Set once handleCreateDataset succeeds — the dataset now exists in piveau,
  // so the Metadata step locks and the remaining steps build up the separate
  // handleAddDistribution call (which can be retried without recreating it).
  const [createdDataset, setCreatedDataset] = useState<{ datasetId: string, catalogueId: string } | null>(null)

  const isLastStep = step === STEPS.length - 1
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

  function addCheck() {
    if (!addColumn) return
    const id = Date.now()
    setAddedChecks(prev => [...prev, {
      id, type: addType, column: addColumn,
      minValue: addMin || undefined, maxValue: addMax || undefined, values: addValues || undefined,
    }])
    setAddMin(''); setAddMax(''); setAddValues('')
  }

  function removeCheck(id: number) {
    setAddedChecks(prev => prev.filter(c => c.id !== id))
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
    exps.push(...addedChecks.map(addedCheckToExpectation))
    return exps
  }

  function validateStep(i: number): string | null {
    if (i === 0) {
      if (!identity.title.trim()) return 'Title is required.'
      if (!identity.description.trim()) return 'Description is required.'
      if (!object.license.trim()) return 'License is required.'
    }
    if (i === 1) {
      if (!file) return 'A dataset file is required.'
    }
    if (i === STEPS.length - 1) {
      if (!confirmRights) return 'Please confirm your contributor responsibilities before submitting.'
    }
    return null
  }

  // Once the dataset is created (step 1's own submit), its metadata step is
  // locked — there's no "update dataset" endpoint, so further edits to
  // identity/object/testbed_context would silently not be reflected.
  const minStep = createdDataset ? 1 : 0

  function goToStep(target: number) {
    const clampedTarget = Math.max(target, minStep)
    if (clampedTarget <= step) { setError(null); setStep(clampedTarget); return }
    for (let i = step; i < clampedTarget; i++) {
      const err = validateStep(i)
      if (err) { setError(err); setStep(i); return }
    }
    setError(null)
    setStep(clampedTarget)
  }

  function handleNext() {
    const err = validateStep(step)
    if (err) return setError(err)
    setError(null)
    setStep(s => Math.min(s + 1, STEPS.length - 1))
  }

  function handleBack() {
    setError(null)
    setStep(s => Math.max(s - 1, minStep))
  }

  /** Operation 1: register the dataset's own metadata. Locks the Metadata step. */
  async function handleCreateDataset() {
    const err = validateStep(0)
    if (err) return setError(err)
    setError(null)

    setSubmitting(true)
    try {
      const created = await createDataset({ identity, object, testbed_context: testbedContext })
      setCreatedDataset({ datasetId: created.dataset_id, catalogueId: created.catalogue_id })
      setStep(1)
    } catch (err2) {
      setError((err2 as Error).message)
    } finally {
      setSubmitting(false)
    }
  }

  /** Operation 2: upload the file as a distribution of the already-created dataset. */
  async function handleAddDistribution(e: FormEvent) {
    e.preventDefault()
    const err = validateStep(step)
    if (err) return setError(err)
    if (!createdDataset) return setError('Create the dataset first.')
    setError(null)

    setSubmitting(true)
    try {
      const res = await addDistribution(
        createdDataset.datasetId, createdDataset.catalogueId, file as File, metrics, buildExpectations()
      )
      setResult(res)
    } catch (err2) {
      setError((err2 as Error).message)
    } finally {
      setSubmitting(false)
    }
  }

  function handleReset() {
    setResult(null)
    setError(null)
    setCreatedDataset(null)
    setStep(0)
    setIdentity(emptyIdentity)
    setObject(emptyObject)
    setTestbedContext(emptyTestbedContext)
    setMetrics(emptyMetrics)
    setFile(null)
    setPreviewRows(null)
    setRowCountEnabled(true)
    setRowCountMin('1')
    setRowCountMax('')
    setColumnChecks({})
    setAddedChecks([])
    setConfirmRights(false)
  }

  if (result) {
    return (
      <div>
        <div className="alert alert-success" role="alert">
          <strong>Dataset submitted.</strong> It has been uploaded to the Data Lake, registered in
          the Staging Catalogue, and a validation run has been triggered.
        </div>
        <div className="card">
          <div className="card-body">
            <dl className="row mb-0 small">
              <dt className="col-sm-4">Dataset ID</dt>
              <dd className="col-sm-8"><code>{result.dataset_id}</code></dd>
              <dt className="col-sm-4">Catalogue</dt>
              <dd className="col-sm-8"><code>{result.catalogue_id}</code></dd>
              <dt className="col-sm-4">Distribution ID</dt>
              <dd className="col-sm-8"><code>{result.distribution_id}</code></dd>
              <dt className="col-sm-4">Object key</dt>
              <dd className="col-sm-8"><code>{result.object_key}</code></dd>
              <dt className="col-sm-4">Catalogue record</dt>
              <dd className="col-sm-8"><code>{result.piveau.distribution_uri}</code></dd>
              <dt className="col-sm-4">Validation run</dt>
              <dd className="col-sm-8"><code>{result.validation_run.dag_run_id}</code></dd>
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
          </div>
        </div>
        <div className="d-flex gap-2 mt-3">
          <button className="btn btn-primary" onClick={() => onNavigate('tasks', {
            dagId: 'dali_dataspace_validate_dataset', runId: result.validation_run.dag_run_id,
          })}>
            View validation run
          </button>
          <button className="btn btn-outline-secondary" onClick={handleReset}>
            Submit another dataset
          </button>
          <button className="btn btn-outline-secondary" onClick={() => onNavigate('datasets', {})}>
            Back to datasets
          </button>
        </div>
      </div>
    )
  }

  return (
    <div>
      <p className="text-muted">
        Registering a dataset is two separate operations: first its metadata is created (6G-DALI
        Metadata Application Profile), then a file is uploaded and registered as a distribution of
        it — configure quality checks and review before that upload, which stages the file and
        triggers automatic validation.
      </p>

      <div className="wizard-progress">
        {STEPS.map((title, i) => {
          const locked = i < minStep
          const state = i === step ? ' active' : i < step ? ' done' : ''
          return (
            <button
              type="button"
              key={title}
              className={`wizard-progress-step${state}`}
              onClick={() => goToStep(i)}
              disabled={locked}
              title={locked ? 'Dataset already created — metadata can no longer be edited' : undefined}
            >
              <span className="wizard-progress-num">{locked ? '✓' : i + 1}</span>
              {title}
            </button>
          )
        })}
      </div>

      {createdDataset && (
        <div className="alert alert-success py-2 small" role="status">
          Dataset created (<code>{createdDataset.datasetId}</code>) — its metadata is locked in; continue below to add a distribution.
        </div>
      )}

      {error && <ErrorMessage message={error} />}

      <form onSubmit={handleAddDistribution}>

        {/* ── Step 1: Metadata ─────────────────────────────────────────── */}
        {step === 0 && (
          <>
            <Card title="Basic Information" obligation="Mandatory">
              <Field label="Title" required>
                <input className="form-control" value={identity.title}
                  onChange={e => setIdentity({ ...identity, title: e.target.value })} required />
              </Field>
              <Field label="Description" required>
                <textarea className="form-control" rows={3} value={identity.description}
                  onChange={e => setIdentity({ ...identity, description: e.target.value })} required />
              </Field>
              <div className="row">
                <div className="col-md-6">
                  <Field label="Publisher">
                    <input className="form-control" value={identity.publisher_name}
                      onChange={e => setIdentity({ ...identity, publisher_name: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-6">
                  <Field label="Contact email">
                    <input type="email" className="form-control" value={identity.contact_email}
                      onChange={e => setIdentity({ ...identity, contact_email: e.target.value })} />
                  </Field>
                </div>
              </div>
              <div className="row">
                <div className="col-md-6">
                  <Field label="License URL" required>
                    <input className="form-control" value={object.license}
                      onChange={e => setObject({ ...object, license: e.target.value })} required />
                  </Field>
                </div>
                <div className="col-md-6">
                  <Field label="Access rights">
                    <select className="form-select" value={object.access_rights}
                      onChange={e => setObject({ ...object, access_rights: e.target.value as typeof object.access_rights })}>
                      <option value="PUBLIC">PUBLIC</option>
                      <option value="RESTRICTED">RESTRICTED</option>
                      <option value="NON_PUBLIC">NON_PUBLIC</option>
                    </select>
                  </Field>
                </div>
              </div>
              <Field label="Keywords" help="Comma-separated">
                <input className="form-control" value={identity.keywords.join(', ')}
                  onChange={e => setIdentity({ ...identity, keywords: splitList(e.target.value) })} />
              </Field>
              <div className="d-flex gap-4">
                <div className="form-check">
                  <input type="checkbox" className="form-check-input" id="gdpr" checked={object.gdpr_compliant}
                    onChange={e => setObject({ ...object, gdpr_compliant: e.target.checked })} />
                  <label className="form-check-label small" htmlFor="gdpr">GDPR compliant</label>
                </div>
                <div className="form-check">
                  <input type="checkbox" className="form-check-input" id="fair" checked={object.fair_compliant}
                    onChange={e => setObject({ ...object, fair_compliant: e.target.checked })} />
                  <label className="form-check-label small" htmlFor="fair">FAIR compliant</label>
                </div>
                <div className="form-check">
                  <input type="checkbox" className="form-check-input" id="pii" checked={object.contains_pii}
                    onChange={e => setObject({ ...object, contains_pii: e.target.checked })} />
                  <label className="form-check-label small" htmlFor="pii">Contains PII</label>
                </div>
              </div>
            </Card>

            <Card title="Dataset & Testbed Details" obligation="Recommended">
              <div className="row">
                <div className="col-md-3">
                  <Field label="Data start date">
                    <input type="date" className="form-control" value={identity.temporal_start}
                      onChange={e => setIdentity({ ...identity, temporal_start: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-3">
                  <Field label="Data end date">
                    <input type="date" className="form-control" value={identity.temporal_end}
                      onChange={e => setIdentity({ ...identity, temporal_end: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-3">
                  <Field label="Environment" help="e.g. urban, indoors, rural">
                    <input className="form-control" value={testbedContext.environment}
                      onChange={e => setTestbedContext({ ...testbedContext, environment: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-3">
                  <Field label="Network domain" help="RAN | Transport | CORE | E2E">
                    <input className="form-control" value={testbedContext.network_domain}
                      onChange={e => setTestbedContext({ ...testbedContext, network_domain: e.target.value })} />
                  </Field>
                </div>
              </div>

              <div className="row">
                <div className="col-md-3">
                  <Field label="RAN NR type" help="NR-SA | NR-NSA | LTE">
                    <input className="form-control" value={testbedContext.ran_new_radio_type}
                      onChange={e => setTestbedContext({ ...testbedContext, ran_new_radio_type: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-3">
                  <Field label="RAN frequency band">
                    <input className="form-control" value={testbedContext.ran_frequency_band}
                      onChange={e => setTestbedContext({ ...testbedContext, ran_frequency_band: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-3">
                  <Field label="RAN bandwidth (MHz)">
                    <input type="number" className="form-control" value={testbedContext.ran_bandwidth_mhz}
                      onChange={e => setTestbedContext({ ...testbedContext, ran_bandwidth_mhz: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-3">
                  <Field label="Core solution" help="OpenSource | Commercial">
                    <input className="form-control" value={testbedContext.core_solution}
                      onChange={e => setTestbedContext({ ...testbedContext, core_solution: e.target.value })} />
                  </Field>
                </div>
              </div>

              <div className="row">
                <div className="col-md-4">
                  <Field label="Compute orchestrator" help="Kubernetes | OpenStack | OSM | ONAP">
                    <input className="form-control" value={testbedContext.compute_orchestrator_type}
                      onChange={e => setTestbedContext({ ...testbedContext, compute_orchestrator_type: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-4">
                  <Field label="Slice type">
                    <input className="form-control" value={testbedContext.slice_type}
                      onChange={e => setTestbedContext({ ...testbedContext, slice_type: e.target.value })} />
                  </Field>
                </div>
                <div className="col-md-4">
                  <Field label="Related vertical" help="e.g. CAM, HEALTH">
                    <input className="form-control" value={testbedContext.related_vertical}
                      onChange={e => setTestbedContext({ ...testbedContext, related_vertical: e.target.value })} />
                  </Field>
                </div>
              </div>

              <Field label="Measurement family / tools" help="Comma-separated, e.g. DRB, RRC / tcpdump, Prometheus exporter">
                <div className="row">
                  <div className="col-md-6">
                    <input className="form-control" placeholder="Measurement family" value={testbedContext.measurement_family.join(', ')}
                      onChange={e => setTestbedContext({ ...testbedContext, measurement_family: splitList(e.target.value) })} />
                  </div>
                  <div className="col-md-6">
                    <input className="form-control" placeholder="Measurement tools" value={testbedContext.measurement_tool.join(', ')}
                      onChange={e => setTestbedContext({ ...testbedContext, measurement_tool: splitList(e.target.value) })} />
                  </div>
                </div>
              </Field>
            </Card>

            <Card title="Additional Information" obligation="Optional">
              <Field label="Produced by (GAIA-X participant URI)">
                <input className="form-control" value={object.produced_by}
                  onChange={e => setObject({ ...object, produced_by: e.target.value })} />
              </Field>
              <div className="row">
                <div className="col-md-6">
                  <Field label="Related publications" help="Comma-separated URIs">
                    <input className="form-control" value={identity.related_publications.join(', ')}
                      onChange={e => setIdentity({ ...identity, related_publications: splitList(e.target.value) })} />
                  </Field>
                </div>
                <div className="col-md-6">
                  <Field label="Additional contributors" help="Comma-separated names">
                    <input className="form-control" value={identity.contributors.join(', ')}
                      onChange={e => setIdentity({ ...identity, contributors: splitList(e.target.value) })} />
                  </Field>
                </div>
              </div>
            </Card>
          </>
        )}

        {/* ── Step 2: Data ─────────────────────────────────────────────── */}
        {step === 1 && (
          <Card title="Data Submission" obligation="Mandatory">
            <Field label="File" required help="Uploaded to the Data Lake at <catalogue_id>/<dataset_id>/<uuid>.<ext> — the original filename is kept as the distribution's title; <uuid>/<ext> are generated at upload time.">
              <input type="file" className="form-control" onChange={handleFileChange} required />
            </Field>
            {file && (
              <div className="alert alert-light border small mb-3">
                <strong>{file.name}</strong> ({(file.size / (1024 * 1024)).toFixed(2)} MB)
              </div>
            )}

            <Field label="Dataset columns / measured variables" help="Comma-separated. Auto-filled from the file above if left empty; used to drive Quality Checks.">
              <input className="form-control" value={metrics.variable_measured.join(', ')}
                onChange={e => setMetrics({ ...metrics, variable_measured: splitList(e.target.value) })} />
            </Field>
            <Field label="Measurement technique">
              <input className="form-control" value={metrics.measurement_technique}
                onChange={e => setMetrics({ ...metrics, measurement_technique: e.target.value })} />
            </Field>

            {previewRows && previewRows.length > 0 && (
              <>
                <label className="form-label small fw-semibold">Detected columns</label>
                <div className="mb-3">
                  {previewRows[0].map(h => (
                    <span key={h} className="badge text-bg-light border me-1 mb-1">{h}</span>
                  ))}
                </div>
                <label className="form-label small fw-semibold">Preview (first rows)</label>
                <div className="table-responsive">
                  <table className="table table-sm table-bordered small mb-0">
                    <thead><tr>{previewRows[0].map((h, i) => <th key={i}>{h}</th>)}</tr></thead>
                    <tbody>
                      {previewRows.slice(1).map((row, ri) => (
                        <tr key={ri}>{row.map((cell, ci) => <td key={ci}>{cell}</td>)}</tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </Card>
        )}

        {/* ── Step 3: Quality Checks ───────────────────────────────────── */}
        {step === 2 && (
          <>
            <Card title="Table-Level Checks" obligation="Recommended">
              <div className="form-check mb-2">
                <input type="checkbox" className="form-check-input" id="rowcount" checked={rowCountEnabled}
                  onChange={e => setRowCountEnabled(e.target.checked)} />
                <label className="form-check-label" htmlFor="rowcount">
                  <code className="small">expect_table_row_count_to_be_between</code>
                </label>
              </div>
              {rowCountEnabled && (
                <div className="row">
                  <div className="col-md-3">
                    <Field label="Min rows">
                      <input className="form-control" value={rowCountMin} onChange={e => setRowCountMin(e.target.value)} />
                    </Field>
                  </div>
                  <div className="col-md-3">
                    <Field label="Max rows">
                      <input className="form-control" placeholder="unbounded" value={rowCountMax} onChange={e => setRowCountMax(e.target.value)} />
                    </Field>
                  </div>
                </div>
              )}
            </Card>

            <Card title="Per-Column Checks" obligation="Auto-generated">
              {columns.length === 0 ? (
                <p className="text-muted small mb-0">
                  No columns declared yet — set "Dataset columns" in the Data step, or upload a
                  CSV/TSV file there to auto-detect them.
                </p>
              ) : (
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
            </Card>

            <Card title="Additional Column Checks" obligation="Optional">
              <div className="d-flex gap-2 flex-wrap align-items-end mb-3">
                <div>
                  <label className="form-label small">Check type</label>
                  <select className="form-select form-select-sm" value={addType} onChange={e => setAddType(e.target.value as AddCheckType)}>
                    {(Object.keys(ADD_CHECK_LABELS) as AddCheckType[]).map(t => (
                      <option key={t} value={t}>{ADD_CHECK_LABELS[t]}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="form-label small">Column</label>
                  <select className="form-select form-select-sm" value={addColumn} onChange={e => setAddColumn(e.target.value)}>
                    <option value="">— select —</option>
                    {columns.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                {addType === 'between' && (
                  <>
                    <div>
                      <label className="form-label small">Min</label>
                      <input className="form-control form-control-sm" style={{ width: 90 }} value={addMin} onChange={e => setAddMin(e.target.value)} />
                    </div>
                    <div>
                      <label className="form-label small">Max</label>
                      <input className="form-control form-control-sm" style={{ width: 90 }} value={addMax} onChange={e => setAddMax(e.target.value)} />
                    </div>
                  </>
                )}
                {(addType === 'inset' || addType === 'notinset') && (
                  <div>
                    <label className="form-label small">Values</label>
                    <input className="form-control form-control-sm" placeholder="val1, val2" value={addValues} onChange={e => setAddValues(e.target.value)} />
                  </div>
                )}
                <button type="button" className="btn btn-sm btn-primary" onClick={addCheck} disabled={!addColumn}>
                  + Add
                </button>
              </div>

              {addedChecks.length > 0 && (
                <ul className="list-group">
                  {addedChecks.map(c => (
                    <li key={c.id} className="list-group-item d-flex justify-content-between align-items-center small">
                      <span>
                        <code>{ADD_CHECK_LABELS[c.type]}</code> — column <code>{c.column}</code>
                        {c.minValue && `, min: ${c.minValue}`}
                        {c.maxValue && `, max: ${c.maxValue}`}
                        {c.values && `, values: ${c.values}`}
                      </span>
                      <button type="button" className="btn btn-sm btn-outline-danger" onClick={() => removeCheck(c.id)}>×</button>
                    </li>
                  ))}
                </ul>
              )}
            </Card>
          </>
        )}

        {/* ── Step 4: Review & Submit ──────────────────────────────────── */}
        {step === 3 && (
          <>
            <Card title="Review your submission" obligation="Mandatory">
              <div className="mb-4">
                <h6 className="text-uppercase small fw-bold text-muted border-bottom pb-2 mb-2">Metadata</h6>
                <dl className="row small mb-0">
                  <dt className="col-sm-3">Title</dt><dd className="col-sm-9">{identity.title || '—'}</dd>
                  <dt className="col-sm-3">Publisher</dt><dd className="col-sm-9">{identity.publisher_name || '—'}</dd>
                  <dt className="col-sm-3">License</dt><dd className="col-sm-9">{object.license || '—'}</dd>
                  <dt className="col-sm-3">Access rights</dt><dd className="col-sm-9">{object.access_rights}</dd>
                  <dt className="col-sm-3">Keywords</dt>
                  <dd className="col-sm-9">
                    {identity.keywords.length
                      ? identity.keywords.map(k => <span key={k} className="badge text-bg-light border me-1">{k}</span>)
                      : '—'}
                  </dd>
                  <dt className="col-sm-3">Columns</dt>
                  <dd className="col-sm-9">
                    {columns.length ? columns.map(c => <code key={c} className="me-2">{c}</code>) : '—'}
                  </dd>
                </dl>
              </div>
              <div className="mb-4">
                <h6 className="text-uppercase small fw-bold text-muted border-bottom pb-2 mb-2">Data</h6>
                <dl className="row small mb-0">
                  <dt className="col-sm-3">File</dt><dd className="col-sm-9">{file ? file.name : '—'}</dd>
                </dl>
              </div>
              <div>
                <h6 className="text-uppercase small fw-bold text-muted border-bottom pb-2 mb-2">Quality Checks</h6>
                <div className="small">
                  {buildExpectations().map((exp, i) => (
                    <span key={i} className="badge text-bg-light border me-1 mb-1 font-monospace">
                      {exp.type}{exp.column ? `(${exp.column})` : ''}
                    </span>
                  ))}
                  {buildExpectations().length === 0 && '—'}
                </div>
              </div>
            </Card>

            <div className="card mb-3">
              <div className="card-body">
                <div className="form-check">
                  <input type="checkbox" className="form-check-input" id="confirm" checked={confirmRights}
                    onChange={e => setConfirmRights(e.target.checked)} />
                  <label className="form-check-label small" htmlFor="confirm">
                    I confirm that I hold the necessary rights to share this dataset under the stated
                    license, and that the information provided is accurate.
                  </label>
                </div>
              </div>
            </div>
          </>
        )}

        <div className="d-flex justify-content-between mb-4">
          <button type="button" className="btn btn-outline-secondary" onClick={() => onNavigate('datasets', {})}>
            Cancel
          </button>
          <div className="d-flex gap-2">
            <button type="button" className="btn btn-outline-secondary" onClick={handleBack} disabled={step === minStep}>
              Back
            </button>
            {step === 0 ? (
              <button type="button" className="btn btn-primary" onClick={handleCreateDataset} disabled={submitting}>
                {submitting && <span className="spinner-border spinner-border-sm me-1" />}
                {submitting ? 'Creating…' : 'Create Dataset'}
              </button>
            ) : isLastStep ? (
              <button type="submit" className="btn btn-primary" disabled={submitting}>
                {submitting && <span className="spinner-border spinner-border-sm me-1" />}
                {submitting ? 'Uploading…' : 'Add Distribution'}
              </button>
            ) : (
              <button type="button" className="btn btn-primary" onClick={handleNext}>
                Next
              </button>
            )}
          </div>
        </div>
      </form>
    </div>
  )
}
