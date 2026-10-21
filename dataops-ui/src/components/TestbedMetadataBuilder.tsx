import { useEffect, useMemo, useState } from 'react'
import { FiDownload, FiCopy, FiUpload, FiSave, FiFilePlus, FiTrash2 } from 'react-icons/fi'
import { getTestbed } from '../api/airflow'
import {
  ALL_FIELDS, SECTIONS, TESTBED_KEYS, defaultValues, importMetadata, newDatasetValues, toJson,
  validateMetadata,
} from '../map/metadataJson'
import type { FieldDef, Values } from '../map/metadataJson'
import ErrorMessage from './ErrorMessage'
import Modal from './Modal'
import type { NavigateFn, Testbed } from '../types'

interface TestbedMetadataBuilderProps {
  /** The testbed to prefill from. Without it the form starts blank. */
  slug: string | null
  onNavigate: NavigateFn
}

const OBLIGATION = { M: ['text-bg-danger', 'mandatory'], R: ['text-bg-warning', 'recommended'], O: ['text-bg-secondary', 'optional'] } as const

// Saved testbed details live in this browser only; every access tolerates storage being blocked.
const storeKey = (slug: string | null) => `dali-metadata-builder:v1:${slug ?? '_'}`
function readSaved(slug: string | null): Values | null {
  try {
    const raw = localStorage.getItem(storeKey(slug))
    return raw ? (JSON.parse(raw) as Values) : null
  } catch { return null }
}
function writeSaved(slug: string | null, v: Values | null): boolean {
  try {
    if (v) localStorage.setItem(storeKey(slug), JSON.stringify(v))
    else localStorage.removeItem(storeKey(slug))
    return true
  } catch { return false }
}

/** What the testbed record already knows; it wins over saved values so a corrected registry entry is not shadowed. */
function fromTestbed(tb: Testbed): Values {
  const v: Values = {}
  if (tb.organisation) v.publisher = tb.organisation
  if (tb.contact_email) v.contact_email = tb.contact_email
  if (tb.produced_by_iri) v.produced_by = tb.produced_by_iri
  return v
}

function FieldInput({ f, value, error, warning, onChange }: {
  f: FieldDef
  value: string | boolean
  error?: string
  warning?: string
  onChange: (v: string | boolean) => void
}) {
  const id = `md-${f.k}`
  if (f.type === 'bool') {
    return (
      <div className="col-12">
        <div className="form-check">
          <input className="form-check-input" type="checkbox" id={id} checked={!!value} onChange={e => onChange(e.target.checked)} />
          <label className="form-check-label" htmlFor={id}>{f.label}</label>
        </div>
        {warning && <div className="form-text text-warning">{warning}</div>}
      </div>
    )
  }
  const cls = `form-control form-control-sm${error ? ' is-invalid' : ''}`
  const str = typeof value === 'string' ? value : ''
  let control: React.ReactNode
  if (f.type === 'select') {
    control = (
      <select id={id} className={`form-select form-select-sm${error ? ' is-invalid' : ''}`} value={str} onChange={e => onChange(e.target.value)}>
        <option value="">{f.req === 'M' ? '' : '— not specified —'}</option>
        {f.opts?.map(o => <option key={o} value={o}>{o}</option>)}
      </select>
    )
  } else if (f.type === 'textarea' || f.type === 'list') {
    control = <textarea id={id} className={cls} rows={f.type === 'list' ? 3 : 3} spellCheck={false} value={str} onChange={e => onChange(e.target.value)} />
  } else {
    const type = ({ date: 'date', email: 'email', int: 'number' } as Record<string, string>)[f.type] ?? 'text'
    control = (
      <input id={id} className={cls} type={type} min={type === 'number' ? 0 : undefined} step={type === 'number' ? 1 : undefined}
        list={f.suggest ? `${id}-list` : undefined} value={str} onChange={e => onChange(e.target.value)} />
    )
  }
  const [badge, badgeText] = f.req ? OBLIGATION[f.req] : [null, null]
  return (
    <div className={f.wide ? 'col-12' : 'col-md-6'}>
      <label className="form-label small mb-1" htmlFor={id}>
        {f.label}{badge && <span className={`badge ${badge} ms-2 fw-normal`}>{badgeText}</span>}
      </label>
      {control}
      {f.suggest && <datalist id={`${id}-list`}>{f.suggest.map(o => <option key={o} value={o} />)}</datalist>}
      {error ? <div className="text-danger small mt-1">{error}</div>
        : warning ? <div className="text-warning small mt-1">{warning}</div>
        : f.hint ? <div className="form-text mt-0">{f.hint}</div> : null}
    </div>
  )
}

export default function TestbedMetadataBuilder({ slug, onNavigate }: TestbedMetadataBuilderProps) {
  const [values, setValues] = useState<Values>(() => ({ ...defaultValues(), ...(readSaved(slug) ?? {}) }))
  const [testbed, setTestbed] = useState<Testbed | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)
  const [importing, setImporting] = useState(false)
  const [importText, setImportText] = useState('')
  const [importError, setImportError] = useState<string | null>(null)

  useEffect(() => {
    if (!slug) return
    getTestbed(slug)
      .then(tb => { setTestbed(tb); setValues(v => ({ ...v, ...fromTestbed(tb) })) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [slug])

  const set = (k: string) => (v: string | boolean) => setValues(cur => ({ ...cur, [k]: v }))

  const { errors, warnings } = useMemo(() => validateMetadata(values), [values])
  const json = useMemo(() => toJson(values), [values])
  const errorCount = Object.keys(errors).length
  const warningCount = Object.keys(warnings).length
  const labelOf = (k: string) => ALL_FIELDS.find(f => f.k === k)?.label ?? k

  function download() {
    const url = URL.createObjectURL(new Blob([json], { type: 'application/json' }))
    const a = document.createElement('a')
    a.href = url
    a.download = 'metadata.json'
    document.body.append(a)
    a.click()
    a.remove()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  }

  async function copy() {
    try {
      await navigator.clipboard.writeText(json)
      setCopied(true)
      setTimeout(() => setCopied(false), 1400)
    } catch {
      setError('Could not copy to the clipboard. Use Download instead.')
    }
  }

  function saveDetails() {
    const keep: Values = {}
    for (const f of ALL_FIELDS) if (f.tc || TESTBED_KEYS.includes(f.k)) keep[f.k] = values[f.k]
    setNotice(writeSaved(slug, keep) ? 'Testbed details saved in this browser.' : 'Could not save: browser storage is unavailable.')
  }

  function runImport() {
    try {
      setValues(importMetadata(defaultValues(), JSON.parse(importText)))
      setImporting(false)
      setImportText('')
      setImportError(null)
      setNotice('Imported. Check the fields below before downloading.')
    } catch (e) {
      setImportError(e instanceof Error ? e.message : String(e))
    }
  }

  return (
    <div>
      {error && <ErrorMessage message={error} />}
      {notice && <div className="alert alert-success py-2" role="status">{notice}</div>}

      <p className="text-muted">
        {testbed ? <>Metadata for datasets from <strong>{testbed.name}</strong>. </> : null}
        Fill in the form and download <code>metadata.json</code>. Upload it to your bucket before, or together with,
        the data files of the dataset. The central registration reads it and registers the dataset
        {testbed ? <> in the catalogue <code>{testbed.catalogue_id}</code></> : ' in the catalogue'}.
        Nothing you type here is sent to the server.
      </p>

      <div className="row g-3">
        <div className="col-xl-7">
          {SECTIONS.map(s => (
            <div className="card mb-3" key={s.id}>
              <div className="card-body">
                <h6 className="mb-0">{s.title}</h6>
                <div className="text-muted small mb-3">{s.desc}</div>
                <div className="row g-2">
                  {s.fields.map(f => (
                    <FieldInput key={f.k} f={f} value={values[f.k] ?? ''} error={errors[f.k]} warning={warnings[f.k]} onChange={set(f.k)} />
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>

        <div className="col-xl-5">
          <div className="position-sticky" style={{ top: 12 }}>
            <div className="card mb-3">
              <div className="card-body">
                <div className={`fw-semibold mb-2 ${errorCount ? 'text-danger' : 'text-success'}`} role="status">
                  {errorCount ? `${errorCount} problem${errorCount !== 1 ? 's' : ''} to fix`
                    : warningCount ? `Valid · ${warningCount} recommended field${warningCount !== 1 ? 's' : ''} empty` : 'Valid'}
                </div>
                {errorCount > 0 && (
                  <ul className="small text-danger ps-3 mb-2">
                    {Object.entries(errors).map(([k, m]) => <li key={k}>{labelOf(k)}: {m}</li>)}
                  </ul>
                )}
                <pre className="small bg-body-tertiary border rounded p-2 mb-2" style={{ maxHeight: '48vh', overflow: 'auto' }}
                  aria-label="Generated metadata.json">{json}</pre>
                <div className="d-flex flex-wrap gap-2">
                  <button className="btn btn-sm btn-primary" disabled={errorCount > 0} onClick={download}>
                    <FiDownload className="me-1" />Download metadata.json
                  </button>
                  <button className="btn btn-sm btn-outline-secondary" onClick={copy}>
                    <FiCopy className="me-1" />{copied ? 'Copied' : 'Copy'}
                  </button>
                  <button className="btn btn-sm btn-outline-secondary" onClick={() => setImporting(true)}>
                    <FiUpload className="me-1" />Import…
                  </button>
                </div>
              </div>
            </div>

            <div className="card mb-3">
              <div className="card-body">
                <h6 className="mb-1">Reuse for the next dataset</h6>
                <p className="text-muted small">
                  Your organisation, contacts, licence and testbed setup rarely change. Save them in this browser,
                  then start the next dataset without retyping them.
                </p>
                <div className="d-flex flex-wrap gap-2">
                  <button className="btn btn-sm btn-outline-primary" onClick={saveDetails}><FiSave className="me-1" />Save testbed details</button>
                  <button className="btn btn-sm btn-outline-secondary" onClick={() => { setValues(newDatasetValues(values)); setNotice('Dataset-specific fields cleared; testbed details kept.') }}>
                    <FiFilePlus className="me-1" />New dataset
                  </button>
                  <button className="btn btn-sm btn-outline-danger" onClick={() => {
                    writeSaved(slug, null)
                    setValues({ ...defaultValues(), ...(testbed ? fromTestbed(testbed) : {}) })
                    setNotice('Saved details removed and form reset.')
                  }}>
                    <FiTrash2 className="me-1" />Forget saved
                  </button>
                </div>
              </div>
            </div>

            {slug && (
              <button className="btn btn-sm btn-link px-0" onClick={() => onNavigate('testbed-detail', { dagId: slug })}>
                ← Back to {testbed?.name ?? slug}
              </button>
            )}
          </div>
        </div>
      </div>

      {importing && (
        <Modal
          title="Import an existing metadata.json"
          onClose={() => setImporting(false)}
          footer={
            <>
              <button className="btn btn-outline-secondary" onClick={() => setImporting(false)}>Cancel</button>
              <button className="btn btn-primary" disabled={!importText.trim()} onClick={runImport}>Load into form</button>
            </>
          }
        >
          <p className="small text-muted">Paste the JSON or choose a file. Known fields fill the form; unknown keys are ignored and camelCase keys are accepted.</p>
          <input type="file" className="form-control form-control-sm mb-2" accept=".json,application/json" aria-label="metadata.json file"
            onChange={async e => { const file = e.target.files?.[0]; if (file) setImportText(await file.text()) }} />
          <textarea className="form-control font-monospace small" rows={10} spellCheck={false} aria-label="metadata.json contents"
            placeholder='{ "title": "…" }' value={importText} onChange={e => setImportText(e.target.value)} />
          {importError && <div className="text-danger small mt-2">Cannot import: {importError}</div>}
        </Modal>
      )}
    </div>
  )
}
