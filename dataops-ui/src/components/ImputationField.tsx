import { useEffect, useRef, useState } from 'react'
import { FiAlertTriangle, FiCode, FiPlus, FiSliders, FiTrash2 } from 'react-icons/fi'
import {
  LIBS, WSP_OPTIONS, buildImputation, newRun, parseImputation, validateForm,
  type Device, type ImputationForm, type Lib, type RunForm,
} from '../lib/imputation'

interface ImputationFieldProps {
  /** The parameter as JSON text, the way the trigger form keeps every parameter. */
  value: string
  onChange: (json: string) => void
  /** What blocks triggering (a typed value that would be dropped); empty when the form can be sent. */
  onErrorsChange?: (errors: string[]) => void
}

const LIB_IDS = Object.keys(LIBS) as Lib[]

/** The form for a JSON text, or null when it is not JSON or has a shape the selectors cannot show. */
function formFromText(text: string): ImputationForm | null {
  if (!text.trim()) return parseImputation(undefined)
  try {
    return parseImputation(JSON.parse(text))
  } catch {
    return null
  }
}

const toJson = (form: ImputationForm) => JSON.stringify(buildImputation(form))

/** The first method of `lib` that no run of the list uses yet, or null when it has none left. */
function unusedMethod(runs: RunForm[], lib: Lib): string | null {
  return LIBS[lib].methods.find(m => !runs.some(r => r.lib === lib && r.method === m.id))?.id ?? null
}

/**
 * The `imputation` parameter of the validate DAG as selectors: which methods to run, whether to fill the gaps, whether
 * to convert units. The JSON the DAG reads is built from them (lib/imputation.ts) and shown underneath; "Edit JSON"
 * switches to the raw text for anything the selectors do not cover.
 */
export default function ImputationField({ value, onChange, onErrorsChange }: ImputationFieldProps) {
  const initial = useRef(formFromText(value)).current
  const [mode, setMode] = useState<'form' | 'json'>(initial ? 'form' : 'json')
  const [form, setForm] = useState<ImputationForm>(initial ?? parseImputation(undefined)!)
  const [jsonError, setJsonError] = useState<string | null>(null)

  const problems = validateForm(form)
  const errorKey = mode === 'form' ? problems.errors.join('\n') : ''

  // Write out what the form builds, so what is shown is what is sent: an empty parameter becomes the DAG's defaults, and
  // a value typed by hand gets the settings it left out (`convert_units`) spelled out.
  useEffect(() => {
    if (initial && toJson(initial) !== value) onChange(toJson(initial))
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once, for the value the field was opened with
  }, [])

  useEffect(() => {
    onErrorsChange?.(errorKey ? errorKey.split('\n') : [])
  }, [errorKey, onErrorsChange])

  function update(next: ImputationForm) {
    setForm(next)
    onChange(toJson(next))
  }

  function updateRun(id: string, patch: (run: RunForm) => RunForm) {
    update({ ...form, runs: form.runs.map(r => (r.id === id ? patch(r) : r)) })
  }

  function changeLib(run: RunForm, lib: Lib) {
    const method = unusedMethod(form.runs.filter(r => r.id !== run.id), lib) ?? LIBS[lib].methods[0].id
    updateRun(run.id, r => ({ ...r, lib, method, device: 'cpu', options: {}, extra: {} }))
  }

  function addRun() {
    // The first lib that still has a method to offer, starting with the one the last run used.
    const last = form.runs[form.runs.length - 1]?.lib
    const order = last ? [last, ...LIB_IDS.filter(l => l !== last)] : LIB_IDS
    for (const lib of order) {
      const method = unusedMethod(form.runs, lib)
      if (method) {
        update({ ...form, runs: [...form.runs, newRun(lib, method)] })
        return
      }
    }
  }

  function showJson() {
    setJsonError(null)
    setMode('json')
  }

  function showSelectors() {
    const parsed = formFromText(value)
    if (!parsed) {
      setJsonError('This JSON is not valid, or has a shape the selectors cannot show. Fix it here, or remove what is not needed.')
      return
    }
    setJsonError(null)
    setForm(parsed)
    setMode('form')
  }

  if (mode === 'json') {
    return (
      <div>
        <textarea
          className={`form-control form-control-sm font-monospace${jsonError ? ' is-invalid' : ''}`}
          rows={9}
          spellCheck={false}
          value={value}
          onChange={e => onChange(e.target.value)}
        />
        {jsonError && <div className="invalid-feedback d-block">{jsonError}</div>}
        <button type="button" className="btn btn-sm btn-outline-secondary mt-2" onClick={showSelectors}>
          <FiSliders className="me-1" />Use selectors
        </button>
      </div>
    )
  }

  const canAdd = LIB_IDS.some(lib => unusedMethod(form.runs, lib))

  return (
    <div className="rounded border p-2">
      <div className="form-check form-switch">
        <input
          id="imputation-fill" className="form-check-input" type="checkbox" role="switch"
          checked={form.impute} onChange={e => update({ ...form, impute: e.target.checked })}
        />
        <label className="form-check-label small" htmlFor="imputation-fill">Fill the gaps</label>
        <div className="form-text mt-0">Off: the bundle is built and nothing is filled.</div>
      </div>

      <div className="form-check form-switch mt-2">
        <input
          id="imputation-units" className="form-check-input" type="checkbox" role="switch"
          checked={form.convertUnits} onChange={e => update({ ...form, convertUnits: e.target.checked })}
        />
        <label className="form-check-label small" htmlFor="imputation-units">Convert units (6G schema)</label>
        <div className="form-text mt-0">
          By column name: <code>ram_limit</code> and <code>ram_usage</code> to MB, latency microseconds to ms,
          <code> cpu_usage</code> percent to a fraction. The converted columns are renamed (<code>_mb</code>,{' '}
          <code>_ms</code>). Off keeps the data as it came.
        </div>
      </div>

      {form.impute && (
        <>
          <div className="d-flex align-items-center justify-content-between mt-3 mb-1">
            <span className="small fw-bold">Methods</span>
            <button type="button" className="btn btn-sm btn-outline-primary" disabled={!canAdd} onClick={addRun}>
              <FiPlus className="me-1" />Add method
            </button>
          </div>

          {form.runs.map(run => {
            const methodHint = LIBS[run.lib].methods.find(m => m.id === run.method)?.hint
            const specs = run.lib === 'wavestitchplus' ? (WSP_OPTIONS[run.method] ?? []) : []
            const fast = run.options.fast === true
            return (
              <div key={run.id} className="border rounded p-2 mb-2 bg-body-tertiary">
                <div className="d-flex gap-2 align-items-center">
                  <select
                    className="form-select form-select-sm" aria-label="Library" style={{ maxWidth: 140 }}
                    value={run.lib} onChange={e => changeLib(run, e.target.value as Lib)}
                  >
                    {LIB_IDS.map(lib => <option key={lib} value={lib}>{LIBS[lib].label}</option>)}
                  </select>
                  <select
                    className="form-select form-select-sm" aria-label="Method"
                    value={run.method}
                    onChange={e => updateRun(run.id, r => ({ ...r, method: e.target.value, options: {}, extra: {} }))}
                  >
                    {LIBS[run.lib].methods.map(m => (
                      <option key={m.id} value={m.id} disabled={m.id !== run.method && form.runs.some(r => r.id !== run.id && r.lib === run.lib && r.method === m.id)}>
                        {m.id}
                      </option>
                    ))}
                  </select>
                  {run.lib === 'wavestitchplus' && (
                    <select
                      className="form-select form-select-sm" aria-label="Device" style={{ maxWidth: 100 }}
                      value={run.device} onChange={e => updateRun(run.id, r => ({ ...r, device: e.target.value as Device }))}
                    >
                      <option value="cpu">CPU</option>
                      <option value="gpu">GPU</option>
                      <option value="auto">Auto</option>
                    </select>
                  )}
                  <button
                    type="button" className="btn btn-sm btn-outline-danger" aria-label="Remove method" title="Remove"
                    onClick={() => update({ ...form, runs: form.runs.filter(r => r.id !== run.id) })}
                  >
                    <FiTrash2 />
                  </button>
                </div>
                {methodHint && <div className="form-text mt-1 mb-0">{methodHint}</div>}

                {specs.length > 0 && (
                  <div className="d-flex flex-wrap gap-2 align-items-end mt-2">
                    {specs.map(spec => {
                      const val = run.options[spec.key]
                      const set = (v: string | boolean) => updateRun(run.id, r => ({ ...r, options: { ...r.options, [spec.key]: v } }))
                      if (spec.kind === 'flag') {
                        return (
                          <div key={spec.key} className="form-check mb-1">
                            <input
                              id={`${run.id}-${spec.key}`} className="form-check-input" type="checkbox"
                              checked={val === true} onChange={e => set(e.target.checked)}
                            />
                            <label className="form-check-label small" htmlFor={`${run.id}-${spec.key}`} title="Tiny settings for a quick end-to-end check">
                              {spec.label}
                            </label>
                          </div>
                        )
                      }
                      const text = typeof val === 'string' ? val : ''
                      const disabled = fast && run.method === 'v1'
                      return (
                        <div key={spec.key} style={{ width: 118 }}>
                          <label className="form-label small mb-0" htmlFor={`${run.id}-${spec.key}`}>{spec.label}</label>
                          {spec.kind === 'choice' ? (
                            <select
                              id={`${run.id}-${spec.key}`} className="form-select form-select-sm"
                              value={text} onChange={e => set(e.target.value)}
                            >
                              <option value="">default</option>
                              {spec.choices?.map(c => <option key={c} value={c}>{c}</option>)}
                            </select>
                          ) : (
                            <input
                              id={`${run.id}-${spec.key}`} type="text" inputMode="decimal"
                              className="form-control form-control-sm" placeholder={spec.placeholder}
                              disabled={disabled} value={text} onChange={e => set(e.target.value)}
                            />
                          )}
                        </div>
                      )
                    })}
                  </div>
                )}
                {run.lib === 'wavestitchplus' && fast && run.method === 'v1' && (
                  <div className="form-text mb-0">Fast sets its own small values; the other options are ignored.</div>
                )}
              </div>
            )
          })}
          {form.runs.some(r => r.lib === 'wavestitchplus') && (
            <div className="form-text mt-0 mb-1">{LIBS.wavestitchplus.hint}</div>
          )}
        </>
      )}

      {problems.errors.map(message => (
        <div key={message} className="small text-danger mt-1"><FiAlertTriangle className="me-1" />{message}</div>
      ))}
      {problems.notes.map(message => (
        <div key={message} className="small text-warning-emphasis mt-1"><FiAlertTriangle className="me-1" />{message}</div>
      ))}

      <details className="mt-2">
        <summary className="small text-muted">JSON sent to the DAG</summary>
        <pre className="small bg-body-tertiary rounded p-2 mb-0 mt-1" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {JSON.stringify(buildImputation(form), null, 2)}
        </pre>
      </details>
      <button type="button" className="btn btn-sm btn-link px-0 mt-1" onClick={showJson}>
        <FiCode className="me-1" />Edit JSON
      </button>
    </div>
  )
}
