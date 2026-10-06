import { useState } from 'react'
import { FiCheckCircle, FiXCircle, FiDownload, FiAlertTriangle } from 'react-icons/fi'
import { downloadTestbedBundle, provisionTestbed, registerTestbed } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import type { NavigateFn, Testbed, TestbedCreateRequest } from '../types'

const SLUG_RE = /^[a-z][a-z0-9-]{1,30}[a-z0-9]$/

const STEP_LABELS: Record<string, string> = {
  bucket: 'Data Lake bucket',
  credentials: 'Scoped Data Lake key',
  catalogue: 'Catalogue',
}

const WIZARD_STEPS = ['Details', 'Identity', 'Provision', 'Connector bundle']

interface FormState {
  slug: string
  name: string
  organisation: string
  contact_email: string
  produced_by_iri: string
  participant_id: string
  bucket: string
  experiment_prefix: string
  dsp_url: string
}

const EMPTY: FormState = {
  slug: '', name: '', organisation: '', contact_email: '', produced_by_iri: '',
  participant_id: '', bucket: '', experiment_prefix: '', dsp_url: '',
}

interface TestbedRegisterProps {
  onNavigate: NavigateFn
}

export default function TestbedRegister({ onNavigate }: TestbedRegisterProps) {
  const [step, setStep] = useState(0)
  const [form, setForm] = useState<FormState>(EMPTY)
  const [testbed, setTestbed] = useState<Testbed | null>(null)
  const [followups, setFollowups] = useState<string[]>([])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const set = (k: keyof FormState) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm(f => ({ ...f, [k]: e.target.value }))

  const slugOk = SLUG_RE.test(form.slug)
  const detailsOk = slugOk && form.name.trim().length > 0

  async function run<T>(fn: () => Promise<T>): Promise<T | undefined> {
    setBusy(true)
    setError(null)
    try {
      return await fn()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      return undefined
    } finally {
      setBusy(false)
    }
  }

  async function handleRegister() {
    const body: TestbedCreateRequest = { slug: form.slug, name: form.name.trim() }
    for (const k of ['organisation', 'contact_email', 'produced_by_iri', 'participant_id', 'bucket', 'experiment_prefix', 'dsp_url'] as const) {
      if (form[k].trim()) body[k] = form[k].trim()
    }
    const created = await run(() => registerTestbed(body))
    if (created) {
      setTestbed(created)
      setStep(2)
      await handleProvision(created.slug)
    }
  }

  async function handleProvision(slug: string) {
    const result = await run(() => provisionTestbed(slug))
    if (result) {
      setTestbed(result.testbed)
      setFollowups(result.followups)
    }
  }

  const provisioned = testbed?.status === 'provisioned'

  return (
    <div style={{ maxWidth: 760 }}>
      <ol className="list-inline mb-4">
        {WIZARD_STEPS.map((label, i) => (
          <li key={label} className="list-inline-item me-3">
            <span className={`badge rounded-pill me-1 ${i === step ? 'text-bg-primary' : i < step ? 'text-bg-success' : 'text-bg-light border'}`}>{i + 1}</span>
            <span className={i === step ? 'fw-semibold' : 'text-muted'}>{label}</span>
          </li>
        ))}
      </ol>

      {error && <ErrorMessage message={error} />}

      {step === 0 && (
        <div className="card"><div className="card-body">
          <div className="mb-3">
            <label className="form-label fw-semibold">Short id (slug)</label>
            <input className={`form-control ${form.slug && !slugOk ? 'is-invalid' : ''}`} value={form.slug}
              onChange={set('slug')} placeholder="e.g. kul" />
            <div className="form-text">3-32 characters: lowercase letters, digits and “-”. Drives the default identity.</div>
          </div>
          <div className="mb-3">
            <label className="form-label fw-semibold">Name</label>
            <input className="form-control" value={form.name} onChange={set('name')} placeholder="KU Leuven" />
          </div>
          <div className="row">
            <div className="col-md-6 mb-3">
              <label className="form-label">Organisation</label>
              <input className="form-control" value={form.organisation} onChange={set('organisation')} />
            </div>
            <div className="col-md-6 mb-3">
              <label className="form-label">Operator contact email</label>
              <input className="form-control" type="email" value={form.contact_email} onChange={set('contact_email')} />
            </div>
          </div>
          <div className="mb-3">
            <label className="form-label">GAIA-X participant IRI (produced_by)</label>
            <input className="form-control" value={form.produced_by_iri} onChange={set('produced_by_iri')} />
          </div>
          <button className="btn btn-primary" disabled={!detailsOk} onClick={() => setStep(1)}>Next</button>
        </div></div>
      )}

      {step === 1 && (
        <div className="card"><div className="card-body">
          <p className="text-muted">
            These are generated from the slug. Leave them empty to accept the defaults, or fill them to adopt a
            testbed that is already running with an existing identity or bucket.
          </p>
          <div className="mb-3">
            <label className="form-label">Participant ID (edc.participant.id)</label>
            <input className="form-control" value={form.participant_id} onChange={set('participant_id')} placeholder={`provider-${form.slug}`} />
          </div>
          <div className="row">
            <div className="col-md-6 mb-3">
              <label className="form-label">Data Lake bucket / catalogue id</label>
              <input className="form-control" value={form.bucket} onChange={set('bucket')} placeholder={`6g-dali-${form.slug}`} />
            </div>
            <div className="col-md-6 mb-3">
              <label className="form-label">Experiment prefix</label>
              <input className="form-control" value={form.experiment_prefix} onChange={set('experiment_prefix')} placeholder="same as the bucket" />
            </div>
          </div>
          <div className="mb-3">
            <label className="form-label">DSP URL (edc.dsp.callback.address)</label>
            <input className="form-control" value={form.dsp_url} onChange={set('dsp_url')} placeholder={`https://edc.${form.slug}.6gdali.eu/protocol`} />
          </div>
          <div className="d-flex gap-2">
            <button className="btn btn-outline-secondary" disabled={busy} onClick={() => setStep(0)}>Back</button>
            <button className="btn btn-primary" disabled={busy} onClick={handleRegister}>
              {busy && <span className="spinner-border spinner-border-sm me-1" />}
              Register &amp; provision
            </button>
          </div>
        </div></div>
      )}

      {step === 2 && testbed && (
        <div className="card"><div className="card-body">
          <h6 className="mb-3">{testbed.name} <code className="small text-muted">{testbed.participant_id}</code></h6>
          <ul className="list-unstyled">
            {Object.keys(STEP_LABELS).map(key => {
              const s = testbed.steps[key]
              return (
                <li key={key} className="mb-2">
                  {!s ? <span className="spinner-border spinner-border-sm me-2" />
                    : s.status === 'ok' ? <FiCheckCircle className="text-success me-2" />
                    : <FiXCircle className="text-danger me-2" />}
                  <strong>{STEP_LABELS[key]}</strong>
                  {s && <span className="text-muted small ms-2">{s.status === 'ok' ? s.detail : `${s.status}: ${s.detail}`}</span>}
                </li>
              )
            })}
          </ul>
          {followups.map(f => (
            <div key={f} className="alert alert-warning py-2 small">
              <FiAlertTriangle className="me-1" />{f}
            </div>
          ))}
          <div className="d-flex gap-2">
            {!provisioned && (
              <button className="btn btn-outline-primary" disabled={busy} onClick={() => handleProvision(testbed.slug)}>
                {busy && <span className="spinner-border spinner-border-sm me-1" />}
                Retry failed steps
              </button>
            )}
            <button className="btn btn-primary" disabled={!provisioned} onClick={() => setStep(3)}>Next</button>
          </div>
        </div></div>
      )}

      {step === 3 && testbed && (
        <div className="card"><div className="card-body">
          <p>
            Give the operator this bundle: connector properties with the assigned identity, a docker-compose
            stack, an nginx vhost and a README. It contains no Data Lake credentials; the central connector
            supplies the scoped destination when the transfer is started.
          </p>
          <div className="d-flex gap-2">
            <button className="btn btn-primary" disabled={busy} onClick={() => run(() => downloadTestbedBundle(testbed.slug))}>
              <FiDownload className="me-1" />Download bundle
            </button>
            <button className="btn btn-outline-secondary" onClick={() => onNavigate('testbed-detail', { dagId: testbed.slug })}>
              Open testbed
            </button>
          </div>
        </div></div>
      )}
    </div>
  )
}
