import { useState } from 'react'
import { FiCheckCircle, FiXCircle, FiDownload, FiAlertTriangle } from 'react-icons/fi'
import { downloadTestbedBundle, provisionTestbed, registerTestbed } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import UploadKit from './UploadKit'
import type { NavigateFn, Testbed, TestbedCreateRequest, UploadCredentials } from '../types'

const SLUG_RE = /^[a-z][a-z0-9]{2,31}$/

/** The slug as typed: lowercase, letters and digits only (anything else is dropped as it is typed). */
const cleanSlug = (value: string) => value.toLowerCase().replace(/[^a-z0-9]/g, '')

const STEP_LABELS: Record<string, string> = {
  bucket: 'Data Lake bucket',
  credentials: 'Scoped Data Lake key',
  catalogue: 'Catalogue',
}

/** The extra steps of a testbed hosted by the facility connector. */
const HOSTED_STEP_LABELS: Record<string, string> = {
  upload_bucket: 'Upload bucket',
  upload_key: 'Upload key',
  asset: 'Asset on the facility connector',
  transfer: 'Transfer into the Data Lake',
}

/** What each connector choice means, shown under the choice. */
const HOSTING_HINT = {
  own: 'The testbed runs its own connector (a bundle is generated for it) and offers its data from there.',
  facility: 'The testbed has no connector. It gets an upload bucket and key in the Data Lake, and the facility ' +
    'connector moves each uploaded file into the Data Space.',
}

interface FormState {
  slug: string
  name: string
  organisation: string
  contact_email: string
  produced_by_iri: string
  logo_url: string
  participant_id: string
  bucket: string
  experiment_prefix: string
  dsp_url: string
}

const EMPTY: FormState = {
  slug: '', name: '', organisation: '', contact_email: '', produced_by_iri: '', logo_url: '',
  participant_id: '', bucket: '', experiment_prefix: '', dsp_url: '',
}

/** What each catalogue visibility means, shown under the choice. */
const VISIBILITY_HINT = {
  public: 'Its datasets are visible to everyone (dct:accessRights PUBLIC).',
  private: 'Its datasets are visible only to the testbed’s members and administrators (dct:accessRights NON_PUBLIC).',
}

interface TestbedRegisterProps {
  onNavigate: NavigateFn
}

export default function TestbedRegister({ onNavigate }: TestbedRegisterProps) {
  const [step, setStep] = useState(0)
  const [form, setForm] = useState<FormState>(EMPTY)
  const [isPrivate, setIsPrivate] = useState(false)
  const [hosted, setHosted] = useState(false)
  const [uploadKey, setUploadKey] = useState<UploadCredentials | null>(null)
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
    const fields = ['organisation', 'contact_email', 'produced_by_iri', 'logo_url', 'bucket', 'experiment_prefix',
      ...(hosted ? [] : ['participant_id', 'dsp_url'] as const)] as const
    for (const k of fields) {
      if (form[k].trim()) body[k] = form[k].trim()
    }
    if (isPrivate) body.private = true
    if (hosted) body.hosted = true
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
      if (result.issued_upload_credentials) setUploadKey(result.issued_upload_credentials)  // shown once
    }
  }

  const provisioned = testbed?.status === 'provisioned'
  const isHosted = testbed ? testbed.hosting === 'facility' : hosted
  const wizardSteps = ['Details', 'Identity', 'Provision', isHosted ? 'Upload kit' : 'Connector bundle']
  const stepLabels = isHosted ? { ...STEP_LABELS, ...HOSTED_STEP_LABELS } : STEP_LABELS

  return (
    <div style={{ maxWidth: 760 }}>
      <ol className="list-inline mb-4">
        {wizardSteps.map((label, i) => (
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
              onChange={e => setForm(f => ({ ...f, slug: cleanSlug(e.target.value) }))} placeholder="e.g. kul" maxLength={32} />
            <div className="form-text">3-32 characters: lowercase letters and digits only, starting with a letter. Drives the default identity.</div>
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
          <div className="mb-3">
            <label className="form-label">Logo URL</label>
            <input className="form-control" type="url" value={form.logo_url} onChange={set('logo_url')}
              placeholder="https://example.org/logo.png" />
            <div className="form-text">Optional. A link to the testbed's logo image, shown on its catalogue. Can be added or changed later.</div>
          </div>
          <div className="mb-3">
            <label className="form-label d-block">Catalogue</label>
            {(['public', 'private'] as const).map(v => (
              <div key={v} className="form-check form-check-inline">
                <input className="form-check-input" type="radio" id={`visibility-${v}`} name="visibility"
                  checked={isPrivate === (v === 'private')} onChange={() => setIsPrivate(v === 'private')} />
                <label className="form-check-label text-capitalize" htmlFor={`visibility-${v}`}>{v}</label>
              </div>
            ))}
            <div className="form-text">
              {VISIBILITY_HINT[isPrivate ? 'private' : 'public']} This cannot be changed once the catalogue exists.
              Private catalogues need piveau access control on this deployment.
            </div>
          </div>
          <div className="mb-3">
            <label className="form-label d-block">Connector</label>
            {([['own', 'Its own connector'], ['facility', 'Hosted by the facility connector']] as const).map(([v, label]) => (
              <div key={v} className="form-check form-check-inline">
                <input className="form-check-input" type="radio" id={`hosting-${v}`} name="hosting"
                  checked={hosted === (v === 'facility')} onChange={() => setHosted(v === 'facility')} />
                <label className="form-check-label" htmlFor={`hosting-${v}`}>{label}</label>
              </div>
            ))}
            <div className="form-text">{HOSTING_HINT[hosted ? 'facility' : 'own']} This cannot be changed later.</div>
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
          {!hosted && (
            <div className="mb-3">
              <label className="form-label">Participant ID (edc.participant.id)</label>
              <input className="form-control" value={form.participant_id} onChange={set('participant_id')} placeholder={`provider-${form.slug}`} />
            </div>
          )}
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
          {hosted ? (
            <div className="form-text mb-3">
              Hosted by the facility connector: the testbed uploads to <code>{(form.bucket.trim() || `6g-dali-${form.slug}`)}-upload</code>.
            </div>
          ) : (
            <div className="mb-3">
              <label className="form-label">DSP URL (edc.dsp.callback.address)</label>
              <input className="form-control" value={form.dsp_url} onChange={set('dsp_url')} placeholder={`https://edc.${form.slug}.6gdali.eu/protocol`} />
            </div>
          )}
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
            {Object.keys(stepLabels).map(key => {
              const s = testbed.steps[key]
              return (
                <li key={key} className="mb-2">
                  {!s ? <span className="spinner-border spinner-border-sm me-2" />
                    : s.status === 'ok' ? <FiCheckCircle className="text-success me-2" />
                    : <FiXCircle className="text-danger me-2" />}
                  <strong>{stepLabels[key]}</strong>
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

      {step === 3 && testbed && isHosted && (
        <>
          <UploadKit testbed={testbed} issued={uploadKey} />
          <div className="mt-3">
            <button className="btn btn-outline-secondary" onClick={() => onNavigate('testbed-detail', { dagId: testbed.slug })}>
              Open testbed
            </button>
          </div>
        </>
      )}

      {step === 3 && testbed && !isHosted && (
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
