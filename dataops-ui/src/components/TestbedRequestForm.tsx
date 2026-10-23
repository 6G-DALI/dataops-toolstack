import { useState } from 'react'
import { requestTestbed } from '../api/airflow'
import keycloak from '../auth/keycloak'
import ErrorMessage from './ErrorMessage'
import type { NavigateFn, TestbedRequestBody } from '../types'

const SLUG_RE = /^[a-z][a-z0-9-]{1,30}[a-z0-9]$/

/** What each connector choice means, shown under the choice. */
const HOSTING_HINT = {
  own: 'The testbed runs its own connector (a bundle is generated for it) and offers its data from there.',
  facility: 'The testbed has no connector. It gets an upload bucket and key in the Data Lake, and the facility ' +
    'connector moves each uploaded file into the Data Space.',
}

const VISIBILITY_HINT = {
  public: 'Its datasets are visible to everyone.',
  private: 'Its datasets are visible only to the testbed’s members and administrators.',
}

interface TestbedRequestFormProps {
  onNavigate: NavigateFn
}

/**
 * A user asks for a testbed to be registered. Nothing is created in the Data Space until an administrator
 * approves; the request is stored and shown to the requester (and the administrators) in the Testbeds list.
 */
export default function TestbedRequestForm({ onNavigate }: TestbedRequestFormProps) {
  const email = (keycloak.tokenParsed as { email?: string } | undefined)?.email ?? ''
  const [form, setForm] = useState({
    slug: '', name: '', organisation: '', contact_email: email, dsp_url: '', logo_url: '', note: '',
  })
  const [isPrivate, setIsPrivate] = useState(false)
  const [hosted, setHosted] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const set = (k: keyof typeof form) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
    setForm(f => ({ ...f, [k]: e.target.value }))

  const slugOk = SLUG_RE.test(form.slug)
  const valid = slugOk && form.name.trim().length > 0 && form.note.trim().length > 0

  async function submit() {
    const body: TestbedRequestBody = { slug: form.slug, name: form.name.trim(), note: form.note.trim() }
    for (const k of ['organisation', 'contact_email', 'logo_url', ...(hosted ? [] : ['dsp_url'] as const)] as const) {
      if (form[k].trim()) body[k] = form[k].trim()
    }
    if (isPrivate) body.private = true
    if (hosted) body.hosted = true
    setBusy(true)
    setError(null)
    try {
      const created = await requestTestbed(body)
      onNavigate('testbed-detail', { dagId: created.slug })
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setBusy(false)
    }
  }

  return (
    <div style={{ maxWidth: 760 }}>
      <p className="text-muted">
        Tell us about the testbed you want to join the Data Space. An administrator reviews the request; once it is
        approved you become the testbed’s owner, and it is set up for you to manage and deploy.
      </p>
      {error && <ErrorMessage message={error} />}
      <div className="card"><div className="card-body">
        <div className="mb-3">
          <label className="form-label fw-semibold" htmlFor="req-slug">Short id (slug)</label>
          <input id="req-slug" className={`form-control ${form.slug && !slugOk ? 'is-invalid' : ''}`} value={form.slug}
            onChange={set('slug')} placeholder="e.g. kul" />
          <div className="form-text">
            3-32 characters: lowercase letters, digits and “-”. It names the testbed’s bucket and catalogue and cannot be
            changed later.
          </div>
        </div>
        <div className="mb-3">
          <label className="form-label fw-semibold" htmlFor="req-name">Name</label>
          <input id="req-name" className="form-control" value={form.name} onChange={set('name')} placeholder="KU Leuven" />
        </div>
        <div className="row">
          <div className="col-md-6 mb-3">
            <label className="form-label" htmlFor="req-org">Organisation</label>
            <input id="req-org" className="form-control" value={form.organisation} onChange={set('organisation')} />
          </div>
          <div className="col-md-6 mb-3">
            <label className="form-label" htmlFor="req-contact">Operator contact email</label>
            <input id="req-contact" className="form-control" type="email" value={form.contact_email}
              onChange={set('contact_email')} />
          </div>
        </div>
        <div className="mb-3">
          <label className="form-label fw-semibold" htmlFor="req-note">What is this testbed, and what will it share?</label>
          <textarea id="req-note" className="form-control" rows={4} value={form.note} onChange={set('note')}
            maxLength={2000} />
          <div className="form-text">Shown to the administrators who decide on the request.</div>
        </div>
        <div className="mb-3">
          <label className="form-label d-block">Connector</label>
          {([['own', 'Its own connector'], ['facility', 'Hosted by the facility connector']] as const).map(([v, label]) => (
            <div key={v} className="form-check form-check-inline">
              <input className="form-check-input" type="radio" id={`req-hosting-${v}`} name="req-hosting"
                checked={hosted === (v === 'facility')} onChange={() => setHosted(v === 'facility')} />
              <label className="form-check-label" htmlFor={`req-hosting-${v}`}>{label}</label>
            </div>
          ))}
          <div className="form-text">{HOSTING_HINT[hosted ? 'facility' : 'own']} This cannot be changed later.</div>
        </div>
        {!hosted && (
          <div className="mb-3">
            <label className="form-label" htmlFor="req-dsp">DSP URL of your connector</label>
            <input id="req-dsp" className="form-control" value={form.dsp_url} onChange={set('dsp_url')}
              placeholder={`https://edc.${form.slug || 'your-testbed'}.6gdali.eu/protocol`} />
            <div className="form-text">Optional. Leave it empty to use the address shown, and change it once the connector is deployed.</div>
          </div>
        )}
        <div className="mb-3">
          <label className="form-label d-block">Catalogue</label>
          {(['public', 'private'] as const).map(v => (
            <div key={v} className="form-check form-check-inline">
              <input className="form-check-input" type="radio" id={`req-visibility-${v}`} name="req-visibility"
                checked={isPrivate === (v === 'private')} onChange={() => setIsPrivate(v === 'private')} />
              <label className="form-check-label text-capitalize" htmlFor={`req-visibility-${v}`}>{v}</label>
            </div>
          ))}
          <div className="form-text">{VISIBILITY_HINT[isPrivate ? 'private' : 'public']} This cannot be changed once the catalogue exists.</div>
        </div>
        <div className="mb-3">
          <label className="form-label" htmlFor="req-logo">Logo URL</label>
          <input id="req-logo" className="form-control" type="url" value={form.logo_url} onChange={set('logo_url')}
            placeholder="https://example.org/logo.png" />
          <div className="form-text">Optional. Shown on the testbed’s catalogue.</div>
        </div>
        <div className="d-flex gap-2">
          <button className="btn btn-primary" disabled={!valid || busy} onClick={submit}>
            {busy && <span className="spinner-border spinner-border-sm me-1" />}
            Submit request
          </button>
          <button className="btn btn-outline-secondary" disabled={busy} onClick={() => onNavigate('testbeds')}>Cancel</button>
        </div>
      </div></div>
    </div>
  )
}
