import { useState } from 'react'
import { FiCheck, FiX, FiTrash2 } from 'react-icons/fi'
import { approveTestbedRequest, rejectTestbedRequest, withdrawTestbedRequest } from '../api/airflow'
import { isTestbedAdmin } from '../auth/testbedAccess'
import ErrorMessage from './ErrorMessage'
import { TestbedStatusBadge } from './TestbedList'
import type { NavigateFn, Testbed } from '../types'

interface TestbedRequestViewProps {
  tb: Testbed
  onNavigate: NavigateFn
  /** Reload the testbed (after a decision it is no longer a request); `followups` are what an approval left to do. */
  onChanged: (followups?: string[]) => void
}

function Row({ label, children }: { label: string, children: React.ReactNode }) {
  return (
    <div className="row mb-2">
      <div className="col-sm-4 text-muted small text-uppercase fw-semibold">{label}</div>
      <div className="col-sm-8">{children}</div>
    </div>
  )
}

/**
 * A testbed that is still a request (waiting for, or refused by, an administrator). The requester sees what they
 * asked for and the decision, and can withdraw it; an administrator can approve or reject it. Everything else about a
 * testbed (assets, members, settings…) exists only once it is approved.
 */
export default function TestbedRequestView({ tb, onNavigate, onChanged }: TestbedRequestViewProps) {
  const admin = isTestbedAdmin()
  const [note, setNote] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function run(fn: () => Promise<unknown>, after?: () => void) {
    setBusy(true)
    setError(null)
    try {
      await fn()
      if (after) after()
      else onChanged()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  const pending = tb.status === 'requested'

  return (
    <div style={{ maxWidth: 760 }}>
      <div className="d-flex align-items-center gap-2 mb-3">
        <h5 className="mb-0">{tb.name}</h5>
        <code className="small text-muted">{tb.slug}</code>
        <TestbedStatusBadge status={tb.status} />
      </div>

      {error && <ErrorMessage message={error} />}

      {pending && !admin && (
        <div className="alert alert-info">
          Your request is waiting for an administrator. You will become this testbed’s owner when it is approved, and it
          will appear in your Testbeds list as a regular testbed.
        </div>
      )}
      {tb.status === 'rejected' && (
        <div className="alert alert-danger">
          <strong>This request was not approved.</strong>
          {tb.decision_note && <div className="mt-1">{tb.decision_note}</div>}
          <div className="small mt-1">You can withdraw it to free the short id, and send a new request.</div>
        </div>
      )}

      <div className="card mb-3"><div className="card-body">
        <Row label="Requested by">{tb.requested_by_email ?? tb.created_by}</Row>
        <Row label="Requested">{tb.created_at}</Row>
        {tb.organisation && <Row label="Organisation">{tb.organisation}</Row>}
        {tb.contact_email && <Row label="Contact">{tb.contact_email}</Row>}
        <Row label="Connector">
          {tb.hosting === 'facility' ? 'Hosted by the facility connector' : <>Its own connector <span className="text-muted small">{tb.dsp_url}</span></>}
        </Row>
        <Row label="Catalogue">{tb.visibility}</Row>
        <Row label="Bucket / catalogue"><code>{tb.bucket}</code></Row>
        {tb.request_note && <Row label="Request">{<span style={{ whiteSpace: 'pre-wrap' }}>{tb.request_note}</span>}</Row>}
        {tb.logo_url && <Row label="Logo"><span className="text-break small">{tb.logo_url}</span></Row>}
      </div></div>

      {admin && pending && (
        <div className="card mb-3"><div className="card-body">
          <label className="form-label fw-semibold" htmlFor="decision-note">Note to the requester (optional)</label>
          <textarea id="decision-note" className="form-control mb-3" rows={2} value={note} maxLength={2000}
            onChange={e => setNote(e.target.value)} />
          <div className="d-flex gap-2">
            <button className="btn btn-success" disabled={busy}
              onClick={() => run(async () => {
                const r = await approveTestbedRequest(tb.slug, note.trim() || undefined)
                onChanged(r.followups)
              }, () => {})}>
              {busy ? <span className="spinner-border spinner-border-sm me-1" /> : <FiCheck className="me-1" />}
              Approve &amp; provision
            </button>
            <button className="btn btn-outline-danger" disabled={busy}
              onClick={() => run(() => rejectTestbedRequest(tb.slug, note.trim() || undefined))}>
              <FiX className="me-1" />Reject
            </button>
          </div>
          <div className="form-text mt-2">
            Approving makes {tb.requested_by_email ?? 'the requester'} the testbed’s owner and creates its Data Lake
            bucket, key and catalogue.
          </div>
        </div></div>
      )}

      <div className="d-flex gap-2">
        <button className="btn btn-outline-secondary" onClick={() => onNavigate('testbeds')}>Back to testbeds</button>
        <button className="btn btn-outline-danger" disabled={busy}
          onClick={() => run(() => withdrawTestbedRequest(tb.slug), () => onNavigate('testbeds'))}>
          <FiTrash2 className="me-1" />{admin ? 'Delete request' : 'Withdraw request'}
        </button>
      </div>
    </div>
  )
}
