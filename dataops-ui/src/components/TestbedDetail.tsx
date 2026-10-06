import { useCallback, useEffect, useState } from 'react'
import { FiCheckCircle, FiXCircle, FiMinusCircle, FiDownload, FiRefreshCw, FiKey, FiTrash2 } from 'react-icons/fi'
import {
  deleteTestbed, downloadTestbedBundle, getTestbed, getTestbedAudit,
  provisionTestbed, rotateTestbedCredentials,
} from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Modal from './Modal'
import { TestbedStatusBadge } from './TestbedList'
import type { NavigateFn, Testbed, TestbedAuditEntry } from '../types'

interface TestbedDetailProps {
  slug: string
  onNavigate: NavigateFn
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="row mb-2">
      <div className="col-sm-4 text-muted small text-uppercase fw-semibold">{label}</div>
      <div className="col-sm-8">{children}</div>
    </div>
  )
}

export default function TestbedDetail({ slug, onNavigate }: TestbedDetailProps) {
  const [tb, setTb] = useState<Testbed | null>(null)
  const [audit, setAudit] = useState<TestbedAuditEntry[]>([])
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState(false)

  const load = useCallback(() => {
    Promise.all([getTestbed(slug), getTestbedAudit(slug)])
      .then(([t, a]) => { setTb(t); setAudit(a.entries) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [slug])

  useEffect(load, [load])

  async function act(fn: () => Promise<unknown>, done?: string) {
    setBusy(true)
    setError(null)
    setNotice(null)
    try {
      await fn()
      if (done) setNotice(done)
      load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  if (!tb) return error ? <ErrorMessage message={error} /> : <LoadingSpinner />

  return (
    <div>
      {error && <ErrorMessage message={error} />}
      {notice && <div className="alert alert-success py-2">{notice}</div>}

      <div className="d-flex flex-wrap gap-2 mb-3">
        <button className="btn btn-sm btn-primary" disabled={busy} onClick={() => act(() => downloadTestbedBundle(slug))}>
          <FiDownload className="me-1" />Connector bundle
        </button>
        <button className="btn btn-sm btn-outline-primary" disabled={busy}
          onClick={() => act(() => provisionTestbed(slug), 'Provisioning re-run.')}>
          <FiRefreshCw className="me-1" />Re-run provisioning
        </button>
        <button className="btn btn-sm btn-outline-secondary" disabled={busy || !tb.has_credentials}
          onClick={() => act(async () => { const r = await rotateTestbedCredentials(slug); setNotice(r.note) })}>
          <FiKey className="me-1" />Rotate Data Lake key
        </button>
        <button className="btn btn-sm btn-outline-danger ms-auto" disabled={busy} onClick={() => setConfirmDelete(true)}>
          <FiTrash2 className="me-1" />Deregister
        </button>
      </div>

      <div className="row g-3">
        <div className="col-lg-7">
          <div className="card"><div className="card-body">
            <h6 className="mb-3">{tb.name} <TestbedStatusBadge status={tb.status} /></h6>
            <Field label="Organisation">{tb.organisation ?? '—'}</Field>
            <Field label="Contact">{tb.contact_email ?? '—'}</Field>
            <Field label="Participant ID"><code>{tb.participant_id}</code></Field>
            <Field label="Experiment prefix"><code>{tb.experiment_prefix}</code></Field>
            <Field label="Bucket / catalogue"><code>{tb.bucket}</code></Field>
            <Field label="DSP endpoint"><span className="text-break">{tb.dsp_url}</span></Field>
            <Field label="produced_by">{tb.produced_by_iri ? <span className="text-break">{tb.produced_by_iri}</span> : '—'}</Field>
            <Field label="Registered">{tb.created_at}{tb.created_by ? ` by ${tb.created_by}` : ''}</Field>
          </div></div>
        </div>

        <div className="col-lg-5">
          <div className="card mb-3"><div className="card-body">
            <h6 className="mb-3">Provisioning</h6>
            <ul className="list-unstyled mb-0">
              {['bucket', 'credentials', 'catalogue'].map(key => {
                const s = tb.steps[key]
                return (
                  <li key={key} className="mb-1">
                    {!s ? <FiMinusCircle className="text-muted me-2" />
                      : s.status === 'ok' ? <FiCheckCircle className="text-success me-2" />
                      : <FiXCircle className="text-danger me-2" />}
                    <strong className="text-capitalize">{key}</strong>
                    <span className="text-muted small ms-2">{s ? s.detail : 'not run'}</span>
                  </li>
                )
              })}
            </ul>
          </div></div>

          <div className="card"><div className="card-body">
            <h6 className="mb-3">Activity</h6>
            {audit.length === 0 ? <span className="text-muted small">No activity yet</span> : (
              <ul className="list-unstyled small mb-0">
                {audit.slice(0, 10).map((a, i) => (
                  <li key={i} className="mb-1">
                    <span className="text-muted">{a.ts.replace('T', ' ').replace('+00:00', '')}</span>{' '}
                    <strong>{a.action}</strong>{a.actor ? ` · ${a.actor}` : ''}
                  </li>
                ))}
              </ul>
            )}
          </div></div>
        </div>
      </div>

      {confirmDelete && (
        <Modal
          title="Deregister testbed"
          onClose={() => setConfirmDelete(false)}
          footer={
            <>
              <button className="btn btn-outline-secondary" onClick={() => setConfirmDelete(false)}>Cancel</button>
              <button className="btn btn-danger" disabled={busy}
                onClick={async () => {
                  setConfirmDelete(false)
                  await act(() => deleteTestbed(slug))
                  onNavigate('testbeds')
                }}>
                Deregister
              </button>
            </>
          }
        >
          <p>
            This disables <strong>{tb.name}</strong>&rsquo;s Data Lake key and removes it from the registry.
            Its bucket, catalogue and the data in them are left untouched.
          </p>
        </Modal>
      )}
    </div>
  )
}
