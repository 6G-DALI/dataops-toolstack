import { useCallback, useEffect, useState } from 'react'
import { FiCheckCircle, FiXCircle, FiMinusCircle, FiDownload, FiRefreshCw, FiKey, FiTrash2, FiSearch, FiActivity, FiExternalLink, FiFileText, FiPlay } from 'react-icons/fi'
import {
  deleteTestbed, discoverTestbedAssets, getTestbedDeletionPreview, downloadTestbedBundle, findTestbedTransfer, negotiateTestbedAsset, startTestbedTransfer, getTestbed, getTestbedAssets, getTestbedAudit,
  provisionTestbed, rotateTestbedCredentials,
} from '../api/airflow'
import { bucketUrl, catalogueUrl } from '../config'
import CopyableId from './ui/CopyableId'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Modal from './Modal'
import { TestbedStatusBadge } from './TestbedList'
import TestbedTimeline from './TestbedTimeline'
import type { DeletionPreview, DeregisterResponse, NavigateFn, Testbed, TestbedAsset, TestbedAuditEntry } from '../types'

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
  const [assets, setAssets] = useState<TestbedAsset[]>([])
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [deleteBucket, setDeleteBucket] = useState(false)
  const [deleteCatalogue, setDeleteCatalogue] = useState(false)
  const [confirmText, setConfirmText] = useState('')
  const [preview, setPreview] = useState<DeletionPreview | null>(null)
  const [deregistered, setDeregistered] = useState<DeregisterResponse | null>(null)
  const [confirmStart, setConfirmStart] = useState<TestbedAsset | null>(null)

  const load = useCallback(() => {
    Promise.all([getTestbed(slug), getTestbedAudit(slug), getTestbedAssets(slug)])
      .then(([t, a, as]) => { setTb(t); setAudit(a.entries); setAssets(as.assets) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [slug])

  useEffect(load, [load])

  // Opening the deregister dialog starts from the safe choice and looks up what could be deleted.
  useEffect(() => {
    if (!confirmDelete) return
    setDeleteBucket(false)
    setDeleteCatalogue(false)
    setConfirmText('')
    setPreview(null)
    getTestbedDeletionPreview(slug).then(setPreview).catch(() => setPreview(null))
  }, [confirmDelete, slug])

  const deletingData = deleteBucket || deleteCatalogue

  async function handleDeregister() {
    setBusy(true)
    setError(null)
    try {
      setDeregistered(await deleteTestbed(slug, {
        deleteBucket, deleteCatalogue, confirm: deletingData ? confirmText : undefined,
      }))
    } catch (e) {
      // Nothing is removed from the registry on a failure, so show the error on the page and refresh it.
      setConfirmDelete(false)
      setError(e instanceof Error ? e.message : String(e))
      load()
    } finally {
      setBusy(false)
    }
  }

  function closeDeregister() {
    if (deregistered) onNavigate('testbeds')
    else setConfirmDelete(false)
  }

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

  // Where each provisioned thing can be looked at. Omitted when the matching URL is not configured.
  const links: Record<string, string | null> = {
    bucket: bucketUrl(tb.bucket),
    catalogue: catalogueUrl(tb.catalogue_id),
  }

  return (
    <div>
      {error && <ErrorMessage message={error} />}
      {notice && <div className="alert alert-success py-2">{notice}</div>}

      <TestbedTimeline testbed={tb} assets={assets} />

      <div className="d-flex flex-wrap gap-2 mb-3">
        <button className="btn btn-sm btn-primary" disabled={busy} onClick={() => act(() => downloadTestbedBundle(slug))}>
          <FiDownload className="me-1" />Connector bundle
        </button>
        <button className="btn btn-sm btn-primary" disabled={busy}
          title="Ask the testbed's connector, through the central connector, which assets it offers"
          onClick={() => act(async () => {
            const r = await discoverTestbedAssets(slug)
            setNotice(r.offered > 0 ? `Found ${r.offered} offered asset${r.offered !== 1 ? 's' : ''}.` : 'Connector reachable, but it offers no assets yet.')
          })}>
          <FiSearch className="me-1" />Find asset
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
            <Field label="Data Lake access key">
              {tb.s3_access_key ? (
                <>
                  <CopyableId value={tb.s3_access_key} />
                  <div className="text-muted small">Scoped to this bucket. The secret key is kept encrypted and not shown.</div>
                </>
              ) : (
                <span className="text-muted">{tb.status === 'adopted' ? 'none yet (run provisioning)' : '—'}</span>
              )}
            </Field>
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
                    {links[key] && (
                      <a href={links[key]!} target="_blank" rel="noreferrer" className="ms-1 small"
                        title={`Open ${tb.bucket} (${key})`}>
                        <FiExternalLink />
                      </a>
                    )}
                    <span className="text-muted small ms-2">{s ? s.detail : 'not run'}</span>
                  </li>
                )
              })}
            </ul>
          </div></div>

          <div className="card mb-3"><div className="card-body">
            <h6 className="mb-3">Assets</h6>
            {assets.length === 0 ? (
              <span className="text-muted small">None yet. Register the asset on the testbed's connector, then click “Find asset”.</span>
            ) : (
              <ul className="list-unstyled small mb-0">
                {assets.map(a => (
                  <li key={a.asset_id} className="mb-2">
                    <code>{a.asset_id}</code>{' '}
                    <span className={`badge ${a.present ? 'text-bg-info' : 'text-bg-warning'}`}>
                      {a.present ? a.status : 'no longer offered'}
                    </span>
                    {a.title && <div className="text-muted">{a.title}</div>}
                    {(() => {
                      const agreed = !!a.contract_agreement_id && a.negotiation_state === 'FINALIZED'
                      const running = a.transfer_state === 'STARTED'
                      return (
                        <>
                          <div className="d-flex flex-wrap align-items-center gap-2 mt-1">
                            <button className="btn btn-sm btn-outline-primary py-0" disabled={busy || !a.present || agreed}
                              title={agreed ? 'A contract is already agreed' : 'Negotiate a contract for this asset through the central connector'}
                              onClick={() => act(async () => {
                                const r = await negotiateTestbedAsset(slug, a.asset_id)
                                setNotice(r.result === 'agreed' ? `Contract agreed for ${a.asset_id}.`
                                  : r.result === 'already_agreed' ? `${a.asset_id} already has an agreed contract.`
                                  : r.result === 'in_progress'
                                    ? `Negotiation for ${a.asset_id} is still running (${r.asset.negotiation_state}). Use Find transfer to refresh.`
                                    : r.message ?? `The negotiation for ${a.asset_id} failed. Check the offer and try again.`)
                              })}>
                              <FiFileText className="me-1" />Negotiate contract
                            </button>
                            <button className="btn btn-sm btn-outline-success py-0"
                              disabled={busy || !agreed || running || !tb.has_credentials}
                              title={running ? 'A transfer is already running'
                                : !tb.has_credentials ? 'Run provisioning first: this testbed has no Data Lake key'
                                : !agreed ? 'Negotiate a contract first'
                                : 'Start the transfer from the testbed to the central data lake'}
                              onClick={() => setConfirmStart(a)}>
                              <FiPlay className="me-1" />Start transfer
                            </button>
                            <button className="btn btn-sm btn-outline-secondary py-0" disabled={busy}
                              title="Look up this asset's contract and transfer on the central connector"
                              onClick={() => act(async () => {
                                const r = await findTestbedTransfer(slug, a.asset_id)
                                const contract = r.agreement?.agreement_id
                                  ? `Contract ${r.agreement.negotiation_state ?? 'found'}`
                                  : 'No contract found'
                                const transfer = r.active ? 'transfer running'
                                  : r.transfers.length ? `no running transfer (latest: ${r.transfers[0].state})`
                                  : 'no transfer found'
                                setNotice(`${a.asset_id}: ${contract}, ${transfer}.`)
                              })}>
                              <FiActivity className="me-1" />Find transfer
                            </button>
                          </div>
                          <div className="d-flex flex-wrap align-items-center gap-2 mt-1">
                            {a.contract_agreement_id ? (
                              <>
                                <span className={`badge ${a.negotiation_state === 'FINALIZED' ? 'text-bg-success' : 'text-bg-secondary'}`}
                                  title={a.negotiation_id ? `negotiation ${a.negotiation_id}` : undefined}>
                                  contract {(a.negotiation_state ?? 'agreed').toLowerCase()}
                                </span>
                                <CopyableId value={a.contract_agreement_id} maxWidth={170} />
                              </>
                            ) : a.negotiation_state ? (
                              <span className="badge text-bg-secondary" title={a.negotiation_id ?? undefined}>
                                negotiation {a.negotiation_state.toLowerCase()}
                              </span>
                            ) : null}
                            {a.transfer_state ? (
                              <span className={`badge ${running ? 'text-bg-success' : 'text-bg-secondary'}`} title={a.transfer_id ?? undefined}>
                                {running ? 'transfer active' : `transfer ${a.transfer_state.toLowerCase()}`}
                              </span>
                            ) : a.transfer_checked_at ? <span className="text-muted">no transfer found</span> : null}
                          </div>
                        </>
                      )
                    })()}
                  </li>
                ))}
              </ul>
            )}
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

      {confirmStart && (
        <Modal
          title="Start transfer"
          onClose={() => setConfirmStart(null)}
          footer={
            <>
              <button className="btn btn-outline-secondary" onClick={() => setConfirmStart(null)}>Cancel</button>
              <button className="btn btn-success" disabled={busy}
                onClick={async () => {
                  const asset = confirmStart
                  setConfirmStart(null)
                  await act(async () => {
                    const r = await startTestbedTransfer(slug, asset.asset_id)
                    setNotice(r.result === 'started' ? `Transfer for ${asset.asset_id} started and running.`
                      : r.result === 'already_running' ? `A transfer for ${asset.asset_id} is already running.`
                      : `Transfer for ${asset.asset_id} requested (${r.state}). Use Find transfer to refresh.`)
                  })
                }}>
                Start transfer
              </button>
            </>
          }
        >
          <p>
            This starts a long-running transfer that streams <strong>{tb.name}</strong>&rsquo;s bucket
            into the central Data Lake and registers each file in the catalogue, using the testbed&rsquo;s own
            Data Lake key. It keeps running until it is terminated.
          </p>
          <p className="mb-0 small text-muted">Asset <code>{confirmStart.asset_id}</code> &middot; contract <code>{confirmStart.contract_agreement_id}</code></p>
        </Modal>
      )}

      {confirmDelete && (
        <Modal
          title={deregistered ? 'Testbed deregistered' : 'Deregister testbed'}
          onClose={closeDeregister}
          footer={deregistered ? (
            <button className="btn btn-primary" onClick={closeDeregister}>Close</button>
          ) : (
            <>
              <button className="btn btn-outline-secondary" onClick={() => setConfirmDelete(false)}>Cancel</button>
              <button className="btn btn-danger" disabled={busy || (deletingData && confirmText !== slug)} onClick={handleDeregister}>
                {busy && <span className="spinner-border spinner-border-sm me-1" />}
                {deletingData ? 'Deregister and delete' : 'Deregister'}
              </button>
            </>
          )}
        >
          {deregistered ? (
            <>
              <ul className="small">
                {Object.entries(deregistered.results).map(([step, r]) => (
                  <li key={step}>
                    <strong className="text-capitalize">{step === 'key' ? 'Data Lake key' : step}</strong>: {r.status.replace('_', ' ')}
                    {r.objects_deleted ? ` (${r.objects_deleted} objects)` : ''}
                  </li>
                ))}
              </ul>
              {deregistered.followups.map(f => <div key={f} className="alert alert-warning py-2 small">{f}</div>)}
            </>
          ) : (
            <>
              <p>
                This disables <strong>{tb.name}</strong>&rsquo;s Data Lake key and removes it from the registry.
                {!deletingData && ' Its bucket, catalogue and the data in them are left untouched.'}
              </p>
              {preview && preview.running_transfers.length > 0 && (
                <div className="alert alert-warning py-2 small">
                  A transfer is running for <code>{preview.running_transfers.join(', ')}</code>. It will fail once the key is removed.
                </div>
              )}
              <div className="form-check mb-2">
                <input className="form-check-input" type="checkbox" id="del-catalogue" checked={deleteCatalogue}
                  onChange={e => setDeleteCatalogue(e.target.checked)} />
                <label className="form-check-label" htmlFor="del-catalogue">
                  Also delete the catalogue <code>{tb.catalogue_id}</code> in piveau
                  {preview?.datasets != null && <> and its <strong>{preview.datasets}</strong> dataset{preview.datasets !== 1 ? 's' : ''}</>}
                  {preview?.datasets_error && <span className="text-muted"> (could not count its datasets)</span>}
                </label>
              </div>
              <div className="form-check mb-2">
                <input className="form-check-input" type="checkbox" id="del-bucket" checked={deleteBucket}
                  onChange={e => setDeleteBucket(e.target.checked)} />
                <label className="form-check-label" htmlFor="del-bucket">
                  Also delete the Data Lake bucket <code>{tb.bucket}</code>
                  {preview?.objects != null && <> with all <strong>{preview.objects}{preview.objects_truncated ? '+' : ''}</strong> object{preview.objects !== 1 ? 's' : ''} in it</>}
                  {preview?.objects_error && <span className="text-muted"> (could not count its objects)</span>}
                </label>
              </div>
              {deletingData && (
                <div className="mt-3">
                  <div className="alert alert-danger py-2 small">
                    This cannot be undone. {deleteCatalogue && 'Deleting the catalogue removes the datasets in it. '}
                    {deleteBucket && 'Deleting the bucket removes every file in it. '}
                    EDC assets registered for those files on the central connector are not removed.
                  </div>
                  <label className="form-label small" htmlFor="del-confirm">Type <code>{slug}</code> to confirm</label>
                  <input id="del-confirm" className="form-control" autoComplete="off" value={confirmText}
                    onChange={e => setConfirmText(e.target.value)} />
                </div>
              )}
            </>
          )}
        </Modal>
      )}
    </div>
  )
}
