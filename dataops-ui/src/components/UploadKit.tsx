import { useState } from 'react'
import { FiAlertTriangle, FiKey } from 'react-icons/fi'
import { rotateUploadCredentials } from '../api/airflow'
import CopyableId from './ui/CopyableId'
import type { Testbed, UploadCredentials } from '../types'

interface UploadKitProps {
  testbed: Testbed
  /** A secret just issued by provisioning, shown until the page is left. */
  issued?: UploadCredentials | null
  /** Called after a rotation, so the page can refresh the testbed (new access key) and its audit log. */
  onRotated?: () => void
}

/**
 * What a testbed hosted by the facility connector needs to upload its datasets: the endpoint, its upload bucket
 * and its key. The secret is shown once, right after it was created; when it is lost, a new key replaces it.
 */
export default function UploadKit({ testbed, issued, onRotated }: UploadKitProps) {
  const [fresh, setFresh] = useState<UploadCredentials | null>(issued ?? null)
  const [busy, setBusy] = useState(false)
  const [confirm, setConfirm] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const creds = fresh ?? issued ?? null
  const endpoint = creds?.endpoint ?? testbed.upload_endpoint ?? ''
  const bucket = creds?.bucket ?? testbed.upload_bucket ?? ''
  const accessKey = creds?.access_key ?? testbed.upload_access_key

  async function rotate() {
    setBusy(true)
    setError(null)
    try {
      setFresh(await rotateUploadCredentials(testbed.slug))
      setConfirm(false)
      onRotated?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="card"><div className="card-body">
      <h6 className="mb-1">Upload kit</h6>
      <div className="text-muted small mb-3">
        This testbed has no connector of its own: it uploads its datasets here, and the facility connector moves
        each new file into the Data Space.
      </div>

      {error && <div className="alert alert-danger py-2 small">{error}</div>}

      <dl className="row small mb-2">
        <dt className="col-sm-4">Endpoint</dt>
        <dd className="col-sm-8">{endpoint ? <CopyableId value={endpoint} /> : <span className="text-muted">not configured</span>}</dd>
        <dt className="col-sm-4">Bucket</dt>
        <dd className="col-sm-8"><CopyableId value={bucket} /></dd>
        <dt className="col-sm-4">Access key</dt>
        <dd className="col-sm-8">
          {accessKey ? <CopyableId value={accessKey} /> : <span className="text-muted">none yet (run provisioning)</span>}
        </dd>
        <dt className="col-sm-4">Secret key</dt>
        <dd className="col-sm-8">
          {creds ? <CopyableId value={creds.secret_key} /> : <span className="text-muted">shown only when the key is created</span>}
        </dd>
      </dl>

      {creds && (
        <div className="alert alert-warning py-2 small">
          <FiAlertTriangle className="me-1" />
          Copy the secret key now and send it to the testbed securely. It is not stored and will not be shown again;
          if it is lost, create a new key.
        </div>
      )}

      <details className="small mb-3">
        <summary>How the testbed uploads</summary>
        <p className="mt-2 mb-1">
          One folder per dataset, with its <code>metadata.json</code> and its data files. With the AWS CLI:
        </p>
        <pre className="bg-body-tertiary p-2 rounded mb-1"><code>{[
          `export AWS_ACCESS_KEY_ID=${accessKey ?? '<access key>'}`,
          'export AWS_SECRET_ACCESS_KEY=<secret key>',
          `aws s3 cp ./my-dataset/ s3://${bucket}/my-dataset/ --recursive --endpoint-url ${endpoint || '<endpoint>'}`,
        ].join('\n')}</code></pre>
        <p className="mb-0 text-muted">
          Upload <code>metadata.json</code> first, then the data files. Only files uploaded while the transfer is
          running are picked up (it is started when the testbed is provisioned; see the Assets tab).
        </p>
      </details>

      {confirm ? (
        <div className="d-flex flex-wrap align-items-center gap-2">
          <span className="small">The current key stops working.</span>
          <button className="btn btn-sm btn-warning" disabled={busy} onClick={rotate}>
            {busy && <span className="spinner-border spinner-border-sm me-1" />}Create a new key
          </button>
          <button className="btn btn-sm btn-outline-secondary" disabled={busy} onClick={() => setConfirm(false)}>Cancel</button>
        </div>
      ) : (
        <button className="btn btn-sm btn-outline-primary" disabled={busy || !testbed.upload_access_key}
          title={testbed.upload_access_key ? 'Replace the upload key: use when the secret is lost' : 'Run provisioning first'}
          onClick={() => setConfirm(true)}>
          <FiKey className="me-1" />New upload key
        </button>
      )}
    </div></div>
  )
}
