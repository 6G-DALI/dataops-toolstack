import { useEffect, useState } from 'react'
import { FiPlus, FiExternalLink, FiFileText } from 'react-icons/fi'
import { getTestbeds } from '../api/airflow'
import { canRequestTestbed, isTestbedAdmin } from '../auth/testbedAccess'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import TestbedStatusIcons from './TestbedStatusIcons'
import type { NavigateFn, Testbed } from '../types'

export function TestbedStatusBadge({ status }: { status: string }) {
  const cls = status === 'provisioned' ? 'text-bg-success'
    : status === 'adopted' ? 'text-bg-info'
    : status === 'rejected' ? 'text-bg-danger'
    : status === 'draft' ? 'text-bg-secondary' : 'text-bg-warning'
  return <span className={`badge ${cls}`}>{status === 'requested' ? 'awaiting approval' : status}</span>
}

interface TestbedListProps {
  onNavigate: NavigateFn
}

export default function TestbedList({ onNavigate }: TestbedListProps) {
  const [testbeds, setTestbeds] = useState<Testbed[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getTestbeds()
      .then(r => setTestbeds(r.testbeds))
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  if (error) return <ErrorMessage message={error} />
  if (!testbeds) return <LoadingSpinner />

  return (
    <div>
      <div className="d-flex align-items-center justify-content-between mb-3">
        <span className="text-muted small">
          {testbeds.length} testbed{testbeds.length !== 1 ? 's' : ''} registered
        </span>
        <div className="d-flex gap-2">
          <button className="btn btn-sm btn-outline-primary" onClick={() => onNavigate('testbed-metadata')}>
            <FiFileText className="me-1" />
            Metadata builder
          </button>
          {isTestbedAdmin() && (
            <button className="btn btn-sm btn-primary" onClick={() => onNavigate('testbed-register')}>
              <FiPlus className="me-1" />
              Register testbed
            </button>
          )}
          {canRequestTestbed() && (
            <button className="btn btn-sm btn-primary" onClick={() => onNavigate('testbed-request')}>
              <FiPlus className="me-1" />
              Request testbed registration
            </button>
          )}
        </div>
      </div>

      <div className="card">
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th>Slug</th>
                  <th>Name</th>
                  <th>Organization</th>
                  <th>Participant ID</th>
                  <th>Bucket / catalogue</th>
                  <th>Type</th>
                  <th>DSP endpoint</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {testbeds.length === 0 ? (
                  <tr><td colSpan={8} className="text-center text-muted py-4">No testbeds registered yet</td></tr>
                ) : testbeds.map(tb => (
                  <tr key={tb.slug} role="button" onClick={() => onNavigate('testbed-detail', { dagId: tb.slug })}>
                    <td><code className="small">{tb.slug}</code></td>
                    <td className="fw-semibold">{tb.name}</td>
                    <td className="small">{tb.organisation ?? '—'}</td>
                    <td><code className="small">{tb.participant_id}</code></td>
                    <td><code className="small">{tb.bucket}</code></td>
                    <td className="small text-nowrap">{tb.hosting === 'facility' ? 'Hosted' : 'Own connector'}</td>
                    <td className="small text-break">{tb.dsp_url}</td>
                    <td className="text-nowrap"><TestbedStatusIcons testbed={tb} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      <p className="text-muted small mt-3 mb-0">
        <FiExternalLink className="me-1" />
        Status, left to right: registered, bucket, key, catalogue, provisioned, connector, asset, contract, transfer.
        Green is reached, red failed, outlined is the next level; hover an icon for the detail.
      </p>
    </div>
  )
}
