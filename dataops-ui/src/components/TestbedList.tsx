import { useEffect, useState } from 'react'
import { FiPlus, FiExternalLink } from 'react-icons/fi'
import { getTestbeds } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import type { NavigateFn, Testbed } from '../types'

export function TestbedStatusBadge({ status }: { status: string }) {
  const cls = status === 'provisioned' ? 'text-bg-success' : status === 'draft' ? 'text-bg-secondary' : 'text-bg-warning'
  return <span className={`badge ${cls}`}>{status}</span>
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
        <button className="btn btn-sm btn-primary" onClick={() => onNavigate('testbed-register')}>
          <FiPlus className="me-1" />
          Register testbed
        </button>
      </div>

      <div className="card">
        <div className="card-body p-0">
          <div className="table-responsive">
            <table className="table table-hover align-middle mb-0">
              <thead>
                <tr>
                  <th>Testbed</th>
                  <th style={{ width: 170 }}>Participant ID</th>
                  <th style={{ width: 200 }}>Bucket / catalogue</th>
                  <th>DSP endpoint</th>
                  <th style={{ width: 120 }}>Status</th>
                </tr>
              </thead>
              <tbody>
                {testbeds.length === 0 ? (
                  <tr><td colSpan={5} className="text-center text-muted py-4">No testbeds registered yet</td></tr>
                ) : testbeds.map(tb => (
                  <tr key={tb.slug} role="button" onClick={() => onNavigate('testbed-detail', { dagId: tb.slug })}>
                    <td>
                      <div className="fw-semibold">{tb.name}</div>
                      <code className="small text-muted">{tb.slug}</code>
                      {tb.organisation && <div className="small text-muted">{tb.organisation}</div>}
                    </td>
                    <td><code className="small">{tb.participant_id}</code></td>
                    <td><code className="small">{tb.bucket}</code></td>
                    <td className="small text-break">{tb.dsp_url}</td>
                    <td>
                      <TestbedStatusBadge status={tb.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      <p className="text-muted small mt-3 mb-0">
        <FiExternalLink className="me-1" />
        Connection and transfer status will appear here once the central connector integration lands.
      </p>
    </div>
  )
}
