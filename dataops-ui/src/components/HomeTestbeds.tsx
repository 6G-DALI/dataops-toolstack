import { useEffect, useState } from 'react'
import { FiServer } from 'react-icons/fi'
import { getTestbeds } from '../api/airflow'
import { isTestbedAdmin } from '../auth/testbedAccess'
import { TestbedStatusBadge } from './TestbedList'
import type { NavigateFn, Testbed } from '../types'

interface HomeTestbedsProps {
  onNavigate: NavigateFn
}

/**
 * The testbeds the signed-in user can see, for the home page. The orchestrator already filters the list
 * to the user's testbeds (all of them for a testbed admin), so this only shows what it is given.
 */
export default function HomeTestbeds({ onNavigate }: HomeTestbedsProps) {
  const [testbeds, setTestbeds] = useState<Testbed[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getTestbeds()
      .then(r => setTestbeds(r.testbeds))
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  const provisioned = testbeds?.filter(t => t.status === 'provisioned').length ?? 0

  return (
    <div className="card mb-4">
      <div className="card-header d-flex align-items-center justify-content-between">
        <span><FiServer className="me-2" aria-hidden="true" />{isTestbedAdmin() ? 'Testbeds' : 'Your testbeds'}</span>
        <button type="button" className="btn btn-sm btn-outline-secondary" onClick={() => onNavigate('testbeds', {})}>
          View all
        </button>
      </div>
      <div className="card-body p-0">
        {error ? (
          <div className="state-panel-inline" role="alert">
            <p className="state-panel-title">Testbeds unavailable</p>
            <p className="state-panel-text">{error}</p>
          </div>
        ) : testbeds === null ? (
          <div className="p-3 text-muted small">Loading…</div>
        ) : testbeds.length === 0 ? (
          <div className="state-panel-inline">
            <p className="state-panel-title">No testbeds</p>
            <p className="state-panel-text">
              {isTestbedAdmin()
                ? 'No testbed is registered yet.'
                : 'You are not a member of any testbed. Ask a DataOps administrator to add you to one.'}
            </p>
          </div>
        ) : (
          <>
            <div className="px-3 py-2 text-muted small border-bottom">
              {testbeds.length} testbed{testbeds.length !== 1 ? 's' : ''}, {provisioned} provisioned
            </div>
            <div className="table-responsive">
              <table className="table table-hover align-middle mb-0">
                <thead>
                  <tr><th>Testbed</th><th>Organisation</th><th>Status</th></tr>
                </thead>
                <tbody>
                  {testbeds.map(tb => (
                    <tr key={tb.slug} role="button" style={{ cursor: 'pointer' }}
                      onClick={() => onNavigate('testbed-detail', { dagId: tb.slug })}>
                      <td className="fw-medium">{tb.name}</td>
                      <td className="small text-secondary">{tb.organisation ?? '—'}</td>
                      <td><TestbedStatusBadge status={tb.status} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
