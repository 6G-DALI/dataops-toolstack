import { useCallback, useEffect, useState } from 'react'
import { FiUserPlus, FiX } from 'react-icons/fi'
import { addTestbedMember, getTestbedMembers, removeTestbedMember } from '../api/airflow'
import type { TestbedMember } from '../types'

interface TestbedMembersProps {
  slug: string
  /** Called after a change so the page can refresh its audit log. */
  onChanged?: () => void
}

/**
 * The people who can use this testbed: the members of its Keycloak group. Admins add an existing Keycloak
 * user by e-mail address and remove members. Shown to testbed admins only.
 */
export default function TestbedMembers({ slug, onChanged }: TestbedMembersProps) {
  const [members, setMembers] = useState<TestbedMember[] | null>(null)
  const [email, setEmail] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(() => {
    getTestbedMembers(slug)
      .then(r => { setMembers(r.members); setError(null) })
      .catch(e => { setMembers([]); setError(e instanceof Error ? e.message : String(e)) })
  }, [slug])

  useEffect(load, [load])

  async function change(action: () => Promise<unknown>) {
    setBusy(true)
    setError(null)
    try {
      await action()
      load()
      onChanged?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="card mb-3"><div className="card-body">
      <h6 className="mb-1">Members</h6>
      <div className="text-muted small mb-3">
        Can see and work with this testbed only. They need to have signed in to a DALI application once.
      </div>

      {error && <div className="alert alert-danger py-2 small">{error}</div>}

      {members === null ? (
        <span className="text-muted small">Loading…</span>
      ) : members.length === 0 ? (
        <span className="text-muted small">No members yet.</span>
      ) : (
        <ul className="list-unstyled mb-3">
          {members.map(m => (
            <li key={m.id} className="d-flex align-items-center mb-1">
              <div className="text-break">
                <span>{m.email ?? m.username}</span>
                {m.name && <span className="text-muted small ms-2">{m.name}</span>}
                {!m.enabled && <span className="badge text-bg-secondary ms-2">disabled</span>}
              </div>
              <button className="btn btn-sm btn-link text-danger ms-auto py-0" disabled={busy}
                title={`Remove ${m.email ?? m.username} from this testbed`}
                onClick={() => change(() => removeTestbedMember(slug, m.id))}>
                <FiX />
              </button>
            </li>
          ))}
        </ul>
      )}

      <form className="d-flex gap-2" onSubmit={e => {
        e.preventDefault()
        const value = email.trim()
        if (value) change(async () => { await addTestbedMember(slug, value); setEmail('') })
      }}>
        <input type="email" className="form-control form-control-sm" placeholder="name@example.org"
          value={email} onChange={e => setEmail(e.target.value)} disabled={busy} />
        <button type="submit" className="btn btn-sm btn-primary text-nowrap" disabled={busy || !email.trim()}>
          <FiUserPlus className="me-1" />Add
        </button>
      </form>
    </div></div>
  )
}
