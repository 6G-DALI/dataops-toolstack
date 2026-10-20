import { useCallback, useEffect, useState } from 'react'
import { FiUserPlus, FiX } from 'react-icons/fi'
import { addCatalogueMember, getCatalogueMembers, removeCatalogueMember, setCatalogueMemberLevel } from '../api/airflow'
import type { CatalogueAccessLevel, CatalogueMembers } from '../types'

interface CatalogueAccessProps {
  slug: string
  /** The testbed's catalogue visibility: on a public catalogue everyone can already read the datasets. */
  visibility?: 'public' | 'private'
  /** Called after a change so the page can refresh its audit log. */
  onChanged?: () => void
}

const LEVELS: { value: CatalogueAccessLevel; label: string; help: string }[] = [
  { value: 'reader', label: 'Reader', help: 'sees the datasets' },
  { value: 'contributor', label: 'Contributor', help: 'also creates and edits datasets' },
  { value: 'publisher', label: 'Publisher', help: 'also deletes and publishes datasets' },
]

/**
 * Who can see and edit the testbed catalogue's datasets in the Data Space: the members of the role groups piveau
 * keeps for the catalogue in Keycloak. Separate from the testbed's administrators (TestbedMembers), who work with
 * the testbed in DataOps. Shown to anyone who can open the testbed; the orchestrator enforces who may change it.
 */
export default function CatalogueAccess({ slug, visibility, onChanged }: CatalogueAccessProps) {
  const [data, setData] = useState<CatalogueMembers | null>(null)
  const [email, setEmail] = useState('')
  const [level, setLevel] = useState<CatalogueAccessLevel>('reader')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(() => {
    getCatalogueMembers(slug)
      .then(r => { setData(r); setError(null) })
      .catch(e => { setData(null); setError(e instanceof Error ? e.message : String(e)) })
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
      <h6 className="mb-1">Catalogue access</h6>
      <div className="text-muted small mb-3">
        Who can see and edit the datasets of <code>{data?.catalogue_id ?? 'the testbed catalogue'}</code> in the Data Space.
        {visibility === 'private'
          ? ' The catalogue is private: only these people (and Data Space operators) see its datasets.'
          : ' The catalogue is public: everyone already sees its datasets, so only Contributor and Publisher change anything.'}
        {' '}Changes apply within about 30 seconds. People need to have signed in to a DALI application once.
      </div>

      {error && <div className="alert alert-danger py-2 small">{error}</div>}

      {data === null ? (
        !error && <span className="text-muted small">Loading…</span>
      ) : !data.available ? (
        <span className="text-muted small">
          The Data Space has no access groups for this catalogue yet: they are created with the catalogue when piveau
          access control is on.
        </span>
      ) : (
        <>
          {data.members.length === 0 ? (
            <div className="text-muted small mb-3">Nobody has been given access yet.</div>
          ) : (
            <div className="table-responsive mb-3">
              <table className="table table-sm table-hover align-middle mb-0">
                <thead>
                  <tr>
                    <th scope="col">E-mail</th>
                    <th scope="col">Name</th>
                    <th scope="col">Access level</th>
                    <th scope="col" className="text-end"><span className="visually-hidden">Actions</span></th>
                  </tr>
                </thead>
                <tbody>
                  {data.members.map(m => (
                    <tr key={m.id}>
                      <td className="text-break">{m.email ?? m.username}</td>
                      <td className="text-muted">
                        {m.name ?? '—'}
                        {!m.enabled && <span className="badge text-bg-secondary ms-2">disabled</span>}
                      </td>
                      <td>
                        <select className="form-select form-select-sm w-auto" value={m.level} disabled={busy}
                          aria-label={`Access level of ${m.email ?? m.username}`}
                          onChange={e => change(() => setCatalogueMemberLevel(slug, m.id, e.target.value as CatalogueAccessLevel))}>
                          {LEVELS.map(l => <option key={l.value} value={l.value}>{l.label}</option>)}
                        </select>
                      </td>
                      <td className="text-end">
                        <button className="btn btn-sm btn-link text-danger py-0" disabled={busy}
                          title={`Remove ${m.email ?? m.username}'s access to the catalogue`}
                          onClick={() => change(() => removeCatalogueMember(slug, m.id))}>
                          <FiX />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <form className="d-flex gap-2" onSubmit={e => {
            e.preventDefault()
            const value = email.trim()
            if (value) change(async () => { await addCatalogueMember(slug, value, level); setEmail('') })
          }}>
            <input type="email" className="form-control form-control-sm" placeholder="name@example.org"
              value={email} onChange={e => setEmail(e.target.value)} disabled={busy} />
            <select className="form-select form-select-sm w-auto" value={level} disabled={busy}
              aria-label="Access level" onChange={e => setLevel(e.target.value as CatalogueAccessLevel)}>
              {LEVELS.map(l => <option key={l.value} value={l.value}>{l.label}</option>)}
            </select>
            <button type="submit" className="btn btn-sm btn-primary text-nowrap" disabled={busy || !email.trim()}>
              <FiUserPlus className="me-1" />Give access
            </button>
          </form>
          <div className="text-muted small mt-2">
            {LEVELS.map(l => `${l.label}: ${l.help}`).join(' · ')}
          </div>
        </>
      )}
    </div></div>
  )
}
