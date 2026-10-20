import { useCallback, useEffect, useState } from 'react'
import { FiEdit2, FiExternalLink, FiRefreshCw } from 'react-icons/fi'
import { editTestbedDataset, getTestbedDatasets } from '../api/airflow'
import { catalogueDatasetUrl } from '../config'
import Modal from './Modal'
import type { DatasetAccessLevel, DatasetEditRequest, TestbedDataset, TestbedDatasets as Listing } from '../types'

const LEVEL_LABEL: Record<DatasetAccessLevel, string> = {
  public: 'Public',
  restricted: 'Signed-in users',
  internal: 'Members only',
}

const LEVEL_HELP: Record<DatasetAccessLevel, string> = {
  public: 'Everyone, signed in or not.',
  restricted: 'Anyone signed in to 6G-DALI.',
  internal: 'Only the people given access to this catalogue (Access tab) and Data Space operators.',
}

function LevelBadge({ level }: { level?: DatasetAccessLevel | null }) {
  if (!level) return <span className="text-muted small">—</span>
  const cls = level === 'public' ? 'text-bg-light border' : level === 'restricted' ? 'text-bg-info' : 'text-bg-warning'
  return <span className={`badge ${cls}`}>{LEVEL_LABEL[level]}</span>
}

interface Draft {
  title: string
  description: string
  keywords: string
  license: string
  level: DatasetAccessLevel | ''
}

function draftOf(d: TestbedDataset): Draft {
  return {
    title: d.title ?? '', description: d.description ?? '', keywords: (d.keywords ?? []).join(', '),
    license: d.license ?? '', level: d.access_level ?? '',
  }
}

/** Only what was changed is sent: the orchestrator keeps the rest, and leaves the level alone unless it changed. */
function changesOf(d: TestbedDataset, draft: Draft): DatasetEditRequest {
  const was = draftOf(d)
  const out: DatasetEditRequest = {}
  if (draft.title.trim() !== was.title) out.title = draft.title.trim()
  if (draft.description.trim() !== was.description) out.description = draft.description.trim()
  const keywords = draft.keywords.split(',').map(k => k.trim()).filter(Boolean)
  if (keywords.join('\u0000') !== (d.keywords ?? []).join('\u0000')) out.keywords = keywords
  if (draft.license.trim() !== was.license) out.license = draft.license.trim()
  if (draft.level && draft.level !== was.level) out.access_level = draft.level
  return out
}

/**
 * The datasets of the testbed's catalogue, with their visibility, and a form to edit a dataset's main fields and
 * its visibility. Edits change the catalogue only, never the testbed's metadata.json.
 */
export default function TestbedDatasets({ slug, onChanged }: { slug: string; onChanged?: () => void }) {
  const [data, setData] = useState<Listing | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [editing, setEditing] = useState<TestbedDataset | null>(null)
  const [draft, setDraft] = useState<Draft | null>(null)
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)

  const load = useCallback(() => {
    setLoading(true)
    getTestbedDatasets(slug)
      .then(r => { setData(r); setError(null) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [slug])

  useEffect(load, [load])

  function open(d: TestbedDataset) {
    setEditing(d)
    setDraft(draftOf(d))
    setSaveError(null)
  }

  async function save() {
    if (!editing || !draft) return
    const changes = changesOf(editing, draft)
    if (Object.keys(changes).length === 0) { setEditing(null); return }
    setSaving(true)
    setSaveError(null)
    try {
      await editTestbedDataset(slug, editing.id, changes)
      setNotice(`Saved “${changes.title ?? editing.title}”.`)
      setEditing(null)
      load()
      onChanged?.()
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  const set = (k: keyof Draft) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
    setDraft(d => d && { ...d, [k]: e.target.value })
  const levels = data?.levels ?? []

  return (
    <div className="card"><div className="card-body">
      <div className="d-flex align-items-center mb-3">
        <div className="text-muted small me-3">
          The datasets of <code>{data?.catalogue_id ?? 'the testbed catalogue'}</code>, including the private ones.
          Edits change the catalogue only; the testbed's metadata.json stays as it was shared.
        </div>
        <button className="btn btn-sm btn-outline-secondary ms-auto text-nowrap" disabled={loading} onClick={load}>
          <FiRefreshCw className="me-1" />Refresh
        </button>
      </div>

      {notice && <div className="alert alert-success py-2 small">{notice}</div>}
      {error && <div className="alert alert-danger py-2 small">{error}</div>}

      {loading && !data ? (
        <span className="text-muted small">Loading…</span>
      ) : data && data.datasets.length === 0 ? (
        <span className="text-muted small">No datasets in this catalogue yet.</span>
      ) : data && (
        <div className="table-responsive">
          <table className="table table-sm table-hover align-middle mb-0">
            <thead>
              <tr>
                <th scope="col">Dataset</th>
                <th scope="col">Visibility</th>
                <th scope="col" className="text-end">Files</th>
                <th scope="col">Updated</th>
                <th scope="col" className="text-end"><span className="visually-hidden">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {data.datasets.map(d => {
                const link = catalogueDatasetUrl(d.id)
                return (
                  <tr key={d.id}>
                    <td className="text-break">
                      <div>{d.title}</div>
                      <code className="small text-muted">{d.id}</code>
                      {d.error && <div className="small text-danger">{d.error}</div>}
                    </td>
                    <td><LevelBadge level={d.access_level} /></td>
                    <td className="text-end">{d.distributions ?? '—'}</td>
                    <td className="small text-muted text-nowrap">{(d.modified ?? '').slice(0, 10) || '—'}</td>
                    <td className="text-end text-nowrap">
                      {link && (
                        <a href={link} target="_blank" rel="noreferrer" className="btn btn-sm btn-link py-0" title="Open in the catalogue">
                          <FiExternalLink />
                        </a>
                      )}
                      <button className="btn btn-sm btn-outline-primary py-0" disabled={!!d.error} onClick={() => open(d)}>
                        <FiEdit2 className="me-1" />Edit
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {editing && draft && (
        <Modal
          title={`Edit dataset`}
          onClose={() => !saving && setEditing(null)}
          width={720}
          footer={
            <>
              <button className="btn btn-outline-secondary" disabled={saving} onClick={() => setEditing(null)}>Cancel</button>
              <button className="btn btn-primary" disabled={saving || !draft.title.trim()} onClick={save}>
                {saving && <span className="spinner-border spinner-border-sm me-1" />}Save
              </button>
            </>
          }
        >
          {saveError && <div className="alert alert-danger py-2 small">{saveError}</div>}
          <div className="small text-muted mb-3"><code>{editing.id}</code></div>
          <div className="mb-3">
            <label className="form-label fw-semibold" htmlFor="ds-title">Title</label>
            <input id="ds-title" className="form-control" value={draft.title} onChange={set('title')} />
          </div>
          <div className="mb-3">
            <label className="form-label" htmlFor="ds-desc">Description</label>
            <textarea id="ds-desc" className="form-control" rows={4} value={draft.description} onChange={set('description')} />
          </div>
          <div className="mb-3">
            <label className="form-label" htmlFor="ds-kw">Keywords</label>
            <input id="ds-kw" className="form-control" value={draft.keywords} onChange={set('keywords')} placeholder="5G, RAN, latency" />
            <div className="form-text">Separated by commas.</div>
          </div>
          <div className="mb-3">
            <label className="form-label" htmlFor="ds-lic">Licence</label>
            <input id="ds-lic" className="form-control" type="url" value={draft.license} onChange={set('license')}
              placeholder="https://creativecommons.org/licenses/by/4.0/" />
          </div>
          <div className="mb-3">
            <label className="form-label" htmlFor="ds-level">Visibility</label>
            {!data?.access_control ? (
              <div className="form-text">Visibility needs piveau access control, which is off on this deployment.</div>
            ) : (
              <>
                <select id="ds-level" className="form-select" value={draft.level} onChange={set('level')}
                  disabled={levels.length < 2}>
                  {!draft.level && <option value="">unknown</option>}
                  {levels.map(l => <option key={l} value={l}>{LEVEL_LABEL[l]}</option>)}
                </select>
                <div className="form-text">
                  {draft.level ? LEVEL_HELP[draft.level] : ''}
                  {levels.length < 2 && ' The catalogue is private, so its datasets are members only.'}
                </div>
              </>
            )}
          </div>
          <div className="alert alert-secondary py-2 small mb-0">
            This changes the dataset in the catalogue. The testbed's metadata.json is not changed, so re-registering the
            dataset from the asset monitor brings back its original fields and visibility.
          </div>
        </Modal>
      )}
    </div></div>
  )
}
