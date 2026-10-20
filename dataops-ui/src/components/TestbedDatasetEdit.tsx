import { useEffect, useState } from 'react'
import { FiArrowLeft, FiExternalLink } from 'react-icons/fi'
import { editTestbedDataset, getTestbedDataset } from '../api/airflow'
import { catalogueDatasetUrl } from '../config'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import type { DatasetAccessLevel, DatasetEditRequest, NavigateFn, TestbedDataset, TestbedDatasetDetail } from '../types'

export const LEVEL_LABEL: Record<DatasetAccessLevel, string> = {
  public: 'Public',
  restricted: 'Signed-in users',
  internal: 'Members only',
}

const LEVEL_HELP: Record<DatasetAccessLevel, string> = {
  public: 'Everyone, signed in or not.',
  restricted: 'Anyone signed in to 6G-DALI.',
  internal: 'Only the people given access to this catalogue (Access tab) and Data Space operators.',
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

interface Props {
  slug: string
  datasetId: string
  onNavigate: NavigateFn
}

/**
 * Edit one dataset of a testbed's catalogue: its main fields and its visibility. Changes the catalogue only; the
 * testbed's metadata.json stays as it was shared.
 */
export default function TestbedDatasetEdit({ slug, datasetId, onNavigate }: Props) {
  const [data, setData] = useState<TestbedDatasetDetail | null>(null)
  const [draft, setDraft] = useState<Draft | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    getTestbedDataset(slug, datasetId)
      .then(r => { setData(r); setDraft(draftOf(r.dataset)) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [slug, datasetId])

  const back = () => onNavigate('testbed-detail', { dagId: slug, runId: 'datasets' })

  async function save() {
    if (!data || !draft) return
    const changes = changesOf(data.dataset, draft)
    if (Object.keys(changes).length === 0) { back(); return }
    setSaving(true)
    setError(null)
    try {
      await editTestbedDataset(slug, datasetId, changes)
      back()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  if (!data || !draft) return error ? <ErrorMessage message={error} /> : <LoadingSpinner />

  const set = (k: keyof Draft) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
    setDraft(d => d && { ...d, [k]: e.target.value })
  const levels = data.levels
  const link = catalogueDatasetUrl(datasetId)
  const changed = Object.keys(changesOf(data.dataset, draft)).length > 0

  return (
    <div style={{ maxWidth: 820 }}>
      {error && <ErrorMessage message={error} />}

      <div className="card mb-3"><div className="card-body">
        <div className="d-flex flex-wrap align-items-center gap-2 mb-3">
          <code className="text-muted">{datasetId}</code>
          <span className="text-muted small">in <code>{data.catalogue_id}</code></span>
          <span className="text-muted small">· {data.dataset.distributions ?? 0} file(s)</span>
          {link && (
            <a href={link} target="_blank" rel="noreferrer" className="small ms-auto">
              Open in the catalogue <FiExternalLink />
            </a>
          )}
        </div>

        <div className="mb-3">
          <label className="form-label fw-semibold" htmlFor="ds-title">Title</label>
          <input id="ds-title" className="form-control" value={draft.title} onChange={set('title')} />
        </div>
        <div className="mb-3">
          <label className="form-label" htmlFor="ds-desc">Description</label>
          <textarea id="ds-desc" className="form-control" rows={6} value={draft.description} onChange={set('description')} />
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
          {!data.access_control ? (
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
        <div className="alert alert-secondary py-2 small">
          This changes the dataset in the catalogue. The testbed's metadata.json is not changed, so re-registering the
          dataset from the asset monitor brings back its original fields and visibility.
        </div>

        <div className="d-flex gap-2">
          <button className="btn btn-outline-secondary" disabled={saving} onClick={back}>
            <FiArrowLeft className="me-1" />Back
          </button>
          <button className="btn btn-primary" disabled={saving || !changed || !draft.title.trim()} onClick={save}>
            {saving && <span className="spinner-border spinner-border-sm me-1" />}Save
          </button>
        </div>
      </div></div>
    </div>
  )
}
