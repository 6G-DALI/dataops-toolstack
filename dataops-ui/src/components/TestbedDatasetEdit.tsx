import { useEffect, useState } from 'react'
import { FiArrowLeft, FiCode, FiExternalLink } from 'react-icons/fi'
import { editTestbedDataset, getTestbedDataset, getTestbedDatasetRecord, replaceTestbedDatasetRecord } from '../api/airflow'
import { isTestbedAdmin } from '../auth/testbedAccess'
import { catalogueDatasetUrl } from '../config'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import type {
  DatasetAccessLevel, DatasetCreator, DatasetEditRequest, NavigateFn, TestbedDataset, TestbedDatasetDetail,
} from '../types'

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

/** EU data themes (publications.europa.eu data-theme), as the metadata.json builder offers them. */
const THEMES: [string, string][] = [
  ['TECH', 'Science and technology'], ['ENER', 'Energy'], ['TRAN', 'Transport'], ['ENVI', 'Environment'],
  ['SOCI', 'Population and society'], ['ECON', 'Economy and finance'], ['GOVE', 'Government and public sector'],
  ['HEAL', 'Health'], ['AGRI', 'Agriculture, fisheries, forestry and food'], ['EDUC', 'Education, culture and sport'],
  ['INTR', 'International issues'], ['JUST', 'Justice, legal system and public safety'], ['REGI', 'Regions and cities'],
]

const NO_CREATOR: DatasetCreator = { name: '', kind: 'Person', email: '', orcid: '', affiliation: '' }

interface Draft {
  title: string
  description: string
  keywords: string
  license: string
  level: DatasetAccessLevel | ''
  produced_by: string
  publisher: string
  contact_email: string
  creator: DatasetCreator
  themes: string[]
  language: string
  spatial: string
  temporal_start: string
  temporal_end: string
  version: string
  contains_pii: '' | 'yes' | 'no'
}

function draftOf(d: TestbedDataset): Draft {
  return {
    title: d.title ?? '', description: d.description ?? '', keywords: (d.keywords ?? []).join(', '),
    license: d.license ?? '', level: d.access_level ?? '',
    produced_by: d.produced_by ?? '', publisher: d.publisher ?? '', contact_email: d.contact_email ?? '',
    creator: { ...NO_CREATOR, ...(d.creator ?? {}) }, themes: [...(d.themes ?? [])].sort(),
    language: d.language ?? '', spatial: d.spatial ?? '',
    temporal_start: d.temporal_start ?? '', temporal_end: d.temporal_end ?? '', version: d.version ?? '',
    contains_pii: d.contains_pii == null ? '' : d.contains_pii ? 'yes' : 'no',
  }
}

const TEXT_FIELDS = ['produced_by', 'publisher', 'contact_email', 'language', 'spatial', 'version'] as const

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
  for (const k of TEXT_FIELDS) if (draft[k].trim() !== was[k]) out[k] = draft[k].trim()
  const creator = Object.fromEntries(Object.entries(draft.creator).map(([k, v]) => [k, v.trim()])) as unknown as DatasetCreator
  if (JSON.stringify(creator) !== JSON.stringify(was.creator)) out.creator = creator
  const themes = [...draft.themes].sort()
  if (themes.join() !== was.themes.join()) out.themes = themes
  if (draft.temporal_start !== was.temporal_start || draft.temporal_end !== was.temporal_end) {
    out.temporal_start = draft.temporal_start
    out.temporal_end = draft.temporal_end
  }
  if (draft.contains_pii && draft.contains_pii !== was.contains_pii) out.contains_pii = draft.contains_pii === 'yes'
  return out
}

interface Props {
  slug: string
  datasetId: string
  onNavigate: NavigateFn
}

/**
 * Edit one dataset of a testbed's catalogue: its fields and its visibility. Changes the catalogue only; the testbed's
 * metadata.json stays as it was shared. Platform administrators can also edit the whole record as Turtle.
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
  const setCreator = (k: keyof DatasetCreator) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setDraft(d => d && { ...d, creator: { ...d.creator, [k]: e.target.value } })
  const toggleTheme = (code: string) =>
    setDraft(d => d && { ...d, themes: d.themes.includes(code) ? d.themes.filter(t => t !== code) : [...d.themes, code] })
  const levels = data.levels
  const link = catalogueDatasetUrl(datasetId)
  const changed = Object.keys(changesOf(data.dataset, draft)).length > 0
  const knownThemes = new Set(THEMES.map(([c]) => c))
  const otherThemes = draft.themes.filter(t => !knownThemes.has(t))

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

        <h6 className="text-uppercase text-muted small fw-semibold mb-3">Description</h6>
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
          <label className="form-label">Themes</label>
          <div className="d-flex flex-wrap gap-2">
            {THEMES.map(([code, label]) => (
              <label key={code} className="form-check form-check-inline small m-0" title={label}>
                <input type="checkbox" className="form-check-input" checked={draft.themes.includes(code)}
                  onChange={() => toggleTheme(code)} />
                <span className="form-check-label">{code}</span>
              </label>
            ))}
            {otherThemes.map(t => (
              <label key={t} className="form-check form-check-inline small m-0">
                <input type="checkbox" className="form-check-input" checked onChange={() => toggleTheme(t)} />
                <span className="form-check-label text-break">{t}</span>
              </label>
            ))}
          </div>
          <div className="form-text">EU data themes; most 5G/6G datasets are TECH.</div>
        </div>
        <div className="row g-3 mb-3">
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-ver">Version</label>
            <input id="ds-ver" className="form-control" value={draft.version} onChange={set('version')} placeholder="1.0" />
          </div>
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-lang">Language</label>
            <input id="ds-lang" className="form-control" value={draft.language} onChange={set('language')} placeholder="ENG" />
          </div>
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-pii">Contains personal data</label>
            <select id="ds-pii" className="form-select" value={draft.contains_pii} onChange={set('contains_pii')}>
              {!draft.contains_pii && <option value="">not stated</option>}
              <option value="no">No</option>
              <option value="yes">Yes</option>
            </select>
          </div>
        </div>

        <h6 className="text-uppercase text-muted small fw-semibold mt-4 mb-3">Coverage</h6>
        <div className="row g-3 mb-3">
          <div className="col-sm-6">
            <label className="form-label" htmlFor="ds-spatial">Location</label>
            <input id="ds-spatial" className="form-control" value={draft.spatial} onChange={set('spatial')} placeholder="Sophia Antipolis, France" />
          </div>
          <div className="col-6 col-sm-3">
            <label className="form-label" htmlFor="ds-from">From</label>
            <input id="ds-from" type="date" className="form-control" value={draft.temporal_start} onChange={set('temporal_start')} />
          </div>
          <div className="col-6 col-sm-3">
            <label className="form-label" htmlFor="ds-to">To</label>
            <input id="ds-to" type="date" className="form-control" value={draft.temporal_end} onChange={set('temporal_end')} />
          </div>
        </div>

        <h6 className="text-uppercase text-muted small fw-semibold mt-4 mb-3">Provenance and contact</h6>
        <div className="mb-3">
          <label className="form-label" htmlFor="ds-pb">Produced by</label>
          <input id="ds-pb" className="form-control" type="url" value={draft.produced_by} onChange={set('produced_by')}
            placeholder="https://dataspace.6gdali.eu/participant/eurecom" />
          <div className="form-text">The organisation's GAIA-X participant IRI (gax:producedBy).</div>
        </div>
        <div className="row g-3 mb-3">
          <div className="col-sm-6">
            <label className="form-label" htmlFor="ds-pub">Publisher</label>
            <input id="ds-pub" className="form-control" value={draft.publisher} onChange={set('publisher')} placeholder="EURECOM" />
          </div>
          <div className="col-sm-6">
            <label className="form-label" htmlFor="ds-contact">Contact e-mail</label>
            <input id="ds-contact" type="email" className="form-control" value={draft.contact_email} onChange={set('contact_email')} />
          </div>
        </div>
        <div className="row g-3 mb-3">
          <div className="col-sm-8">
            <label className="form-label" htmlFor="ds-cr">Creator</label>
            <input id="ds-cr" className="form-control" value={draft.creator.name} onChange={setCreator('name')} />
          </div>
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-crk">Creator is a</label>
            <select id="ds-crk" className="form-select" value={draft.creator.kind} onChange={setCreator('kind')}>
              <option value="Person">Person</option>
              <option value="Organization">Organisation</option>
            </select>
          </div>
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-cre">Creator e-mail</label>
            <input id="ds-cre" type="email" className="form-control" value={draft.creator.email} onChange={setCreator('email')} />
          </div>
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-cro">ORCID</label>
            <input id="ds-cro" className="form-control" value={draft.creator.orcid} onChange={setCreator('orcid')} placeholder="0000-0002-1825-0097" />
          </div>
          <div className="col-sm-4">
            <label className="form-label" htmlFor="ds-cra">Affiliation</label>
            <input id="ds-cra" className="form-control" value={draft.creator.affiliation} onChange={setCreator('affiliation')} />
          </div>
        </div>

        <h6 className="text-uppercase text-muted small fw-semibold mt-4 mb-3">Licence and visibility</h6>
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
              <select id="ds-level" className="form-select" value={draft.level} onChange={set('level')}>
                {!draft.level && <option value="">unknown</option>}
                {levels.map(l => <option key={l} value={l}>{LEVEL_LABEL[l]}</option>)}
              </select>
              <div className="form-text">
                {draft.level ? LEVEL_HELP[draft.level] : ''}
                {' '}New datasets start as {LEVEL_LABEL[data.default_level ?? 'internal']}; choosing Signed-in users or
                Public here publishes this one.
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

      {isTestbedAdmin() && (
        <RecordEditor slug={slug} datasetId={datasetId} onSaved={d => { setData({ ...data, dataset: d }); setDraft(draftOf(d)) }} />
      )}
    </div>
  )
}

/** The whole record as Turtle, for what the form does not cover. Platform administrators only (the orchestrator checks). */
function RecordEditor({ slug, datasetId, onSaved }: { slug: string; datasetId: string; onSaved: (d: TestbedDataset) => void }) {
  const [open, setOpen] = useState(false)
  const [turtle, setTurtle] = useState<string | null>(null)
  const [loaded, setLoaded] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  function load() {
    setError(null)
    setTurtle(null)
    getTestbedDatasetRecord(slug, datasetId)
      .then(r => { setTurtle(r.turtle); setLoaded(r.turtle) })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }

  async function save() {
    if (turtle == null) return
    setSaving(true)
    setError(null)
    setSaved(false)
    try {
      onSaved(await replaceTestbedDatasetRecord(slug, datasetId, turtle))
      setSaved(true)
      load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="card mb-3 border-warning-subtle"><div className="card-body">
      <div className="d-flex align-items-center gap-2">
        <FiCode />
        <span className="fw-semibold">Advanced: the whole record</span>
        <span className="badge text-bg-warning">Administrators</span>
        <button className="btn btn-sm btn-outline-secondary ms-auto" onClick={() => { if (!open) load(); setOpen(!open) }}>
          {open ? 'Close' : 'Edit as Turtle'}
        </button>
      </div>
      {open && (
        <div className="mt-3">
          <div className="form-text mb-2">
            Every field of the record, including the testbed context and the files. Saving replaces the record; the
            dataset's IRI must stay the same, every file must stay, and the visibility is kept (set it in the form
            above). Unsaved changes in the form above are not included.
          </div>
          {error && <ErrorMessage message={error} />}
          {saved && <div className="alert alert-success py-2 small">Saved.</div>}
          {turtle == null ? (!error && <LoadingSpinner />) : (
            <>
              <textarea className="form-control font-monospace small" rows={24} spellCheck={false} value={turtle}
                onChange={e => { setTurtle(e.target.value); setSaved(false) }} style={{ whiteSpace: 'pre', overflowX: 'auto' }} />
              <div className="d-flex gap-2 mt-2">
                <button className="btn btn-outline-secondary btn-sm" disabled={saving || turtle === loaded} onClick={() => setTurtle(loaded)}>
                  Discard changes
                </button>
                <button className="btn btn-warning btn-sm" disabled={saving || turtle === loaded} onClick={save}>
                  {saving && <span className="spinner-border spinner-border-sm me-1" />}Save record
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div></div>
  )
}
