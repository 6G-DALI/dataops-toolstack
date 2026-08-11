import { useEffect, useMemo, useRef, useState } from 'react'
import type { ChangeEvent, FormEvent, ReactNode } from 'react'
import { addDistributionRdf, createDatasetRdf } from '../api/airflow'
import {
  TURTLE_PREFIXES,
  TURTLE_PREFIX_COUNT,
  buildDatasetBody,
  buildDatasetTurtle,
  buildDistributionBody,
  buildDistributionTurtle,
} from '../map/datasetTurtle'
import CommaListInput, { splitList } from './CommaListInput'
import ErrorMessage from './ErrorMessage'
import SubmissionProgressModal, { initialSubmission } from './SubmissionProgressModal'
import type { SubmissionState } from './SubmissionProgressModal'
import type {
  DatasetAgentInput,
  DatasetIdentityInput,
  DatasetObjectInput,
  DistributionMetricsInput,
  GreatExpectation,
  NavigateFn,
  TestbedContextInput,
} from '../types'
import '../styles/DatasetCreator.css'

interface DatasetCreatorProps {
  onNavigate: NavigateFn
}

// sns_project_name starts empty with '6G-DALI' shown only as a placeholder —
// prefilling it would silently publish the wrong project name for a dataset
// contributed by any other SNS-JU project. validateStep(0) requires it.
const emptyIdentity: DatasetIdentityInput = {
  title: '', description: '', sns_project_name: '', publisher_name: '', contact_email: '',
  creators: [], contributors: [], keywords: [], related_publications: [], language: 'ENG', spatial: '',
  issued: '', temporal_start: '', temporal_end: '', version: '1.0',
}

const emptyObject: DatasetObjectInput = {
  license: 'https://creativecommons.org/licenses/by/4.0/', access_rights: 'PUBLIC',
  gdpr_compliant: true, fair_compliant: true, contains_pii: false, produced_by: '',
}

// ran_max_end_devices starts blank, not '1' — it has a UI field now, and an
// unanswered question must stay unanswered rather than being published as a
// confident "1 device" (the backend omits the triple entirely when null).
const emptyTestbedContext: TestbedContextInput = {
  underlay_platform: '', environment: '', network_domain: '', ran_3gpp_release: '',
  ran_new_radio_type: '', ran_split: '', ran_focused_technology: '', ran_coverage_type: '',
  ran_frequency_band: [], ran_bandwidth_mhz: '', ran_max_end_devices: '', ran_mobility_model: '',
  core_release: '', core_solution: '', transport_type: '', compute_orchestrator_type: '',
  compute_gpu_use: false, compute_virtualization_type: '', compute_infrastructure_type: '',
  traffic_origin: '', traffic_pattern: '', slice_type: '', reference_plane: '', related_vertical: '',
  observation_point_horizontal: '', observation_point_vertical: '', measurement_family: [], measurement_tool: [],
}

const emptyMetrics: DistributionMetricsInput = {
  variable_measured: [], measurement_technique: '',
}

function trimmedNonEmpty(values: string[]): string[] {
  return values.map(s => s.trim()).filter(Boolean)
}

// An append-able list leaves a blank row behind whenever "+ Add" is clicked and
// not filled in. Those must be dropped before submitting — the backend emits one
// triple per entry, so a blank would be published as an empty RDF literal.
function cleanIdentity(identity: DatasetIdentityInput): DatasetIdentityInput {
  return {
    ...identity,
    creators: identity.creators
      .map(c => ({ ...c, name: c.name.trim(), orcid: c.orcid.trim(), affiliation: c.affiliation.trim() }))
      .filter(c => c.name),
  }
}

function cleanTestbedContext(tc: TestbedContextInput): TestbedContextInput {
  return {
    ...tc,
    ran_frequency_band: trimmedNonEmpty(tc.ran_frequency_band),
    measurement_tool: trimmedNonEmpty(tc.measurement_tool),
  }
}

// Cap preview cell length so a single huge value (e.g. a long hex-encoded
// measurement) can't blow out the table layout — full data is never loaded
// here anyway, only the first 64KB is read for the preview.
const PREVIEW_CELL_MAX = 120

function clampCell(value: string): string {
  return value.length > PREVIEW_CELL_MAX ? value.slice(0, PREVIEW_CELL_MAX) + '…' : value
}

/** Parse a CSV/TSV preview into a header row + up to 5 data rows. */
function previewDelimited(text: string, sep?: string): string[][] {
  const lines = text.split('\n').filter(l => l.trim().length > 0)
  const delimiter = sep ?? (lines[0]?.includes('\t') ? '\t' : ',')
  return lines.slice(0, 6).map(l => l.split(delimiter).map(c => clampCell(c.trim())))
}

/** Parse a JSON Lines / NDJSON preview (one JSON object per line) into a header
 *  row (keys of the first object) + up to 5 data rows, aligned to those keys. */
function previewJsonl(text: string): string[][] {
  const lines = text.split('\n').filter(l => l.trim().length > 0)
  const objects: Record<string, unknown>[] = []
  // The last line of a 64KB slice is very likely truncated mid-object — drop
  // anything that doesn't parse rather than failing the whole preview.
  for (const line of lines.slice(0, 6)) {
    try {
      const obj = JSON.parse(line)
      if (obj && typeof obj === 'object' && !Array.isArray(obj)) objects.push(obj as Record<string, unknown>)
    } catch {
      // ignore an unparseable (likely truncated) line
    }
  }
  if (objects.length === 0) return []
  const header = Object.keys(objects[0])
  const cellText = (v: unknown): string =>
    v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v)
  const dataRows = objects.map(obj => header.map(k => clampCell(cellText(obj[k]))))
  return [header, ...dataRows]
}

// ── Shared field/card building blocks ───────────────────────────────────────

interface FieldProps {
  label: string
  required?: boolean
  children: ReactNode
  help?: string
}

function Field({ label, required, children, help }: FieldProps) {
  return (
    <div className="mb-3">
      <label className="form-label small">
        {label} {required && <span className="text-danger">*</span>}
      </label>
      {children}
      {help && <div className="form-text">{help}</div>}
    </div>
  )
}

/** A repeatable free-text property, edited as append-able rows rather than one
 *  comma-separated box — needed wherever a value can legitimately contain a
 *  comma (measurement tools) or where each entry becomes its own RDF triple
 *  and must stay individually queryable (RAN frequency bands). */
interface StringListFieldProps {
  label: string
  values: string[]
  onChange: (values: string[]) => void
  help?: string
  placeholder?: string
  addLabel?: string
}

function StringListField({ label, values, onChange, help, placeholder, addLabel = '+ Add' }: StringListFieldProps) {
  return (
    <Field label={label} help={help}>
      {values.map((value, i) => (
        <div className="input-group input-group-sm mb-2" key={i}>
          <input
            className="form-control"
            placeholder={placeholder}
            value={value}
            onChange={e => onChange(values.map((v, j) => (j === i ? e.target.value : v)))}
          />
          <button
            type="button"
            className="btn btn-outline-danger"
            aria-label={`Remove ${label} entry ${i + 1}`}
            onClick={() => onChange(values.filter((_, j) => j !== i))}
          >
            ×
          </button>
        </div>
      ))}
      <button type="button" className="btn btn-sm btn-outline-secondary" onClick={() => onChange([...values, ''])}>
        {addLabel}
      </button>
    </Field>
  )
}

const emptyCreator: DatasetAgentInput = { kind: 'Person', name: '', orcid: '', affiliation: '' }

/** dct:creator rows. `kind` picks the emitted rdf:type — the MAP credits both
 *  named researchers (foaf:Person, usually with an ORCID) and producing
 *  institutions (foaf:Organization). */
function CreatorsField({ values, onChange }: { values: DatasetAgentInput[], onChange: (v: DatasetAgentInput[]) => void }) {
  function update(i: number, patch: Partial<DatasetAgentInput>) {
    onChange(values.map((c, j) => (j === i ? { ...c, ...patch } : c)))
  }
  return (
    <Field
      label="Creators"
      help="Who produced the dataset (dct:creator) — named researchers, or the institution itself. ORCID accepts a bare 0000-0000-0000-0000 identifier or a full URL."
    >
      {values.map((c, i) => (
        <div className="row g-2 mb-2" key={i}>
          <div className="col-md-2">
            <select
              className="form-select form-select-sm"
              aria-label={`Creator ${i + 1} type`}
              value={c.kind}
              onChange={e => update(i, { kind: e.target.value as DatasetAgentInput['kind'] })}
            >
              <option value="Person">Person</option>
              <option value="Organization">Organisation</option>
            </select>
          </div>
          <div className="col-md-3">
            <input className="form-control form-control-sm" placeholder="Name" value={c.name}
              onChange={e => update(i, { name: e.target.value })} />
          </div>
          <div className="col-md-3">
            <input className="form-control form-control-sm" placeholder="ORCID (optional)" value={c.orcid}
              onChange={e => update(i, { orcid: e.target.value })} />
          </div>
          <div className="col-md-3">
            <input className="form-control form-control-sm" placeholder="Affiliation (optional)" value={c.affiliation}
              onChange={e => update(i, { affiliation: e.target.value })} />
          </div>
          <div className="col-md-1">
            <button
              type="button"
              className="btn btn-sm btn-outline-danger w-100"
              aria-label={`Remove creator ${i + 1}`}
              onClick={() => onChange(values.filter((_, j) => j !== i))}
            >
              ×
            </button>
          </div>
        </div>
      ))}
      <button type="button" className="btn btn-sm btn-outline-secondary" onClick={() => onChange([...values, emptyCreator])}>
        + Add creator
      </button>
    </Field>
  )
}

// The urn:6gdali:… tokens the orchestrator substitutes on submission. Matched
// rather than listed so the dataset sentinels and the distribution placeholders
// are both covered by one rule.
const PLACEHOLDER_RE = /urn:6gdali:[\w:-]+/g

/** Split a line into plain text and placeholder tokens, so the latter can be
 *  coloured differently from real values. */
function highlightPlaceholders(line: string): ReactNode[] {
  const parts: ReactNode[] = []
  let last = 0
  for (const match of line.matchAll(PLACEHOLDER_RE)) {
    const at = match.index ?? 0
    if (at > last) parts.push(line.slice(last, at))
    parts.push(<span className="rdf-placeholder" key={at}>{match[0]}</span>)
    last = at + match[0].length
  }
  if (last < line.length) parts.push(line.slice(last))
  return parts
}

/**
 * Renders a Turtle document one line per element, colouring the substitution
 * placeholders and flashing whichever lines the last keystroke changed.
 *
 * The flash works by giving a changed line a new React key so it remounts:
 * simply adding a class cannot replay a CSS animation that has already run on
 * that element. Line diffing happens in an effect rather than during render so
 * the previous-value ref is never mutated mid-render.
 */
function TurtleBlock({ text }: { text: string }) {
  const lines = useMemo(() => text.split('\n'), [text])
  const previousLines = useRef<string[] | null>(null)
  // `tick` makes each round of changes distinct, so a line changed twice in a
  // row gets a fresh key both times and animates twice.
  const [changed, setChanged] = useState<{ lines: Set<number>, tick: number }>(
    () => ({ lines: new Set(), tick: 0 })
  )

  useEffect(() => {
    const previous = previousLines.current
    previousLines.current = lines
    // Skip the first pass: every line is "new" then, and flashing the whole
    // document on open would be noise rather than signal.
    if (previous === null) return
    const next = new Set<number>()
    lines.forEach((line, i) => {
      if (previous[i] !== line) next.add(i)
    })
    if (next.size > 0) setChanged(c => ({ lines: next, tick: c.tick + 1 }))
  }, [lines])

  return (
    <pre className="bg-body-secondary border-top">
      {lines.map((line, i) => {
        const isChanged = changed.lines.has(i)
        return (
          <span
            // Remounts on change (tick differs), which restarts the animation.
            key={isChanged ? `${i}-${changed.tick}` : `${i}-static`}
            className={`rdf-line${isChanged ? ' rdf-line-changed' : ''}`}
          >
            {highlightPlaceholders(line)}
            {'\n'}
          </span>
        )
      })}
    </pre>
  )
}

/** Live side panel showing the MAP graph the form currently describes.
 *
 *  The dataset section is not a rendering of what will be published — it is
 *  what will be published, byte for byte, apart from the two sentinel
 *  identifiers the orchestrator swaps for the dataset id it mints (the record
 *  is submitted as RDF via POST /datasets/rdf). Once the dataset exists, it is
 *  what *was* published. The distribution section is a projection instead: that
 *  node is assembled server-side at upload time. */
function RdfSidePanel({ turtle, distributionTurtle, submitted, open, onClose }: {
  turtle: string
  /** Preview of the distribution node the Data step will add, once a file is chosen. */
  distributionTurtle: string | null
  submitted: boolean
  /** Stays mounted when closed so the collapse can animate; the CSS defers
   *  `visibility: hidden` to the end of it, which also takes the panel's
   *  buttons out of the tab order. */
  open: boolean
  onClose: () => void
}) {
  const [copied, setCopied] = useState(false)

  // The whole graph, on every step — the dataset always, and the distribution
  // as soon as a file exists to describe. Gating the distribution on the file
  // rather than on the wizard step is also what keeps the document internally
  // consistent: buildDatasetBody adds the dcat:distribution link under exactly
  // the same condition, so the link never appears without its target below it.
  const showDistribution = distributionTurtle !== null

  // One continuous Turtle document rather than two independent blocks: the
  // dataset carries its dcat:distribution link (added by buildDatasetBody's
  // distributionRef) and the distribution is declared directly below it.
  const body = showDistribution ? `${turtle}\n\n${distributionTurtle}` : turtle

  // Prefixes are counted and copied even while collapsed — they are part of the
  // document, not decoration.
  const copyText = `${TURTLE_PREFIXES}\n\n${body}\n`
  const lineCount = copyText.trimEnd().split('\n').length

  async function copy() {
    try {
      await navigator.clipboard.writeText(copyText)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard access can be denied (insecure origin, permissions) — the
      // text is selectable in the <pre> either way, so this is not worth an error.
    }
  }

  return (
    <aside
      className={`rdf-panel${open ? '' : ' rdf-panel-collapsed'}`}
      aria-label="Semantic view of the submission"
      aria-hidden={!open}
    >
      <div className="card rdf-panel-inner">
        <div className="card-header d-flex align-items-center justify-content-between gap-2">
          <span className="fw-semibold text-uppercase small">Semantic View</span>
          <div className="d-flex align-items-center gap-2">
            <span className="badge text-bg-secondary">Turtle · {lineCount} lines</span>
            <button type="button" className="btn btn-sm btn-outline-secondary py-0" onClick={copy}>
              {copied ? 'Copied' : 'Copy'}
            </button>
            <button type="button" className="btn-close btn-sm" aria-label="Hide semantic view" onClick={onClose} />
          </div>
        </div>
        <div className="card-body rdf-panel-body">
          {/* Boilerplate, identical on every submission — collapsed by default
              so it doesn't push the actual graph out of view. */}
          <details className="rdf-prefixes">
            <summary>
              <span className="rdf-prefixes-caret" aria-hidden="true">›</span>
              {TURTLE_PREFIX_COUNT} prefix declarations
            </summary>
            <pre className="bg-body-secondary">{TURTLE_PREFIXES}</pre>
          </details>

          <p className="form-text px-3 pt-2 mb-2">
            {submitted
              ? 'Dataset submitted to the Staging Catalogue as shown.'
              : 'The dataset is submitted as shown when you create it; updates as you type.'}{' '}
            {showDistribution
              ? 'Its distribution is submitted with the file.'
              : 'Choose a file in the Data step to see the distribution it will register.'}{' '}
            The <code>urn:6gdali:…</code> values are the identifiers assigned during submission.
          </p>
          <TurtleBlock text={body} />
        </div>
      </div>
    </aside>
  )
}

type Obligation = 'Mandatory' | 'Recommended' | 'Optional' | 'Auto-generated'

const OBLIGATION_CLASS: Record<Obligation, string> = {
  Mandatory: 'text-bg-danger',
  Recommended: 'text-bg-warning',
  Optional: 'text-bg-success',
  'Auto-generated': 'text-bg-danger',
}

interface CardProps {
  title: string
  obligation: Obligation
  children: ReactNode
}

function Card({ title, obligation, children }: CardProps) {
  return (
    <div className="card mb-3">
      <div className="card-header d-flex align-items-center justify-content-between">
        <span className="fw-semibold text-uppercase small">{title}</span>
        <span className={`badge ${OBLIGATION_CLASS[obligation]}`}>{obligation}</span>
      </div>
      <div className="card-body">{children}</div>
    </div>
  )
}

// ── Quality check state ──────────────────────────────────────────────────────

interface ColumnCheckState {
  exist: boolean
  notnull: boolean
}

type AddCheckType = 'between' | 'date' | 'inset' | 'notinset' | 'allnull'

const ADD_CHECK_LABELS: Record<AddCheckType, string> = {
  between: 'ExpectColumnValuesToBeBetween',
  date: 'ExpectColumnValuesToBeDateutilParseable',
  inset: 'ExpectColumnValuesToBeInSet',
  notinset: 'ExpectColumnValuesToNotBeInSet',
  allnull: 'ExpectColumnValuesToBeNull',
}

interface AddedCheck {
  id: number
  type: AddCheckType
  column: string
  minValue?: string
  maxValue?: string
  values?: string
}

function addedCheckToExpectation(c: AddedCheck): GreatExpectation {
  switch (c.type) {
    case 'between':
      return {
        type: 'expect_column_values_to_be_between', column: c.column,
        ...(c.minValue ? { min_value: Number(c.minValue) } : {}),
        ...(c.maxValue ? { max_value: Number(c.maxValue) } : {}),
      }
    case 'date':
      return { type: 'expect_column_values_to_be_dateutil_parseable', column: c.column }
    case 'inset':
      return { type: 'expect_column_values_to_be_in_set', column: c.column, value_set: splitList(c.values ?? '') }
    case 'notinset':
      return { type: 'expect_column_values_to_not_be_in_set', column: c.column, value_set: splitList(c.values ?? '') }
    case 'allnull':
      return { type: 'expect_column_values_to_be_null', column: c.column }
  }
}

const STEPS = ['Metadata', 'Data', 'Quality Checks', 'Review & Submit'] as const

export default function DatasetCreator({ onNavigate }: DatasetCreatorProps) {
  const [step, setStep] = useState(0)
  const [identity, setIdentity] = useState(emptyIdentity)
  const [object, setObject] = useState(emptyObject)
  const [testbedContext, setTestbedContext] = useState(emptyTestbedContext)
  const [metrics, setMetrics] = useState(emptyMetrics)
  const [file, setFile] = useState<File | null>(null)
  const [previewRows, setPreviewRows] = useState<string[][] | null>(null)

  const [rowCountEnabled, setRowCountEnabled] = useState(true)
  const [rowCountMin, setRowCountMin] = useState('1')
  const [rowCountMax, setRowCountMax] = useState('')
  const [columnChecks, setColumnChecks] = useState<Record<string, ColumnCheckState>>({})
  const [addType, setAddType] = useState<AddCheckType>('between')
  const [addColumn, setAddColumn] = useState('')
  const [addMin, setAddMin] = useState('')
  const [addMax, setAddMax] = useState('')
  const [addValues, setAddValues] = useState('')
  const [addedChecks, setAddedChecks] = useState<AddedCheck[]>([])
  const [confirmRights, setConfirmRights] = useState(false)
  // Shown by default: the record is what actually gets submitted, so it should
  // be visible while the form is filled in rather than opt-in.
  const [showRdf, setShowRdf] = useState(true)

  const [submitting, setSubmitting] = useState(false)
  // Non-null once submit has been pressed: drives the progress modal, which
  // replaces the form until it is dismissed.
  const [submission, setSubmission] = useState<SubmissionState | null>(null)
  const [error, setError] = useState<string | null>(null)
  // Registering a dataset is two API calls, and both happen on submit. This is
  // set between them — so it is normally only observable when the first
  // succeeded and the second failed. It then serves two purposes: a retry
  // reuses the dataset instead of registering a duplicate, and the Metadata step
  // locks, because the dataset already exists in piveau and there is no update
  // endpoint for further edits to reach.
  const [createdDataset, setCreatedDataset] = useState<{ datasetId: string, catalogueId: string } | null>(null)

  const isLastStep = step === STEPS.length - 1
  const columns = metrics.variable_measured

  // Blank list rows are stripped first, so neither the submitted document nor
  // the panel shows empty literals the submitter never entered.
  const mapInput = useMemo(
    () => ({
      identity: cleanIdentity(identity),
      object,
      testbedContext: cleanTestbedContext(testbedContext),
    }),
    [identity, object, testbedContext]
  )

  // What actually goes to POST /datasets/rdf. Deliberately built without a
  // dcat:distribution link: the distribution does not exist yet at this point,
  // so publishing the link would leave a dangling reference in the catalogue.
  const submittedTurtle = useMemo(() => buildDatasetTurtle(mapInput), [mapInput])

  // The display forms — prefix block hoisted out, and the dataset carrying the
  // link to the distribution declared below it, so the panel reads as one graph.
  const datasetBody = useMemo(
    () => buildDatasetBody(mapInput, { distributionRef: file !== null }),
    [mapInput, file]
  )

  // Only once a file is chosen — before that there is no distribution to describe.
  // The body (no @prefix block) is for the panel, which renders the prefixes
  // once above the whole graph; the submitted form must be a standalone
  // document, so it carries its own.
  const distributionBody = useMemo(
    () => (file ? buildDistributionBody({ file, metrics, license: object.license }) : null),
    [file, metrics, object.license]
  )

  const submittedDistributionTurtle = useMemo(
    () => (file ? buildDistributionTurtle({ file, metrics, license: object.license }) : null),
    [file, metrics, object.license]
  )

  // The submission state starts null (no submission yet), so every partial
  // update has to fall back to the initial shape rather than spreading null.
  function updateSubmission(patch: Partial<SubmissionState>) {
    setSubmission(s => ({ ...(s ?? initialSubmission), ...patch }))
  }

  function ensureColumnCheck(col: string): ColumnCheckState {
    return columnChecks[col] ?? { exist: true, notnull: true }
  }

  function toggleColumnCheck(col: string, key: keyof ColumnCheckState, value: boolean) {
    setColumnChecks(prev => ({ ...prev, [col]: { ...ensureColumnCheck(col), [key]: value } }))
  }

  function handleFileChange(e: ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0] ?? null
    setFile(f)
    setPreviewRows(null)
    if (!f) return

    const ext = f.name.split('.').pop()?.toLowerCase()
    if (ext !== 'csv' && ext !== 'tsv' && ext !== 'jsonl' && ext !== 'ndjson') return

    const isJsonl = ext === 'jsonl' || ext === 'ndjson'
    const reader = new FileReader()
    reader.onload = ev => {
      const text = String(ev.target?.result ?? '')
      const rows = isJsonl
        ? previewJsonl(text)
        : previewDelimited(text, ext === 'tsv' ? '\t' : undefined)
      if (!rows || rows.length === 0) return
      setPreviewRows(rows)
      if (columns.length === 0) {
        setMetrics(m => ({ ...m, variable_measured: rows[0] }))
      }
    }
    // JSONL rows can be individually huge (e.g. a long hex-encoded measurement
    // value), so read a larger slice than for CSV/TSV — otherwise a single
    // first line bigger than the slice is truncated and never parses.
    reader.readAsText(f.slice(0, isJsonl ? 4 * 1024 * 1024 : 65536))
  }

  function addCheck() {
    if (!addColumn) return
    const id = Date.now()
    setAddedChecks(prev => [...prev, {
      id, type: addType, column: addColumn,
      minValue: addMin || undefined, maxValue: addMax || undefined, values: addValues || undefined,
    }])
    setAddMin(''); setAddMax(''); setAddValues('')
  }

  function removeCheck(id: number) {
    setAddedChecks(prev => prev.filter(c => c.id !== id))
  }

  function buildExpectations(): GreatExpectation[] {
    const exps: GreatExpectation[] = []
    if (rowCountEnabled) {
      exps.push({
        type: 'expect_table_row_count_to_be_between',
        min_value: Number(rowCountMin) || 1,
        ...(rowCountMax ? { max_value: Number(rowCountMax) } : {}),
      })
    }
    for (const col of columns) {
      const cs = ensureColumnCheck(col)
      if (cs.exist) exps.push({ type: 'expect_column_to_exist', column: col })
      if (cs.notnull) exps.push({ type: 'expect_column_values_to_not_be_null', column: col })
    }
    exps.push(...addedChecks.map(addedCheckToExpectation))
    return exps
  }

  function validateStep(i: number): string | null {
    if (i === 0) {
      if (!identity.title.trim()) return 'Title is required.'
      if (!identity.description.trim()) return 'Description is required.'
      if (!object.license.trim()) return 'License is required.'
      // dali:snsProjectName is mandatory in the MAP at Violation severity.
      if (!identity.sns_project_name.trim()) return 'SNS project is required.'
    }
    if (i === 1) {
      if (!file) return 'A dataset file is required.'
    }
    if (i === STEPS.length - 1) {
      if (!confirmRights) return 'Please confirm your contributor responsibilities before submitting.'
    }
    return null
  }

  // Once the dataset is created (step 1's own submit), its metadata step is
  // locked — there's no "update dataset" endpoint, so further edits to
  // identity/object/testbed_context would silently not be reflected.
  const minStep = createdDataset ? 1 : 0

  function goToStep(target: number) {
    const clampedTarget = Math.max(target, minStep)
    if (clampedTarget <= step) { setError(null); setStep(clampedTarget); return }
    for (let i = step; i < clampedTarget; i++) {
      const err = validateStep(i)
      if (err) { setError(err); setStep(i); return }
    }
    setError(null)
    setStep(clampedTarget)
  }

  function handleNext() {
    const err = validateStep(step)
    if (err) return setError(err)
    setError(null)
    setStep(s => Math.min(s + 1, STEPS.length - 1))
  }

  function handleBack() {
    setError(null)
    setStep(s => Math.max(s - 1, minStep))
  }

  /**
   * The whole submission, from the Review step: register the dataset's metadata,
   * then upload the file as its distribution. Two API calls, because the file
   * cannot be uploaded until the dataset it belongs to exists — but one action,
   * so nothing is written to the catalogue until the submitter presses submit.
   */
  // The event is optional so the progress modal's Retry can call this directly
  // rather than having to synthesise a submit event.
  async function handleSubmit(e?: FormEvent) {
    e?.preventDefault()

    // Re-validate every step, not just this one: the submitter can step back and
    // clear a field they had already filled in, and only the forward navigation
    // path checks the steps in between.
    for (let i = 0; i < STEPS.length; i++) {
      const err = validateStep(i)
      if (err) {
        setError(err)
        setStep(Math.max(i, minStep))
        return
      }
    }
    setError(null)
    setSubmitting(true)

    // Reuse the dataset when retrying after a half-failed submission — creating
    // it again would leave a duplicate, empty dataset behind.
    let dataset = createdDataset
    setSubmission({
      ...initialSubmission,
      dataset: dataset ? 'done' : 'running',
      distribution: 'running',
      datasetId: dataset?.datasetId,
      catalogueId: dataset?.catalogueId,
    })

    try {
      if (!dataset) {
        updateSubmission({ dataset: 'running', distribution: 'pending' })
        // Submitted as RDF, not as JSON field groups: the graph published to the
        // catalogue is the same one the panel showed, so what the submitter
        // reviewed is what lands there.
        const created = await createDatasetRdf(submittedTurtle)
        dataset = { datasetId: created.dataset_id, catalogueId: created.catalogue_id }
        setCreatedDataset(dataset)
        updateSubmission({
          dataset: 'done',
          datasetId: created.dataset_id,
          catalogueId: created.catalogue_id,
        })
      }

      updateSubmission({ distribution: 'running' })
      // Submitted as RDF alongside the file: the distribution document the panel
      // showed is the one registered, and the response carries it back with the
      // upload-time placeholders resolved.
      const res = await addDistributionRdf(
        dataset.datasetId, dataset.catalogueId, file as File,
        submittedDistributionTurtle as string, metrics, buildExpectations()
      )
      // The distribution call also triggers the validation DAG, so validation
      // moves to 'running' here; the modal then polls the run for its real state.
      updateSubmission({ distribution: 'done', validation: 'running', result: res })
    } catch (err) {
      const message = (err as Error).message
      setError(message)
      setSubmission(s => {
        const prev = s ?? initialSubmission
        // Blame whichever call was in flight: if the dataset had not been
        // registered yet, that is the step that failed.
        const datasetFailed = prev.dataset === 'running'
        return {
          ...prev,
          dataset: datasetFailed ? 'failed' : prev.dataset,
          distribution: datasetFailed ? 'pending' : 'failed',
          error: message,
        }
      })
    } finally {
      setSubmitting(false)
    }
  }

  function handleReset() {
    setError(null)
    setSubmission(null)
    setCreatedDataset(null)
    setStep(0)
    setIdentity(emptyIdentity)
    setObject(emptyObject)
    setTestbedContext(emptyTestbedContext)
    setMetrics(emptyMetrics)
    setFile(null)
    setPreviewRows(null)
    setRowCountEnabled(true)
    setRowCountMin('1')
    setRowCountMax('')
    setColumnChecks({})
    setAddedChecks([])
    setConfirmRights(false)
    setShowRdf(false)
  }

  return (
    <div>
      {submission && (
        <SubmissionProgressModal
          submission={submission}
          busy={submitting}
          onClose={() => setSubmission(null)}
          onRetry={() => { void handleSubmit() }}
          onSubmitAnother={handleReset}
          onNavigate={onNavigate}
        />
      )}

      <div className="d-flex justify-content-between align-items-start gap-3">
        <p className="text-muted">
          Describe the dataset (6G-DALI Metadata Application Profile), attach its file, configure
          quality checks, then submit. Nothing is written to the catalogue until you submit, so you
          can move between the steps freely — submitting then registers the metadata, uploads the
          file as a distribution and triggers automatic validation.
        </p>
        {/* Toggles from every step, not just Metadata — the record is what
            actually gets submitted, so it should always be inspectable. */}
        <button
          type="button"
          className="btn btn-sm btn-outline-secondary flex-shrink-0 d-flex align-items-center gap-2"
          onClick={() => setShowRdf(v => !v)}
          aria-expanded={showRdf}
          title="Show the MAP graph this form currently describes"
        >
          <span className={`rdf-toggle-chevron${showRdf ? '' : ' closed'}`} aria-hidden="true">›</span>
          {showRdf ? 'Hide semantic view' : 'Show semantic view'}
        </button>
      </div>

      <div className="creator-layout">
        <div className="creator-main">
          <div className="wizard-progress">
            {STEPS.map((title, i) => {
              const locked = i < minStep
              const state = i === step ? ' active' : i < step ? ' done' : ''
              return (
                <button
                  type="button"
                  key={title}
                  className={`wizard-progress-step${state}`}
                  onClick={() => goToStep(i)}
                  disabled={locked}
                  title={locked ? 'Dataset already created — metadata can no longer be edited' : undefined}
                >
                  <span className="wizard-progress-num">{locked ? '✓' : i + 1}</span>
                  {title}
                </button>
              )
            })}
          </div>

          {createdDataset && (
            <div className="alert alert-warning py-2 small" role="alert">
              The dataset was registered (<code>{createdDataset.datasetId}</code>) but its file was
              not uploaded. Its metadata is locked in — there is no update endpoint, so further edits
              could not reach it — but you can fix the Data or Quality Checks steps and retry the
              upload. Retrying reuses this dataset rather than registering a second one.
            </div>
          )}

          {error && <ErrorMessage message={error} />}

          <form onSubmit={handleSubmit}>

            {/* ── Step 1: Metadata ─────────────────────────────────────────── */}
            {step === 0 && (
              <>
                <Card title="Basic Information" obligation="Mandatory">
                  <Field label="Title" required>
                    <input className="form-control" value={identity.title}
                      onChange={e => setIdentity({ ...identity, title: e.target.value })} required />
                  </Field>
                  <Field label="Description" required>
                    <textarea className="form-control" rows={3} value={identity.description}
                      onChange={e => setIdentity({ ...identity, description: e.target.value })} required />
                  </Field>
                  <div className="row">
                    <div className="col-md-6">
                      <Field label="Publisher">
                        <input className="form-control" value={identity.publisher_name}
                          onChange={e => setIdentity({ ...identity, publisher_name: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-6">
                      <Field label="Contact email">
                        <input type="email" className="form-control" value={identity.contact_email}
                          onChange={e => setIdentity({ ...identity, contact_email: e.target.value })} />
                      </Field>
                    </div>
                  </div>
                  <div className="row">
                    <div className="col-md-6">
                      <Field label="License URL" required>
                        <input className="form-control" value={object.license}
                          onChange={e => setObject({ ...object, license: e.target.value })} required />
                      </Field>
                    </div>
                    <div className="col-md-6">
                      <Field label="Access rights">
                        <select className="form-select" value={object.access_rights}
                          onChange={e => setObject({ ...object, access_rights: e.target.value as typeof object.access_rights })}>
                          <option value="PUBLIC">PUBLIC</option>
                          <option value="RESTRICTED">RESTRICTED</option>
                          <option value="NON_PUBLIC">NON_PUBLIC</option>
                        </select>
                      </Field>
                    </div>
                  </div>
                  <div className="row">
                    <div className="col-md-4">
                      <Field label="SNS project" required help="The SNS-JU project that produced the data — not necessarily 6G-DALI.">
                        <input className="form-control" placeholder="6G-DALI" value={identity.sns_project_name}
                          onChange={e => setIdentity({ ...identity, sns_project_name: e.target.value })} required />
                      </Field>
                    </div>
                    <div className="col-md-4">
                      <Field label="Version">
                        <input className="form-control" value={identity.version}
                          onChange={e => setIdentity({ ...identity, version: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-4">
                      <Field label="First published" help="Upstream publication date for a harvested dataset. Defaults to today if left blank.">
                        <input type="date" className="form-control" value={identity.issued}
                          onChange={e => setIdentity({ ...identity, issued: e.target.value })} />
                      </Field>
                    </div>
                  </div>
                  <Field label="Keywords" help="Comma-separated">
                    <CommaListInput values={identity.keywords}
                      onChange={keywords => setIdentity({ ...identity, keywords })} />
                  </Field>
                  <div className="d-flex gap-4">
                    <div className="form-check">
                      <input type="checkbox" className="form-check-input" id="gdpr" checked={object.gdpr_compliant}
                        onChange={e => setObject({ ...object, gdpr_compliant: e.target.checked })} />
                      <label className="form-check-label small" htmlFor="gdpr">GDPR compliant</label>
                    </div>
                    <div className="form-check">
                      <input type="checkbox" className="form-check-input" id="fair" checked={object.fair_compliant}
                        onChange={e => setObject({ ...object, fair_compliant: e.target.checked })} />
                      <label className="form-check-label small" htmlFor="fair">FAIR compliant</label>
                    </div>
                    <div className="form-check">
                      <input type="checkbox" className="form-check-input" id="pii" checked={object.contains_pii}
                        onChange={e => setObject({ ...object, contains_pii: e.target.checked })} />
                      <label className="form-check-label small" htmlFor="pii">Contains PII</label>
                    </div>
                  </div>
                </Card>

                <Card title="Attribution" obligation="Recommended">
                  <CreatorsField
                    values={identity.creators}
                    onChange={creators => setIdentity({ ...identity, creators })}
                  />
                  <Field label="Contributors" help="Comma-separated names (dct:contributor) — anyone who contributed without being credited as a creator above.">
                    <CommaListInput values={identity.contributors}
                      onChange={contributors => setIdentity({ ...identity, contributors })} />
                  </Field>
                </Card>

                <Card title="Dataset & Testbed Details" obligation="Recommended">
                  <div className="row">
                    <div className="col-md-3">
                      <Field label="Data start date">
                        <input type="date" className="form-control" value={identity.temporal_start}
                          onChange={e => setIdentity({ ...identity, temporal_start: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-3">
                      <Field label="Data end date">
                        <input type="date" className="form-control" value={identity.temporal_end}
                          onChange={e => setIdentity({ ...identity, temporal_end: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-3">
                      <Field label="Environment" help="e.g. urban, indoors, rural">
                        <input className="form-control" value={testbedContext.environment}
                          onChange={e => setTestbedContext({ ...testbedContext, environment: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-3">
                      <Field label="Network domain" help="RAN | Transport | CORE | E2E">
                        <input className="form-control" value={testbedContext.network_domain}
                          onChange={e => setTestbedContext({ ...testbedContext, network_domain: e.target.value })} />
                      </Field>
                    </div>
                  </div>

                  <div className="row">
                    <div className="col-md-3">
                      <Field label="RAN NR type" help="NR-SA | NR-NSA | LTE">
                        <input className="form-control" value={testbedContext.ran_new_radio_type}
                          onChange={e => setTestbedContext({ ...testbedContext, ran_new_radio_type: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-3">
                      <Field label="RAN bandwidth (MHz)">
                        <input type="text" inputMode="numeric" className="form-control" value={testbedContext.ran_bandwidth_mhz}
                          onChange={e => setTestbedContext({ ...testbedContext, ran_bandwidth_mhz: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-3">
                      <Field label="Max end devices">
                        <input type="text" inputMode="numeric" className="form-control" placeholder="unspecified"
                          value={testbedContext.ran_max_end_devices}
                          onChange={e => setTestbedContext({ ...testbedContext, ran_max_end_devices: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-3">
                      <Field label="Core solution" help="OpenSource | Commercial">
                        <input className="form-control" value={testbedContext.core_solution}
                          onChange={e => setTestbedContext({ ...testbedContext, core_solution: e.target.value })} />
                      </Field>
                    </div>
                  </div>

                  <StringListField
                    label="RAN frequency bands"
                    values={testbedContext.ran_frequency_band}
                    onChange={ran_frequency_band => setTestbedContext({ ...testbedContext, ran_frequency_band })}
                    placeholder="e.g. n78, 3.5GHz, 5 GHz (IEEE 802.11 WLAN)"
                    help="One entry per band — a testbed operating on several bands at once (e.g. Wi-Fi MLO across 2.4 GHz and 5 GHz) records each separately."
                    addLabel="+ Add band"
                  />

                  <div className="row">
                    <div className="col-md-4">
                      <Field label="Compute orchestrator" help="Kubernetes | OpenStack | OSM | ONAP">
                        <input className="form-control" value={testbedContext.compute_orchestrator_type}
                          onChange={e => setTestbedContext({ ...testbedContext, compute_orchestrator_type: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-4">
                      <Field label="Slice type">
                        <input className="form-control" value={testbedContext.slice_type}
                          onChange={e => setTestbedContext({ ...testbedContext, slice_type: e.target.value })} />
                      </Field>
                    </div>
                    <div className="col-md-4">
                      <Field label="Related vertical" help="e.g. CAM, HEALTH">
                        <input className="form-control" value={testbedContext.related_vertical}
                          onChange={e => setTestbedContext({ ...testbedContext, related_vertical: e.target.value })} />
                      </Field>
                    </div>
                  </div>

                  <Field label="Measurement family" help="Comma-separated short codes, e.g. L1M, DRB, RRC">
                    <CommaListInput values={testbedContext.measurement_family}
                      onChange={measurement_family => setTestbedContext({ ...testbedContext, measurement_family })} />
                  </Field>

                  {/* Appended one row at a time rather than comma-split: tool
                      descriptions routinely contain commas of their own, which a
                      single comma-separated box would shred into several tools. */}
                  <StringListField
                    label="Measurement tools"
                    values={testbedContext.measurement_tool}
                    onChange={measurement_tool => setTestbedContext({ ...testbedContext, measurement_tool })}
                    placeholder="e.g. per-link CSI and PER statistics collector, 1 s sampling interval"
                    help="One entry per tool. Commas within a single tool's description are kept as-is."
                    addLabel="+ Add tool"
                  />
                </Card>

                <Card title="Additional Information" obligation="Optional">
                  <Field label="Produced by (GAIA-X participant URI)">
                    <input className="form-control" value={object.produced_by}
                      onChange={e => setObject({ ...object, produced_by: e.target.value })} />
                  </Field>
                  <Field label="Related publications" help="Comma-separated URIs">
                    <CommaListInput values={identity.related_publications}
                      onChange={related_publications => setIdentity({ ...identity, related_publications })} />
                  </Field>
                </Card>

              </>
            )}

            {/* ── Step 2: Data ─────────────────────────────────────────────── */}
            {step === 1 && (
              <Card title="Data Submission" obligation="Mandatory">
                <Field label="File" required help="Uploaded to the Data Lake at <catalogue_id>/<dataset_id>/<uuid>.<ext> — the original filename is kept as the distribution's title; <uuid>/<ext> are generated at upload time.">
                  <input type="file" className="form-control" onChange={handleFileChange} required />
                </Field>
                {file && (
                  <div className="alert alert-light border small mb-3">
                    <strong>{file.name}</strong> ({(file.size / (1024 * 1024)).toFixed(2)} MB)
                  </div>
                )}

                <Field label="Dataset columns / measured variables" help="Comma-separated. Auto-filled from a CSV/TSV/JSONL file above if left empty; used to drive Quality Checks.">
                  <CommaListInput values={metrics.variable_measured}
                    onChange={variable_measured => setMetrics({ ...metrics, variable_measured })} />
                </Field>
                <Field label="Measurement technique">
                  <input className="form-control" value={metrics.measurement_technique}
                    onChange={e => setMetrics({ ...metrics, measurement_technique: e.target.value })} />
                </Field>

                {previewRows && previewRows.length > 0 && (
                  <>
                    <label className="form-label small fw-semibold">Detected columns</label>
                    <div className="mb-3">
                      {previewRows[0].map(h => (
                        <span key={h} className="badge text-bg-light border me-1 mb-1">{h}</span>
                      ))}
                    </div>
                    <label className="form-label small fw-semibold">Preview (first rows)</label>
                    <div className="table-responsive">
                      <table className="table table-sm table-bordered small mb-0">
                        <thead><tr>{previewRows[0].map((h, i) => <th key={i}>{h}</th>)}</tr></thead>
                        <tbody>
                          {previewRows.slice(1).map((row, ri) => (
                            <tr key={ri}>{row.map((cell, ci) => <td key={ci}>{cell}</td>)}</tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </Card>
            )}

            {/* ── Step 3: Quality Checks ───────────────────────────────────── */}
            {step === 2 && (
              <>
                <Card title="Table-Level Checks" obligation="Recommended">
                  <div className="form-check mb-2">
                    <input type="checkbox" className="form-check-input" id="rowcount" checked={rowCountEnabled}
                      onChange={e => setRowCountEnabled(e.target.checked)} />
                    <label className="form-check-label" htmlFor="rowcount">
                      <code className="small">expect_table_row_count_to_be_between</code>
                    </label>
                  </div>
                  {rowCountEnabled && (
                    <div className="row">
                      <div className="col-md-3">
                        <Field label="Min rows">
                          <input className="form-control" value={rowCountMin} onChange={e => setRowCountMin(e.target.value)} />
                        </Field>
                      </div>
                      <div className="col-md-3">
                        <Field label="Max rows">
                          <input className="form-control" placeholder="unbounded" value={rowCountMax} onChange={e => setRowCountMax(e.target.value)} />
                        </Field>
                      </div>
                    </div>
                  )}
                </Card>

                <Card title="Per-Column Checks" obligation="Auto-generated">
                  {columns.length === 0 ? (
                    <p className="text-muted small mb-0">
                      No columns declared yet — set "Dataset columns" in the Data step, or upload a
                      CSV/TSV/JSONL file there to auto-detect them.
                    </p>
                  ) : (
                    <div className="table-responsive">
                      <table className="table table-sm align-middle mb-0">
                        <thead>
                          <tr><th>Column</th><th className="text-center">Exists</th><th className="text-center">Not Null</th></tr>
                        </thead>
                        <tbody>
                          {columns.map(col => {
                            const cs = ensureColumnCheck(col)
                            return (
                              <tr key={col}>
                                <td><code className="small">{col}</code></td>
                                <td className="text-center">
                                  <input type="checkbox" className="form-check-input" checked={cs.exist}
                                    onChange={e => toggleColumnCheck(col, 'exist', e.target.checked)} />
                                </td>
                                <td className="text-center">
                                  <input type="checkbox" className="form-check-input" checked={cs.notnull}
                                    onChange={e => toggleColumnCheck(col, 'notnull', e.target.checked)} />
                                </td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </Card>

                <Card title="Additional Column Checks" obligation="Optional">
                  <div className="d-flex gap-2 flex-wrap align-items-end mb-3">
                    <div>
                      <label className="form-label small">Check type</label>
                      <select className="form-select form-select-sm" value={addType} onChange={e => setAddType(e.target.value as AddCheckType)}>
                        {(Object.keys(ADD_CHECK_LABELS) as AddCheckType[]).map(t => (
                          <option key={t} value={t}>{ADD_CHECK_LABELS[t]}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="form-label small">Column</label>
                      <select className="form-select form-select-sm" value={addColumn} onChange={e => setAddColumn(e.target.value)}>
                        <option value="">— select —</option>
                        {columns.map(c => <option key={c} value={c}>{c}</option>)}
                      </select>
                    </div>
                    {addType === 'between' && (
                      <>
                        <div>
                          <label className="form-label small">Min</label>
                          <input className="form-control form-control-sm" style={{ width: 90 }} value={addMin} onChange={e => setAddMin(e.target.value)} />
                        </div>
                        <div>
                          <label className="form-label small">Max</label>
                          <input className="form-control form-control-sm" style={{ width: 90 }} value={addMax} onChange={e => setAddMax(e.target.value)} />
                        </div>
                      </>
                    )}
                    {(addType === 'inset' || addType === 'notinset') && (
                      <div>
                        <label className="form-label small">Values</label>
                        <input className="form-control form-control-sm" placeholder="val1, val2" value={addValues} onChange={e => setAddValues(e.target.value)} />
                      </div>
                    )}
                    <button type="button" className="btn btn-sm btn-primary" onClick={addCheck} disabled={!addColumn}>
                      + Add
                    </button>
                  </div>

                  {addedChecks.length > 0 && (
                    <ul className="list-group">
                      {addedChecks.map(c => (
                        <li key={c.id} className="list-group-item d-flex justify-content-between align-items-center small">
                          <span>
                            <code>{ADD_CHECK_LABELS[c.type]}</code> — column <code>{c.column}</code>
                            {c.minValue && `, min: ${c.minValue}`}
                            {c.maxValue && `, max: ${c.maxValue}`}
                            {c.values && `, values: ${c.values}`}
                          </span>
                          <button type="button" className="btn btn-sm btn-outline-danger" onClick={() => removeCheck(c.id)}>×</button>
                        </li>
                      ))}
                    </ul>
                  )}
                </Card>
              </>
            )}

            {/* ── Step 4: Review & Submit ──────────────────────────────────── */}
            {step === 3 && (
              <>
                <Card title="Review your submission" obligation="Mandatory">
                  <div className="mb-4">
                    <h6 className="text-uppercase small fw-bold text-muted border-bottom pb-2 mb-2">Metadata</h6>
                    <dl className="row small mb-0">
                      <dt className="col-sm-3">Title</dt><dd className="col-sm-9">{identity.title || '—'}</dd>
                      <dt className="col-sm-3">SNS project</dt><dd className="col-sm-9">{identity.sns_project_name || '—'}</dd>
                      <dt className="col-sm-3">Version</dt><dd className="col-sm-9">{identity.version || '—'}</dd>
                      <dt className="col-sm-3">First published</dt>
                      <dd className="col-sm-9">{identity.issued || 'today (default)'}</dd>
                      <dt className="col-sm-3">Publisher</dt><dd className="col-sm-9">{identity.publisher_name || '—'}</dd>
                      <dt className="col-sm-3">Creators</dt>
                      <dd className="col-sm-9">
                        {cleanIdentity(identity).creators.length
                          ? cleanIdentity(identity).creators.map(c => (
                            <div key={c.name}>
                              {c.name}
                              {c.affiliation && <span className="text-muted"> — {c.affiliation}</span>}
                              {c.orcid && <code className="ms-1 small">{c.orcid}</code>}
                            </div>
                          ))
                          : '—'}
                      </dd>
                      <dt className="col-sm-3">License</dt><dd className="col-sm-9">{object.license || '—'}</dd>
                      <dt className="col-sm-3">Access rights</dt><dd className="col-sm-9">{object.access_rights}</dd>
                      <dt className="col-sm-3">Keywords</dt>
                      <dd className="col-sm-9">
                        {identity.keywords.length
                          ? identity.keywords.map(k => <span key={k} className="badge text-bg-light border me-1">{k}</span>)
                          : '—'}
                      </dd>
                      <dt className="col-sm-3">Columns</dt>
                      <dd className="col-sm-9">
                        {columns.length ? columns.map(c => <code key={c} className="me-2">{c}</code>) : '—'}
                      </dd>
                    </dl>
                  </div>
                  <div className="mb-4">
                    <h6 className="text-uppercase small fw-bold text-muted border-bottom pb-2 mb-2">Data</h6>
                    <dl className="row small mb-0">
                      <dt className="col-sm-3">File</dt><dd className="col-sm-9">{file ? file.name : '—'}</dd>
                    </dl>
                  </div>
                  <div>
                    <h6 className="text-uppercase small fw-bold text-muted border-bottom pb-2 mb-2">Quality Checks</h6>
                    <div className="small">
                      {buildExpectations().map((exp, i) => (
                        <span key={i} className="badge text-bg-light border me-1 mb-1 font-monospace">
                          {exp.type}{exp.column ? `(${exp.column})` : ''}
                        </span>
                      ))}
                      {buildExpectations().length === 0 && '—'}
                    </div>
                  </div>
                </Card>

                <div className="card mb-3">
                  <div className="card-body">
                    <div className="form-check">
                      <input type="checkbox" className="form-check-input" id="confirm" checked={confirmRights}
                        onChange={e => setConfirmRights(e.target.checked)} />
                      <label className="form-check-label small" htmlFor="confirm">
                        I confirm that I hold the necessary rights to share this dataset under the stated
                        license, and that the information provided is accurate.
                      </label>
                    </div>
                  </div>
                </div>
              </>
            )}

            <div className="d-flex justify-content-between mb-4">
              <button type="button" className="btn btn-outline-secondary" onClick={() => onNavigate('datasets', {})}>
                Cancel
              </button>
              <div className="d-flex gap-2">
                <button type="button" className="btn btn-outline-secondary" onClick={handleBack} disabled={step === minStep}>
                  Back
                </button>
                {isLastStep ? (
                  <button type="submit" className="btn btn-primary" disabled={submitting}>
                    {submitting && <span className="spinner-border spinner-border-sm me-1" />}
                    {submitting ? 'Submitting…' : createdDataset ? 'Retry upload' : 'Submit dataset'}
                  </button>
                ) : (
                  <button type="button" className="btn btn-primary" onClick={handleNext}>
                    Next
                  </button>
                )}
              </div>
            </div>
          </form>
        </div>

        {/* Always mounted — a conditionally-rendered element cannot animate its
            own removal. The collapsed state is CSS-only (see .rdf-panel). */}
        <RdfSidePanel
          turtle={datasetBody}
          distributionTurtle={distributionBody}
          submitted={createdDataset !== null}
          open={showRdf}
          onClose={() => setShowRdf(false)}
        />
      </div>
    </div>
  )
}
