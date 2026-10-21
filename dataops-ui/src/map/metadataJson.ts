/**
 * The testbed `metadata.json` that the s3-asset-monitor turns into a catalogue record: field list, defaults,
 * validation and import. Mirrors DatasetMetadata / TestbedContext in s3-asset-monitor and
 * 6gdali-metadata-application-profile/Testbed-Metadata-JSON-guide.md, which are the source of truth for the
 * key names and accepted values. Keys are snake_case; the monitor also accepts camelCase aliases on import.
 */

export type FieldType = 'text' | 'textarea' | 'date' | 'email' | 'uri' | 'orcid' | 'int' | 'bool' | 'select' | 'list'
/** M = mandatory, R = recommended, O = optional. */
export type Obligation = 'M' | 'R' | 'O'

export interface FieldDef {
  k: string
  label: string
  type: FieldType
  req?: Obligation
  def?: string | boolean
  /** Fixed choices of a select. */
  opts?: string[]
  /** Suggestions of a free-text field. */
  suggest?: string[]
  hint?: string
  wide?: boolean
  /** Offers a "Generate" button that fills in a UUID. */
  uuid?: boolean
  /** A select whose values "true"/"false" are written as JSON booleans. */
  boolish?: boolean
  /** A list whose every entry must be a URI. */
  uriList?: boolean
}

export interface SectionDef {
  id: string
  title: string
  desc: string
  /** Fields belong inside the `testbed_context` object. */
  tc?: boolean
  fields: FieldDef[]
}

export type Values = Record<string, string | boolean>

export const LICENSES = [
  'https://creativecommons.org/licenses/by/4.0/',
  'https://creativecommons.org/licenses/by-sa/4.0/',
  'https://creativecommons.org/licenses/by-nc/4.0/',
  'https://opensource.org/licenses/MIT',
  'https://opensource.org/licenses/Apache-2.0',
]
const THEMES = ['TECH', 'ENER', 'TRAN', 'ENVI', 'SOCI', 'ECON', 'GOVE', 'HEAL', 'AGRI', 'EDUC', 'INTR', 'JUST', 'REGI']
const LANGS = ['ENG', 'GRC', 'DEU', 'FRA', 'ITA', 'SPA', 'POR', 'NLD', 'FIN', 'SWE', 'POL']

export const SECTIONS: SectionDef[] = [
  { id: 'core', title: 'Core identity', desc: 'What the dataset is.', fields: [
    { k: 'title', label: 'Title', type: 'text', req: 'M', wide: true },
    { k: 'description', label: 'Description', type: 'textarea', req: 'M', wide: true, hint: 'Free text describing the content.' },
    { k: 'issued', label: 'Issued', type: 'date', req: 'M', hint: 'Date first published. modified is set to the same value on first registration.' },
    { k: 'version', label: 'Version', type: 'text', req: 'O', hint: 'e.g. "1.0"' },
    { k: 'identifier', label: 'Identifier', type: 'text', req: 'O', uuid: true, hint: 'UUID or DOI. If empty, the dataset directory name is used.' },
    { k: 'spatial', label: 'Spatial coverage', type: 'text', req: 'R', hint: 'e.g. "Athens, Greece"' },
    { k: 'temporal_start', label: 'Temporal coverage start', type: 'date', req: 'R' },
    { k: 'temporal_end', label: 'Temporal coverage end', type: 'date', req: 'R', hint: 'Start and end are only published together.' },
  ] },
  { id: 'rights', title: 'Rights and compliance', desc: "Licence, access conditions and the owner's declarations.", fields: [
    { k: 'license', label: 'Licence (URI)', type: 'uri', req: 'M', def: LICENSES[0], suggest: LICENSES, wide: true },
    { k: 'access_rights', label: 'Access rights', type: 'select', req: 'M', def: 'PUBLIC', opts: ['PUBLIC', 'RESTRICTED', 'NON_PUBLIC'] },
    { k: 'sns_project_name', label: 'SNS-JU project name', type: 'text', req: 'M', def: '6G-DALI', hint: 'Leave as default unless instructed.' },
    { k: 'gdpr_compliant', label: 'The dataset complies with GDPR', type: 'bool', req: 'M', def: true },
    { k: 'fair_compliant', label: 'The dataset follows the FAIR principles', type: 'bool', req: 'M', def: true },
    { k: 'contains_pii', label: 'The data contains personally identifiable information', type: 'bool', req: 'M', def: false },
    { k: 'exposed_through', label: 'Exposed through (URI)', type: 'uri', req: 'O', hint: 'Data space endpoint serving the dataset. Normally left to the platform.' },
  ] },
  { id: 'class', title: 'Classification', desc: 'How people find it.', fields: [
    { k: 'theme', label: 'EU data theme', type: 'text', req: 'R', def: 'TECH', suggest: THEMES, hint: 'Most 5G/6G datasets use TECH.' },
    { k: 'language', label: 'Language', type: 'text', req: 'O', def: 'ENG', suggest: LANGS, hint: 'EU language code, three capital letters.' },
    { k: 'keywords', label: 'Keywords', type: 'list', req: 'R', wide: true, hint: 'One per line. English where possible.' },
  ] },
  { id: 'agents', title: 'Agents', desc: 'Who produced and maintains the dataset.', fields: [
    { k: 'publisher', label: 'Publisher (organisation)', type: 'text', req: 'R' },
    { k: 'produced_by', label: 'Produced by (participant URI)', type: 'uri', req: 'R', hint: "Your organisation's 6G-DALI participant URI." },
    { k: 'creator_name', label: 'Creator name', type: 'text', req: 'R', hint: 'Person or team.' },
    { k: 'creator_kind', label: 'Creator is', type: 'select', req: 'O', def: 'Person', opts: ['Person', 'Organization'] },
    { k: 'creator_email', label: 'Creator e-mail', type: 'email', req: 'R' },
    { k: 'contact_email', label: 'Contact e-mail', type: 'email', req: 'R', hint: 'For dataset enquiries.' },
    { k: 'creator_orcid', label: 'Creator ORCID', type: 'orcid', req: 'O', hint: '0000-0002-1825-0097 or the full URL.' },
    { k: 'creator_affiliation', label: 'Creator affiliation', type: 'text', req: 'O' },
    { k: 'contributors', label: 'Contributors', type: 'list', req: 'O', hint: 'One name per line.' },
    { k: 'related_publications', label: 'Related publications (URIs)', type: 'list', req: 'O', uriList: true, hint: 'One DOI or arXiv URI per line.' },
  ] },
  { id: 'content', title: 'Content description', desc: "How the data was measured. Each file's columns, format, size and checksum are read from the file itself and attached to its catalogue entry.", fields: [
    { k: 'measurement_technique', label: 'Measurement technique', type: 'text', req: 'R', wide: true, hint: 'e.g. "Prometheus-based 5G NR KPI monitoring with 1s scrape interval"' },
    { k: 'columns', label: 'Columns / variables', type: 'list', req: 'O', wide: true, hint: 'Only for files without a header line (not CSV or TSV): a CSV\'s columns are read from its header.' },
  ] },
  { id: 'infra', title: 'Testbed context: infrastructure', tc: true, desc: 'SNS-JU Common Metadata Template group 3A. Fill in only what applies.', fields: [
    { k: 'underlay_platform', label: 'Underlay platform (URI)', type: 'uri' },
    { k: 'environment', label: 'Environment', type: 'select', opts: ['indoors', 'urban', 'rural', 'mixed'] },
    { k: 'network_domain', label: 'Network domain', type: 'select', opts: ['RAN', 'Transport', 'CORE', 'E2E'] },
    { k: 'ran_3gpp_release', label: 'RAN 3GPP release', type: 'select', opts: ['Release 15', 'Release 16', 'Release 17', 'Release 18'] },
    { k: 'ran_nr_type', label: 'RAN NR type', type: 'select', opts: ['NR-SA', 'NR-NSA', 'LTE'] },
    { k: 'ran_split', label: 'RAN split', type: 'select', opts: ['DU-RU split', 'No-Split', 'CU-DU split'] },
    { k: 'ran_focused_technology', label: 'RAN focused technology', type: 'select', opts: ['O-RAN', 'JSAC', 'RIC', 'No_focus'] },
    { k: 'ran_coverage_type', label: 'RAN coverage type', type: 'select', opts: ['Single_Macro', 'Single_Micro', 'Multicell_setup'] },
    { k: 'ran_frequency_band', label: 'RAN frequency bands', type: 'list', hint: 'One per line, e.g. n78.' },
    { k: 'ran_bandwidth_mhz', label: 'RAN bandwidth (MHz)', type: 'int', suggest: ['20', '50', '100'] },
    { k: 'ran_max_end_devices', label: 'RAN max end devices', type: 'int' },
    { k: 'ran_mobility_model', label: 'RAN mobility model', type: 'select', opts: ['static', 'pedestrian', 'vehicular', 'UAV'] },
    { k: 'core_release', label: 'Core release', type: 'text', suggest: ['Release 17'] },
    { k: 'core_solution', label: 'Core solution', type: 'select', opts: ['OpenSource', 'Commercial'] },
    { k: 'transport_type', label: 'Transport type', type: 'select', opts: ['wired', 'microwave', 'fiber_optics', 'satellite'] },
    { k: 'compute_orchestrator_type', label: 'Compute orchestrator', type: 'select', opts: ['Kubernetes', 'OpenStack', 'OSM', 'ONAP'] },
    { k: 'compute_gpu_use', label: 'Compute uses GPU', type: 'select', opts: ['true', 'false'], boolish: true },
    { k: 'compute_virtualization_type', label: 'Compute virtualization', type: 'select', opts: ['KVM', 'Docker', 'Bare-metal'] },
    { k: 'compute_infrastructure_type', label: 'Compute infrastructure', type: 'text', suggest: ['private-edge node', 'public cloud', 'HPC cluster'] },
  ] },
  { id: 'service', title: 'Testbed context: service', tc: true, desc: 'Template group 3B.', fields: [
    { k: 'traffic_origin', label: 'Traffic origin', type: 'select', opts: ['Manual', 'Application'] },
    { k: 'traffic_pattern', label: 'Traffic pattern', type: 'text', suggest: ['UL/DL UDP', 'DL TCP', 'UL UDP', 'UL TCP'] },
    { k: 'slice_type', label: 'Slice type', type: 'select', opts: ['slice101', 'Multi-slice', 'No_slicing'] },
    { k: 'reference_plane', label: 'Reference plane', type: 'select', opts: ['control plane', 'data plane', 'management plane'] },
    { k: 'related_vertical', label: 'Related vertical', type: 'select', opts: ['Vertical_agnostic', 'CAM', 'HEALTH', 'INDUSTRY', 'MEDIA'] },
  ] },
  { id: 'exp', title: 'Testbed context: experimentation and metrics', tc: true, desc: 'Template group 3C.', fields: [
    { k: 'observation_point_horizontal', label: 'Observation point (horizontal)', type: 'select',
      opts: ['E2E Application layer', 'End device to Access', 'DU to CU', 'Access to Edge', 'Access to Core', 'Core to Cloud'] },
    { k: 'observation_point_vertical', label: 'Observation point (vertical)', type: 'select',
      opts: ['Radio Level', 'Network Layer', 'Application Layer', 'Compute Resource-level', 'cross-layer'] },
    { k: 'measurement_family', label: 'Measurement families (3GPP TS 28.552)', type: 'list', hint: 'One per line, e.g. DRB, RRC, RRU, L1M, PEE, QF.' },
    { k: 'measurement_tool', label: 'Measurement tools', type: 'list', hint: 'One per line, e.g. Prometheus exporter, tcpdump, iperf3.' },
  ] },
]

/** Fields describing the organisation and testbed rather than one dataset: kept between datasets. */
export const TESTBED_KEYS = [
  'license', 'access_rights', 'sns_project_name', 'gdpr_compliant', 'fair_compliant', 'contains_pii',
  'theme', 'language', 'publisher', 'produced_by', 'creator_name', 'creator_kind', 'creator_email',
  'contact_email', 'creator_orcid', 'creator_affiliation', 'contributors', 'spatial',
]

export interface FlatField extends FieldDef {
  tc: boolean
}
export const ALL_FIELDS: FlatField[] = SECTIONS.flatMap(s => s.fields.map(f => ({ ...f, tc: !!s.tc })))

const today = () => new Date().toISOString().slice(0, 10)

/** Every field at its default; `issued` starts at today. */
export function defaultValues(): Values {
  const v: Values = {}
  for (const f of ALL_FIELDS) v[f.k] = f.type === 'bool' ? !!f.def : (f.def as string | undefined) ?? ''
  v.issued = today()
  return v
}

/** Clears the dataset-specific fields and keeps the testbed-wide ones (see TESTBED_KEYS and testbed_context). */
export function newDatasetValues(current: Values): Values {
  const fresh = defaultValues()
  for (const f of ALL_FIELDS) if (f.tc || TESTBED_KEYS.includes(f.k)) fresh[f.k] = current[f.k]
  return fresh
}

const lines = (s: string) => s.split(/\r?\n/).map(x => x.trim()).filter(Boolean)

function readValue(f: FlatField, raw: string | boolean | undefined): unknown {
  if (f.type === 'bool') return !!raw
  const s = typeof raw === 'string' ? raw.trim() : ''
  if (f.type === 'list') { const l = lines(typeof raw === 'string' ? raw : ''); return l.length ? l : undefined }
  if (!s) return undefined
  if (f.type === 'int') return /^\d+$/.test(s) ? parseInt(s, 10) : s // invalid text is kept so validation flags it
  if (f.boolish) return s === 'true'
  return s
}

/** The object to serialise: guide order, empty optional fields left out, `testbed_context` only when used. */
export function buildMetadata(values: Values): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  const tc: Record<string, unknown> = {}
  for (const f of ALL_FIELDS) {
    const v = readValue(f, values[f.k])
    if (v !== undefined) (f.tc ? tc : out)[f.k] = v
  }
  if (Object.keys(tc).length) out.testbed_context = tc
  return out
}

export function toJson(values: Values): string {
  return JSON.stringify(buildMetadata(values), null, 2) + '\n'
}

/* ---------- Validation ---------- */

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/
const realDate = (s: string) => DATE_RE.test(s) && !isNaN(Date.parse(s)) && new Date(s).toISOString().slice(0, 10) === s
const EMAIL_RE = /^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/
const URI_RE = /^[a-z][a-z0-9+.-]*:\/\/[^\s<>"]+$/i
const ORCID_RE = /^(https?:\/\/orcid\.org\/)?\d{4}-\d{4}-\d{4}-\d{3}[\dX]$/

export interface Validation {
  errors: Record<string, string>
  warnings: Record<string, string>
}

export function validateMetadata(values: Values): Validation {
  const data = buildMetadata(values)
  const tcData = (data.testbed_context ?? {}) as Record<string, unknown>
  const errors: Record<string, string> = {}
  const warnings: Record<string, string> = {}
  const err = (k: string, m: string) => { errors[k] ??= m }
  const warn = (k: string, m: string) => { warnings[k] ??= m }

  for (const f of ALL_FIELDS) {
    const v = (f.tc ? tcData : data)[f.k]
    if (v === undefined || v === '') {
      if (f.req === 'M' && f.type !== 'bool') err(f.k, 'Required.')
      else if (f.req === 'R') warn(f.k, 'Recommended: improves discoverability.')
      continue
    }
    const s = typeof v === 'string' ? v : ''
    if (f.type === 'date' && !realDate(s)) err(f.k, 'Use YYYY-MM-DD, a real date, no time.')
    if (f.type === 'email' && !EMAIL_RE.test(s)) err(f.k, 'Not a valid e-mail address.')
    if (f.type === 'uri' && !URI_RE.test(s)) err(f.k, 'Must be a full URI such as https://…')
    if (f.type === 'orcid' && !ORCID_RE.test(s)) err(f.k, 'Expected 0000-0002-1825-0097 or the orcid.org URL.')
    if (f.type === 'int' && typeof v !== 'number') err(f.k, 'Whole number only.')
    if (f.uriList && Array.isArray(v) && v.some(x => !URI_RE.test(String(x)))) err(f.k, 'Every entry must be a full URI.')
  }
  const str = (k: string) => (typeof data[k] === 'string' ? (data[k] as string) : '')
  if (str('language') && !/^[A-Z]{3}$/.test(str('language'))) err('language', 'Three capital letters, e.g. ENG.')
  if (str('theme') && !/^[A-Z]{4}$/.test(str('theme'))) warn('theme', 'EU data theme codes are four capital letters, e.g. TECH.')
  const ts = str('temporal_start')
  const te = str('temporal_end')
  if (!!ts !== !!te) err(ts ? 'temporal_end' : 'temporal_start', 'Provide both start and end, or neither.')
  if (ts && te && realDate(ts) && realDate(te) && ts > te) err('temporal_end', 'End is before start.')
  if (/\s|\//.test(str('identifier'))) err('identifier', 'No spaces or slashes.')
  if (data.contains_pii && data.gdpr_compliant) warn('contains_pii', 'Contains PII: double-check the GDPR declaration.')
  return { errors, warnings }
}

/* ---------- Import ---------- */

const KEY_ALIASES: Record<string, string> = {
  ran3gppRelease: 'ran_3gpp_release', ranNewRadioType: 'ran_nr_type', ran_new_radio_type: 'ran_nr_type',
}
const toSnake = (k: string) => KEY_ALIASES[k] ?? k.replace(/([A-Z])/g, m => '_' + m.toLowerCase())

/** Applies a parsed metadata.json to the form values. Unknown keys are ignored; camelCase keys are accepted. */
export function importMetadata(base: Values, json: unknown): Values {
  if (!json || typeof json !== 'object' || Array.isArray(json)) throw new Error('Top level must be a JSON object.')
  const flat: Record<string, unknown> = {}
  const walk = (o: Record<string, unknown>) => {
    for (const [k, v] of Object.entries(o)) {
      const key = toSnake(k)
      if (key === 'testbed_context' && v && typeof v === 'object' && !Array.isArray(v)) walk(v as Record<string, unknown>)
      else flat[key] = v
    }
  }
  walk(json as Record<string, unknown>)
  const next = { ...base }
  for (const f of ALL_FIELDS) {
    if (!(f.k in flat)) continue
    const v = flat[f.k]
    if (f.type === 'bool') next[f.k] = v === true || v === 'true'
    else if (f.type === 'list') next[f.k] = (Array.isArray(v) ? v : [v]).map(String).join('\n')
    else next[f.k] = String(v)
  }
  return next
}

/** A random v4 UUID; crypto.randomUUID needs a secure context, which a plain-http deployment is not. */
export function uuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') return crypto.randomUUID()
  const b = crypto.getRandomValues(new Uint8Array(16))
  b[6] = (b[6] & 0x0f) | 0x40
  b[8] = (b[8] & 0x3f) | 0x80
  const h = Array.from(b, x => x.toString(16).padStart(2, '0')).join('')
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}
