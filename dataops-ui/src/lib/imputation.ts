/**
 * The `imputation` parameter of the validate DAG (dali_dataspace_validate_dataset), as a form.
 *
 * The DAG takes one JSON object: which methods to run over the regularized bundle (`runs`), whether to fill the gaps at
 * all (`impute`), and whether the bundle's columns are unit-converted (`convert_units`). Typing that by hand is how a
 * missing closing bracket reaches Airflow as a string, so the trigger form builds it from selectors instead
 * (ImputationField), with these two pure functions in between: buildImputation turns the form into the JSON the DAG
 * reads and parseImputation reads one back, keeping every key it does not know so nothing is lost on the way.
 *
 * What the DAG accepts is documented in airflow/dags/dali_dataspace_validate_dataset.py and
 * airflow/plugins/dali/processing.py (imputation_runs); the methods and options below are the ones it can run.
 */

export type Lib = 'darts' | 'imputegap' | 'wavestitchplus'
export type Device = 'cpu' | 'gpu' | 'auto'

export interface MethodInfo {
  id: string
  hint: string
}

export interface LibInfo {
  label: string
  /** Where it runs, and what to expect. */
  hint: string
  methods: MethodInfo[]
}

export const LIBS: Record<Lib, LibInfo> = {
  darts: {
    label: 'Darts',
    hint: 'Interpolation in the DataOps worker, seconds. Kalman runs the vendored Darts runner and is slower.',
    methods: [
      { id: 'linear', hint: 'Straight line between the observed neighbours' },
      { id: 'nearest', hint: 'Copy of the nearest observed value' },
      { id: 'auto', hint: 'Linear or nearest, chosen by Darts' },
      { id: 'quadratic', hint: 'Spline of order 2 (needs SciPy)' },
      { id: 'cubic', hint: 'Spline of order 3 (needs SciPy)' },
      { id: 'slinear', hint: 'Linear spline (needs SciPy)' },
      { id: 'zero', hint: 'Last observed value carried forward (needs SciPy)' },
      { id: 'kalman', hint: 'Kalman filter, Darts runner' },
    ],
  },
  imputegap: {
    label: 'ImputeGAP',
    hint: 'Statistical fills in the DataOps worker, seconds.',
    methods: [
      { id: 'interpolation', hint: 'Interpolation between the observed neighbours' },
      { id: 'mean', hint: 'Column mean' },
      { id: 'mean_by_series', hint: 'Mean per series' },
      { id: 'min', hint: 'Column minimum' },
      { id: 'zero', hint: 'Zero' },
    ],
  },
  wavestitchplus: {
    label: 'WaveStitch+',
    hint: 'Diffusion model on the WaveStitch+ service. Minutes to hours on CPU: try "Fast" first.',
    methods: [
      { id: 'v1', hint: 'EM diffusion: trains on the bundle, then imputes' },
      { id: 'v2', hint: 'v1 output anchored to an interpolation prior (needs v1, which runs first)' },
      { id: 'harpoon', hint: 'Inference on the v1 checkpoint with bound guidance (needs v1, which runs first)' },
    ],
  },
}

export type OptionKind = 'flag' | 'int' | 'float' | 'choice'

export interface OptionSpec {
  key: string
  label: string
  kind: OptionKind
  /** The runner's own default, shown as the placeholder: leaving the field empty uses it. */
  placeholder?: string
  choices?: string[]
}

/** The options of each WaveStitch+ method that the form offers. Any other goes in through the JSON editor. */
export const WSP_OPTIONS: Record<string, OptionSpec[]> = {
  v1: [
    { key: 'fast', label: 'Fast', kind: 'flag' },
    { key: 'em_iterations', label: 'EM rounds', kind: 'int', placeholder: '3' },
    { key: 'epochs_per_em', label: 'Epochs per round', kind: 'int', placeholder: '50' },
    { key: 'ddim_steps', label: 'Sampling steps', kind: 'int', placeholder: '30' },
    { key: 'repaint_rounds', label: 'Repaint rounds', kind: 'int', placeholder: '3' },
  ],
  v2: [
    { key: 'prior', label: 'Prior', kind: 'choice', choices: ['auto', 'linear', 'nearest'] },
    { key: 'tau', label: 'Tau', kind: 'float', placeholder: '8' },
    { key: 'hard_prior', label: 'Hard prior', kind: 'int', placeholder: '32' },
  ],
  harpoon: [
    { key: 'ddim_steps', label: 'Sampling steps', kind: 'int', placeholder: '50' },
    { key: 'repaint_rounds', label: 'Repaint rounds', kind: 'int', placeholder: '3' },
    { key: 'bound_lambda', label: 'Bound weight', kind: 'float', placeholder: '0.3' },
    { key: 'prior_lambda', label: 'Prior weight', kind: 'float', placeholder: '0.25' },
  ],
}

export interface RunForm {
  /** Only for React keys; not part of the JSON. */
  id: string
  lib: Lib
  method: string
  /** WaveStitch+ only. */
  device: Device
  /** WaveStitch+ options as typed: '' is unset, a flag is true or absent. */
  options: Record<string, string | boolean>
  /** Everything else the run carried (`engine`, `v1_params`, options the form does not offer), kept as it was. */
  extra: Record<string, unknown>
}

export interface ImputationForm {
  /** false: build the bundle but fill nothing. */
  impute: boolean
  /** Convert the 6G-schema units by column name (MB, ms, fraction). The DAG leaves the data alone by default. */
  convertUnits: boolean
  runs: RunForm[]
  /** Top-level keys the form does not know (`build_bundle`, `prepared_dir`, ...), kept as they were. */
  extra: Record<string, unknown>
}

/** What the DAG runs when the parameter is empty (DEFAULT_IMPUTATION_CONFIG). */
export const DEFAULT_RUNS: ReadonlyArray<readonly [Lib, string]> = [
  ['darts', 'linear'], ['darts', 'nearest'], ['darts', 'kalman'],
  ['wavestitchplus', 'v1'], ['wavestitchplus', 'v2'], ['wavestitchplus', 'harpoon'],
]

let nextId = 1
const newId = () => `run-${nextId++}`

export function newRun(lib: Lib, method: string): RunForm {
  return { id: newId(), lib, method, device: 'cpu', options: {}, extra: {} }
}

export function defaultForm(): ImputationForm {
  return { impute: true, convertUnits: false, runs: DEFAULT_RUNS.map(([lib, method]) => newRun(lib, method)), extra: {} }
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
const isLib = (v: unknown): v is Lib => typeof v === 'string' && Object.prototype.hasOwnProperty.call(LIBS, v)
const isDevice = (v: unknown): v is Device => v === 'cpu' || v === 'gpu' || v === 'auto'

// ── form → JSON ─────────────────────────────────────────────────────────────────────────────────────────────────

/** The number typed in an option field, or undefined when it is empty or not a number of the kind. */
function optionNumber(kind: OptionKind, text: string): number | undefined {
  const t = text.trim()
  if (!t) return undefined
  const n = Number(t)
  if (!Number.isFinite(n)) return undefined
  if (kind === 'int' && !Number.isInteger(n)) return undefined
  return n
}

function buildRun(run: RunForm): Record<string, unknown> {
  const out: Record<string, unknown> = { ...run.extra, lib: run.lib, method: run.method }
  if (run.lib !== 'wavestitchplus') return out
  if (run.device !== 'cpu') out.device = run.device
  const params: Record<string, unknown> = isObject(run.extra.params) ? { ...run.extra.params } : {}
  for (const spec of WSP_OPTIONS[run.method] ?? []) {
    const value = run.options[spec.key]
    delete params[spec.key]
    if (spec.kind === 'flag') {
      if (value === true) params[spec.key] = true
    } else if (typeof value === 'string' && value.trim()) {
      const parsed = spec.kind === 'choice' ? value : optionNumber(spec.kind, value)
      if (parsed !== undefined) params[spec.key] = parsed
    }
  }
  if (Object.keys(params).length > 0) out.params = params
  else delete out.params
  return out
}

/** The JSON the DAG reads. */
export function buildImputation(form: ImputationForm): Record<string, unknown> {
  const out: Record<string, unknown> = { ...form.extra }
  delete out.impute
  delete out.convert_units
  delete out.runs
  if (!form.impute) out.impute = false
  out.convert_units = form.convertUnits
  out.runs = form.runs.map(buildRun)
  return out
}

export interface FormProblems {
  /** The form cannot be sent as it is: a typed value would be dropped from the JSON. */
  errors: string[]
  /** Worth knowing; the DAG copes. */
  notes: string[]
}

/** What is wrong with the form, in words. */
export function validateForm(form: ImputationForm): FormProblems {
  const errors: string[] = []
  const notes: string[] = []
  const seen = new Set<string>()
  for (const run of form.runs) {
    const key = `${run.lib}/${run.method}`
    if (seen.has(key)) notes.push(`${LIBS[run.lib].label} ${run.method} is listed twice; the DAG runs it once.`)
    seen.add(key)
    if (run.lib !== 'wavestitchplus') continue
    for (const spec of WSP_OPTIONS[run.method] ?? []) {
      const value = run.options[spec.key]
      if ((spec.kind === 'int' || spec.kind === 'float') && typeof value === 'string' && value.trim()
          && optionNumber(spec.kind, value) === undefined) {
        errors.push(`${run.method}: "${spec.label}" must be ${spec.kind === 'int' ? 'a whole number' : 'a number'}.`)
      }
    }
  }
  if (form.impute && form.runs.length === 0) notes.push('No method selected: the DAG falls back to Darts linear.')
  const wsp = form.runs.filter(r => r.lib === 'wavestitchplus').map(r => r.method)
  if (wsp.some(m => m !== 'v1') && !wsp.includes('v1')) {
    notes.push('v2 and harpoon need v1, which then runs first with its default settings. That is slow: add v1 to choose its settings.')
  }
  return { errors, notes }
}

// ── JSON → form ─────────────────────────────────────────────────────────────────────────────────────────────────

function parseRun(item: unknown): RunForm | null {
  let raw: Record<string, unknown>
  if (typeof item === 'string') {
    const [lib, ...rest] = item.split('/')
    raw = { lib, method: rest.join('/') }
  } else if (isObject(item)) {
    raw = item
  } else {
    return null
  }
  const { lib, method, device, params, ...extra } = raw
  if (!isLib(lib) || typeof method !== 'string') return null
  if (!LIBS[lib].methods.some(m => m.id === method)) return null
  const run = newRun(lib, method)
  run.extra = extra
  if (lib !== 'wavestitchplus') {
    // `device` and `params` mean nothing to the other engines; keep them rather than drop what was typed.
    if (device !== undefined) run.extra.device = device
    if (params !== undefined) run.extra.params = params
    return run
  }
  if (device !== undefined) {
    if (!isDevice(device)) return null
    run.device = device
  }
  if (params !== undefined) {
    if (!isObject(params)) return null
    const unknown: Record<string, unknown> = {}
    const specs = WSP_OPTIONS[method] ?? []
    for (const [key, value] of Object.entries(params)) {
      const spec = specs.find(s => s.key === key)
      if (!spec) unknown[key] = value
      else if (spec.kind === 'flag') run.options[key] = value === true
      else run.options[key] = String(value)
    }
    if (Object.keys(unknown).length > 0) run.extra.params = unknown
  }
  return run
}

/**
 * The form for a JSON value of the parameter, or null when it has a shape the form cannot show (the caller then keeps
 * the JSON editor). An empty value is what the DAG treats as "use the defaults", and gives the default form.
 */
export function parseImputation(value: unknown): ImputationForm | null {
  if (value === null || value === undefined || value === '') return defaultForm()
  if (!isObject(value)) return null
  if (Object.keys(value).length === 0) return defaultForm()

  const { impute, convert_units: convertUnits, runs, lib, method, ...extra } = value
  if (impute !== undefined && typeof impute !== 'boolean') return null
  if (convertUnits !== undefined && typeof convertUnits !== 'boolean') return null

  let parsed: RunForm[] = []
  if (runs !== undefined) {
    if (!Array.isArray(runs)) return null
    for (const item of runs) {
      const run = parseRun(item)
      if (!run) return null
      parsed.push(run)
    }
  } else if (lib !== undefined || method !== undefined) {
    // A single `lib`/`method` pair is one run, as the DAG reads it.
    const run = parseRun({ lib: lib ?? 'darts', method: method ?? 'linear' })
    if (!run) return null
    parsed = [run]
  }
  return { impute: impute !== false, convertUnits: convertUnits === true, runs: parsed, extra }
}
