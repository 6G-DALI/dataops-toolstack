import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { FiDownload, FiExternalLink } from 'react-icons/fi'
import { getDagRun, getDatasets, getDistributions, getRunArtifactCsv, getRunArtifacts, getRunArtifactText } from '../api/airflow'
import type { RunArtifact, RunArtifacts, SeriesPoint } from '../types'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import MetricCard from './ui/MetricCard'
import SeriesChart from './ui/SeriesChart'
import CopyableId from './ui/CopyableId'
import { EffectsTable, ImputationView, IssuesView, LineageTable, MergedQualityView, PassFail, RemediationView } from './ui/PipelineReportView'
import { catalogueDatasetUrl } from '../config'
import '../styles/Chart.css'
import '../styles/RunResults.css'

/**
 * What a dali_dataspace_validate_dataset run produced.
 *
 * The tabs mirror the WaveStitchPlus dashboard's cleaning-side view
 * (dashboard/app.py, the `subset is None` branch): Overview, Quality,
 * Remediation, and the data itself. Its Imputation / Metrics / Distribution /
 * Long-gap tabs are deliberately absent — they compare imputation methods
 * against masked ground truth from a prepared/generated experiment bundle, and
 * this DAG builds its bundle without held-out truth. MAE/RMSE/MAPE would have
 * nothing to score against. What the imputation step did is reported on the
 * Issues & remediation tab instead.
 *
 * The run reports its own outputs: upload_artifacts returns {name: object key},
 * the orchestrator reads that XCom and serves the objects from the bucket the
 * run's conf names. Nothing here guesses key patterns.
 */

/**
 * What each artifact is, keyed by the name the run publishes it under
 * (dali.datalake._ARTIFACT_SUFFIXES). A bare `output_csv` next to a bucket path
 * told a reader nothing about which of five near-identical CSVs to download;
 * the stage each one represents is the thing worth saying.
 *
 * Declaration order is provenance order — each frame is derived from the one
 * above it, ending with the report that describes the whole run. The endpoint
 * returns artifacts alphabetically (routers/runs.py sorts the XCom), which
 * interleaved the imputed splits with the raw frame and broke the chain; the
 * list is re-sorted by this map's key order instead.
 *
 * An unknown name falls back to itself with no note and sorts to the end, so a
 * new artifact appears in the list rather than disappearing until this map is
 * updated.
 */
const ARTIFACTS: Record<string, { label: string, note: string, derivedFrom?: string }> = {
  input_csv: {
    label: 'Raw',
    note: 'The frame exactly as it arrived over EDC, before anything was changed.',
  },
  soft_cleaned_csv: {
    label: 'Soft-cleaned',
    derivedFrom: 'Raw',
    note: 'The conservative clean — empty and duplicate rows dropped, column names normalised — before any per-issue remediation.',
  },
  output_csv: {
    label: 'Remediated',
    derivedFrom: 'Soft-cleaned',
    note: 'The pipeline’s output: outliers clipped and fillable values filled. In time-series mode gaps are left for imputation instead.',
  },
  imputed_train_csv: {
    label: 'Imputed — train split',
    derivedFrom: 'Remediated',
    note: 'The training half of the regularized timeline, with missing values filled. Carries the imputer’s own feature columns, including is_gap, which marks the rows that were synthesised.',
  },
  imputed_test_csv: {
    label: 'Imputed — test split',
    derivedFrom: 'Remediated',
    note: 'The test half of the regularized timeline, with missing values filled.',
  },
  imputed_final_csv: {
    label: 'Imputed — full timeline',
    derivedFrom: 'the imputed splits',
    note: 'The analysis-ready frame: both splits stitched back into one gap-free series ordered by time, keeping a “split” label and dropping the imputer’s working columns. Read this one if you read only one.',
  },
  report_json: {
    label: 'Run report',
    note: 'Not a frame: cleaning, profiling and remediation in full, with the merged quality report under “dali_quality”.',
  },
}

const ARTIFACT_ORDER = Object.keys(ARTIFACTS)

function byProvenance(a: RunArtifact, b: RunArtifact): number {
  const ai = ARTIFACT_ORDER.indexOf(a.name)
  const bi = ARTIFACT_ORDER.indexOf(b.name)
  if (ai === bi) return a.name.localeCompare(b.name)
  if (ai === -1) return 1
  if (bi === -1) return -1
  return ai - bi
}

/** Artifact names as dali.processing.run_dataops_pipeline publishes them. */
const RAW = 'input_csv'
const SOFT = 'soft_cleaned_csv'
const REMEDIATED = 'output_csv'
const IMPUTED_TRAIN = 'imputed_train_csv'
const IMPUTED_TEST = 'imputed_test_csv'
const IMPUTED_FINAL = 'imputed_final_csv'

/**
 * Columns the bundle adds for modelling, not measurements of anything.
 *
 * preprocess_csv appends cyclic time features and gap bookkeeping to the
 * regularized frame. They are numeric, so the column picker would offer them
 * beside the real measures; `is_gap` is kept out of the picker too but read
 * separately — it is what marks a row as synthesised.
 */
const BUNDLE_FEATURE_COLUMNS = new Set([
  't_norm', 'sin_day', 'cos_day', 'is_gap', 'time_since_last_obs', 'time_to_next_obs',
  // The stitched timeline's train/test label — a category, not a measurement.
  'split',
])

/** Flag column marking rows the regularizer inserted, and imputation then filled. */
const GAP_FLAG = 'is_gap'

/**
 * Unit suffixes preprocess_csv appends when it converts (convert_units=True):
 * ram_usage → ram_usage_mb, mean → mean_ms. The cleaning report's
 * column_mapping does not cover these — it only records snake_casing — so a
 * column selected on the remediated frame would not be found on an imputed one.
 */
const UNIT_SUFFIXES = ['_mb', '_ms']

/**
 * Frames are fetched whole, as a series of ranged requests of this size.
 *
 * A single capped request meant the chart showed a prefix of a frame and said
 * so — tolerable for the pipeline's own three, misleading for the imputed
 * splits, which are the regularized timeline at four times the rows and were
 * cut off a quarter of the way through. Walking the windows costs a few more
 * round trips and gets the whole file.
 */
const CSV_WINDOW_BYTES = 4 * 1024 * 1024
/** Where the walk gives up rather than pulling something unbounded into a tab. */
const CSV_TOTAL_CAP = 64 * 1024 * 1024
/** Points actually drawn; beyond this observed values are stride-sampled. */
const MAX_PLOT_POINTS = 8000

/**
 * Minimal RFC4180-ish parser: pandas quotes any field containing a comma.
 *
 * Every line is a data row here — no header is peeled off. That split matters
 * once a frame is loaded in chunks: the server sends the CSV header only in
 * the first window of a walk (dataops-orchestrator's own contract), so a
 * continuation chunk is pure data and must not have its first row mistaken
 * for one.
 */
function splitCsvRows(text: string): string[][] {
  const rows: string[][] = []
  let row: string[] = []
  let field = ''
  let quoted = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++ } else { quoted = false }
      } else field += c
    } else if (c === '"') quoted = true
    else if (c === ',') { row.push(field); field = '' }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = '' }
    else if (c !== '\r') field += c
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row) }
  return rows
}

function parseCsv(text: string): { header: string[], rows: string[][] } {
  const rows = splitCsvRows(text)
  const header = rows.shift() ?? []
  return { header, rows }
}

const isBlank = (v: string | undefined) =>
  v === undefined || v.trim() === '' || v.trim().toLowerCase() === 'nan'

function numericColumns(header: string[], rows: string[][]): string[] {
  return header.filter((_, col) => {
    let seen = 0
    for (let i = 0; i < rows.length && seen < 8; i++) {
      const v = rows[i][col]
      if (isBlank(v)) continue
      if (Number.isNaN(Number(v))) return false
      seen++
    }
    return seen > 0
  })
}

/** How to read a column as an instant, and which column that is. */
interface TimeAxis {
  idx: number
  /** The cell's value as milliseconds since the epoch. */
  toMs: (value: string) => number
}

/**
 * An epoch number's unit, inferred from its magnitude — or null if it is too
 * small to be an epoch at all and is just a number.
 *
 * Mirrors dataops.ts_checks._diffs_in_seconds, which reads the same CSVs as
 * "epoch s or epoch ms" by testing the median against 1e12. A plain measurement
 * (a latency in ms, a row count) never reaches 1e9, so the floor separates a
 * timestamp from an ordinary numeric column without consulting its name.
 */
function epochScale(sample: number): ((n: number) => number) | null {
  if (sample >= 1e15) return n => n / 1000   // microseconds
  if (sample >= 1e12) return n => n          // milliseconds
  if (sample >= 1e9) return n => n * 1000    // seconds
  return null
}

/**
 * The column to plot against, and how to read it.
 *
 * `preferred` is the timestamp column the pipeline itself resolved
 * (report.validation.timestamp_column, plus whatever that column is called at
 * other stages) — authoritative, and tried first. Only failing that does this
 * scan for a column that looks like time.
 *
 * Epoch numbers are the reason this exists. The previous detector skipped any
 * column whose first value parsed as a number, so an epoch `time` column was
 * rejected and the series fell back to plotting against the row index — a
 * dataset with perfectly good timestamps drawn as if it had none. The numeric
 * guard was not wrong to be there (Date.parse("27413") yields a date), it was
 * just too blunt: the magnitude test replaces it.
 */
function timeAxis(frame: Frame, preferred: string[]): TimeAxis | null {
  const first = preferred.map(n => frame.header.indexOf(n)).filter(i => i >= 0)
  const rest = frame.header.map((_, i) => i).filter(i => !first.includes(i))

  for (const idx of [...first, ...rest]) {
    const v = frame.rows.find(r => !isBlank(r[idx]))?.[idx]
    if (!v) continue
    const n = Number(v)
    if (!Number.isNaN(n)) {
      const scale = epochScale(Math.abs(n))
      if (scale) return { idx, toMs: raw => scale(Number(raw)) }
      continue
    }
    if (!Number.isNaN(Date.parse(v))) return { idx, toMs: raw => Date.parse(raw) }
  }
  return null
}

type Frame = { header: string[], rows: string[][] }
type StageId = 'raw' | 'soft' | 'remediated' | 'imputed_train' | 'imputed_test' | 'imputed_final'

/**
 * Whether a stage's frame already covers the page boundary `bound` (in ms) —
 * or, when there is no boundary (a run with no usable timestamp column pages
 * nothing, so nothing is ever "capped"), whether the walk fetching it has
 * simply finished, since the whole frame is what an unpaginated view needs.
 *
 * Shared between the lazy loader's own stop condition and the "ready to
 * paint" flag, so the two can never disagree about what counts as enough
 * data for the page on screen — a `bound` of null and a `done` that is only
 * ever true once the file is exhausted also means a still-loading unpaginated
 * frame correctly reports not caught up, rather than trivially "ready".
 */
function caughtUpTo(frame: Frame | null, bound: number | null, done: boolean, timeNames: string[]): boolean {
  if (bound === null) return done
  if (!frame || frame.rows.length === 0) return false
  const axis = timeAxis(frame, timeNames)
  const lastRow = frame.rows[frame.rows.length - 1]
  const lastMs = axis && lastRow ? axis.toMs(lastRow[axis.idx]) : null
  return lastMs !== null && !Number.isNaN(lastMs) && lastMs >= bound
}

/**
 * The pipeline's own three frames, which the stage picker switches between.
 *
 * The imputed frames are deliberately not among them: they are the regularized
 * timeline in converted units, four times the rows, so putting them in the same
 * picker made the reader compare two things that share neither scale nor row
 * count by flipping between them. They get their own chart below, where all
 * three can be seen at once.
 */
const STAGES: { id: StageId, label: string, artifact: string }[] = [
  { id: 'raw', label: 'Raw', artifact: RAW },
  { id: 'soft', label: 'Soft-cleaned', artifact: SOFT },
  { id: 'remediated', label: 'Remediated', artifact: REMEDIATED },
]

/** Plotted together on the imputed chart: the stitched timeline and its splits. */
const IMPUTED_SERIES: { id: StageId, label: string, artifact: string }[] = [
  { id: 'imputed_final', label: 'Full timeline', artifact: IMPUTED_FINAL },
  { id: 'imputed_train', label: 'Train split', artifact: IMPUTED_TRAIN },
  { id: 'imputed_test', label: 'Test split', artifact: IMPUTED_TEST },
]

/** Every frame the Data tab fetches, whichever chart it belongs to. */
const FETCHED = [...STAGES, ...IMPUTED_SERIES]

/**
 * The one frame fetched to completion, up front — every other wanted frame is
 * loaded lazily, only as far as whichever page is on screen (see the CSV
 * loading effects below). Same priority `pages` itself falls back through:
 * raw defines every page boundary, so it is what everything else is measured
 * against; failing that, whatever this run actually published first.
 */
function pickRefStage<T extends { id: StageId }>(wanted: T[]): T | null {
  const byId = (id: StageId) => wanted.find(w => w.id === id)
  return byId('raw') ?? byId('soft') ?? byId('remediated') ?? wanted[0] ?? null
}

type Tab = 'overview' | 'quality' | 'remediation' | 'data' | 'config'

interface Props {
  dagId: string
  runId: string
}

export default function RunResults({ dagId, runId }: Props) {
  const [data, setData] = useState<RunArtifacts | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [tab, setTab] = useState<Tab>('overview')

  const [frames, setFrames] = useState<{
    stages: Partial<Record<StageId, Frame>>
    truncated: boolean
  } | null>(null)
  const [csvError, setCsvError] = useState<string | null>(null)
  // Mirrors `frames` for the loader below to read synchronously — an async
  // loop reading `frames` itself would see whatever was current when the
  // closure was created, not what a concurrent chunk just appended.
  const framesRef = useRef(frames)
  useEffect(() => { framesRef.current = frames }, [frames])
  /** Per-stage fetch progress for the lazy loader: how far read, and whether
   *  that stage's file is exhausted. Not state — it drives an async loop, not
   *  a render. */
  const cursorsRef = useRef<Partial<Record<StageId, { offset: number, done: boolean, header: string[] | null }>>>({})
  /** Stages currently mid-fetch, so a second effect run (e.g. a fast page
   *  flip) does not start a second overlapping walk of the same file. */
  const loadingRef = useRef<Set<StageId>>(new Set())
  /** Flipped on every new run load, so a walk left over from the previous run
   *  stops appending to a frame nobody is looking at anymore. */
  const loadCancelledRef = useRef(false)
  /** How far (in ms) each stage needs to be loaded for the page on screen,
   *  refreshed every time the page changes. A loop already mid-fetch reads
   *  this fresh each iteration rather than a value captured when it started,
   *  so paging forward again before a walk finishes still lands on the right
   *  target instead of stopping short at the page that was current when the
   *  walk began. */
  const untilMsRef = useRef<Partial<Record<StageId, number | null>>>({})
  /**
   * Whether each lazily-loaded stage (every `FETCHED` stage besides the
   * reference one, which is fetched to completion up front) has walked far
   * enough to cover the page on screen.
   *
   * Read as a render-driving flag rather than folded into `frames` itself:
   * the two charts painting mid-walk — a partial line for whichever stage
   * happens to be a chunk behind — was the "weird lines" symptom paging
   * produced. Both charts wait on this together and paint the whole page in
   * one go instead.
   */
  const [stageReady, setStageReady] = useState<Partial<Record<StageId, boolean>>>({})
  const [column, setColumn] = useState<string>('')
  // Which stage of the frame is plotted. Remediated by default — it is the
  // pipeline's output — but the soft-cleaned and raw frames are uploaded too,
  // and seeing what a stage looked like is the reason they are.
  const [stage, setStage] = useState<StageId>('remediated')
  // One zoom window for both charts. They plot the same timeline on a shared x
  // axis, so zooming one and not the other would leave two plots that look
  // comparable and are not — the exact failure the shared axes exist to avoid.
  const [zoom, setZoom] = useState<{ min: number, max: number } | null>(null)
  // Same reasoning as `zoom`: one crosshair position for both charts, so
  // pointing at a value on one side shows the corresponding position on the
  // other rather than requiring two separate hovers to compare them by eye.
  const [hoverX, setHoverX] = useState<number | null>(null)
  /**
   * Imputed series switched off from the chart's legend.
   *
   * Defaults to the full timeline alone — the one to read if you only read
   * one — with the splits available a click away via the view switch below,
   * or individually via the legend's own per-series toggles, which this
   * state still drives either way.
   */
  const [hiddenSeries, setHiddenSeries] = useState<string[]>(['imputed_train', 'imputed_test'])

  function toggleSeries(key: string) {
    setHiddenSeries(prev => {
      const next = prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]
      // Never all three: an empty chart is not a state worth being able to reach
      // by clicking, and the way out of it is not obvious.
      return next.length >= IMPUTED_SERIES.length ? prev : next
    })
  }

  // The switch's two presets, named rather than derived from `hiddenSeries`
  // directly so a manual per-series toggle (still available in the legend)
  // can leave neither button showing as active without that reading as broken.
  const IMPUTED_VIEW_FULL: string[] = ['imputed_train', 'imputed_test']
  const IMPUTED_VIEW_SPLITS: string[] = ['imputed_final']
  const imputedView: 'full' | 'splits' | null =
    hiddenSeries.length === IMPUTED_VIEW_FULL.length && IMPUTED_VIEW_FULL.every(k => hiddenSeries.includes(k))
      ? 'full'
      : hiddenSeries.length === IMPUTED_VIEW_SPLITS.length && IMPUTED_VIEW_SPLITS.every(k => hiddenSeries.includes(k))
        ? 'splits'
        : null
  // The run's dag_run.conf — what this run was asked to do. Fetched here rather
  // than passed down, so the component works the same on the standalone
  // #/run-results route as it does embedded beside the task list.
  const [conf, setConf] = useState<Record<string, unknown> | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setZoom(null)
    setHoverX(null)
    setPage(0)
    getRunArtifacts(dagId, runId)
      .then(d => { if (!cancelled) setData(d) })
      .catch(e => { if (!cancelled) setError((e as Error).message) })
      .finally(() => { if (!cancelled) setLoading(false) })
    // Supplementary — a run with no readable conf still shows its results.
    getDagRun(dagId, runId)
      .then(run => {
        if (cancelled) return
        const c = run.conf
        setConf(c && Object.keys(c).length > 0 ? c : null)
      })
      .catch(() => { /* the Configuration tab simply says there was none */ })
    return () => { cancelled = true }
  }, [dagId, runId])

  // Human-readable labels for the run's dataset/distribution, resolved from
  // the catalogue rather than run.conf — the conf only records the UUIDs the
  // pipeline was invoked with. Supplementary: the Artifacts panel already
  // falls back to the id itself, so a lookup failure (an id from a deleted
  // dataset, a catalogue with no read access) is silently left unresolved
  // rather than surfaced as an error.
  const [catalogueNames, setCatalogueNames] = useState<{ dataset?: string, asset?: string }>({})
  useEffect(() => {
    setCatalogueNames({})
    if (!data?.dataset_id) return
    const datasetId = data.dataset_id
    const assetId = data.asset_id ?? undefined
    const catalogueId = data.catalogue_id ?? undefined
    let cancelled = false
    getDatasets(catalogueId)
      .then(res => {
        if (cancelled) return
        const match = res.datasets.find(d => d.dataset_id === datasetId || d.id === datasetId)
        if (match?.name) setCatalogueNames(prev => ({ ...prev, dataset: match.name }))
      })
      .catch(() => { /* label stays the raw id */ })
    if (assetId) {
      getDistributions(datasetId, catalogueId)
        .then(res => {
          if (cancelled) return
          const match = res.distributions.find(
            d => d.asset_id === assetId || d.id === assetId || d.distribution_id === assetId,
          )
          const label = match?.asset_title || match?.name
          if (label) setCatalogueNames(prev => ({ ...prev, asset: label }))
        })
        .catch(() => { /* label stays the raw id */ })
    }
    return () => { cancelled = true }
  }, [data?.dataset_id, data?.asset_id, data?.catalogue_id])

  /**
   * Frames are fetched only once the artifact list says which exist. A run
   * that failed before remediation, or one from before the raw frame was
   * uploaded, still renders everything it does have.
   *
   * Only the reference stage (`pickRefStage`) is fetched here, to completion
   * — it is what `pages` below cuts into windows, so its row count has to be
   * known up front. Every other wanted frame is loaded lazily, only as far as
   * the page on screen, by the effect after `pages` — fetching a frame's
   * whole duration when paging shows one window of it at a time defeats the
   * point of paging.
   */
  useEffect(() => {
    if (!data) return
    const names = new Set(data.artifacts.map(a => a.name))
    const wanted = FETCHED.filter(st => names.has(st.artifact))
    const ref = pickRefStage(wanted)
    cursorsRef.current = {}
    loadingRef.current = new Set()
    loadCancelledRef.current = false
    setFrames(null)
    // A new run reuses the same StageId keys ('soft', 'imputed_train', …) —
    // without this a stage this run hasn't fetched a byte of yet could read
    // as "ready" from the previous run's flag and paint nothing as if it
    // were a deliberately empty series.
    setStageReady({})
    if (!ref) return

    let cancelled = false
    setCsvError(null)
    getRunArtifactCsv(dagId, runId, ref.artifact, CSV_WINDOW_BYTES, CSV_TOTAL_CAP)
      .then(result => {
        if (cancelled) return
        cursorsRef.current[ref.id] = {
          offset: result.fetched, done: !result.truncated, header: null,
        }
        setFrames({ stages: { [ref.id]: parseCsv(result.text) }, truncated: result.truncated })
      })
      .catch(e => { if (!cancelled) setCsvError((e as Error).message) })
    return () => { cancelled = true; loadCancelledRef.current = true }
  }, [data, dagId, runId])

  const available = useMemo(
    () => STAGES.filter(st => frames?.stages[st.id]),
    [frames],
  )
  // Falls back to the latest stage this run actually published, so selecting a
  // stage a run never produced shows that run's own last frame rather than
  // nothing at all.
  const selected = available.find(st => st.id === stage) ?? available[available.length - 1] ?? null
  const plotFrame = selected ? frames?.stages[selected.id] ?? null : null

  /**
   * The frame the plotted one is diffed against, to mark what changed.
   *
   * Only stages with the same row count can be compared, because the diff is
   * positional: remediation rewrites cells in place, so remediated and
   * soft-cleaned align, but cleaning *drops* empty and duplicate rows, so soft
   * and raw usually do not. Rather than assume, the lengths are checked — a
   * run whose clean dropped nothing can still be diffed against its raw frame.
   */
  const baseline = useMemo(() => {
    if (!frames || !selected) return null
    const { raw, soft } = frames.stages
    if (selected.id === 'remediated') return soft ?? null
    if (selected.id === 'soft' && raw && soft && raw.rows.length === soft.rows.length) return raw
    // An imputed split shares no row alignment with anything — it is the
    // regularized grid, four times longer and split in two. It needs no
    // baseline: the bundle marks its synthesised rows itself, in is_gap.
    return null
  }, [frames, selected])

  /**
   * Every name a column answers to, in both directions.
   *
   * clean_dataframe snake_cases the headers, so the same measure is `Lat 99` on
   * the raw frame and `lat_99` on the cleaned ones. The pipeline records the
   * original → cleaned map it used (dataops.cleaning.build_column_mapping); the
   * inverse is built here so a column can be followed either way across stages.
   */
  const columnAliases = useMemo(() => {
    const mapping = (data?.report?.cleaning?.column_mapping ?? {}) as Record<string, string>
    const aliases: Record<string, string[]> = {}
    const link = (a: string, b: string) => {
      if (a === b) return
      ;(aliases[a] ??= []).push(b)
      ;(aliases[b] ??= []).push(a)
    }
    for (const [original, cleaned] of Object.entries(mapping)) {
      link(original, cleaned)
      // …and the unit-converted name the imputed bundle uses for the same
      // measure. Derived rather than mapped: the pipeline records no table for
      // these renames, and the suffix is the whole of the rule.
      for (const suffix of UNIT_SUFFIXES) link(cleaned, `${cleaned}${suffix}`)
    }
    return aliases
  }, [data])

  /**
   * Names the timestamp column answers to across stages.
   *
   * The pipeline already resolved which column is the timeline and recorded it
   * (minimal_dataops writes validation.timestamp_column); trusting that beats
   * re-deriving it here and disagreeing with the report next to it.
   */
  const timeNames = useMemo(() => {
    const name = data?.report?.validation?.timestamp_column
    if (typeof name !== 'string' || !name) return []
    return [name, ...(columnAliases[name] ?? [])]
  }, [data, columnAliases])

  const timeline = useMemo(
    () => (plotFrame ? timeAxis(plotFrame, timeNames) : null),
    [plotFrame, timeNames],
  )

  // The timeline is the axis, not a measure — offering it as something to plot
  // against itself is a diagonal line and a wasted slot in the picker.
  const columns = useMemo(() => {
    if (!plotFrame) return []
    const all = numericColumns(plotFrame.header, plotFrame.rows)
    const timeName = timeline === null ? null : plotFrame.header[timeline.idx]
    return all.filter(c => c !== timeName && !BUNDLE_FEATURE_COLUMNS.has(c))
  }, [plotFrame, timeline])


  /**
   * The column actually plotted for the current stage.
   *
   * `column` stays whatever the reader picked, even while a stage that has no
   * such column is shown: switching stage used to overwrite the selection with
   * that stage's first column, so the measure being monitored was lost and did
   * not come back on switching away again. The fallback is resolved for display
   * only, and the reader's choice returns as soon as a stage carrying it does.
   */
  const plottedColumn = useMemo(() => {
    if (columns.length === 0) return ''
    if (columns.includes(column)) return column
    for (const alias of columnAliases[column] ?? []) {
      if (columns.includes(alias)) return alias
    }
    return columns[0]
  }, [columns, column, columnAliases])

  // Only ever seeds the initial choice; never overwrites one the reader made.
  useEffect(() => {
    if (!column && columns.length) setColumn(columns[0])
  }, [columns, column])

  /**
   * The y range for a column, taken across every frame that carries it.
   *
   * A scale fitted per frame redraws each of them to fill the plot, so
   * remediation clipping an outlier would leave the line looking the same, only
   * relabelled. One range across all of them holds the axis still, and — now
   * that the imputed chart sits beside this one — makes the two charts directly
   * comparable, because both ask this for their own column and get the same
   * answer whenever that column is the same.
   *
   * Matched on the exact name, deliberately. The alias that carries a selection
   * across frames is also a unit conversion (`ram_usage` in bytes becomes
   * `ram_usage_mb`), and a range spanning both would put one of them on the
   * floor. Frames naming a column identically share a scale; converted ones form
   * their own, and the charts say so.
   */
  const yDomainFor = useCallback((columnName: string): [number, number] | undefined => {
    if (!frames || !columnName) return undefined
    let lo = Infinity
    let hi = -Infinity
    for (const frame of Object.values(frames.stages)) {
      if (!frame?.header.includes(columnName)) continue
      const idx = frame.header.indexOf(columnName)
      for (const row of frame.rows) {
        const raw = row[idx]
        if (isBlank(raw)) continue
        const v = Number(raw)
        if (Number.isNaN(v)) continue
        if (v < lo) lo = v
        if (v > hi) hi = v
      }
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return undefined
    if (lo === hi) { lo -= 1; hi += 1 }
    const pad = (hi - lo) * 0.08
    return [lo - pad, hi + pad]
  }, [frames])

  const yDomain = useMemo(() => yDomainFor(plottedColumn), [yDomainFor, plottedColumn])

  /**
   * One x range for every stage, matching yDomain's purpose on the other axis.
   *
   * Stages hold different numbers of rows — cleaning drops empty and duplicate
   * ones — so a per-stage scale stretches the shorter frame across the full
   * width and the drop becomes invisible. Shared, a cleaned frame that lost its
   * tail visibly stops short of the raw one's.
   *
   * Only shared when every stage measures x the same way: a frame with a usable
   * timestamp column is plotted against time and one without against its row
   * index, and those two domains have nothing to do with each other.
   */
  const xDomain = useMemo<[number, number] | undefined>(() => {
    if (!frames) return undefined
    let lo = Infinity
    let hi = -Infinity
    let sawTime: boolean | null = null
    for (const frame of Object.values(frames.stages)) {
      if (!frame || frame.rows.length === 0) continue
      const axis = timeAxis(frame, timeNames)
      const isTime = axis !== null
      if (sawTime === null) sawTime = isTime
      else if (sawTime !== isTime) return undefined

      if (axis === null) {
        lo = Math.min(lo, 0)
        hi = Math.max(hi, frame.rows.length - 1)
        continue
      }
      for (const row of frame.rows) {
        const t = axis.toMs(row[axis.idx])
        if (Number.isNaN(t)) continue
        if (t < lo) lo = t
        if (t > hi) hi = t
      }
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi) || lo === hi) return undefined
    return [lo, hi]
  }, [frames, timeNames])

  /**
   * The whole run's timeline, cut into pages of `MAX_PLOT_POINTS` raw rows
   * each — the window shown is full resolution, never stride-sampled, so
   * paging trades "see everything, thinned" for "see one stretch, exactly as
   * recorded."
   *
   * Always cut against the raw frame specifically, never the currently
   * selected stage: cleaning drops rows, so paging against a cleaned frame
   * would shift page boundaries — and reset the page count — every time the
   * stage picker changed. Raw is what every other frame is a version of, so
   * its rows are the one stable ruler. Falls back to whatever frame exists
   * when a run published no raw one.
   */
  // Indirected through its own memo so `pages` below only recomputes when the
  // reference stage itself changes — it is fetched once, to completion, and
  // never touched again, but every *other* stage streaming in more rows as
  // paging continues would otherwise still change `frames`'s identity and
  // re-sort the reference on every chunk of an unrelated frame.
  const refFrame = useMemo<Frame | null>(() => {
    if (!frames) return null
    return frames.stages.raw ?? frames.stages.soft ?? frames.stages.remediated
      ?? Object.values(frames.stages).find((f): f is Frame => !!f) ?? null
  }, [frames])

  const pages = useMemo<{ min: number, max: number }[]>(() => {
    const ref = refFrame
    if (!ref || ref.rows.length === 0) return []
    const axis = timeAxis(ref, timeNames)
    // Paging is a time-window concept; without a real timestamp column a raw
    // row index means nothing on any other frame, which usually has a
    // different row count of its own.
    if (!axis) return []
    const xs = ref.rows
      .map(r => axis.toMs(r[axis.idx]))
      .filter(x => !Number.isNaN(x))
      .sort((a, b) => a - b)
    if (xs.length === 0) return []
    const chunks: { min: number, max: number }[] = []
    for (let i = 0; i < xs.length; i += MAX_PLOT_POINTS) {
      const slice = xs.slice(i, i + MAX_PLOT_POINTS)
      chunks.push({ min: slice[0], max: slice[slice.length - 1] })
    }
    return chunks
  }, [refFrame, timeNames])

  const [page, setPage] = useState(0)
  const pageCount = pages.length
  const pageBounds = pageCount > 0 ? pages[Math.min(page, pageCount - 1)] : null
  // The page window, once there is one, replaces the whole-run domain — both
  // charts are now showing a stretch of the timeline, not all of it.
  const pagedXDomain: [number, number] | undefined = pageBounds ? [pageBounds.min, pageBounds.max] : xDomain

  // A run reload can shrink the page count (or lose paging entirely); clamp
  // rather than leave the reader stranded on a page that no longer exists.
  useEffect(() => {
    if (pageCount === 0) { if (page !== 0) setPage(0); return }
    if (page > pageCount - 1) setPage(pageCount - 1)
  }, [pageCount, page])

  // A stale manual zoom or crosshair from the previous page would either
  // point at data that is no longer drawn or, worse, silently still line up
  // with the new page by coincidence — clearing both is the only reading
  // that is never wrong.
  useEffect(() => { setZoom(null); setHoverX(null) }, [page])

  /** Every lazily-loaded stage this run actually published — the same set
   *  `others` resolves to inside the loader effect below, kept as its own
   *  memo so the readiness gate can read it without duplicating the
   *  wanted/ref resolution a second time at render. */
  const otherStageIds = useMemo(() => {
    if (!data) return [] as StageId[]
    const names = new Set(data.artifacts.map(a => a.name))
    const wanted = FETCHED.filter(st => names.has(st.artifact))
    const ref = pickRefStage(wanted)
    return wanted.filter(st => st.id !== ref?.id).map(st => st.id)
  }, [data])
  /** True once every stage the two Data-tab charts draw from has caught up to
   *  the page on screen — gates painting them, per the loader effect below. */
  const pageReady = otherStageIds.every(id => stageReady[id] === true)

  /**
   * Every frame besides the reference stage, walked forward only as far as
   * the page on screen — fetching a frame's whole duration when paging shows
   * one window of it at a time would defeat the point of paging. Extends
   * existing progress rather than restarting: paging forward asks each file
   * for a bit more, paging back reuses what's already there (the check at the
   * top of `extend`'s loop), and switching stage or the imputed-chart view
   * never triggers a fetch of its own, since every wanted frame here already
   * advances together, in step with the page.
   */
  useEffect(() => {
    if (!data) return
    const names = new Set(data.artifacts.map(a => a.name))
    const wanted = FETCHED.filter(st => names.has(st.artifact))
    const ref = pickRefStage(wanted)
    if (!ref) return
    const others = wanted.filter(st => st.id !== ref.id)
    const untilMs = pageBounds ? pageBounds.max : null
    others.forEach(st => { untilMsRef.current[st.id] = untilMs })

    // The page just changed (or this is the first run for it) — recompute
    // readiness against what each stage already has, synchronously. A page
    // visited before, whose frames already reach this far, needs no spinner
    // at all; anything short of that is marked not-ready until `extend`
    // below catches its walk up to `untilMs`.
    setStageReady(prev => {
      const next = { ...prev }
      others.forEach(st => {
        const frame = framesRef.current?.stages[st.id] ?? null
        const done = cursorsRef.current[st.id]?.done ?? false
        next[st.id] = caughtUpTo(frame, untilMs, done, timeNames)
      })
      return next
    })

    async function extend(st: { id: StageId, artifact: string }) {
      if (loadingRef.current.has(st.id)) return
      loadingRef.current.add(st.id)
      try {
        let cursor = cursorsRef.current[st.id] ?? { offset: 0, done: false, header: null as string[] | null }
        while (!loadCancelledRef.current && !cursor.done) {
          // Re-read on every pass: a target set before this loop started may
          // have moved on to a later page by the time this iteration runs.
          const bound = untilMsRef.current[st.id] ?? null
          if (caughtUpTo(framesRef.current?.stages[st.id] ?? null, bound, cursor.done, timeNames)) {
            setStageReady(prev => ({ ...prev, [st.id]: true }))
            break
          }

          const chunk = await getRunArtifactText(dagId, runId, st.artifact, CSV_WINDOW_BYTES, cursor.offset)
          // The server sends the CSV header only in the first window of a
          // walk — a continuation chunk is pure data (dataops-orchestrator's
          // own contract), so only the very first fetch for this stage runs
          // it through the header-peeling parser.
          const newRows = cursor.header === null
            ? (() => { const p = parseCsv(chunk.text); cursor.header = p.header; return p.rows })()
            : splitCsvRows(chunk.text)
          // Not "did this chunk come back smaller than requested" — the
          // server always trims a window to its last full CSV row, so a
          // chunk lands a few bytes short of `CSV_WINDOW_BYTES` on almost
          // every call, mid-file included. The real end-of-data signal is the
          // byte offset catching up to the object's reported total size (see
          // the identical fix in getRunArtifactCsv, api/airflow.ts).
          const reachedEnd = chunk.totalSize > 0 && chunk.nextOffset >= chunk.totalSize
          cursor = { offset: chunk.nextOffset, done: !chunk.truncated || reachedEnd, header: cursor.header }
          cursorsRef.current[st.id] = cursor

          if (newRows.length > 0) {
            setFrames(prev => {
              const prevStages = prev?.stages ?? {}
              const prevFrame = prevStages[st.id]
              const header = prevFrame?.header ?? cursor.header ?? []
              const rows = prevFrame ? prevFrame.rows.concat(newRows) : newRows
              return { stages: { ...prevStages, [st.id]: { header, rows } }, truncated: prev?.truncated ?? false }
            })
          }

          // Same hard stop as the reference stage's own full fetch — a frame
          // this large gets flagged the same way regardless of which loader
          // hit the cap.
          if (!cursor.done && cursor.offset >= CSV_TOTAL_CAP) {
            cursor = { ...cursor, done: true }
            cursorsRef.current[st.id] = cursor
            setFrames(prev => (prev ? { ...prev, truncated: true } : prev))
          }
        }
        // The file ran out before reaching the page bound (a run shorter than
        // the reference stage, or one with no more rows at all) — that is
        // still "as caught up as this stage will ever get" for the page.
        if (cursor.done) setStageReady(prev => ({ ...prev, [st.id]: true }))
      } catch (e) {
        if (!loadCancelledRef.current) setCsvError((e as Error).message)
      } finally {
        loadingRef.current.delete(st.id)
      }
    }

    others.forEach(st => { void extend(st) })
  }, [data, dagId, runId, pageBounds, timeNames])

  const series = useMemo<{ points: SeriesPoint[], windowed: boolean, isTime: boolean, total: number }>(() => {
    if (!plotFrame || !plottedColumn) return { points: [], windowed: false, isTime: false, total: 0 }
    const col = plotFrame.header.indexOf(plottedColumn)
    if (col < 0) return { points: [], windowed: false, isTime: false, total: 0 }

    const before = baseline
    const beforeCol = before ? before.header.indexOf(plottedColumn) : -1

    const all: SeriesPoint[] = []
    for (let i = 0; i < plotFrame.rows.length; i++) {
      const raw = plotFrame.rows[i][col]
      if (isBlank(raw)) continue
      const y = Number(raw)
      if (Number.isNaN(y)) continue
      // What remediation did to this cell, by comparing against the frame as it
      // stood before: a blank that now has a value was filled, and a value that
      // moved was adjusted (an outlier winsorized). Without the earlier frame
      // nothing is claimed and every point is simply observed.
      let changed = false
      if (beforeCol >= 0) {
        const prev = before?.rows[i]?.[beforeCol]
        changed = isBlank(prev) || Number(prev) !== y
      }
      const x = timeline === null ? i : timeline.toMs(plotFrame.rows[i][timeline.idx])
      all.push({ x: Number.isNaN(x) ? i : x, y, changed })
    }

    // Full resolution within the current page, whatever that comes to — no
    // stride here. The window was sized against the raw frame specifically
    // to land near MAX_PLOT_POINTS; a stage with more rows in the same
    // stretch (or one before a raw frame existed to page against) is shown
    // in full regardless, per the reasoning on `pages` above.
    const windowed = pageBounds ? all.filter(p => p.x >= pageBounds.min && p.x <= pageBounds.max) : all
    return {
      points: windowed,
      windowed: pageBounds !== null && windowed.length !== all.length,
      isTime: timeline !== null,
      total: all.length,
    }
  }, [plotFrame, baseline, plottedColumn, timeline, pageBounds])

  /**
   * The three imputed frames as one chart's worth of series.
   *
   * Built separately from the stage chart because nothing about them lines up
   * with it: converted units, four times the rows, and split in two. Here they
   * do line up with each other — same columns, same grid — so they belong on
   * shared axes, which is the only way to see that the splits tile the timeline
   * rather than duplicating it.
   *
   * The full timeline leads, because SeriesChart's crosshair follows the first
   * series and that is the one a reader is asking about.
   */
  const imputedChart = useMemo(() => {
    if (!frames) return null
    const present = IMPUTED_SERIES.filter(sr => frames.stages[sr.id])
    if (present.length === 0) return null

    // Its own column list: these frames name the measure differently (units) and
    // carry the imputer's working columns, so the stage chart's picker does not
    // apply. The first frame decides, since all three share a schema.
    const first = frames.stages[present[0].id]!
    const axis = timeAxis(first, timeNames)
    const columns = numericColumns(first.header, first.rows)
      .filter(c => !BUNDLE_FEATURE_COLUMNS.has(c) && c !== (axis && first.header[axis.idx]))

    // Derived from the stage chart's column, never held separately: the two
    // frames name the same measure differently (the bundle converts units, so
    // `ram_usage` becomes `ram_usage_mb`), and two independent selections let
    // the pair drift onto different measures while still looking like a
    // comparison. One choice, resolved into each frame's own vocabulary.
    const wanted = columns.includes(plottedColumn)
      ? plottedColumn
      : (columnAliases[plottedColumn] ?? []).find(a => columns.includes(a)) ?? ''
    const column = columns.includes(wanted) ? wanted : columns[0] ?? ''
    if (!column) return { columns: [], column: '', series: [], isTime: false, gapCount: 0 }

    let gapCount = 0
    const series = present.map(sr => {
      const frame = frames.stages[sr.id]!
      const col = frame.header.indexOf(column)
      const t = timeAxis(frame, timeNames)
      const gapCol = frame.header.indexOf(GAP_FLAG)
      const points: SeriesPoint[] = []
      for (let i = 0; i < frame.rows.length; i++) {
        const raw = frame.rows[i][col]
        if (isBlank(raw)) continue
        const y = Number(raw)
        if (Number.isNaN(y)) continue
        const changed = gapCol >= 0 && Number(frame.rows[i][gapCol]) !== 0
        // Counted across the splits, which are disjoint and do carry the flag.
        // The stitched timeline drops it, so counting the first series — which
        // is that timeline — would always have reported zero.
        if (changed) gapCount++
        const x = t ? t.toMs(frame.rows[i][t.idx]) : i
        points.push({ x: Number.isNaN(x) ? i : x, y, changed })
      }
      // Same page window as the stage chart, full resolution within it — no
      // stride here either. These frames run four times longer than raw, so
      // a page landing near MAX_PLOT_POINTS raw rows can still hand a series
      // here more than that many points; shown in full regardless, per the
      // reasoning on `pages` above.
      const windowed = pageBounds ? points.filter(p => p.x >= pageBounds.min && p.x <= pageBounds.max) : points
      return {
        key: sr.id,
        label: sr.label,
        points: windowed,
        total: points.length,
        shown: windowed.length,
      }
    })

    return { columns, column, series, isTime: axis !== null, gapCount }
  }, [frames, timeNames, plottedColumn, columnAliases, pageBounds])

  function download(name: string, key: string) {
    // Fetched rather than linked: the endpoint needs the bearer token, which a
    // plain href cannot carry.
    getRunArtifactText(dagId, runId, name, 64 * 1024 * 1024)
      .then(({ text }) => {
        const url = URL.createObjectURL(new Blob([text]))
        const el = document.createElement('a')
        el.href = url
        el.download = key.split('/').pop() ?? name
        el.click()
        URL.revokeObjectURL(url)
      })
      .catch(err => setCsvError((err as Error).message))
  }

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />
  if (!data) return null

  if (data.artifacts.length === 0) {
    return (
      <div className="alert alert-secondary mb-0">
        This run has not published any artifacts
        {data.state && data.state !== 'success' ? ` — it is ${data.state}.` : '.'}
        {' '}Results appear once <code>upload_artifacts</code> completes.
      </div>
    )
  }

  const report = data.report
  // The merged quality report — both GX suites plus pandera — which
  // dali.datalake.upload_artifacts nests inside the pipeline's report.json.
  // Everything else on this page comes from the pipeline's own sections, which
  // describe only what the pipeline did.
  const merged = report?.dali_quality ?? null
  const mergedStats = merged?.statistics ?? {}
  const cleaning = report?.cleaning ?? {}
  const validation = report?.validation ?? {}
  const re = report?.validation_comparison?.remediation_effect
  const filled = re?.missing_cells_before !== undefined && re?.missing_cells_after !== undefined
    ? re.missing_cells_before - re.missing_cells_after
    : null

  // What remediation reported doing. In time_series mode it deliberately fills
  // nothing — gaps and missing values are recorded with
  // status "deferred_to_imputation" and left for the imputation handoff — so a
  // run can be entirely successful and still have nothing filled.
  const actions = report?.remediation?.actions ?? []
  const deferred = actions.filter(a => a.status === 'deferred_to_imputation')
  const filledCells = filled ?? 0
  // What the markers mean depends on which two stages are being diffed: against
  // the soft-cleaned frame they are remediation's work, against the raw frame
  // they are the clean's.
  const markerLabel = selected?.id === 'soft'
    ? 'Changed by cleaning'
    : filledCells > 0 ? 'Imputed' : 'Adjusted'

  const formatX = series.isTime
    ? (x: number) => new Date(x).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '')
    : (x: number) => `#${x}`

  const TABS: { id: Tab, label: string }[] = [
    { id: 'overview', label: 'Overview' },
      { id: 'quality', label: 'Quality' },
    { id: 'remediation', label: 'Issues & remediation' },
    { id: 'data', label: 'Data' },
    { id: 'config', label: 'Configuration' },
  ]

  return (
    <div className="run-results">
      {/* Outside the tabs on purpose: these five are the run's headline, and a
          reader switching to Quality or Data should not lose the shape of the
          dataset and the verdict while doing it. Everything below the strip is
          a view; this is the run. */}
      {/* MetricCard emits its own grid column (col-6 col-sm-3) — HomePage drops
          them straight into a row and relies on it, four to a line. This strip
          has five, which the shared component's 4-up default cannot fit on one
          line, so .run-results-metrics (RunResults.css) overrides the column
          sizing to five equal columns rather than changing MetricCard itself.
          The status tile is not a MetricCard, so it carries the same column
          classes by hand to line up with the other four. */}
      <div className="row g-3 mb-4 run-results-metrics">
        <div className="col-6 col-sm-3">
          {/* The run's actual verdict, across both GX suites and pandera —
              the same figure published to the catalogue. Every other tile
              here describes the pipeline alone, so without this the default
              tab would report only half of what the run checked. Falls back
              to the pipeline's own pandera result on a report from before
              the two DAGs were merged. Leads the strip: it's the headline
              verdict, read before the row counts that explain it. */}
          <div className="run-results-status">
            <span className="run-results-status-label">
              {merged ? 'Quality checks' : 'Validation'}
            </span>
            <span className="run-results-status-value">
              <PassFail value={merged ? merged.success : validation.pandera_passed} />
            </span>
            <span className="run-results-status-note">
              {merged
                ? `${mergedStats.successful_expectations ?? 0}/${mergedStats.evaluated_expectations ?? 0} checks`
                + ` · ${Object.keys(merged.sources ?? {}).length} sources`
                : validation.mode ? <>pipeline only · mode {String(validation.mode)}</> : 'pipeline only'}
            </span>
          </div>
        </div>
        <MetricCard label="Rows in" value={cleaning.input_rows ?? null} />
        <MetricCard label="Rows out (cleaned)" value={cleaning.output_rows ?? null} />
        {/* cleaning.output_rows never differs from input_rows in time_series mode —
            gap rows are deferred to imputation rather than added during cleaning
            (see IssuesView's docstring above). This is the row count that actually
            changes: the regularized grid imputation filled, once gaps are
            materialised as rows. Named to read alongside "Rows out (cleaned)"
            rather than against it — this is a later, larger stage, not a
            contradiction of it. Absent (not `0`) on a run that never built a
            bundle, per MetricCard's null-vs-zero convention. */}
        <MetricCard
          label="Rows out (imputed)"
          value={report?.imputation?.bundle?.regularized_rows ?? null}
        />
        <MetricCard label="Missing cells filled" value={filled} tone={filled ? 'warning' : 'default'} />
      </div>

      <ul className="nav nav-tabs run-results-tabs" role="tablist">
        {TABS.map(t => (
          <li className="nav-item" key={t.id} role="presentation">
            <button
              type="button"
              role="tab"
              aria-selected={tab === t.id}
              className={`nav-link${tab === t.id ? ' active' : ''}`}
              onClick={() => setTab(t.id)}
            >
              {t.label}
            </button>
          </li>
        ))}
      </ul>

      {tab === 'overview' && (
        <>
          {/* The stages and what they changed, side by side: the second table
              is a per-stage reading of the first, and the pair is read across
              rather than in sequence. Lineage is the wider of the two. */}
          <div className="row g-3">
            <div className="col-xl-7">
              <section className="card h-100">
                <div className="card-body">
                  <h2 className="h6">Lineage</h2>
                  <p className="text-muted small">
                    raw → soft-cleaned → remediated. The pipeline always writes the
                    conservatively soft-cleaned frame alongside the remediated one.
                  </p>
                  {report
                    ? <LineageTable report={report} />
                    : <p className="text-muted small mb-0">This run published no report.</p>}
                </div>
              </section>
            </div>

            <div className="col-xl-5">
              <section className="card h-100">
                <div className="card-body">
                  <h2 className="h6">What each stage changed</h2>
                  {report
                    ? <EffectsTable report={report} />
                    : <p className="text-muted small mb-0">This run published no report.</p>}
                </div>
              </section>
            </div>
          </div>
        </>
      )}

      {tab === 'quality' && (
        <section className="card">
          <div className="card-body">
            {/* Every check made on the frame as received, in one place: the
                run's verdict, then pandera and Great Expectations, then what
                failed. There is no second "and also the pipeline ran…" section
                — that was one execution reported as two. */}
            {merged ? (
              <MergedQualityView quality={merged} />
            ) : (
              // Silence here would read as "the run only did this", which is
              // wrong: it means the merged report is missing, not absent work.
              <div className="alert alert-secondary">
                This run stored no merged quality report. Runs from before the
                validation and processing DAGs were merged look like this.
              </div>
            )}
            {!report && (
              <p className="text-muted small mb-0">This run published no report.</p>
            )}
            {validation.errors && validation.errors.length > 0 && (
              <div className="alert alert-warning mt-4 mb-0">
                <strong>
                  Validation reported {validation.errors.length} issue
                  {validation.errors.length === 1 ? '' : 's'}.
                </strong>
                <ul className="mb-0 mt-2">
                  {validation.errors.slice(0, 8).map((e, i) => <li key={i}><code>{e}</code></li>)}
                </ul>
                {validation.errors.length > 8 && (
                  <p className="mb-0 mt-2 small text-muted">
                    {validation.errors.length - 8} more in the report artifact.
                  </p>
                )}
              </div>
            )}
          </div>
        </section>
      )}

      {/* Side by side: the left panel is what was wrong, the right is what was
          done about it. Reading them as a pair is the point of the tab, and
          stacked they were far enough apart to be read as unrelated lists. */}
      {tab === 'remediation' && (report ? (
        <div className="row g-3">
          <div className="col-xl-6">
            <section className="card h-100">
              <div className="card-body">
                <h2 className="h6">Issues detected</h2>
                <p className="text-muted small">
                  What the pipeline's own profiling found in the frame as it
                  arrived — gaps, missingness, out-of-band values, timestamp
                  disorder. Counted in pandas alongside the Great Expectations
                  run, not derived from it, so they can differ from the checks on
                  the Quality tab: a column is flagged here for having any value
                  outside its quantile band, while the matching expectation
                  passes unless it breaches a threshold.
                </p>
                <IssuesView report={report} />
              </div>
            </section>
          </div>
          <div className="col-xl-6">
            <section className="card h-100">
              <div className="card-body">
                <h2 className="h6">What the pipeline did about them</h2>
                <p className="text-muted small">
                  And how the same suite scored the frame afterwards. The
                  re-check measures the <em>remediated</em> frame — not the one
                  the catalogue serves — so it is reported here and never
                  published as a quality measurement.
                </p>
                <RemediationView report={report} />

                <hr className="my-4" />
                <h3 className="h6">Imputation</h3>
                {report.imputation ? (
                  <ImputationView imputation={report.imputation} />
                ) : (
                  <p className="text-muted small mb-0">
                    This run recorded no imputation step. Runs from before the step
                    reported its own status look like this.
                  </p>
                )}
              </div>
            </section>
          </div>
        </div>
      ) : (
        <section className="card">
          <div className="card-body">
            <p className="text-muted small mb-0">This run published no report.</p>
          </div>
        </section>
      ))}

      {/* Side by side, but not evenly: a series needs width to be readable at
          all, while the artifact list is a handful of names. Two thirds / one
          third rather than half each. */}
      {/* The two charts side by side, on the same axes: the left is the frame as
          the pipeline left it, the right is that timeline once the gaps were
          filled. Reading them as a pair is the point, which a stacked layout
          loses the moment one of them scrolls out of view. */}
      {tab === 'data' && (
        <>
          {pageCount > 1 && (
            <div className="run-results-pager" role="group" aria-label="Timeline page">
              <button
                type="button"
                className="btn btn-sm btn-outline-secondary"
                disabled={page <= 0}
                onClick={() => setPage(p => Math.max(p - 1, 0))}
              >
                ← Earlier
              </button>
              <span className="text-muted small">
                Page {page + 1} of {pageCount}
                {pageBounds && (
                  <> · {formatX(pageBounds.min)} – {formatX(pageBounds.max)}</>
                )}
                {' '}· both charts, full resolution, no sampling
              </span>
              <button
                type="button"
                className="btn btn-sm btn-outline-secondary"
                disabled={page >= pageCount - 1}
                onClick={() => setPage(p => Math.min(p + 1, pageCount - 1))}
              >
                Later →
              </button>
            </div>
          )}
        <div className="row g-3">
          <div className="col-xl-6">
          <section className="card h-100">
            <div className="card-body">
              {/* One filter row above the chart it scopes — never inside the plot. */}
              <div className="run-results-chart-header">
                <h2 className="h6 mb-0">
                  {selected ? `${selected.label} series` : 'Series'}
                </h2>
                {available.length > 1 && (
                  <label className="run-results-column-picker">
                    <span className="text-muted small">Stage</span>
                    <select
                      className="form-select form-select-sm"
                      value={selected?.id ?? ''}
                      onChange={e => setStage(e.target.value as StageId)}
                    >
                      {available.map(st => <option key={st.id} value={st.id}>{st.label}</option>)}
                    </select>
                  </label>
                )}
                {columns.length > 1 && (
                  <label className="run-results-column-picker">
                    <span className="text-muted small">Column</span>
                    <select
                      className="form-select form-select-sm"
                      value={plottedColumn}
                      onChange={e => setColumn(e.target.value)}
                    >
                      {columns.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </label>
                )}
              </div>

              {csvError && <ErrorMessage message={csvError} />}
              {/* Waits on pageReady, not just on `frames` existing: painting the
                  instant a chunk lands drew a partial line for whichever stage
                  was a chunk behind on a page flip — the "weird lines" a
                  half-loaded page produced. A page already fully fetched (the
                  first page, or one revisited) has pageReady true immediately,
                  so this never adds a spinner where there used to be none. */}
              {(!frames || !pageReady) && !csvError && (
                <div className="run-results-chart-loading">
                  <LoadingSpinner />
                </div>
              )}
              {frames && pageReady && (
                <>
                  {/* Mirrors the imputed chart's caption below: same slot, same
                      reserved height (.run-results-chart-caption), so the two
                      plots start at the same y regardless of which stage is
                      selected or how long its blurb runs. */}
                  <p className="text-muted small run-results-chart-caption">
                    {selected ? `The ${selected.label.toLowerCase()} frame, full resolution, one page at a time.` : ''}
                    {' '}Marked points ({markerLabel.toLowerCase()}) are cells that differ from the frame before this one, where there is one to compare against.
                  </p>
                  <SeriesChart
                    points={series.points}
                    label={plottedColumn}
                    formatX={formatX}
                    markerLabel={markerLabel}
                    yDomain={yDomain}
                    xDomain={pagedXDomain}
                    range={zoom}
                    onRangeChange={setZoom}
                    hoverX={hoverX}
                    onHoverXChange={setHoverX}
                  />
                  {/* One paragraph, always rendered, combining whichever of the
                      three notes apply — matches the imputed chart's single
                      bottom caption rather than stacking a variable number of
                      blocks that would push the two cards out of alignment. */}
                  <p className="text-muted small mb-0 mt-2 run-results-chart-caption">
                    {!baseline && (
                      selected?.id === 'raw'
                        ? 'The raw frame is what arrived over EDC — there is no earlier stage to compare it against, so every point is shown as observed. '
                        : selected?.id === 'soft' && frames.stages.raw
                          ? 'Cleaning dropped rows, so the soft-cleaned frame no longer lines up row-for-row with the raw one and the cells it changed cannot be identified positionally. Every point is shown as observed. '
                          : 'This run published no earlier frame to compare against, so the points this stage changed cannot be identified — every point is shown as observed. '
                    )}
                    {(frames.truncated || series.windowed) && (
                      <>
                        {frames.truncated && 'A frame this large was cut off partway through. '}
                        {series.windowed && `Showing ${series.points.length.toLocaleString()} of ${series.total.toLocaleString()} rows — this page only, full resolution. `}
                        Download the artifact for the complete data.
                      </>
                    )}
                  </p>
                  {/* The remediation callout is its own thing — a distinct alert
                      about what the pipeline did, not a plot caption — so it sits
                      outside the aligned caption/chart/caption cluster above. */}
                  {deferred.length > 0 && (
                    <div className="alert alert-info mt-3 mb-0">
                      <strong>Remediation filled nothing, by design.</strong>
                      <p className="mb-2 mt-2 small">
                        In <code>{String(report?.validation?.mode ?? 'time_series')}</code> mode the
                        pipeline clips outliers but hands gaps to the imputation step rather
                        than filling them itself. What that step then did — and why, when it
                        filled nothing — is on the Issues &amp; remediation tab.
                      </p>
                      <ul className="mb-0 small">
                        {deferred.map((a, i) => (
                          <li key={i}>
                            <code>{a.issue}</code> — {String(a.action)}
                            {Array.isArray(a.columns) && a.columns.length > 0
                              && <> · {(a.columns as string[]).join(', ')}</>}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </>
              )}
            </div>
          </section>
          </div>

          {imputedChart && imputedChart.series.length > 0 && (
            <div className="col-xl-6">
              <section className="card h-100">
                <div className="card-body">
                  <div className="run-results-chart-header">
                    <h2 className="h6 mb-0">Imputed timeline</h2>
                    <div className="btn-group btn-group-sm" role="group" aria-label="Series shown">
                      <button
                        type="button"
                        className={`btn btn-outline-secondary${imputedView === 'full' ? ' active' : ''}`}
                        onClick={() => setHiddenSeries(IMPUTED_VIEW_FULL)}
                      >
                        Full timeline
                      </button>
                      <button
                        type="button"
                        className={`btn btn-outline-secondary${imputedView === 'splits' ? ' active' : ''}`}
                        onClick={() => setHiddenSeries(IMPUTED_VIEW_SPLITS)}
                      >
                        Train / test splits
                      </button>
                    </div>
                    {imputedChart.columns.length > 1 && (
                      <label className="run-results-column-picker">
                        <span className="text-muted small">Column</span>
                        <select
                          className="form-select form-select-sm"
                          value={imputedChart.column}
                          onChange={e => setColumn(e.target.value)}
                        >
                          {imputedChart.columns.map(c => <option key={c} value={c}>{c}</option>)}
                        </select>
                      </label>
                    )}
                  </div>
                  {/* Same pageReady gate as the stage chart, so the two charts
                      never show one painted and the other still streaming —
                      they wait on the same underlying flag and flip together. */}
                  {pageReady ? (
                    <>
                      <p className="text-muted small run-results-chart-caption">
                        The stitched timeline with the two splits it was built from: they
                        tile it rather than repeating it, so each covers a stretch of the
                        whole.{' '}
                        {imputedChart.column === plottedColumn
                          ? 'Both charts are on the same axes, so the two sides can be read straight across.'
                          : <>The x axis matches the chart on the left, but the y axis
                              cannot: this frame reports <code>{imputedChart.column}</code>{' '}
                              where that one reports <code>{plottedColumn}</code>, and the
                              bundle converted the units.</>}
                      </p>
                      <SeriesChart
                        points={imputedChart.series[0].points}
                        primaryKey={imputedChart.series[0].key}
                        primaryLabel={imputedChart.series[0].label}
                        overlays={imputedChart.series.slice(1)}
                        hidden={hiddenSeries}
                        onToggle={toggleSeries}
                        counts={Object.fromEntries(
                          imputedChart.series.map(s => [s.key, { shown: s.shown, total: s.total }]),
                        )}
                        label={imputedChart.column}
                        formatX={imputedChart.isTime ? formatX : (x: number) => `#${x}`}
                        yDomain={yDomainFor(imputedChart.column)}
                        xDomain={pagedXDomain}
                        range={zoom}
                        onRangeChange={setZoom}
                        hoverX={hoverX}
                        onHoverXChange={setHoverX}
                      />
                      {/* Always rendered — matches the stage chart's bottom
                          caption, which is also a single always-present slot —
                          so the two cards end at the same height whether or not
                          this run's bundle has anything to report here. */}
                      <p className="text-muted small mb-0 mt-2 run-results-chart-caption">
                        {imputedChart.gapCount > 0 && (
                          <>
                            {imputedChart.gapCount.toLocaleString()} rows across the two
                            splits were synthesised by regularization and filled by
                            imputation; the rest are observations. The count comes from the
                            splits because the stitched timeline drops the{' '}
                            <code>is_gap</code> flag that carries it — worth knowing if you
                            download the full frame on its own.
                          </>
                        )}
                      </p>
                    </>
                  ) : (
                    <div className="run-results-chart-loading">
                      <LoadingSpinner />
                    </div>
                  )}
                </div>
              </section>
            </div>
          )}

          <div className="col-12">
          <section className="card">
            <div className="card-body">
              <h2 className="h6">Artifacts</h2>
              {data.dataset_id && (
                <p className="small text-muted mb-3 d-flex flex-wrap align-items-center gap-1">
                  {/* Name leads, the UUID that identifies it in the catalogue
                      trails as a copyable aside — matches how DatasetList
                      names a row and keeps the id only as supporting detail. */}
                  <span>Dataset</span>
                  {catalogueDatasetUrl(data.dataset_id) ? (
                    <a
                      href={catalogueDatasetUrl(data.dataset_id)!}
                      target="_blank"
                      rel="noreferrer"
                      title="View in catalogue"
                    >
                      {catalogueNames.dataset ?? data.dataset_id} <FiExternalLink aria-hidden="true" />
                    </a>
                  ) : (
                    <strong>{catalogueNames.dataset ?? data.dataset_id}</strong>
                  )}
                  <CopyableId value={data.dataset_id} maxWidth={340} />
                  {data.asset_id && (
                    <>
                      <span>· distribution</span>
                      {/* piveau has no distribution-specific page — a
                          distribution is shown inline on its dataset's page —
                          so this points at the same catalogue URL as above. */}
                      {catalogueDatasetUrl(data.dataset_id) ? (
                        <a
                          href={catalogueDatasetUrl(data.dataset_id)!}
                          target="_blank"
                          rel="noreferrer"
                          title="View dataset in catalogue"
                        >
                          {catalogueNames.asset ?? data.asset_id} <FiExternalLink aria-hidden="true" />
                        </a>
                      ) : (
                        <strong>{catalogueNames.asset ?? data.asset_id}</strong>
                      )}
                      <CopyableId value={data.asset_id} maxWidth={340} />
                    </>
                  )}
                </p>
              )}
              <ul className="run-results-artifacts">
                {data.artifacts.slice().sort(byProvenance).map(a => (
                  <li key={a.name}>
                    {/* The object key is not shown: a long bucket path that
                        wraps over several lines and says nothing about which of
                        five near-identical CSVs this is. The download still
                        uses it, and it stays in the run's XCom for anyone who
                        needs the exact object. */}
                    {/* Not btn-link btn-sm: those pin a smaller font that the
                        view's own type scale cannot override, which is what
                        made this list the smallest text on the page. */}
                    <button
                      type="button"
                      className="run-results-artifact"
                      onClick={() => download(a.name, a.key)}
                    >
                      <FiDownload className="run-results-artifact-icon" aria-hidden="true" />
                      <span className="run-results-artifact-label">
                        {ARTIFACTS[a.name]?.label ?? a.name}
                        {ARTIFACTS[a.name]?.derivedFrom && (
                          <span className="run-results-artifact-from">
                            from {ARTIFACTS[a.name].derivedFrom}
                          </span>
                        )}
                      </span>
                      {ARTIFACTS[a.name] && (
                        <span className="run-results-artifact-note">{ARTIFACTS[a.name].note}</span>
                      )}
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          </section>
          </div>
        </div>
        </>
      )}

      {tab === 'config' && (
        <section className="card">
          <div className="card-body">
            <h2 className="h6">Run configuration</h2>
            <p className="text-muted small">
              The <code>dag_run.conf</code> this run was triggered with. Parameters
              left out fall back to the DAG's own defaults, which are documented on
              each parameter in Airflow rather than repeated here.
            </p>
            {conf ? (
              <div className="table-responsive">
                <table className="table table-sm align-middle mb-0">
                  <thead>
                    <tr>
                      <th scope="col">Parameter</th>
                      <th scope="col">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(conf).map(([k, v]) => (
                      <tr key={k}>
                        <td><code>{k}</code></td>
                        <td>
                          {/* Objects and arrays are pretty-printed rather than
                              stringified onto one line: `expectations` is a list
                              of configs, and one long line is unreadable.
                              dataset_id/asset_id get the same name + catalogue
                              link + copyable uuid treatment as the Artifacts
                              panel — same lookups, same fallback to the raw id. */}
                          {typeof v === 'object' && v !== null ? (
                            <pre className="run-results-conf-value mb-0">{JSON.stringify(v, null, 2)}</pre>
                          ) : k === 'dataset_id' || k === 'asset_id' ? (
                            <span className="d-inline-flex flex-wrap align-items-center gap-1">
                              {(() => {
                                const idStr = String(v)
                                const label = k === 'dataset_id' ? catalogueNames.dataset : catalogueNames.asset
                                // A distribution has no page of its own — it is
                                // shown inline on its dataset's — so an
                                // asset_id link points at the dataset instead.
                                const linkId = k === 'dataset_id'
                                  ? idStr
                                  : (conf.dataset_id ? String(conf.dataset_id) : idStr)
                                const url = catalogueDatasetUrl(linkId)
                                return url ? (
                                  <a href={url} target="_blank" rel="noreferrer" title="View in catalogue">
                                    {label ?? idStr} <FiExternalLink aria-hidden="true" />
                                  </a>
                                ) : (
                                  <span>{label ?? idStr}</span>
                                )
                              })()}
                              <CopyableId value={String(v)} maxWidth={340} />
                            </span>
                          ) : (
                            <code>{String(v)}</code>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-muted small mb-0">
                This run was triggered with no configuration.
              </p>
            )}
          </div>
        </section>
      )}
    </div>
  )
}
