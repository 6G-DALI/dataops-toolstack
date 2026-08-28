import { useEffect, useMemo, useState } from 'react'
import { FiDownload } from 'react-icons/fi'
import { getRunArtifacts, getRunArtifactText } from '../api/airflow'
import type { RunArtifacts, SeriesPoint } from '../types'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import MetricCard from './ui/MetricCard'
import SeriesChart from './ui/SeriesChart'
import CopyableId from './ui/CopyableId'
import { EffectsTable, LineageTable, PassFail, QualityView } from './ui/PipelineReportView'
import '../styles/Chart.css'
import '../styles/RunResults.css'

/**
 * What a dali_dataspace_process_dataset run produced.
 *
 * The tabs mirror the WaveStitchPlus dashboard's cleaning-side view
 * (dashboard/app.py, the `subset is None` branch): Overview, Quality &
 * remediation, and the data itself. Its Imputation / Metrics / Distribution /
 * Long-gap tabs are deliberately absent — they compare imputation methods
 * against masked ground truth from a prepared/generated experiment bundle, and
 * this DAG runs with `imputation.build_bundle = false`. MAE/RMSE/MAPE would
 * have nothing to score against.
 *
 * The run reports its own outputs: upload_artifacts returns {name: object key},
 * the orchestrator reads that XCom and serves the objects from the bucket the
 * run's conf names. Nothing here guesses key patterns.
 */

/** Artifact names as dali.processing.run_dataops_pipeline publishes them. */
const RAW = 'input_csv'
const SOFT = 'soft_cleaned_csv'
const REMEDIATED = 'output_csv'

/** Enough rows for the shape of a signal without pulling a large frame. */
const CSV_BYTE_BUDGET = 2 * 1024 * 1024
/** Points actually drawn; beyond this observed values are stride-sampled. */
const MAX_PLOT_POINTS = 2000

/** Minimal RFC4180-ish parser: pandas quotes any field containing a comma. */
function parseCsv(text: string): { header: string[], rows: string[][] } {
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

/** First column whose values parse as dates — the pipeline's timestamp column. */
function timeColumn(header: string[], rows: string[][]): number | null {
  for (let col = 0; col < header.length; col++) {
    const v = rows.find(r => !isBlank(r[col]))?.[col]
    if (!v || !Number.isNaN(Number(v))) continue
    if (!Number.isNaN(Date.parse(v))) return col
  }
  return null
}

type Frame = { header: string[], rows: string[][] }
type Tab = 'overview' | 'quality' | 'data'

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
    raw: Frame | null
    soft: Frame | null
    remediated: Frame | null
    truncated: boolean
  } | null>(null)
  const [csvError, setCsvError] = useState<string | null>(null)
  const [column, setColumn] = useState<string>('')

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    getRunArtifacts(dagId, runId)
      .then(d => { if (!cancelled) setData(d) })
      .catch(e => { if (!cancelled) setError((e as Error).message) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [dagId, runId])

  // Frames are fetched only once the artifact list says which exist. A run that
  // failed before remediation, or one from before the raw frame was uploaded,
  // still renders everything it does have.
  useEffect(() => {
    if (!data) return
    const names = new Set(data.artifacts.map(a => a.name))
    const wanted = [RAW, SOFT, REMEDIATED].filter(n => names.has(n))
    if (wanted.length === 0) return

    let cancelled = false
    setCsvError(null)
    Promise.all(wanted.map(n => getRunArtifactText(dagId, runId, n, CSV_BYTE_BUDGET)))
      .then(results => {
        if (cancelled) return
        const byName = new Map(wanted.map((n, i) => [n, results[i]]))
        const frame = (n: string) => {
          const r = byName.get(n)
          return r ? parseCsv(r.text) : null
        }
        setFrames({
          raw: frame(RAW),
          soft: frame(SOFT),
          remediated: frame(REMEDIATED),
          truncated: results.some(r => r.truncated),
        })
      })
      .catch(e => { if (!cancelled) setCsvError((e as Error).message) })
    return () => { cancelled = true }
  }, [data, dagId, runId])

  const plotFrame = frames?.remediated ?? frames?.soft ?? frames?.raw ?? null

  const columns = useMemo(
    () => (plotFrame ? numericColumns(plotFrame.header, plotFrame.rows) : []),
    [plotFrame],
  )

  useEffect(() => {
    if (columns.length && !columns.includes(column)) setColumn(columns[0])
  }, [columns, column])

  const series = useMemo<{ points: SeriesPoint[], sampled: boolean, isTime: boolean }>(() => {
    if (!plotFrame || !column) return { points: [], sampled: false, isTime: false }
    const col = plotFrame.header.indexOf(column)
    if (col < 0) return { points: [], sampled: false, isTime: false }

    const before = frames?.soft
    const beforeCol = before ? before.header.indexOf(column) : -1
    const tCol = timeColumn(plotFrame.header, plotFrame.rows)

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
      const x = tCol === null ? i : Date.parse(plotFrame.rows[i][tCol])
      all.push({ x: Number.isNaN(x) ? i : x, y, changed })
    }

    if (all.length <= MAX_PLOT_POINTS) return { points: all, sampled: false, isTime: tCol !== null }
    // Stride-sample, but never drop a changed point — they are the story.
    const stride = Math.ceil(all.length / MAX_PLOT_POINTS)
    return {
      points: all.filter((p, i) => p.changed || i % stride === 0),
      sampled: true,
      isTime: tCol !== null,
    }
  }, [plotFrame, frames, column])

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
  const markerLabel = filledCells > 0 ? 'Imputed' : 'Adjusted'

  const formatX = series.isTime
    ? (x: number) => new Date(x).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '')
    : (x: number) => `#${x}`

  const TABS: { id: Tab, label: string }[] = [
    { id: 'overview', label: 'Overview' },
    { id: 'quality', label: 'Quality & remediation' },
    { id: 'data', label: 'Data' },
  ]

  return (
    <div className="run-results">
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
          <div className="row g-3 mb-4">
            <div className="col-sm-6 col-xl-3">
              <MetricCard label="Rows in" value={cleaning.input_rows ?? null} />
            </div>
            <div className="col-sm-6 col-xl-3">
              <MetricCard label="Rows out" value={cleaning.output_rows ?? null} />
            </div>
            <div className="col-sm-6 col-xl-3">
              <MetricCard label="Missing cells filled" value={filled} tone={filled ? 'warning' : 'default'} />
            </div>
            <div className="col-sm-6 col-xl-3">
              <div className="run-results-status">
                <span className="run-results-status-label">Validation</span>
                <span className="run-results-status-value">
                  <PassFail value={validation.pandera_passed} />
                </span>
                <span className="run-results-status-note">
                  {validation.mode && <>mode {String(validation.mode)}</>}
                </span>
              </div>
            </div>
          </div>

          <section className="card mb-4">
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

          <section className="card">
            <div className="card-body">
              <h2 className="h6">What each stage changed</h2>
              {report
                ? <EffectsTable report={report} />
                : <p className="text-muted small mb-0">This run published no report.</p>}
            </div>
          </section>
        </>
      )}

      {tab === 'quality' && (
        <section className="card">
          <div className="card-body">
            {report
              ? <QualityView report={report} />
              : <p className="text-muted small mb-0">This run published no report.</p>}
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

      {tab === 'data' && (
        <>
          <section className="card mb-4">
            <div className="card-body">
              {/* One filter row above the chart it scopes — never inside the plot. */}
              <div className="run-results-chart-header">
                <h2 className="h6 mb-0">
                  {frames?.remediated ? 'Remediated series' : 'Series'}
                </h2>
                {columns.length > 1 && (
                  <label className="run-results-column-picker">
                    <span className="text-muted small">Column</span>
                    <select
                      className="form-select form-select-sm"
                      value={column}
                      onChange={e => setColumn(e.target.value)}
                    >
                      {columns.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </label>
                )}
              </div>

              {csvError && <ErrorMessage message={csvError} />}
              {!frames && !csvError && <LoadingSpinner />}
              {frames && (
                <>
                  <SeriesChart
                    points={series.points}
                    label={column}
                    formatX={formatX}
                    markerLabel={markerLabel}
                  />
                  {deferred.length > 0 && (
                    <div className="alert alert-info mt-3 mb-0">
                      <strong>No values were imputed, by design.</strong>
                      <p className="mb-2 mt-2 small">
                        In <code>{String(report?.validation?.mode ?? 'time_series')}</code> mode the
                        pipeline clips outliers but leaves gaps and missing values for the
                        imputation handoff, which this run did not build
                        (<code>imputation.build_bundle</code> is false). Trigger the DAG with{' '}
                        <code>{'{"imputation": {"build_bundle": true}}'}</code> to produce a
                        regularized bundle and have them filled.
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
                  {!frames.soft && (
                    <p className="text-muted small mb-0 mt-2">
                      This run published no soft-cleaned frame, so the points
                      remediation changed cannot be identified — every point is
                      shown as observed.
                    </p>
                  )}
                  {(frames.truncated || series.sampled) && (
                    <p className="text-muted small mb-0 mt-2">
                      {frames.truncated && 'Only the first part of each frame was fetched. '}
                      {series.sampled && `Showing ${series.points.length.toLocaleString()} of the fetched rows — every changed point is kept, observed points are sampled. `}
                      Download the artifact for the complete data.
                    </p>
                  )}
                </>
              )}
            </div>
          </section>

          <section className="card">
            <div className="card-body">
              <h2 className="h6">Artifacts</h2>
              {data.dataset_id && (
                <p className="small text-muted mb-3">
                  Dataset <CopyableId value={data.dataset_id} />
                  {data.asset_id && <> · distribution <CopyableId value={data.asset_id} /></>}
                </p>
              )}
              <ul className="run-results-artifacts">
                {data.artifacts.map(a => (
                  <li key={a.name}>
                    <button
                      type="button"
                      className="btn btn-link btn-sm p-0"
                      onClick={() => download(a.name, a.key)}
                    >
                      <FiDownload aria-hidden="true" /> {a.name}
                    </button>
                    <code className="run-results-key">{a.key}</code>
                  </li>
                ))}
              </ul>
            </div>
          </section>
        </>
      )}
    </div>
  )
}
