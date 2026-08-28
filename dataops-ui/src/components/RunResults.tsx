import { useEffect, useMemo, useState } from 'react'
import { FiAlertTriangle, FiCheckCircle, FiDownload } from 'react-icons/fi'
import { getRunArtifacts, getRunArtifactText } from '../api/airflow'
import type { RunArtifacts, SeriesPoint } from '../types'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import MetricCard from './ui/MetricCard'
import SeriesChart from './ui/SeriesChart'
import CopyableId from './ui/CopyableId'
import '../styles/Chart.css'
import '../styles/RunResults.css'

/**
 * What a dali_dataspace_process_dataset run produced.
 *
 * The run reports its own outputs: upload_artifacts returns {name: object key},
 * the orchestrator reads that XCom and serves the artifacts from the same Data
 * Space bucket the distribution came from. Nothing here guesses key patterns.
 *
 * The chart answers the question the pipeline exists to answer — which values
 * were filled in — by diffing the two CSVs it always writes: soft_cleaned is
 * the frame before per-issue remediation, remediated is after, so a position
 * that is empty in the first and present in the second was imputed.
 */

/** Enough rows for the shape of a signal without pulling a large frame. */
const CSV_BYTE_BUDGET = 2 * 1024 * 1024
/** Points actually drawn. Beyond this the line is stride-sampled — see below. */
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

interface Props {
  dagId: string
  runId: string
}

export default function RunResults({ dagId, runId }: Props) {
  const [data, setData] = useState<RunArtifacts | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const [csv, setCsv] = useState<{
    header: string[]
    remediated: string[][]
    soft: string[][]
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

  // The frames are fetched only once the artifact list confirms both exist —
  // a run that failed before remediation still renders its report and links.
  useEffect(() => {
    if (!data) return
    const names = new Set(data.artifacts.map(a => a.name))
    if (!names.has('remediated_csv') || !names.has('soft_cleaned_csv')) return

    let cancelled = false
    setCsvError(null)
    Promise.all([
      getRunArtifactText(dagId, runId, 'remediated_csv', CSV_BYTE_BUDGET),
      getRunArtifactText(dagId, runId, 'soft_cleaned_csv', CSV_BYTE_BUDGET),
    ])
      .then(([rem, soft]) => {
        if (cancelled) return
        const r = parseCsv(rem.text)
        const s = parseCsv(soft.text)
        setCsv({
          header: r.header,
          remediated: r.rows,
          soft: s.rows,
          truncated: rem.truncated || soft.truncated,
        })
      })
      .catch(e => { if (!cancelled) setCsvError((e as Error).message) })
    return () => { cancelled = true }
  }, [data, dagId, runId])

  const columns = useMemo(
    () => (csv ? numericColumns(csv.header, csv.remediated) : []),
    [csv],
  )

  useEffect(() => {
    if (columns.length && !columns.includes(column)) setColumn(columns[0])
  }, [columns, column])

  const series = useMemo<{ points: SeriesPoint[], sampled: boolean, isTime: boolean }>(() => {
    if (!csv || !column) return { points: [], sampled: false, isTime: false }
    const col = csv.header.indexOf(column)
    if (col < 0) return { points: [], sampled: false, isTime: false }

    const tCol = timeColumn(csv.header, csv.remediated)
    const all: SeriesPoint[] = []
    for (let i = 0; i < csv.remediated.length; i++) {
      const raw = csv.remediated[i][col]
      if (isBlank(raw)) continue
      const y = Number(raw)
      if (Number.isNaN(y)) continue
      // Same row position in the pre-remediation frame. Blank there and present
      // here is precisely what "imputed" means for this pipeline.
      const before = csv.soft[i]?.[col]
      const x = tCol === null ? i : Date.parse(csv.remediated[i][tCol])
      all.push({ x: Number.isNaN(x) ? i : x, y, imputed: isBlank(before) })
    }

    if (all.length <= MAX_PLOT_POINTS) {
      return { points: all, sampled: false, isTime: tCol !== null }
    }
    // Stride-sample, but never drop an imputed point — they are the story.
    const stride = Math.ceil(all.length / MAX_PLOT_POINTS)
    const points = all.filter((p, i) => p.imputed || i % stride === 0)
    return { points, sampled: true, isTime: tCol !== null }
  }, [csv, column])

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

  const cleaning = data.report?.cleaning ?? {}
  const validation = data.report?.validation ?? {}
  const quality = data.report?.quality ?? {}
  const validationPassed = validation.pandera_passed
  const qualityPassed = quality.report?.gx_passed
  const imputedCount = series.points.filter(p => p.imputed).length

  const formatX = series.isTime
    ? (x: number) => new Date(x).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '')
    : (x: number) => `#${x}`

  return (
    <div className="run-results">
      <div className="row g-3 mb-4">
        <div className="col-sm-6 col-xl-3">
          <MetricCard label="Rows in" value={cleaning.input_rows ?? null} />
        </div>
        <div className="col-sm-6 col-xl-3">
          <MetricCard label="Rows out" value={cleaning.output_rows ?? null} />
        </div>
        <div className="col-sm-6 col-xl-3">
          <MetricCard
            label="Imputed (plotted)"
            value={csv ? imputedCount : null}
            tone={imputedCount > 0 ? 'warning' : 'default'}
          />
        </div>
        <div className="col-sm-6 col-xl-3">
          {/* Status is never colour alone: an icon and a word carry it too. */}
          <div className={`run-results-status run-results-status-${validationPassed === false ? 'critical' : validationPassed === true ? 'good' : 'unknown'}`}>
            <span className="run-results-status-label">Validation</span>
            <span className="run-results-status-value">
              {validationPassed === true && <><FiCheckCircle aria-hidden="true" /> Passed</>}
              {validationPassed === false && <><FiAlertTriangle aria-hidden="true" /> Failed</>}
              {validationPassed === undefined && '—'}
            </span>
            {/* Two checks run over the frame — pandera's schema validation and
                Great Expectations' quality suite. Both belong on this tile:
                they answer the same question and a run can pass one and fail
                the other. */}
            <span className="run-results-status-note">
              {validation.mode && <>mode {String(validation.mode)}</>}
              {qualityPassed !== undefined && (
                <> · quality {qualityPassed ? 'passed' : 'failed'}</>
              )}
            </span>
          </div>
        </div>
      </div>

      {validation.errors && validation.errors.length > 0 && (
        <div className="alert alert-warning">
          <strong>Validation reported {validation.errors.length} issue{validation.errors.length === 1 ? '' : 's'}.</strong>
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

      <section className="card mb-4">
        <div className="card-body">
          {/* One filter row above the chart it scopes — never inside the plot. */}
          <div className="run-results-chart-header">
            <h2 className="h6 mb-0">Remediated series</h2>
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
          {!csv && !csvError && <LoadingSpinner />}
          {csv && (
            <>
              <SeriesChart points={series.points} label={column} formatX={formatX} />
              {(csv.truncated || series.sampled) && (
                <p className="text-muted small mb-0 mt-2">
                  {csv.truncated && 'Only the first part of the frame was fetched. '}
                  {series.sampled && `Showing ${series.points.length.toLocaleString()} of the fetched rows — every imputed point is kept, observed points are sampled. `}
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
                <a
                  href={`${window.location.origin}`}
                  onClick={e => {
                    e.preventDefault()
                    // Fetched rather than linked: the endpoint needs the bearer
                    // token, which a plain href cannot carry.
                    getRunArtifactText(dagId, runId, a.name, 64 * 1024 * 1024)
                      .then(({ text }) => {
                        const url = URL.createObjectURL(new Blob([text]))
                        const el = document.createElement('a')
                        el.href = url
                        el.download = a.key.split('/').pop() ?? a.name
                        el.click()
                        URL.revokeObjectURL(url)
                      })
                      .catch(err => setCsvError((err as Error).message))
                  }}
                >
                  <FiDownload aria-hidden="true" /> {a.name}
                </a>
                <code className="run-results-key">{a.key}</code>
              </li>
            ))}
          </ul>
        </div>
      </section>
    </div>
  )
}
