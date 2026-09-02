import { useState } from 'react'
import { FiAlertTriangle, FiCheckCircle, FiChevronDown, FiChevronRight, FiMinus } from 'react-icons/fi'
import type { ImputationReport, MergedQualityReport, PipelineReport, QualityCheckResult, StageShape } from '../../types'

/**
 * The report.json views, mirroring the WaveStitchPlus dashboard's two
 * cleaning-side tabs (dashboard/app.py, the `subset is None` branch):
 * "Overview" and "Quality & remediation".
 *
 * Its other tabs — Imputation, Metrics, Distribution, Long-gap — are not
 * reproduced, and cannot be from this DAG's output: they compare several
 * imputation methods against masked ground truth held in a prepared/generated
 * experiment bundle. MAE/RMSE/MAPE have nothing to score against here; there is
 * no held-out truth in a remediated frame. What the imputation step itself did
 * is reported by ImputationView below.
 *
 * Everything below comes from report.json, which the artifacts endpoint already
 * inlines — no extra fetch.
 */

function Num({ value }: { value: number | null | undefined }) {
  if (value === null || value === undefined) return <span className="report-na">—</span>
  return <span className="report-num">{value.toLocaleString()}</span>
}

/** Pass/fail with an icon and a word — never colour alone (§14.1). */
export function PassFail({ value, labels = ['Passed', 'Failed'] }: {
  value: boolean | null | undefined
  labels?: [string, string]
}) {
  if (value === null || value === undefined) {
    return <span className="report-state report-state-unknown"><FiMinus aria-hidden="true" /> Not run</span>
  }
  return value
    ? <span className="report-state report-state-good"><FiCheckCircle aria-hidden="true" /> {labels[0]}</span>
    : <span className="report-state report-state-bad"><FiAlertTriangle aria-hidden="true" /> {labels[1]}</span>
}

function shape(s: StageShape | undefined) {
  return s ? { rows: s.rows, cols: s.cols } : { rows: undefined, cols: undefined }
}

/**
 * Lineage: raw → soft-cleaned → remediated.
 *
 * The dashboard leads with this and it is the pipeline's actual story — three
 * stages, each a narrowing of the last. Deliberately a table, not a chart: three
 * stages by two measures is a shape people read faster as numbers, and a
 * three-bar chart of it would be decoration.
 */
export function LineageTable({ report }: { report: PipelineReport }) {
  const vc = report.validation_comparison ?? {}
  const ds = vc.dataset_shape ?? {}
  const soft = ds.soft_cleaned ?? ds.cleaned
  const ce = vc.cleaning_effect ?? {}
  const re = vc.remediation_effect ?? {}

  const stages: { name: string, note: string, s: StageShape | undefined, missing?: number }[] = [
    { name: 'Raw', note: 'as transferred over EDC', s: ds.raw, missing: ce.missing_cells_before },
    { name: 'Soft-cleaned', note: 'before per-issue remediation', s: soft, missing: ce.missing_cells_after },
    { name: 'Remediated', note: 'the pipeline output', s: ds.remediated, missing: re.missing_cells_after },
  ]

  if (!ds.raw && !soft && !ds.remediated) {
    return <p className="text-muted small mb-0">This report carries no stage comparison.</p>
  }

  return (
    <div className="table-responsive">
      <table className="table table-sm align-middle mb-0">
        <thead>
          <tr>
            <th scope="col">Stage</th>
            <th scope="col" className="text-end">Rows</th>
            <th scope="col" className="text-end">Columns</th>
            <th scope="col" className="text-end">Missing cells</th>
          </tr>
        </thead>
        <tbody>
          {stages.map(st => {
            const { rows, cols } = shape(st.s)
            return (
              <tr key={st.name}>
                <td>
                  {st.name}
                  <span className="report-stage-note">{st.note}</span>
                </td>
                <td className="text-end"><Num value={rows} /></td>
                <td className="text-end"><Num value={cols} /></td>
                <td className="text-end"><Num value={st.missing} /></td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

/** What each stage actually changed. */
export function EffectsTable({ report }: { report: PipelineReport }) {
  const vc = report.validation_comparison ?? {}
  const ce = vc.cleaning_effect ?? {}
  const re = vc.remediation_effect ?? {}
  const cleaning = report.cleaning ?? {}

  const rows: [string, number | undefined][] = [
    ['Rows dropped (empty or duplicate)', ce.dropped_rows],
    ['Duplicate rows before → after', undefined],
    ['Duplicate timestamps collapsed', ce.duplicate_timestamps_collapsed ?? cleaning.duplicate_timestamps as number | undefined],
    ['Non-monotonic timestamps sorted', ce.non_monotonic_timestamps_sorted ?? cleaning.non_monotonic_timestamps as number | undefined],
    ['Missing cells filled by remediation',
      re.missing_cells_before !== undefined && re.missing_cells_after !== undefined
        ? re.missing_cells_before - re.missing_cells_after
        : undefined],
    ['Outlier cells clipped', re.outlier_cells_clipped],
  ]

  return (
    <div className="table-responsive">
      <table className="table table-sm align-middle mb-0">
        <tbody>
          {rows.map(([label, value]) => (
            <tr key={label}>
              <td>{label}</td>
              <td className="text-end">
                {label.startsWith('Duplicate rows')
                  ? <><Num value={ce.duplicate_rows_before} /> → <Num value={ce.duplicate_rows_after} /></>
                  : <Num value={value} />}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/**
 * What was wrong with the frame as it arrived, counted by kind.
 *
 * Not a view of the Great Expectations results — these are the pipeline's own
 * pandas measurements, made alongside the GX run and over the soft-cleaned
 * frame: detect_time_gaps for ts_gaps, isna().mean() per column for missing,
 * a quantile-band scan for outliers, inspect_timestamp_order for
 * timestamp_order (see dataops.ts_checks.run). Only failed_columns comes from
 * GX, and in time_series mode it is a 0/1 flag rather than a count.
 *
 * It lives on the Issues & remediation tab because it pairs with what the
 * pipeline did about each family, not with the checks it is easily mistaken
 * for. MergedQualityView remains the single place a check's outcome is stated.
 */
export function IssuesView({ report }: { report: PipelineReport }) {
  const quality = report.quality ?? {}
  const issues = quality.issue_summary ?? report.validation_comparison?.issue_counts ?? {}

  if (Object.keys(issues).length === 0) {
    return <p className="text-muted small mb-0">No issues were detected in the frame as received.</p>
  }

  return (
    <div className="table-responsive">
      <table className="table table-sm align-middle mb-0">
        <tbody>
          {Object.entries(issues).map(([k, v]) => (
            <tr key={k}>
              <td><code>{k}</code></td>
              <td className="text-end"><Num value={typeof v === 'number' ? v : undefined} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}


/**
 * What the imputation step did — including, and especially, when it did nothing.
 *
 * A run that fills nothing is the common case here and not a failure: gaps are
 * only fillable once the timeline has been regularized onto a uniform grid, and
 * preprocess_csv refuses to build that grid when it would be mostly holes
 * (sparse_skip_pct, default 80%). A series sampled every ~13s with a 41-hour
 * break in it lands there. Reporting the outcome as "0 cells filled" invited
 * reading a correct decision as a broken step, so the status and its reason
 * lead, and the numbers support them.
 */
const IMPUTATION_STATUS: Record<string, { label: string, tone: 'good' | 'neutral' | 'bad' }> = {
  imputed:         { label: 'Values filled', tone: 'good' },
  nothing_to_fill: { label: 'Nothing to fill', tone: 'neutral' },
  no_bundle:       { label: 'Not attempted', tone: 'neutral' },
  disabled:        { label: 'Switched off', tone: 'neutral' },
  error:           { label: 'Failed', tone: 'bad' },
}

export function ImputationView({ imputation }: { imputation: ImputationReport }) {
  const bundle = imputation.bundle ?? {}
  const files = Object.entries(imputation.files ?? {})
  const final = imputation.final

  // A report written before the step recorded its own status carries only the
  // runner's per-split numbers. Deriving the totals and the verdict from those
  // keeps an existing run readable instead of labelling it "Unknown" — it just
  // cannot say *why* nothing was filled, which is what the status was added for.
  const filled = imputation.filled
    ?? files.reduce((n, [, f]) => n + (f.filled ?? 0), 0)
  const missing = imputation.missing_before
    ?? files.reduce((n, [, f]) => n + (f.nan_before ?? 0), 0)
  const derived = imputation.status
    ?? (imputation.error ? 'error' : files.length === 0 ? undefined : filled > 0 ? 'imputed' : 'nothing_to_fill')
  const status = IMPUTATION_STATUS[derived ?? ''] ?? { label: derived ?? 'Not reported', tone: 'neutral' as const }

  return (
    <>
      <div className="report-checks">
        <div className="report-check">
          <span className="report-check-label">Imputation</span>
          {derived === 'imputed'
            ? <PassFail value={true} labels={['Values filled', 'Failed']} />
            : derived === 'error'
              ? <PassFail value={false} labels={['Values filled', 'Failed']} />
              : <span className="report-state report-state-unknown"><FiMinus aria-hidden="true" /> {status.label}</span>}
          {imputation.method && (
            <span className="report-check-note">{imputation.lib}/{imputation.method}</span>
          )}
        </div>
        {files.length > 0 && (
          <div className="report-check">
            <span className="report-check-label">Cells filled</span>
            <span className="report-check-note">
              <Num value={filled} /> of <Num value={missing} /> missing
            </span>
          </div>
        )}
      </div>

      {imputation.reason
        ? <p className="text-muted small mt-3 mb-0">{imputation.reason}</p>
        : derived === 'nothing_to_fill' && (
            <p className="text-muted small mt-3 mb-0">
              The bundle held no missing values. This run predates the step recording
              why, so the reason is not available — re-run it to get one.
            </p>
          )}

      {/* The grid decision is the thing a reader needs when nothing was filled:
          without it, "no missing values" reads as a contradiction of the gap
          count on the left. */}
      {bundle.regularized === false && (
        <p className="text-muted small mt-2 mb-0">
          The timeline was left at its observed rows: a uniform grid at{' '}
          <Num value={bundle.base_dt} />s across this span would have been more than{' '}
          <Num value={bundle.sparse_skip_pct} />% empty, so regularization was skipped
          and no gaps were materialised to fill.
        </p>
      )}

      {/* The stitched timeline is the run's actual deliverable, so it is stated
          before the splits it was built from rather than as a footnote to them. */}
      {final && !final.error && (
        <div className="report-checks mt-3">
          <div className="report-check">
            <span className="report-check-label">Full timeline</span>
            <span className="report-check-note">
              <Num value={final.rows} /> rows, gap-free
            </span>
          </div>
          <div className="report-check">
            <span className="report-check-label">Gaps closed</span>
            <span className="report-check-note">
              <Num value={final.gaps_before} /> → <Num value={final.gaps_after} />
              {typeof final.fill_rate === 'number' && ` · ${(final.fill_rate * 100).toFixed(1)}% filled`}
            </span>
          </div>
        </div>
      )}

      {final?.error && (
        <p className="text-muted small mt-3 mb-0">
          The splits were imputed, but stitching them into one timeline failed:{' '}
          <code>{final.error}</code>
        </p>
      )}

      {files.length > 0 && (
        <div className="table-responsive mt-3">
          <table className="table table-sm align-middle mb-0">
            <thead>
              <tr>
                <th scope="col">Split</th>
                <th scope="col" className="text-end">Rows</th>
                <th scope="col" className="text-end">Missing</th>
                <th scope="col" className="text-end">Filled</th>
              </tr>
            </thead>
            <tbody>
              {files.map(([kind, f]) => (
                <tr key={kind}>
                  <td>{kind}</td>
                  <td className="text-end"><Num value={f.rows} /></td>
                  <td className="text-end"><Num value={f.nan_before} /></td>
                  <td className="text-end"><Num value={f.filled} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}


/**
 * What the pipeline did about the problems above, and whether it worked.
 *
 * The re-check is the pipeline's own suite run a second time over the
 * *remediated* frame. It is reported here and nowhere else: it measures a frame
 * the catalogue does not serve, so dali.validation.merge_quality_report
 * deliberately publishes no dqv:QualityMeasurement for it.
 */
export function RemediationView({ report }: { report: PipelineReport }) {
  const after = report.quality_after ?? null
  const remediation = report.remediation ?? {}
  const effect = report.validation_comparison?.remediation_effect
  const gxAfter = after?.report?.gx

  return (
    <>
      <div className="report-checks">
        <div className="report-check">
          <span className="report-check-label">Great Expectations — after remediation</span>
          {after
            ? <PassFail value={after.gx_passed} />
            : <span className="report-state report-state-unknown"><FiMinus aria-hidden="true" /> Not re-checked</span>}
          {gxAfter?.evaluated !== undefined && (
            <span className="report-check-note">{gxAfter.passed}/{gxAfter.evaluated} expectations</span>
          )}
        </div>
        <div className="report-check">
          <span className="report-check-label">Missing cells</span>
          <span className="report-check-note">
            <Num value={effect?.missing_cells_before} /> → <Num value={effect?.missing_cells_after} />
          </span>
        </div>
        <div className="report-check">
          <span className="report-check-label">Outlier cells clipped</span>
          <span className="report-check-note"><Num value={effect?.outlier_cells_clipped} /></span>
        </div>
      </div>

      <h3 className="h6 mt-4">Remediation actions</h3>
      {(remediation.actions ?? []).length === 0 ? (
        <p className="text-muted small mb-0">
          The pipeline applied no per-issue remediation
          {remediation.mode ? ` (mode ${String(remediation.mode)})` : ''}.
        </p>
      ) : (
        <ul className="report-actions">
          {(remediation.actions ?? []).map((a, i) => (
            <li key={i}>
              <code>{a.issue ?? 'action'}</code>
              {Object.entries(a)
                .filter(([k]) => k !== 'issue')
                .map(([k, v]) => (
                  <span key={k} className="report-action-detail">{k}: {String(v)}</span>
                ))}
            </li>
          ))}
        </ul>
      )}
    </>
  )
}


/**
 * The merged quality report — every check the run made, from both regimes.
 *
 * QualityView above reads the pipeline's own summary of its GX/pandera run;
 * this reads report.dali_quality, which dali.validation.merge_quality_report
 * builds by normalising the requested GX suite, the pipeline's auto-generated
 * GX suite and its pandera check into one comparable list with a combined
 * total. It is the same document published to the catalogue as
 * dqv:QualityMeasurement nodes, so what a user reads here is what a catalogue
 * consumer reads there.
 */
const SOURCE_LABELS: Record<string, string> = {
  pandera:            'Pandera schema',
  great_expectations: 'Great Expectations',
}

/** The totals entry is rendered as the headline, not as a row among the checks. */
const TOTALS_EXPECTATION = 'expect_all_quality_checks_to_pass'

export function MergedQualityView({ quality }: { quality: MergedQualityReport }) {
  const [onlyFailures, setOnlyFailures] = useState(false)
  // Expectation types the reader has opened. Collapsed is the default: a suite
  // runs the same few expectations over every column, so the groups and their
  // tallies are the summary, and the per-column rows are the detail you go
  // looking for. Each header states its own verdict either way, so starting
  // folded defers the detail rather than hiding it.
  const [expanded, setExpanded] = useState<Set<string>>(new Set())

  function toggleGroup(type: string) {
    setExpanded(prev => {
      const next = new Set(prev)
      if (!next.delete(type)) next.add(type)
      return next
    })
  }

  const stats = quality.statistics ?? {}
  // The post-remediation re-check is reported on the Remediation tab, not here:
  // it measures the remediated frame, while every other source measured the
  // frame as received, and it contributes nothing to the total. Sources arrive
  // in the order the report lists them — pandera, then Great Expectations.
  const sources = Object.entries(quality.sources ?? {})
    .filter(([key]) => key in SOURCE_LABELS)
  // Every check the run made. The totals row is the headline above, not a check
  // among the others.
  const checks = (quality.results ?? []).filter(r => r.expectation_type !== TOTALS_EXPECTATION)
  const failedCount = checks.filter(r => !r.success).length
  const shown = onlyFailures ? checks.filter(r => !r.success) : checks
  const groups = groupByExpectation(shown)
  const percent = stats.success_percent

  return (
    <section className="report-merged-quality">
      <div className="report-checks">
        <div className="report-check">
          <span className="report-check-label">All quality checks</span>
          <PassFail value={quality.success} />
          <span className="report-check-note">
            <Num value={stats.successful_expectations} />
            {' of '}
            <Num value={stats.evaluated_expectations} />
            {' passed'}
            {percent !== undefined && ` (${percent.toFixed(1)}%)`}
          </span>
        </div>
      </div>

      {sources.length > 0 && (
        <>
          {/* Two rows, and they add up to the headline: every GX expectation
              the run made is one execution, however many places this DAG
              happened to declare them in. */}
          <h3 className="h6 mt-4">Checks run</h3>
          <div className="table-responsive">
            <table className="table table-sm align-middle mb-0">
              <thead>
                <tr>
                  <th scope="col">Check</th>
                  <th scope="col">Result</th>
                  <th scope="col" className="text-end">Passed</th>
                </tr>
              </thead>
              <tbody>
                {sources.map(([key, source]) => (
                  <tr key={key}>
                    <td>{SOURCE_LABELS[key] ?? key}</td>
                    <td><PassFail value={source.success} /></td>
                    <td className="text-end">
                      {source.evaluated === undefined ? (
                        // pandera contributes one check, not a count — its
                        // error list is what carries the detail.
                        <span className="report-check-note">
                          {source.errors ? `${source.errors} error(s)` : '—'}
                        </span>
                      ) : (
                        <>
                          <Num value={source.passed} />
                          {' / '}
                          <Num value={source.evaluated} />
                        </>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      <div className="report-checks-header">
        <h3 className="h6 mb-0">
          Every check <span className="report-check-note">({checks.length})</span>
        </h3>
        {failedCount > 0 && (
          <div className="form-check form-switch mb-0">
            <input
              className="form-check-input"
              type="checkbox"
              role="switch"
              id="report-only-failures"
              checked={onlyFailures}
              onChange={e => setOnlyFailures(e.target.checked)}
            />
            <label className="form-check-label small" htmlFor="report-only-failures">
              Only the {failedCount} that failed
            </label>
          </div>
        )}
      </div>

      {checks.length === 0 ? (
        <p className="text-muted small mb-0">This run recorded no individual checks.</p>
      ) : (
        <div className="table-responsive">
          <table className="table table-sm align-middle mb-0">
            <thead>
              <tr>
                <th scope="col">Column</th>
                <th scope="col">Result</th>
                <th scope="col">Detail</th>
              </tr>
            </thead>
            {groups.map(g => (
              <tbody key={g.type}>
                <tr className="report-check-group">
                  <th colSpan={3} scope="colgroup">
                    <button
                      type="button"
                      className="report-group-toggle"
                      aria-expanded={expanded.has(g.type)}
                      onClick={() => toggleGroup(g.type)}
                    >
                      {expanded.has(g.type)
                        ? <FiChevronDown aria-hidden="true" />
                        : <FiChevronRight aria-hidden="true" />}
                      <code>{g.type}</code>
                      <span className="report-group-summary">
                        {g.failed > 0
                          ? `${g.failed} of ${g.checks.length} failed`
                          : `${g.checks.length} passed`}
                      </span>
                    </button>
                  </th>
                </tr>
                {expanded.has(g.type) && g.checks.map((r, i) => (
                  <tr key={`${r.kwargs?.column ?? ''}-${i}`}
                      className={r.success ? undefined : 'report-check-failed'}>
                    <td>
                      {r.kwargs?.column
                        ? <code>{r.kwargs.column}</code>
                        : <span className="report-na">whole table</span>}
                    </td>
                    <td><PassFail value={r.success} /></td>
                    <td className="small">{describeResult(r)}</td>
                  </tr>
                ))}
              </tbody>
            ))}
          </table>
        </div>
      )}
    </section>
  )
}

interface CheckGroup {
  type: string
  checks: QualityCheckResult[]
  failed: number
}

/**
 * One row per expectation, its columns beneath it.
 *
 * A suite asserts the same few expectations over every column, so the flat list
 * repeated `expect_column_values_to_not_be_null` fifteen times and buried what
 * actually varied. Groups with failures come first, and within a group so do the
 * failing columns — a problem stays visible without scrolling a passing suite.
 */
function groupByExpectation(checks: QualityCheckResult[]): CheckGroup[] {
  const byType = new Map<string, QualityCheckResult[]>()
  for (const check of checks) {
    const bucket = byType.get(check.expectation_type)
    if (bucket) bucket.push(check)
    else byType.set(check.expectation_type, [check])
  }

  return [...byType.entries()]
    .map(([type, group]) => ({
      type,
      failed: group.filter(c => !c.success).length,
      checks: group.slice().sort((a, b) => {
        if (a.success !== b.success) return a.success ? 1 : -1
        return (a.kwargs?.column ?? '').localeCompare(b.kwargs?.column ?? '')
      }),
    }))
    .sort((a, b) => {
      if ((a.failed > 0) !== (b.failed > 0)) return a.failed > 0 ? -1 : 1
      return a.type.localeCompare(b.type)
    })
}

/**
 * The most useful thing a check has to say, passed or failed.
 *
 * The regimes report different payloads — GX gives counts and an unexpected
 * percentage, pandera a list of strings, the format check a row count — so this
 * picks the most specific field present rather than dumping the object. A check
 * that carries nothing worth saying gets an em-dash, not "{}".
 */
function describeResult(r: QualityCheckResult): string {
  const result = r.result ?? {}
  const errors = result.errors
  if (Array.isArray(errors) && errors.length > 0) return errors.map(String).join('; ')
  if (typeof result.error === 'string') return result.error
  if (typeof result.unexpected_percent === 'number') {
    const pct = result.unexpected_percent
    const count = typeof result.unexpected_count === 'number' ? result.unexpected_count : null
    const total = typeof result.element_count === 'number' ? result.element_count : null
    if (count !== null && total !== null) {
      return `${count.toLocaleString()} of ${total.toLocaleString()} unexpected (${pct}%)`
    }
    return `${pct}% of values unexpected`
  }
  if (result.observed_value !== undefined) return `observed ${String(result.observed_value)}`
  if (typeof result.row_count === 'number') return `${result.row_count.toLocaleString()} rows`
  if (typeof result.failed === 'number') {
    return `${result.failed} of ${result.evaluated ?? '?'} expectations failed`
  }
  return '—'
}
