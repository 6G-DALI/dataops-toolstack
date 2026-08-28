import { FiAlertTriangle, FiCheckCircle, FiMinus } from 'react-icons/fi'
import type { PipelineReport, StageShape } from '../../types'

/**
 * The report.json views, mirroring the WaveStitchPlus dashboard's two
 * cleaning-side tabs (dashboard/app.py, the `subset is None` branch):
 * "Overview" and "Quality & remediation".
 *
 * Its other tabs — Imputation, Metrics, Distribution, Long-gap — are not
 * reproduced, and cannot be from this DAG's output: they compare several
 * imputation methods against masked ground truth held in a prepared/generated
 * experiment bundle, and dali_dataspace_process_dataset runs with
 * `imputation.build_bundle = false`. MAE/RMSE/MAPE have nothing to score
 * against here; there is no held-out truth in a remediated frame.
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
 * Quality & remediation: the two checks, before and after.
 *
 * The dashboard's phrasing is "GX detected X … after Y", and the before/after
 * pair is the point — remediation is judged by whether it moved the check, not
 * by its final state alone.
 */
export function QualityView({ report }: { report: PipelineReport }) {
  const quality = report.quality ?? {}
  const after = report.quality_after ?? null
  const validation = report.validation ?? {}
  const remediation = report.remediation ?? {}
  const issues = quality.issue_summary ?? report.validation_comparison?.issue_counts ?? {}
  const gxBefore = quality.report?.gx
  const gxAfter = after?.report?.gx

  return (
    <>
      <div className="report-checks">
        <div className="report-check">
          <span className="report-check-label">Great Expectations — before</span>
          <PassFail value={quality.gx_passed} />
          {gxBefore?.evaluated !== undefined && (
            <span className="report-check-note">{gxBefore.passed}/{gxBefore.evaluated} expectations</span>
          )}
        </div>
        <div className="report-check">
          <span className="report-check-label">Great Expectations — after remediation</span>
          {after ? <PassFail value={after.gx_passed} /> : <span className="report-state report-state-unknown"><FiMinus aria-hidden="true" /> Not re-checked</span>}
          {gxAfter?.evaluated !== undefined && (
            <span className="report-check-note">{gxAfter.passed}/{gxAfter.evaluated} expectations</span>
          )}
        </div>
        <div className="report-check">
          <span className="report-check-label">Pandera schema</span>
          <PassFail value={validation.pandera_passed} />
          {validation.mode && <span className="report-check-note">mode {String(validation.mode)}</span>}
        </div>
      </div>

      {Object.keys(issues).length > 0 && (
        <>
          <h3 className="h6 mt-4">Issues detected</h3>
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
        </>
      )}

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
