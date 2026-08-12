import type { ReactNode } from 'react'

/**
 * Metric card — general_gui_guidelines.md §14.3.
 *
 * Deliberate omissions:
 * - No trend arrow or delta. §14.3 forbids showing a trend without a stated
 *   comparison period, and the stats endpoint returns no historical values.
 * - `value` accepts null/undefined and renders as "—" with a caption, so an
 *   absent figure is never displayed as a confident zero (§13.2).
 */

export type MetricTone = 'default' | 'positive' | 'warning' | 'critical' | 'neutral'

interface MetricCardProps {
  label: string
  /** null or undefined means "not reported" — rendered as "—", never as 0. */
  value: number | null | undefined
  unit?: string
  /** Status icon, rendered next to the label — never inside the value. */
  icon?: ReactNode
  tone?: MetricTone
  /** Where this metric drills down to. §13.2: every metric should link out. */
  onNavigate?: () => void
  /** Describes the drill-down target for screen readers. */
  navigateLabel?: string
}

export default function MetricCard({
  label,
  value,
  unit,
  icon,
  tone = 'default',
  onNavigate,
  navigateLabel,
}: MetricCardProps) {
  const unavailable = value === null || value === undefined

  const body = (
    <>
      <div className="metric-label">
        {icon && <span className="metric-icon" aria-hidden="true">{icon}</span>}
        {label}
      </div>
      <div className={`metric-value metric-tone-${unavailable ? 'unavailable' : tone}`}>
        {unavailable ? '—' : value.toLocaleString()}
        {!unavailable && unit && <span className="metric-unit">{unit}</span>}
      </div>
      {unavailable && <div className="metric-caption">Not reported</div>}
    </>
  )

  // A real <button> rather than a div with role="button": the previous version
  // was not keyboard operable (§22).
  if (onNavigate) {
    return (
      <div className="col-6 col-sm-3">
        <button
          type="button"
          className="card metric-card metric-card-interactive h-100 w-100 text-start"
          onClick={onNavigate}
          aria-label={navigateLabel ?? `${label}: view details`}
        >
          <div className="card-body">{body}</div>
        </button>
      </div>
    )
  }

  return (
    <div className="col-6 col-sm-3">
      <div className="card metric-card h-100">
        <div className="card-body">{body}</div>
      </div>
    </div>
  )
}
