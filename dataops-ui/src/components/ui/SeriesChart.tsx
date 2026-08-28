import { useId, useMemo, useRef, useState } from 'react'

/**
 * Time-series comparison chart — inline SVG, no charting library.
 *
 * Two series, one measure, one axis: the column's value as the pipeline left it
 * (`observed`) and the points remediation filled in (`imputed`). Imputed points
 * are markers rather than a second line, because they are not a parallel signal
 * — they are the same signal, at positions where the original had nothing.
 *
 * Colours are the two categorical slots in styles/Chart.css, derived from the
 * theme's cyan and purple and stepped into the dark-mode lightness band. They
 * were validated rather than eyeballed: OKLCH L in [0.48, 0.67], chroma above
 * the floor, CVD ΔE 18.3 (deutan) / 19.0 (tritan), normal-vision ΔE 27.7, and
 * both above 3:1 against the #111620 chart surface.
 *
 * Identity is never carried by colour alone: two series means a legend is
 * always present, both are direct-labelled at their last point, and the table
 * view below the plot is the WCAG-clean twin of everything drawn here.
 */

import type { SeriesPoint } from '../../types'

interface Props {
  points: SeriesPoint[]
  /** Column being plotted; the title names it, so a single series needs no legend box. */
  label: string
  /** Formats an x value for the axis and tooltip. */
  formatX: (x: number) => string
  height?: number
}

const PAD = { top: 16, right: 16, bottom: 28, left: 56 }
const Y_TICKS = 4

function niceNumber(v: number): string {
  if (!Number.isFinite(v)) return '—'
  const abs = Math.abs(v)
  if (abs >= 1000) return v.toFixed(0)
  if (abs >= 10) return v.toFixed(1)
  if (abs >= 0.01) return v.toFixed(3)
  return v.toExponential(1)
}

export default function SeriesChart({ points, label, formatX, height = 260 }: Props) {
  const gradientId = useId()
  const svgRef = useRef<SVGSVGElement>(null)
  const [hover, setHover] = useState<number | null>(null)
  const [showTable, setShowTable] = useState(false)

  // A fixed viewBox with preserveAspectRatio="none" would stretch the marks, so
  // the chart is laid out in a fixed coordinate space and scaled by CSS width.
  const W = 900
  const H = height
  const plotW = W - PAD.left - PAD.right
  const plotH = H - PAD.top - PAD.bottom

  const geom = useMemo(() => {
    if (points.length === 0) return null
    const xs = points.map(p => p.x)
    const ys = points.map(p => p.y)
    const xMin = Math.min(...xs)
    const xMax = Math.max(...xs)
    let yMin = Math.min(...ys)
    let yMax = Math.max(...ys)
    if (yMin === yMax) { yMin -= 1; yMax += 1 }          // a flat series still needs a band
    const padY = (yMax - yMin) * 0.08
    yMin -= padY
    yMax += padY

    const sx = (x: number) => PAD.left + (xMax === xMin ? plotW / 2 : ((x - xMin) / (xMax - xMin)) * plotW)
    const sy = (y: number) => PAD.top + plotH - ((y - yMin) / (yMax - yMin)) * plotH

    return { xMin, xMax, yMin, yMax, sx, sy }
  }, [points, plotW, plotH])

  if (!geom || points.length === 0) {
    return <p className="text-muted small mb-0">No numeric values to plot for this column.</p>
  }

  const { yMin, yMax, sx, sy } = geom

  // The line is the whole series in order; imputed positions are marked on top
  // of it rather than breaking it, so the reader sees one signal with filled gaps.
  const path = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${sx(p.x).toFixed(2)},${sy(p.y).toFixed(2)}`).join(' ')
  const imputed = points.filter(p => p.imputed)
  const last = points[points.length - 1]
  const lastImputed = imputed[imputed.length - 1]

  const yTicks = Array.from({ length: Y_TICKS + 1 }, (_, i) => yMin + ((yMax - yMin) * i) / Y_TICKS)
  const hovered = hover === null ? null : points[hover]

  /** Nearest point on the x axis, so a 2px line does not demand a pinpoint hit. */
  function onMove(e: React.MouseEvent<SVGSVGElement>) {
    const svg = svgRef.current
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const x = ((e.clientX - rect.left) / rect.width) * W
    let best = 0
    let bestD = Infinity
    for (let i = 0; i < points.length; i++) {
      const d = Math.abs(sx(points[i].x) - x)
      if (d < bestD) { bestD = d; best = i }
    }
    setHover(best)
  }

  return (
    <div className="series-chart">
      <div className="series-chart-legend" role="list">
        <span className="series-chart-key" role="listitem">
          <span className="series-chart-swatch series-observed" aria-hidden="true" />
          Observed
        </span>
        <span className="series-chart-key" role="listitem">
          <span className="series-chart-swatch series-imputed" aria-hidden="true" />
          Imputed ({imputed.length.toLocaleString()})
        </span>
        <button
          type="button"
          className="btn btn-sm btn-outline-secondary series-chart-table-toggle"
          onClick={() => setShowTable(v => !v)}
          aria-expanded={showTable}
        >
          {showTable ? 'Hide values' : 'Show values'}
        </button>
      </div>

      <svg
        ref={svgRef}
        className="series-chart-svg"
        viewBox={`0 0 ${W} ${H}`}
        role="img"
        aria-label={`${label}: ${points.length} points, ${imputed.length} imputed`}
        onMouseMove={onMove}
        onMouseLeave={() => setHover(null)}
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" className="series-chart-fill-top" />
            <stop offset="100%" className="series-chart-fill-bottom" />
          </linearGradient>
        </defs>

        {/* Solid hairline grid, one shade off the surface — never dashed. */}
        {yTicks.map(t => (
          <g key={t}>
            <line
              className="series-chart-grid"
              x1={PAD.left} x2={W - PAD.right}
              y1={sy(t)} y2={sy(t)}
            />
            <text className="series-chart-tick" x={PAD.left - 8} y={sy(t)} textAnchor="end" dominantBaseline="middle">
              {niceNumber(t)}
            </text>
          </g>
        ))}

        <text className="series-chart-tick" x={PAD.left} y={H - 8}>{formatX(points[0].x)}</text>
        <text className="series-chart-tick" x={W - PAD.right} y={H - 8} textAnchor="end">{formatX(last.x)}</text>

        <path className="series-chart-area" d={`${path} L${sx(last.x)},${PAD.top + plotH} L${sx(points[0].x)},${PAD.top + plotH} Z`} fill={`url(#${gradientId})`} />
        <path className="series-chart-line series-observed-stroke" d={path} />

        {/* Imputed markers: >=8px, each with a 2px surface ring so overlaps stay
            readable rather than merging into a blob. */}
        {imputed.map((p, i) => (
          <circle
            key={`${p.x}-${i}`}
            className="series-chart-marker series-imputed-fill"
            cx={sx(p.x)} cy={sy(p.y)} r={4}
          />
        ))}

        {/* Selective direct labels: the last point of each series, never every point. */}
        <text className="series-chart-direct series-observed-text" x={sx(last.x) - 6} y={sy(last.y) - 10} textAnchor="end">
          {niceNumber(last.y)}
        </text>
        {lastImputed && (
          <text className="series-chart-direct series-imputed-text" x={sx(lastImputed.x) - 6} y={sy(lastImputed.y) + 18} textAnchor="end">
            imputed
          </text>
        )}

        {hovered && (
          <g>
            <line className="series-chart-crosshair" x1={sx(hovered.x)} x2={sx(hovered.x)} y1={PAD.top} y2={PAD.top + plotH} />
            <circle
              className={`series-chart-marker ${hovered.imputed ? 'series-imputed-fill' : 'series-observed-fill'}`}
              cx={sx(hovered.x)} cy={sy(hovered.y)} r={5}
            />
          </g>
        )}
      </svg>

      {/* Tooltips enhance; they never gate a value — the table below carries all of them. */}
      {hovered && (
        <p className="series-chart-readout" aria-live="polite">
          <span className="series-chart-readout-x">{formatX(hovered.x)}</span>
          <span className="series-chart-readout-y">{niceNumber(hovered.y)}</span>
          {hovered.imputed && <span className="series-chart-readout-tag">imputed</span>}
        </p>
      )}

      {showTable && (
        <div className="table-responsive series-chart-table">
          <table className="table table-sm table-hover align-middle mb-0">
            <thead>
              <tr><th scope="col">Position</th><th scope="col">{label}</th><th scope="col">Source</th></tr>
            </thead>
            <tbody>
              {points.map((p, i) => (
                <tr key={`${p.x}-${i}`}>
                  <td>{formatX(p.x)}</td>
                  <td>{niceNumber(p.y)}</td>
                  <td>{p.imputed ? 'Imputed' : 'Observed'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
