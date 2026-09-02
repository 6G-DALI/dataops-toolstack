import { useId, useMemo, useRef, useState } from 'react'

/**
 * Time-series comparison chart — inline SVG, no charting library.
 *
 * Two series, one measure, one axis: the column's value as the pipeline left it
 * (`observed`) and the points remediation changed (`markerLabel` names what it
 * did — filled a blank, or moved an outlier). The changed points are markers
 * rather than a second line, because they are not a parallel signal — they are
 * the same signal, at positions the pipeline touched.
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
  /** Names the marker series — what remediation did to these points. Omitted,
   *  no markers are drawn: on a frame where most rows are synthesised, marking
   *  them all is a solid band of colour that says nothing. */
  markerLabel?: string
  /** Names the primary series in the legend. */
  primaryLabel?: string
  /**
   * Further series on the same axes.
   *
   * For comparing frames of the same measure that share no row alignment — the
   * imputed splits against the timeline they were stitched into — where a diff
   * is impossible and colour is the only way to tell them apart. They join the
   * domain but not the crosshair: the readout follows the primary series, which
   * is why the fullest one should be passed as `points`.
   */
  overlays?: { key: string, label: string, points: SeriesPoint[] }[]
  /**
   * The zoom window, lifted out so several charts can share one.
   *
   * Left undefined the chart keeps its own, which is what a lone chart wants.
   * Supplied — with `onRangeChange` to move it — the window belongs to the
   * caller, so two charts of the same timeline zoom together instead of
   * drifting apart the moment one of them is dragged.
   */
  range?: { min: number, max: number } | null
  onRangeChange?: (range: { min: number, max: number } | null) => void
  /**
   * The hovered x position, lifted out so several charts can share one
   * crosshair — the same reason `range` is liftable. Left undefined the
   * chart keeps its own hover state; supplied (with `onHoverXChange`) two
   * charts of related series can be pointed at together, each resolving the
   * shared x to the nearest point in its own data.
   */
  hoverX?: number | null
  onHoverXChange?: (x: number | null) => void
  /** Identifies the primary series for `hidden` and `onToggle`. */
  primaryKey?: string
  /**
   * Series keys currently switched off, with `onToggle` to switch them.
   *
   * Supplied, the legend becomes the control: three lines of the same measure
   * overlap almost everywhere, and being able to drop one is the difference
   * between reading the chart and guessing which line is on top. A hidden
   * series leaves the scales alone — the axes are what make the pair
   * comparable, so they must not move when a line is switched off.
   */
  hidden?: string[]
  onToggle?: (key: string) => void
  /**
   * Points actually drawn vs. fetched for a series, keyed by `primaryKey` /
   * an overlay's `key`. Shown in the legend next to the toggle so a reader
   * can tell a thin-looking line apart from a genuinely stride-sampled one,
   * without having to guess from how sparse it looks on screen.
   */
  counts?: Record<string, { shown: number, total: number }>
  /**
   * Fixed y range, already padded, instead of one derived from `points`.
   *
   * Passed when several frames of the same measure are viewed in turn: a scale
   * fitted to each in isolation redraws every one of them to fill the plot, so
   * a stage whose values moved looks identical to one whose values did not.
   * Holding the axis still is what makes the difference visible.
   */
  yDomain?: [number, number]
  /**
   * Fixed x range for the unzoomed chart, for the same reason as `yDomain`:
   * stages hold different numbers of rows, so a scale fitted to each stretches
   * a shorter frame across the full width and hides that rows were dropped.
   * A zoom still overrides it — the window the reader selected is the domain
   * while it is in force.
   */
  xDomain?: [number, number]
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

export default function SeriesChart({
  points, label, formatX, markerLabel, primaryLabel = 'Observed',
  overlays = [], yDomain, xDomain, height = 260, counts,
  range: controlledRange, onRangeChange,
  hoverX: controlledHoverX, onHoverXChange,
  primaryKey = 'primary', hidden = [], onToggle,
}: Props) {
  const gradientId = useId()
  const svgRef = useRef<SVGSVGElement>(null)
  // Data-x, not an index into `tracked`: two shared charts sample their data
  // differently (different stride, different row counts), so an index means
  // nothing across them. Each chart resolves the shared x to its own nearest
  // point below.
  const [ownHoverX, setOwnHoverX] = useState<number | null>(null)
  const hoverControlled = controlledHoverX !== undefined
  const hoverX = hoverControlled ? controlledHoverX : ownHoverX
  const setHoverX = hoverControlled ? (onHoverXChange ?? (() => {})) : setOwnHoverX
  const [showTable, setShowTable] = useState(false)
  // Zoom is an x-range over the data, not a transform of the drawing: the axes,
  // the y scale and the direct labels are all recomputed for the window, so a
  // zoomed chart is a chart of that window rather than a magnified picture of
  // the whole one.
  const [ownRange, setOwnRange] = useState<{ min: number, max: number } | null>(null)
  const controlled = controlledRange !== undefined
  const range = controlled ? controlledRange : ownRange
  const setRange = controlled ? (onRangeChange ?? (() => {})) : setOwnRange
  const [drag, setDrag] = useState<{ from: number, to: number } | null>(null)

  // A fixed viewBox with preserveAspectRatio="none" would stretch the marks, so
  // the chart is laid out in a fixed coordinate space and scaled by CSS width.
  const W = 900
  const H = height
  const plotW = W - PAD.left - PAD.right
  const plotH = H - PAD.top - PAD.bottom

  // The line is drawn in array order (see `line` below), so a point out of x
  // order draws as a stroke that rockets back toward wherever it sits instead
  // of progressing left to right — the "weird line to the start" a paged frame
  // produces when its rows arrive as several fetched chunks stitched together
  // (RunResults' incremental CSV walk) rather than as one read of a sorted
  // file. Sorting here, once, right before anything downstream reads `points`,
  // makes every consumer (the path, the area, the direct labels, the table)
  // agree on ascending x regardless of what order the caller's data arrived in.
  const sortedPoints = useMemo(() => [...points].sort((a, b) => a.x - b.x), [points])

  // A window too small to be a chart is not applied — better to ignore a stray
  // drag than to leave the reader on a blank plot they have to undo.
  const visible = useMemo(() => {
    if (!range) return sortedPoints
    const w = sortedPoints.filter(p => p.x >= range.min && p.x <= range.max)
    return w.length >= 2 ? w : sortedPoints
  }, [sortedPoints, range])

  // Overlays are windowed by the same zoom, and join the scales so no series is
  // drawn off the plot. Sorted for the same reason as the primary series above.
  const visibleOverlays = useMemo(
    () => overlays.map(o => {
      const sorted = [...o.points].sort((a, b) => a.x - b.x)
      return { ...o, points: range ? sorted.filter(p => p.x >= range.min && p.x <= range.max) : sorted }
    }),
    [overlays, range],
  )

  const hiddenSet = new Set(hidden)
  const primaryShown = !hiddenSet.has(primaryKey)
  // Tone is fixed to the series' declared position, not to what is on screen,
  // so hiding one never recolours the others.
  const shownOverlays = visibleOverlays
    .map((o, i) => ({ ...o, tone: i + 1 }))
    .filter(o => !hiddenSet.has(o.key))
  /** The series the crosshair and readout follow — the primary, or the first left. */
  const tracked = primaryShown ? visible : (shownOverlays[0]?.points ?? [])

  const geom = useMemo(() => {
    // Deliberately over the *whole* series, hidden ones included: switching a
    // line off should not rescale the axes under the ones still shown.
    const all = [...visible, ...visibleOverlays.flatMap(o => o.points)]
    if (all.length === 0) return null
    const xs = all.map(p => p.x)
    const ys = all.map(p => p.y)
    // A zoom is the domain while it holds; otherwise the shared range if one was
    // given, and only failing both does the chart fit itself to its own points.
    let xMin: number
    let xMax: number
    if (range) {
      xMin = range.min
      xMax = range.max
    } else if (xDomain) {
      [xMin, xMax] = xDomain
    } else {
      xMin = Math.min(...xs)
      xMax = Math.max(...xs)
    }
    let yMin: number
    let yMax: number
    if (yDomain) {
      [yMin, yMax] = yDomain
    } else {
      yMin = Math.min(...ys)
      yMax = Math.max(...ys)
      if (yMin === yMax) { yMin -= 1; yMax += 1 }        // a flat series still needs a band
      const padY = (yMax - yMin) * 0.08
      yMin -= padY
      yMax += padY
    }

    const sx = (x: number) => PAD.left + (xMax === xMin ? plotW / 2 : ((x - xMin) / (xMax - xMin)) * plotW)
    const sy = (y: number) => PAD.top + plotH - ((y - yMin) / (yMax - yMin)) * plotH

    /** Screen x back to data x — what a drag's endpoints have to be stored as. */
    const ix = (px: number) => xMin + ((px - PAD.left) / plotW) * (xMax - xMin)

    return { xMin, xMax, yMin, yMax, sx, sy, ix }
  }, [visible, visibleOverlays, plotW, plotH, yDomain, xDomain, range])

  if (!geom || visible.length === 0) {
    return <p className="text-muted small mb-0">No numeric values to plot for this column.</p>
  }

  const { yMin, yMax, sx, sy, ix } = geom
  const zoomed = range !== null && visible.length !== points.length

  // The line is the whole series in order; changed positions are marked on top
  // of it rather than breaking it, so the reader sees one signal and what moved.
  const line = (pts: SeriesPoint[]) =>
    pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${sx(p.x).toFixed(2)},${sy(p.y).toFixed(2)}`).join(' ')
  const path = line(visible)
  const changed = markerLabel && primaryShown ? visible.filter(p => p.changed) : []
  const last = tracked[tracked.length - 1] ?? visible[visible.length - 1]
  const lastChanged = changed[changed.length - 1]

  const yTicks = Array.from({ length: Y_TICKS + 1 }, (_, i) => yMin + ((yMax - yMin) * i) / Y_TICKS)

  // The crosshair and its readout are always on. Pointing at a chart to find
  // out whether it can tell you anything is a discovery problem: with nothing
  // drawn until the mouse arrives, the readout's existence is a secret, and it
  // vanishes the moment you look away from the plot to read it. Unpointed, it
  // rests on the last point — the value the series ends at, which the direct
  // label already emphasises — and follows the pointer from there.
  const pinned = hoverX === null
  // Nearest point in *this* chart's own tracked series to the shared x — a
  // second chart of the same measure rarely samples the same rows, so the
  // shared position is a data-x, resolved independently on each side.
  function nearest(x: number): SeriesPoint {
    let best = tracked[0]
    let bestD = Infinity
    for (const p of tracked) {
      const d = Math.abs(p.x - x)
      if (d < bestD) { bestD = d; best = p }
    }
    return best
  }
  const active = pinned ? tracked[tracked.length - 1] : nearest(hoverX)

  /** Pointer position in the chart's own coordinate space. */
  function svgX(e: React.MouseEvent<SVGSVGElement>): number | null {
    const svg = svgRef.current
    if (!svg) return null
    const rect = svg.getBoundingClientRect()
    return ((e.clientX - rect.left) / rect.width) * W
  }

  /** Nearest point on the x axis, so a 2px line does not demand a pinpoint hit. */
  function onMove(e: React.MouseEvent<SVGSVGElement>) {
    const x = svgX(e)
    if (x === null) return
    setHoverX(ix(x))
    if (drag) setDrag({ ...drag, to: ix(x) })
  }

  function onDown(e: React.MouseEvent<SVGSVGElement>) {
    const x = svgX(e)
    if (x === null) return
    setDrag({ from: ix(x), to: ix(x) })
  }

  function onUp() {
    if (!drag) return
    const min = Math.min(drag.from, drag.to)
    const max = Math.max(drag.from, drag.to)
    setDrag(null)
    // A click is a drag of zero width; only a band wider than a few pixels and
    // holding at least two points is a zoom.
    if (Math.abs(sx(max) - sx(min)) < 8) return
    if (points.filter(p => p.x >= min && p.x <= max).length < 2) return
    setRange({ min, max })
    setHoverX(null)
  }

  return (
    <div className="series-chart">
      <div className="series-chart-legend" role="list">
        {[{ key: primaryKey, label: primaryLabel, swatch: 'series-observed' },
          ...overlays.map((o, i) => ({ key: o.key, label: o.label, swatch: `series-alt-${i + 1}` }))]
          .map(k => {
            const off = hiddenSet.has(k.key)
            const count = counts?.[k.key]
            const body = (
              <>
                <span className={`series-chart-swatch ${k.swatch}`} aria-hidden="true" />
                {k.label}
                {count && (
                  <span className="series-chart-key-count">
                    {count.shown < count.total
                      ? ` (${count.shown.toLocaleString()} of ${count.total.toLocaleString()})`
                      : ` (${count.total.toLocaleString()})`}
                  </span>
                )}
              </>
            )
            // A hidden key keeps its place and its swatch — it is how the line
            // is brought back, so it must not vanish when switched off.
            return onToggle ? (
              <button
                type="button"
                key={k.key}
                role="listitem"
                aria-pressed={!off}
                className={`series-chart-key series-chart-key-toggle${off ? ' is-off' : ''}`}
                onClick={() => onToggle(k.key)}
              >
                {body}
              </button>
            ) : (
              <span className="series-chart-key" role="listitem" key={k.key}>{body}</span>
            )
          })}
        {markerLabel && (
          <span className="series-chart-key" role="listitem">
            <span className="series-chart-swatch series-imputed" aria-hidden="true" />
            {markerLabel} ({changed.length.toLocaleString()})
          </span>
        )}
        {zoomed && (
          <button
            type="button"
            className="btn btn-sm btn-outline-secondary"
            onClick={() => { setRange(null); setHoverX(null) }}
          >
            Reset zoom ({visible.length.toLocaleString()} of {points.length.toLocaleString()})
          </button>
        )}
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
        aria-label={[
          `${label}: ${primaryLabel}, ${visible.length} points`,
          ...visibleOverlays.map(o => `${o.label}, ${o.points.length} points`),
          markerLabel ? `${changed.length} ${markerLabel.toLowerCase()}` : '',
          zoomed ? 'zoomed' : '',
        ].filter(Boolean).join('; ')}
        onMouseMove={onMove}
        onMouseDown={onDown}
        onMouseUp={onUp}
        // Back to the resting point rather than blank: the readout keeps its
        // place in the layout, so leaving the plot never reflows the page. A
        // drag that leaves the plot is abandoned rather than committed blind.
        onMouseLeave={() => { setHoverX(null); setDrag(null) }}
        onDoubleClick={() => { setRange(null); setHoverX(null) }}
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

        {drag && (
          <rect
            className="series-chart-selection"
            x={Math.min(sx(drag.from), sx(drag.to))}
            width={Math.abs(sx(drag.to) - sx(drag.from))}
            y={PAD.top}
            height={plotH}
          />
        )}

        <text className="series-chart-tick" x={PAD.left} y={H - 8}>{formatX(visible[0].x)}</text>
        <text className="series-chart-tick" x={W - PAD.right} y={H - 8} textAnchor="end">{formatX(last.x)}</text>

        {/* The area reads as "this one signal"; with several series it would
            hide whichever sits behind it, so it is drawn only when alone. */}
        {overlays.length === 0 && primaryShown && (
          <path className="series-chart-area" d={`${path} L${sx(last.x)},${PAD.top + plotH} L${sx(visible[0].x)},${PAD.top + plotH} Z`} fill={`url(#${gradientId})`} />
        )}
        {shownOverlays.map(o => (
          <path key={o.key} className={`series-chart-line series-alt-${o.tone}-stroke`} d={line(o.points)} />
        ))}
        {primaryShown && <path className="series-chart-line series-observed-stroke" d={path} />}

        {/* Changed-point markers: >=8px, each with a 2px surface ring so overlaps
            stay readable rather than merging into a blob. */}
        {changed.map((p, i) => (
          <circle
            key={`${p.x}-${i}`}
            className="series-chart-marker series-imputed-fill"
            cx={sx(p.x)} cy={sy(p.y)} r={4}
          />
        ))}

        {/* Selective direct labels: the last point of each series, never every point. */}
        {last && (
          <text className="series-chart-direct series-observed-text" x={sx(last.x) - 6} y={sy(last.y) - 10} textAnchor="end">
            {niceNumber(last.y)}
          </text>
        )}
        {markerLabel && lastChanged && (
          <text className="series-chart-direct series-imputed-text" x={sx(lastChanged.x) - 6} y={sy(lastChanged.y) + 18} textAnchor="end">
            {markerLabel.toLowerCase()}
          </text>
        )}

        {active && (
        <g className={pinned ? 'series-chart-cursor series-chart-cursor-resting' : 'series-chart-cursor'}>
          <line className="series-chart-crosshair" x1={sx(active.x)} x2={sx(active.x)} y1={PAD.top} y2={PAD.top + plotH} />
          <circle
            className={`series-chart-marker ${active.changed ? 'series-imputed-fill' : 'series-observed-fill'}`}
            cx={sx(active.x)} cy={sy(active.y)} r={5}
          />
        </g>
        )}
      </svg>

      {/* Always rendered; it enhances but never gates a value — the table below
          carries all of them. */}
      {active && (
      <p className="series-chart-readout" aria-live="polite">
        <span className="series-chart-readout-x">{formatX(active.x)}</span>
        <span className="series-chart-readout-y">{niceNumber(active.y)}</span>
        {active.changed && <span className="series-chart-readout-tag">{markerLabel}</span>}
        {pinned && <span className="series-chart-readout-hint">last point — hover to inspect</span>}
      </p>
      )}

      {showTable && (
        <div className="table-responsive series-chart-table">
          <table className="table table-sm table-hover align-middle mb-0">
            <thead>
              <tr><th scope="col">Position</th><th scope="col">{label}</th><th scope="col">Source</th></tr>
            </thead>
            <tbody>
              {(primaryShown ? visible : []).map((p, i) => (
                <tr key={`primary-${p.x}-${i}`}>
                  <td>{formatX(p.x)}</td>
                  <td>{niceNumber(p.y)}</td>
                  <td>{markerLabel && p.changed ? markerLabel : primaryLabel}</td>
                </tr>
              ))}
              {shownOverlays.flatMap(o => o.points.map((p, i) => (
                <tr key={`${o.key}-${p.x}-${i}`}>
                  <td>{formatX(p.x)}</td>
                  <td>{niceNumber(p.y)}</td>
                  <td>{o.label}</td>
                </tr>
              )))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
