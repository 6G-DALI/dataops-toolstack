import '../styles/Badge.css'

const KNOWN_STATES = [
  'success', 'failed', 'running', 'queued', 'paused',
  'active', 'skipped', 'up_for_retry',
]

export default function StateBadge({ state }) {
  const s = (state || '').toLowerCase()
  const cls = KNOWN_STATES.includes(s) ? `badge-${s}` : 'badge-default'
  return <span className={`badge ${cls}`}>{state || '—'}</span>
}
