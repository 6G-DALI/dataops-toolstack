const STATE_CLASS: Record<string, string> = {
  success:      'text-bg-success',
  active:       'text-bg-success',
  failed:       'text-bg-danger',
  running:      'text-bg-primary',
  queued:       'text-bg-warning',
  paused:       'text-bg-secondary',
  skipped:      'text-bg-info',
  up_for_retry: 'text-bg-warning',
}

interface StateBadgeProps {
  state?: string | null
}

export default function StateBadge({ state }: StateBadgeProps) {
  const s = (state || '').toLowerCase()
  const cls = STATE_CLASS[s] ?? 'text-bg-secondary'
  return <span className={`badge ${cls}`}>{state || '—'}</span>
}
