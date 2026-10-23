import { lifecycleLevels, STAGE_ICONS, type StageState } from './TestbedTimeline'
import { TestbedStatusBadge } from './TestbedList'
import type { Testbed } from '../types'
import '../styles/TestbedStatusIcons.css'

const STATE_WORDS: Record<StageState, string> = {
  done: 'done', failed: 'failed', current: 'next', upcoming: 'not yet', unavailable: 'not available',
}

/**
 * Where a testbed is in its nine-level lifecycle: registered, bucket, key, catalogue, provisioned, connector, asset,
 * contract, transfer. Green is reached, red failed, outlined is the next one. Hover an icon for what it means and when
 * it happened. A request that has not been approved has not started the lifecycle: it shows its decision instead.
 */
export default function TestbedStatusIcons({ testbed }: { testbed: Testbed }) {
  if (testbed.status === 'requested' || testbed.status === 'rejected') {
    return <TestbedStatusBadge status={testbed.status} />
  }
  const levels = lifecycleLevels(testbed, testbed.assets ?? [])
  return (
    <span className="tb-levels" role="list" aria-label={`Lifecycle of ${testbed.name}: ${testbed.status}`}>
      {levels.map(level => {
        const Icon = STAGE_ICONS[level.key]
        const text = `${level.label}: ${STATE_WORDS[level.state]}${level.meta ? ` (${level.meta})` : ''}`
        return (
          <span key={level.key} role="listitem" className={`tb-level tb-level--${level.state}`} title={text}
            aria-label={text}>
            <Icon aria-hidden="true" />
          </span>
        )
      })}
    </span>
  )
}
