import { FiCheck, FiX, FiMinus, FiCircle } from 'react-icons/fi'
import type { Testbed } from '../types'
import '../styles/TestbedTimeline.css'

type StageState = 'done' | 'failed' | 'current' | 'upcoming' | 'unavailable'

interface Stage {
  key: string
  label: string
  state: StageState
  meta: string
}

function shortTime(iso: string | null | undefined): string {
  return iso ? iso.replace('T', ' ').replace('+00:00', '').slice(0, 16) : ''
}

/**
 * The testbed's lifecycle as a left-to-right timeline.
 *
 * The provisioning stages come from the registry's recorded steps, and the lifecycle
 * status (draft / adopted / provisioned) is shown as the first and "Provisioned" stages.
 * "Connector connected" and "Transfer active" are part of the lifecycle but the orchestrator does not track them yet (that needs the central connector
 * integration), so they are drawn as unavailable rather than guessed.
 */
function buildStages(tb: Testbed): Stage[] {
  const adopted = tb.status === 'adopted'
  const provisioning: Stage[] = [
    ['bucket', 'Data Lake bucket'],
    ['credentials', 'Data Lake key'],
    ['catalogue', 'Catalogue'],
  ].map(([key, label]) => {
    const step = tb.steps[key]
    if (step?.status === 'ok') return { key, label, state: 'done' as StageState, meta: shortTime(step.at) }
    if (step?.status === 'failed') return { key, label, state: 'failed' as StageState, meta: String(step.detail).slice(0, 80) }
    if (adopted) {
      // Already running before it was registered here: its bucket and catalogue exist, but
      // it has no bucket-scoped key from us until provisioning is run.
      return key === 'credentials'
        ? { key, label, state: 'upcoming' as StageState, meta: 'no scoped key yet' }
        : { key, label, state: 'done' as StageState, meta: 'existing' }
    }
    return { key, label, state: 'upcoming' as StageState, meta: step ? step.detail : 'not run' }
  })

  const stages: Stage[] = [
    {
      key: 'registered', label: adopted ? 'Adopted' : 'Registered', state: 'done',
      meta: shortTime(tb.created_at),
    },
    ...provisioning,
    {
      key: 'provisioned', label: 'Provisioned',
      state: tb.status === 'provisioned' ? 'done' : 'upcoming',
      meta: tb.status === 'provisioned' ? shortTime(tb.updated_at) : 'run provisioning',
    },
    { key: 'connected', label: 'Connector connected', state: 'unavailable', meta: 'not tracked yet' },
    { key: 'transfer', label: 'Transfer active', state: 'unavailable', meta: 'not tracked yet' },
  ]

  // The first stage that is neither done nor failed is where the testbed currently is.
  const next = stages.findIndex(s => s.state === 'upcoming')
  if (next >= 0) stages[next] = { ...stages[next], state: 'current' }
  return stages
}

const ICONS: Record<StageState, JSX.Element> = {
  done: <FiCheck />,
  failed: <FiX />,
  current: <FiCircle />,
  upcoming: <FiCircle />,
  unavailable: <FiMinus />,
}

export default function TestbedTimeline({ testbed }: { testbed: Testbed }) {
  const stages = buildStages(testbed)
  return (
    <div className="card mb-3">
      <div className="card-body">
        <ol className="tb-timeline" aria-label="Testbed lifecycle">
          {stages.map(s => (
            <li key={s.key} className={`tb-timeline__stage tb-timeline__stage--${s.state}`}
              aria-current={s.state === 'current' ? 'step' : undefined} title={s.meta}>
              <span className="tb-timeline__dot">{ICONS[s.state]}</span>
              <div className="tb-timeline__label">{s.label}</div>
              <div className="tb-timeline__meta">{s.meta}</div>
            </li>
          ))}
        </ol>
      </div>
    </div>
  )
}
