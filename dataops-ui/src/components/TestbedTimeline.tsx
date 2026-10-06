import { FiCheck, FiX, FiMinus, FiCircle } from 'react-icons/fi'
import type { Testbed, TestbedAsset } from '../types'
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
 * "Connector connected" and "Asset registered" come from "Find asset", a catalogue request
 * through the central connector; "Transfer active" comes from "Find transfer" on an asset, a
 * transfer lookup on the central connector.
 */
function buildStages(tb: Testbed, assets: TestbedAsset[]): Stage[] {
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
    connectorStage(tb),
    assetStage(assets),
    contractStage(assets),
    transferStage(assets),
  ]

  // The first stage that is neither done nor failed is where the testbed currently is.
  const next = stages.findIndex(s => s.state === 'upcoming')
  if (next >= 0) stages[next] = { ...stages[next], state: 'current' }
  return stages
}

/** Set by "Find asset": a catalogue request reaching the testbed's connector proves it is up. */
function connectorStage(tb: Testbed): Stage {
  const step = tb.steps.connector
  if (step?.status === 'ok') return { key: 'connected', label: 'Connector connected', state: 'done', meta: shortTime(step.at) }
  if (step?.status === 'failed') {
    return { key: 'connected', label: 'Connector connected', state: 'failed', meta: String(step.detail).slice(0, 80) }
  }
  return { key: 'connected', label: 'Connector connected', state: 'upcoming', meta: 'use Find asset' }
}

function assetStage(assets: TestbedAsset[]): Stage {
  const offered = assets.filter(a => a.present)
  return offered.length > 0
    ? { key: 'asset', label: 'Asset registered', state: 'done', meta: `${offered.length} offered` }
    : { key: 'asset', label: 'Asset registered', state: 'upcoming', meta: 'none found yet' }
}

/** Set by "Negotiate contract" (or found by "Find transfer"): a finalized agreement for an offered asset. */
function contractStage(assets: TestbedAsset[]): Stage {
  const agreed = assets.filter(a => a.present && a.contract_agreement_id && a.negotiation_state !== 'TERMINATED')
  if (agreed.length > 0) return { key: 'contract', label: 'Contract agreed', state: 'done', meta: `${agreed.length} agreed` }
  const negotiating = assets.find(a => a.present && a.negotiation_state && a.negotiation_state !== 'TERMINATED')
  if (negotiating) {
    return { key: 'contract', label: 'Contract agreed', state: 'upcoming', meta: `negotiation ${negotiating.negotiation_state?.toLowerCase()}` }
  }
  return { key: 'contract', label: 'Contract agreed', state: 'upcoming', meta: 'use Negotiate contract' }
}

/** Set by "Find transfer" on an asset: a STARTED transfer from the testbed to the central connector. */
function transferStage(assets: TestbedAsset[]): Stage {
  const running = assets.filter(a => a.present && a.transfer_state === 'STARTED')
  if (running.length > 0) {
    return { key: 'transfer', label: 'Transfer active', state: 'done', meta: shortTime(running[0].transfer_checked_at) }
  }
  const checked = assets.filter(a => a.transfer_checked_at)
  if (checked.length > 0) {
    const state = checked.find(a => a.transfer_state)?.transfer_state
    return { key: 'transfer', label: 'Transfer active', state: 'upcoming', meta: state ? `last state: ${state}` : 'no transfer found' }
  }
  return { key: 'transfer', label: 'Transfer active', state: 'upcoming', meta: 'use Find transfer' }
}

const ICONS: Record<StageState, JSX.Element> = {
  done: <FiCheck />,
  failed: <FiX />,
  current: <FiCircle />,
  upcoming: <FiCircle />,
  unavailable: <FiMinus />,
}

export default function TestbedTimeline({ testbed, assets }: { testbed: Testbed; assets: TestbedAsset[] }) {
  const stages = buildStages(testbed, assets)
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
