import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { getDagRun } from '../api/airflow'
import Modal from './Modal'
import type { DagRun, DistributionSubmitResponse, NavigateFn } from '../types'
import '../styles/SubmissionProgressModal.css'
import { catalogueDatasetUrl } from '../config'

export const VALIDATION_DAG_ID = 'dali_dataspace_validate_dataset'

export type StepState = 'pending' | 'running' | 'done' | 'failed'

/** What the submission has achieved so far. Owned by DatasetCreator, which
 *  advances it as each call resolves, so the modal is a pure view of it. */
export interface SubmissionState {
  dataset: StepState
  distribution: StepState
  validation: StepState
  datasetId?: string
  catalogueId?: string
  result?: DistributionSubmitResponse
  /** Message from whichever step failed; the failed step carries the detail. */
  error?: string
}

export const initialSubmission: SubmissionState = {
  dataset: 'pending',
  distribution: 'pending',
  validation: 'pending',
}

// Airflow run states that mean the pipeline has stopped, so polling can too.
const TERMINAL_RUN_STATES = new Set(['success', 'failed', 'upstream_failed', 'skipped'])
const POLL_INTERVAL_MS = 4000

function StepIcon({ state }: { state: StepState }) {
  if (state === 'running') {
    return <span className="spinner-border spinner-border-sm text-primary" role="status" aria-label="in progress" />
  }
  if (state === 'done') return <span className="submit-step-icon done" aria-label="done">✓</span>
  if (state === 'failed') return <span className="submit-step-icon failed" aria-label="failed">✕</span>
  return <span className="submit-step-icon pending" aria-label="waiting" />
}

interface StepProps {
  state: StepState
  title: string
  children?: ReactNode
  last?: boolean
}

function Step({ state, title, children, last }: StepProps) {
  return (
    <li className={`submit-step submit-step-${state}${last ? ' last' : ''}`}>
      <div className="submit-step-marker">
        <StepIcon state={state} />
      </div>
      <div className="submit-step-body">
        <div className="submit-step-title">{title}</div>
        {children && <div className="submit-step-detail small">{children}</div>}
      </div>
    </li>
  )
}

interface SubmissionProgressModalProps {
  submission: SubmissionState
  /** True while a request is still in flight, so the modal can refuse to close. */
  busy: boolean
  onClose: () => void
  onRetry: () => void
  onSubmitAnother: () => void
  onNavigate: NavigateFn
}

export default function SubmissionProgressModal({
  submission, busy, onClose, onRetry, onSubmitAnother, onNavigate,
}: SubmissionProgressModalProps) {
  const { dataset, distribution, validation, datasetId, result, error } = submission
  const runId = result?.validation_run?.dag_run_id
  const [run, setRun] = useState<DagRun | null>(result?.validation_run ?? null)

  // Poll the validation run until it stops. The DAG is triggered by the
  // distribution call, so it is still queued when that response arrives —
  // without polling the modal would only ever show "triggered".
  useEffect(() => {
    if (!runId) return
    let cancelled = false
    let timer: number | undefined

    async function poll() {
      try {
        const next = await getDagRun(VALIDATION_DAG_ID, runId as string)
        if (cancelled) return
        setRun(next)
        if (next.state && TERMINAL_RUN_STATES.has(next.state)) return
      } catch {
        // A transient failure to read the run must not derail the modal — the
        // submission itself already succeeded. Keep polling.
        if (cancelled) return
      }
      timer = window.setTimeout(poll, POLL_INTERVAL_MS)
    }

    timer = window.setTimeout(poll, 1200)
    return () => {
      cancelled = true
      if (timer !== undefined) window.clearTimeout(timer)
    }
  }, [runId])

  // The validation step's state is derived from the run, not from the caller:
  // the caller only knows the DAG was triggered.
  const runState = run?.state ?? undefined
  const validationState: StepState =
    validation === 'pending' || validation === 'failed' ? validation
    : runState === 'success' ? 'done'
    : runState && TERMINAL_RUN_STATES.has(runState) ? 'failed'
    : 'running'

  const catalogueUrl = datasetId ? catalogueDatasetUrl(datasetId) : null
  const finished = distribution === 'done'
  const failedStep = dataset === 'failed' ? 'dataset' : distribution === 'failed' ? 'distribution' : null

  const title =
    failedStep ? 'Submission incomplete'
    : !finished ? 'Submitting dataset…'
    : validationState === 'running' ? 'Submitted — validating'
    : validationState === 'failed' ? 'Submitted — validation failed'
    : 'Dataset submitted'

  return (
    <Modal
      title={title}
      onClose={busy ? () => {} : onClose}
      width={680}
      footer={
        <>
          {failedStep && (
            <button type="button" className="btn btn-primary" onClick={onRetry} disabled={busy}>
              Retry
            </button>
          )}
          {finished && (
            <>
              {runId && (
                <button
                  type="button"
                  className="btn btn-outline-secondary"
                  onClick={() => onNavigate('tasks', { dagId: VALIDATION_DAG_ID, runId })}
                >
                  Validation run
                </button>
              )}
              <button type="button" className="btn btn-outline-secondary" onClick={onSubmitAnother}>
                Submit another
              </button>
              <button type="button" className="btn btn-primary" onClick={() => onNavigate('datasets', {})}>
                Done
              </button>
            </>
          )}
          {!finished && !failedStep && (
            <button type="button" className="btn btn-outline-secondary" disabled>
              Please wait…
            </button>
          )}
          {failedStep && (
            <button type="button" className="btn btn-outline-secondary" onClick={onClose} disabled={busy}>
              Back to form
            </button>
          )}
        </>
      }
    >
      <ol className="submit-steps">
        <Step state={dataset} title="Register dataset metadata">
          {dataset === 'done' && datasetId ? (
            <>
              <code>{datasetId}</code>
              {catalogueUrl && (
                <>
                  {' · '}
                  <a href={catalogueUrl} target="_blank" rel="noopener noreferrer">
                    View in catalogue
                  </a>
                </>
              )}
            </>
          ) : dataset === 'failed' ? (
            <span className="text-danger">{error}</span>
          ) : dataset === 'running' ? (
            'Publishing the MAP record to the Staging Catalogue…'
          ) : null}
        </Step>

        <Step state={distribution} title="Upload file and append distribution">
          {distribution === 'done' && result ? (
            <>
              Uploaded to <code>{result.object_key}</code>
              {result.edc.status !== 'skipped' && (
                <>
                  {' · EDC: '}
                  <span className={result.edc.status === 'failed' ? 'text-danger' : ''}>
                    {result.edc.status.replace('_', ' ')}
                  </span>
                </>
              )}
            </>
          ) : distribution === 'failed' ? (
            <span className="text-danger">{error}</span>
          ) : distribution === 'running' ? (
            'Uploading to the Data Lake, registering the distribution…'
          ) : null}
        </Step>

        <Step state={validationState} title="Run data quality validation" last>
          {runId ? (
            <>
              <span className="text-capitalize">{runState ?? 'queued'}</span>
              {' · '}
              <a
                href="#"
                onClick={e => { e.preventDefault(); onNavigate('tasks', { dagId: VALIDATION_DAG_ID, runId }) }}
              >
                {runId}
              </a>
              {validationState === 'running' && (
                <div className="text-muted">
                  Great Expectations checks run in Airflow; results are published back to the
                  catalogue when they finish.
                </div>
              )}
            </>
          ) : validation === 'pending' ? (
            'Triggered once the distribution is registered.'
          ) : null}
        </Step>
      </ol>

      {failedStep === 'distribution' && (
        <div className="alert alert-warning small mb-0" role="alert">
          The dataset is registered but has no file. Retrying reuses it rather than registering a
          second one — or close this and correct the Data step first.
        </div>
      )}
    </Modal>
  )
}
