import type {
  RunArtifacts,
  AllTasksResponse,
  CreateDagBody,
  CreateTaskBody,
  CustomTask,
  CustomTasksResponse,
  Dag,
  DagDetails,
  DagRun,
  DagRunsResponse,
  DagsResponse,
  DagTasksResponse,
  DatasetCreateRequest,
  DatasetCreateResponse,
  DeleteResult,
  DistributionMetricsInput,
  DistributionSubmitResponse,
  DatasetsResponse,
  DistributionsResponse,
  CataloguesResponse,
  GreatExpectation,
  RegisterAllResponse,
  ServicesResponse,
  Stats,
  TaskInstancesResponse,
  TaskLogResponse,
  TriggerConf,
} from '../types'
import keycloak from '../auth/keycloak'
import { config } from '../config'

const BASE_URL = config.orchestratorUrl

const headers: Record<string, string> = { 'Content-Type': 'application/json' }

interface RequestOptions extends RequestInit {
  rawText?: boolean
}

/** Refresh the access token when close to expiry and return the current bearer header. */
async function authHeader(): Promise<Record<string, string>> {
  if (!keycloak.authenticated) return {}
  try {
    await keycloak.updateToken(30)
  } catch {
    // Silent refresh failed (e.g. the SSO session ended) — the request may 401
    // below and surface as an error; the user can re-login from the UI.
  }
  return keycloak.token ? { Authorization: `Bearer ${keycloak.token}` } : {}
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { rawText, headers: optionHeaders, ...init } = options
  // FormData bodies must not carry a manual Content-Type — the browser sets
  // its own `multipart/form-data; boundary=...` header, which a fixed
  // 'application/json' default would otherwise clobber.
  const isFormData = init.body instanceof FormData
  const response = await fetch(`${BASE_URL}${path}`, {
    ...init,
    headers: {
      ...(isFormData ? {} : headers),
      ...(await authHeader()),
      ...(optionHeaders as Record<string, string> | undefined),
    },
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`${response.status} ${response.statusText}: ${text}`)
  }
  if (rawText) return response.text() as Promise<T>
  return response.json() as Promise<T>
}

export function getStats(): Promise<Stats> {
  return request<Stats>('/stats')
}

export function getDags(): Promise<DagsResponse> {
  return request<DagsResponse>('/dags')
}

export function getDag(dagId: string): Promise<Dag> {
  return request<Dag>(`/dags/${encodeURIComponent(dagId)}`)
}

export function getDagDetails(dagId: string): Promise<DagDetails> {
  return request<DagDetails>(`/dags/${encodeURIComponent(dagId)}/details`)
}

export function deleteDag(dagId: string): Promise<unknown> {
  return request<unknown>(`/dags/${encodeURIComponent(dagId)}`, { method: 'DELETE' })
}

export function patchDag(dagId: string, isPaused: boolean): Promise<unknown> {
  return request<unknown>(`/dags/${encodeURIComponent(dagId)}/pause?is_paused=${isPaused}`, {
    method: 'PATCH',
  })
}

export function triggerDag(dagId: string, conf: TriggerConf = {}): Promise<DagRun> {
  return request<DagRun>(`/dags/${encodeURIComponent(dagId)}/trigger`, {
    method: 'POST',
    body: JSON.stringify(conf),
  })
}

export function getDagRuns(dagId: string, limit = 10, offset = 0): Promise<DagRunsResponse> {
  return request<DagRunsResponse>(`/dags/${encodeURIComponent(dagId)}/runs?limit=${limit}&offset=${offset}`)
}

export function getDagRun(dagId: string, runId: string): Promise<DagRun> {
  return request<DagRun>(`/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}`)
}

export function getTaskInstances(dagId: string, runId: string): Promise<TaskInstancesResponse> {
  return request<TaskInstancesResponse>(`/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`)
}

export function getDagTasks(dagId: string): Promise<DagTasksResponse> {
  return request<DagTasksResponse>(`/dags/${encodeURIComponent(dagId)}/tasks`)
}

export function getAllTasks(): Promise<AllTasksResponse> {
  return request<AllTasksResponse>('/tasks')
}

export function createDag(body: CreateDagBody): Promise<unknown> {
  return request<unknown>('/dags', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function getDatasets(catalogueId?: string): Promise<DatasetsResponse> {
  const qs = catalogueId ? `?catalogue_id=${encodeURIComponent(catalogueId)}` : ''
  return request<DatasetsResponse>(`/datasets${qs}`)
}

export function getCatalogues(): Promise<CataloguesResponse> {
  return request<CataloguesResponse>('/datasets/catalogues')
}

export function getDistributions(datasetId: string, catalogueId?: string): Promise<DistributionsResponse> {
  const qs = catalogueId ? `?catalogue_id=${encodeURIComponent(catalogueId)}` : ''
  return request<DistributionsResponse>(`/datasets/${encodeURIComponent(datasetId)}/distributions${qs}`)
}

/** Empty string → null; otherwise the parsed number (or null if unparseable).
 *  The form keeps these testbed fields as strings, but the backend expects
 *  Optional[float]/Optional[int] — an empty "" would 422 as unparseable. */
function numericOrNull(value: string, integer = false): number | null {
  const trimmed = value.trim()
  if (trimmed === '') return null
  const n = integer ? parseInt(trimmed, 10) : Number(trimmed)
  return Number.isFinite(n) ? n : null
}

/** Step 1: register the dataset's own metadata. No file yet. */
export function createDataset(payload: DatasetCreateRequest): Promise<DatasetCreateResponse> {
  const tc = payload.testbed_context
  const body = {
    ...payload,
    testbed_context: {
      ...tc,
      ran_bandwidth_mhz: numericOrNull(tc.ran_bandwidth_mhz),
      ran_max_end_devices: numericOrNull(tc.ran_max_end_devices, true),
    },
  }
  return request<DatasetCreateResponse>('/datasets', { method: 'POST', body: JSON.stringify(body) })
}

/** Step 1, RDF-first: submit the MAP record the UI generated itself as Turtle
 *  (see map/datasetTurtle.ts). Counterpart to createDataset above, which stays
 *  available as the JSON API for programmatic clients — the orchestrator serves
 *  both. The body carries sentinel identifiers that the orchestrator replaces
 *  with the dataset_id it mints. */
export function createDatasetRdf(turtle: string): Promise<DatasetCreateResponse> {
  return request<DatasetCreateResponse>('/datasets/rdf', {
    method: 'POST',
    body: turtle,
    headers: { 'Content-Type': 'text/turtle' },
  })
}

/** Step 2: upload a file as a new distribution of an already-created dataset. */
export function addDistribution(
  datasetId: string,
  catalogueId: string,
  file: File,
  metrics: DistributionMetricsInput,
  expectations: GreatExpectation[] = []
): Promise<DistributionSubmitResponse> {
  const body = new FormData()
  body.append('file', file)
  body.append('catalogue_id', catalogueId)
  body.append('metrics', JSON.stringify(metrics))
  body.append('expectations', JSON.stringify(expectations))
  return request<DistributionSubmitResponse>(`/datasets/${encodeURIComponent(datasetId)}/distributions`, { method: 'POST', body })
}

/** Step 2, RDF-first: upload the file together with the dcat:Distribution
 *  document describing it (see map/datasetTurtle.ts), instead of having the
 *  orchestrator compose that node from form fields. Still multipart — the file
 *  has to be uploaded either way — with the Turtle as one more field.
 *
 *  The response's `piveau.turtle` is the document as stored, with the
 *  placeholders resolved to the values assigned during the upload. */
export function addDistributionRdf(
  datasetId: string,
  catalogueId: string,
  file: File,
  turtle: string,
  metrics: DistributionMetricsInput,
  expectations: GreatExpectation[] = []
): Promise<DistributionSubmitResponse> {
  const body = new FormData()
  body.append('file', file)
  body.append('catalogue_id', catalogueId)
  body.append('turtle', turtle)
  body.append('metrics', JSON.stringify(metrics))
  body.append('expectations', JSON.stringify(expectations))
  return request<DistributionSubmitResponse>(
    `/datasets/${encodeURIComponent(datasetId)}/distributions/rdf`, { method: 'POST', body }
  )
}

/** Deletes one distribution — cleans up piveau, the EDC asset, and its S3 object(s). */
export function deleteDistribution(datasetId: string, catalogueId: string, assetId: string): Promise<DeleteResult> {
  const qs = `?catalogue_id=${encodeURIComponent(catalogueId)}`
  return request<DeleteResult>(`/datasets/${encodeURIComponent(datasetId)}/distributions/${encodeURIComponent(assetId)}${qs}`, { method: 'DELETE' })
}

/** Deletes a dataset entirely, including all of its distributions (piveau, EDC, S3). */
export function deleteDataset(datasetId: string, catalogueId: string): Promise<DeleteResult> {
  const qs = `?catalogue_id=${encodeURIComponent(catalogueId)}`
  return request<DeleteResult>(`/datasets/${encodeURIComponent(datasetId)}${qs}`, { method: 'DELETE' })
}

export function getServices(): Promise<ServicesResponse> {
  return request<ServicesResponse>('/services')
}

export function registerAllServices(): Promise<RegisterAllResponse> {
  return request<RegisterAllResponse>('/services/register', { method: 'POST' })
}

export function registerService(serviceId: string): Promise<unknown> {
  return request<unknown>(`/services/${encodeURIComponent(serviceId)}/register`, { method: 'POST' })
}

export function deregisterService(serviceId: string): Promise<unknown> {
  return request<unknown>(`/services/${encodeURIComponent(serviceId)}/register`, { method: 'DELETE' })
}

export function getCustomTasks(): Promise<CustomTasksResponse> {
  return request<CustomTasksResponse>('/tasks/custom')
}

export function getCustomTask(taskId: string): Promise<CustomTask> {
  return request<CustomTask>(`/tasks/custom/${encodeURIComponent(taskId)}`)
}

export function createTask(body: CreateTaskBody): Promise<unknown> {
  return request<unknown>('/tasks', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function getTaskLogs(dagId: string, runId: string, taskId: string, tryNumber = 1): Promise<string> {
  return request<TaskLogResponse>(
    `/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/logs/${tryNumber}`,
    { rawText: false }
  ).then(data => data.log ?? '')
}


// ── Pipeline run results ─────────────────────────────────────────────────────

export function getRunArtifacts(dagId: string, runId: string): Promise<RunArtifacts> {
  return request<RunArtifacts>(
    `/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/artifacts`,
  )
}

/**
 * Fetch one artifact as text.
 *
 * `maxBytes` is served as a byte range, so a large remediated frame is never
 * read out of storage in full; the orchestrator drops any trailing partial line
 * so the CSV always parses. The response header says whether it truncated.
 */
export async function getRunArtifactText(
  dagId: string,
  runId: string,
  name: string,
  maxBytes?: number,
  offset = 0,
): Promise<{ text: string, truncated: boolean, totalSize: number, nextOffset: number }> {
  const params = new URLSearchParams()
  if (maxBytes) params.set('max_bytes', String(maxBytes))
  if (offset) params.set('offset', String(offset))
  const query = params.toString() ? `?${params}` : ''
  const path = `/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}`
    + `/artifacts/${encodeURIComponent(name)}${query}`
  const response = await fetch(`${BASE_URL}${path}`, { headers: await authHeader() })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${await response.text()}`)
  }
  const text = await response.text()
  const next = response.headers.get('X-Artifact-Next-Offset')
  // These are custom headers, so the API has to name them in the CORS
  // expose_headers list or a browser reads them all back as null. When that
  // happens the byte length of what arrived is the honest fallback — the string
  // length is in UTF-16 units, which drifts from the server's byte offsets the
  // moment a frame holds anything outside ASCII.
  return {
    text,
    truncated: response.headers.get('X-Artifact-Truncated') === 'true',
    totalSize: Number(response.headers.get('X-Artifact-Total-Size') || 0),
    // Always a row boundary for a CSV — the server trims both partial lines —
    // so windows can be concatenated without stitching a split row together.
    nextOffset: next !== null ? Number(next) : offset + new TextEncoder().encode(text).length,
  }
}

/**
 * The whole of a CSV artifact, fetched as a series of ranged requests.
 *
 * The endpoint serves a byte window at a time so a large frame is never pulled
 * out of storage in one go; walking it here is what turns that into the
 * complete file without a paging control the reader has to operate. `capBytes`
 * is the point at which the walk gives up rather than pulling something
 * unbounded into a browser tab — it stops on a row boundary and says so.
 */
export async function getRunArtifactCsv(
  dagId: string,
  runId: string,
  name: string,
  windowBytes: number,
  capBytes: number,
): Promise<{ text: string, truncated: boolean, totalSize: number, fetched: number }> {
  let text = ''
  let offset = 0
  let totalSize = 0
  for (;;) {
    const chunk = await getRunArtifactText(dagId, runId, name, windowBytes, offset)
    console.log(
      `[artifact:${name}] bytes ${offset}-${chunk.nextOffset} of ${chunk.totalSize}`
      + ` (received ${chunk.nextOffset - offset}, truncated=${chunk.truncated})`,
    )
    text += chunk.text
    totalSize = chunk.totalSize
    // The server always trims its response to the last full CSV row, so
    // `received` is almost always a few bytes short of `windowBytes` even
    // mid-file — that can't be used as an end-of-data signal. The real
    // end-of-data signal is the byte offset catching up to the object's total
    // size; the truncation header is just a (now-working) confirmation of it.
    const reachedEnd = totalSize > 0 && chunk.nextOffset >= totalSize
    // The server guarantees forward progress; the check is a stop for the one
    // case it cannot fix — a single row longer than the whole window.
    if (chunk.nextOffset <= offset) {
      console.log(`[artifact:${name}] stopped: no forward progress at offset ${offset}`)
      return { text, truncated: true, totalSize, fetched: offset }
    }
    offset = chunk.nextOffset
    if (!chunk.truncated || reachedEnd) {
      console.log(`[artifact:${name}] done: fetched ${offset} of ${totalSize} (reachedEnd=${reachedEnd}, headerTruncated=${chunk.truncated})`)
      return { text, truncated: false, totalSize, fetched: offset }
    }
    if (offset >= capBytes) {
      console.log(`[artifact:${name}] stopped: hit capBytes (${capBytes}) at offset ${offset} of ${totalSize}`)
      return { text, truncated: true, totalSize, fetched: offset }
    }
  }
}
