import type {
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
