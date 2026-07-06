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
  DatasetsResponse,
  RegisterAllResponse,
  ServicesResponse,
  Stats,
  TaskInstancesResponse,
  TaskLogResponse,
  TriggerConf,
} from '../types'
import keycloak from '../auth/keycloak'

const BASE_URL = import.meta.env.VITE_ORCHESTRATOR_URL

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
  const response = await fetch(`${BASE_URL}${path}`, {
    ...init,
    headers: {
      ...headers,
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

export function getDatasets(): Promise<DatasetsResponse> {
  return request<DatasetsResponse>('/datasets')
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
