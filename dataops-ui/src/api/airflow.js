const BASE_URL = import.meta.env.VITE_ORCHESTRATOR_URL

const headers = { 'Content-Type': 'application/json' }

async function request(path, options = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: { ...headers, ...options.headers },
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`${response.status} ${response.statusText}: ${text}`)
  }
  if (options.rawText) return response.text()
  return response.json()
}

export function getStats() {
  return request('/stats')
}

export function getDags() {
  return request('/dags')
}

export function deleteDag(dagId) {
  return request(`/dags/${encodeURIComponent(dagId)}`, { method: 'DELETE' })
}

export function patchDag(dagId, isPaused) {
  return request(`/dags/${encodeURIComponent(dagId)}/pause?is_paused=${isPaused}`, {
    method: 'PATCH',
  })
}

export function triggerDag(dagId, conf = {}) {
  return request(`/dags/${encodeURIComponent(dagId)}/trigger`, {
    method: 'POST',
    body: JSON.stringify(conf),
  })
}

export function getDagRuns(dagId) {
  return request(`/dags/${encodeURIComponent(dagId)}/runs`)
}

export function getTaskInstances(dagId, runId) {
  return request(`/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`)
}

export function getDagTasks(dagId) {
  return request(`/dags/${encodeURIComponent(dagId)}/tasks`)
}

export function getAllTasks() {
  return request('/tasks')
}

export function createDag(body) {
  return request('/dags', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function getDatasets() {
  return request('/datasets')
}

export function getCustomTasks() {
  return request('/tasks/custom')
}

export function getCustomTask(taskId) {
  return request(`/tasks/custom/${encodeURIComponent(taskId)}`)
}

export function createTask(body) {
  return request('/tasks', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function getTaskLogs(dagId, runId, taskId, tryNumber = 1) {
  return request(
    `/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/logs/${tryNumber}`,
    { rawText: false }
  ).then(data => data.log)
}
