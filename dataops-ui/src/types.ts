// Shared domain and API models for the DataOps UI.

// ── Navigation ───────────────────────────────────────────────────────────────
export type View =
  | 'home'
  | 'dags'
  | 'runs'
  | 'tasks'
  | 'logs'
  | 'dag-tasks'
  | 'all-tasks'
  | 'dag-builder'
  | 'task-creator'
  | 'datasets'
  | 'services'

export interface NavParams {
  dagId?: string
  runId?: string
  taskId?: string
  tryNumber?: number
}

export type NavigateFn = (view: View, params?: NavParams) => void

// ── DAGs ─────────────────────────────────────────────────────────────────────
export interface Dag {
  dag_id: string
  owners?: string[]
  is_paused: boolean
}

export interface DagsResponse {
  dags: Dag[]
}

export interface DagParam {
  value?: unknown
  description?: string
  [key: string]: unknown
}

export interface DagDetails {
  params?: Record<string, DagParam>
  [key: string]: unknown
}

// ── DAG runs ─────────────────────────────────────────────────────────────────
export interface DagRun {
  dag_run_id: string
  dag_id?: string
  run_type?: string
  execution_date?: string | null
  start_date?: string | null
  end_date?: string | null
  state?: string | null
  conf?: Record<string, unknown> | null
}

export interface DagRunsResponse {
  dag_runs: DagRun[]
  total_entries?: number
}

// ── Task instances (runtime) ─────────────────────────────────────────────────
export interface TaskInstance {
  task_id: string
  state?: string | null
  start_date?: string | null
  end_date?: string | null
  duration?: number | null
  try_number?: number
}

export interface TaskInstancesResponse {
  task_instances: TaskInstance[]
}

// ── DAG-defined tasks ────────────────────────────────────────────────────────
export interface DagTask {
  task_id: string
  task_type?: string
  owner?: string
  depends_on_past?: boolean
}

export interface DagTasksResponse {
  tasks: DagTask[]
}

// A row in the "All Tasks" / builder library: DAG tasks plus a `dag_id` origin
// (custom tasks use the sentinel dag_id `"custom"`).
export interface AllTask {
  task_id: string
  dag_id: string
  task_type?: string
  owner?: string
  depends_on_past?: boolean
  locked?: boolean
}

export interface AllTasksResponse {
  tasks: AllTask[]
}

// ── Custom (user-authored) tasks ─────────────────────────────────────────────
export interface CustomTask {
  task_id: string
  code?: string
  description?: string
}

export interface CustomTasksResponse {
  tasks: CustomTask[]
}

// ── Home / stats ─────────────────────────────────────────────────────────────
export interface RecentRun {
  dag_id: string
  dag_run_id: string
  state?: string
  start_date?: string | null
}

export interface Stats {
  dags?: { total?: number; active?: number; paused?: number }
  tasks?: { custom?: number }
  recent_runs?: RecentRun[]
}

// ── Datasets ─────────────────────────────────────────────────────────────────
export interface DagRef {
  dag_id: string
}

export interface Dataset {
  id: string
  name?: string
  asset_id?: string
  asset_title?: string
  sns_project_name?: string
  catalog_id?: string
  catalog_title?: string
  catalog_url?: string
  dataset_id?: string
  input_key?: string
  updated_at?: string | null
  extra?: {
    format?: string
    license?: string
    [key: string]: unknown
  }
  variable_measured?: string[]
  producing_dags?: DagRef[]
  consuming_dags?: DagRef[]
  raw?: unknown
}

export interface DatasetsResponse {
  datasets: Dataset[]
}

// ── Services ─────────────────────────────────────────────────────────────────
export interface Service {
  service_id: string
  title: string
  description?: string
  service_type?: string
  framework?: string
  input_format?: string
  output_format?: string
  module?: string
  function?: string
}

export interface ServicesResponse {
  services: Service[]
}

export interface RegisterResult {
  service_id: string
  status: string
  detail?: string
}

export interface RegisterAllResponse {
  results: RegisterResult[]
}

// ── Request bodies ───────────────────────────────────────────────────────────
export interface CreateDagBody {
  dag_id: string
  description?: string
  schedule?: string
  task_ids: string[]
  owner?: string
}

export interface CreateTaskBody {
  task_id: string
  description?: string
  code: string
}

export type TriggerConf = Record<string, unknown>

// ── Task logs ────────────────────────────────────────────────────────────────
// One raw entry as returned by the Airflow structured-log API.
export interface LogContentEntry {
  event?: string
  timestamp?: string
  time?: string
  level?: string
  logger?: string
  filename?: string
  lineno?: number
  message?: string
  msg?: string
}

export interface TaskLogResponse {
  content?: LogContentEntry[]
  continuation_token?: string
  log?: string
}

// A parsed, display-ready log line.
export interface LogEntry {
  timestamp: string | null
  level: string | null
  message: string
  logger: string | null
  source: string | null
  isGroup: boolean
  raw: boolean
}
