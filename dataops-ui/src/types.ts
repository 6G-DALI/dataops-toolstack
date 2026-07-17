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
  | 'dataset-creator'
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
  sns_project_name?: string
  catalog_id?: string
  catalog_title?: string
  catalog_url?: string
  dataset_id?: string
  distribution_count?: number
  updated_at?: string | null
  extra?: {
    formats?: string[]
    publisher?: string
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

export interface Distribution {
  id: string
  name?: string
  distribution_id?: string
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

export interface DistributionsResponse {
  distributions: Distribution[]
}

export interface Catalogue {
  id: string
  title: string
}

export interface CataloguesResponse {
  catalogues: Catalogue[]
}

// ── Dataset submission (Create Dataset page) ────────────────────────────────
// Mirrors dataops-orchestrator/dataset_models.py exactly (snake_case field
// names, since the JSON is sent as-is to the backend Pydantic model).
export interface DatasetIdentityInput {
  title: string
  description: string
  sns_project_name: string
  publisher_name: string
  contact_email: string
  contributors: string[]
  keywords: string[]
  related_publications: string[]
  language: string
  spatial: string
  temporal_start: string
  temporal_end: string
  version: string
}

export interface DatasetObjectInput {
  license: string
  access_rights: 'PUBLIC' | 'RESTRICTED' | 'NON_PUBLIC'
  gdpr_compliant: boolean
  fair_compliant: boolean
  contains_pii: boolean
  produced_by: string
}

// Includes the CMT Content-C fields (observation points, measurement
// family/tools) — these describe the dataset as a whole and are submitted
// at dataset-creation time. The per-file column list and technique
// (variable_measured / measurement_technique) are NOT here — see
// DistributionMetricsInput, submitted per distribution instead (MAP §5.3.E).
export interface TestbedContextInput {
  underlay_platform: string
  environment: string
  network_domain: string
  ran_3gpp_release: string
  ran_new_radio_type: string
  ran_split: string
  ran_focused_technology: string
  ran_coverage_type: string
  ran_frequency_band: string
  ran_bandwidth_mhz: string
  ran_max_end_devices: string
  ran_mobility_model: string
  core_release: string
  core_solution: string
  transport_type: string
  compute_orchestrator_type: string
  compute_gpu_use: boolean
  compute_virtualization_type: string
  compute_infrastructure_type: string
  traffic_origin: string
  traffic_pattern: string
  slice_type: string
  reference_plane: string
  related_vertical: string
  observation_point_horizontal: string
  observation_point_vertical: string
  measurement_family: string[]
  measurement_tool: string[]
}

// Describes one distribution's file — submitted with that distribution via
// POST /datasets/{dataset_id}/distributions, not with the dataset itself.
export interface DistributionMetricsInput {
  variable_measured: string[]
  measurement_technique: string
}

// dataset_id and catalogue_id are NOT sent by the client — dataset_id is a
// server-generated UUID, and catalogue_id is the fixed contributed-datasets
// catalogue, both assigned by the orchestrator (see routers/datasets.py).
// Step 1 of submission: POST /datasets.
export interface DatasetCreateRequest {
  identity: DatasetIdentityInput
  object: DatasetObjectInput
  testbed_context: TestbedContextInput
}

// A single Great Expectations config, forwarded as-is to the validation DAG
// (see dali_dataspace_validate_dataset's `expectations` param).
export interface GreatExpectation {
  type: string
  column?: string
  min_value?: number
  max_value?: number
  [key: string]: unknown
}

export interface DatasetCreateResponse {
  dataset_id: string
  catalogue_id: string
  piveau: { dataset_id: string; dataset_uri: string; status: string; piveau_url: string }
}

// Result of registering a distribution as an EDC asset on our own provider
// connector (see dataops-orchestrator/edc_client.py) — best-effort, so
// "skipped" (EDC not configured) and "failed" are expected outcomes, not
// errors thrown back at the caller.
export interface EdcRegistrationResult {
  status: 'registered' | 'already_registered' | 'skipped' | 'failed'
  asset_id?: string
  reason?: string
  error?: string
}

// Step 2 of submission: POST /datasets/{dataset_id}/distributions.
export interface DistributionSubmitResponse {
  dataset_id: string
  catalogue_id: string
  distribution_id: string
  object_key: string
  distribution_url: string | null
  piveau: { dataset_id: string; distribution_id: string; distribution_uri: string; status: string; piveau_url: string }
  validation_run: DagRun
  edc: EdcRegistrationResult
}

// Result of DELETE /datasets/{dataset_id} or
// DELETE /datasets/{dataset_id}/distributions/{asset_id} (see
// dataops-orchestrator/routers/datasets.py) — one entry per cleaned-up
// system; edc is a single result for a distribution delete, an array (one
// per distribution) for a whole-dataset delete.
export interface DeleteResult {
  dataset_id: string
  catalogue_id: string
  asset_id?: string
  distribution_count?: number
  piveau: { status: string; [key: string]: unknown }
  edc: EdcRegistrationResult | EdcRegistrationResult[]
  s3_deleted_keys: string[]
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
