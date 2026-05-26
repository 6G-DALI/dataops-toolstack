# DataOps — Local Airflow Stack

Local development deployment of Apache Airflow (CeleryExecutor) bundled with the DataOps Orchestrator.

## Stack

| Service | Port | Description |
|---|---|---|
| `airflow-apiserver` | 8080 | Airflow REST API + UI |
| `airflow-scheduler` | — | DAG scheduling |
| `airflow-dag-processor` | — | DAG file parsing |
| `airflow-worker` | — | Celery task execution |
| `airflow-triggerer` | — | Deferred task triggering |
| `dataops-orchestrator` | 8000 | DataOps REST API (FastAPI) |
| `postgres` | — | Airflow metadata database |
| `redis` | 6379 | Celery broker |

## Prerequisites

- Docker with Compose v2 (`docker compose version`)
- At least **4 GB RAM** and **2 CPUs** allocated to Docker
- At least **10 GB** free disk space

## Setup

### 1. Create your `.env` file

```bash
cp .env.example .env
```

Edit `.env` and set `AIRFLOW_UID` to your local user ID:

```bash
# Linux / macOS
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Initialise Airflow

Run once to create the database schema and admin user:

```bash
docker compose up airflow-init
```

Wait for `airflow-init` to exit with code 0.

### 3. Start the stack

```bash
docker compose up -d
```

### 4. Verify

| URL | Credentials |
|---|---|
| Airflow UI: http://localhost:8080 | `airflow` / `airflow` (or as set in `.env`) |
| DataOps Orchestrator: http://localhost:8000/docs | — |
| Orchestrator health: http://localhost:8000/health | — |

## Stopping

```bash
docker compose down
```

To also remove the database volume (full reset):

```bash
docker compose down -v
```

## Directory layout

```
airflow/
  docker-compose.yaml   — full stack definition
  .env.example          — environment variable template
  .env                  — your local config (gitignored)
  dags/                 — DAG files (volume-mounted into Airflow and Orchestrator)
    tasks/              — task Python files written by the Orchestrator
  config/               — Airflow config (airflow.cfg generated on first run)
  plugins/              — custom Airflow plugins
  logs/                 — runtime logs (gitignored)
```

DAG and task files are written here at runtime by the DataOps Orchestrator. The directories are tracked in git; the generated `.py` files are not.

## Optional: Flower (Celery monitor)

```bash
docker compose --profile flower up -d
```

Available at http://localhost:5555.

## Optional: Airflow CLI

```bash
docker compose --profile debug run --rm airflow-cli <command>
# e.g.
docker compose --profile debug run --rm airflow-cli dags list
```
