# DataOps Orchestrator

Middleware API between the React UI and Apache Airflow.

## Running with Docker

```bash
docker run -d \
  -p 8000:8000 \
  -e AIRFLOW_URL=http://your-airflow:8080 \
  -e AIRFLOW_USERNAME=admin \
  -e AIRFLOW_PASSWORD=admin \
  -e CORS_ORIGINS=http://localhost:3000 \
  ghcr.io/<your-org>/dataops-orchestrator:latest
```

## Environment Variables

| Variable             | Default                  | Description                                                                        |
|----------------------|--------------------------|------------------------------------------------------------------------------------|
| `AIRFLOW_URL`        | `http://localhost:8080`  | Airflow instance URL                                                               |
| `AIRFLOW_USERNAME`   | `admin`                  | Airflow username                                                                   |
| `AIRFLOW_PASSWORD`   | `admin`                  | Airflow password                                                                   |
| `CORS_ORIGINS`       | `http://localhost:3000`  | Comma-separated allowed origins                                                    |
| `PORT`               | `8000`                   | Port the server listens on                                                         |
| `HOST`               | `0.0.0.0`                | Bind address                                                                       |
| `MOCK`               | `false`                  | Use mock data instead of real Airflow                                              |
| `AIRFLOW_DAGS_FOLDER`  | `/srv/data/dags`       | Path where generated DAG files are written — mount as a volume shared with Airflow |
| `AIRFLOW_TASKS_FOLDER` | `/srv/data/tasks`      | Path where task Python files are stored — mount as a volume shared with Airflow    |

## Health check

```
GET /health
```

Returns `{"status": "ok"}` when the service is up.
