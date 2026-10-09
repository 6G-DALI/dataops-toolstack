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

## Testbed registry (`/testbeds`)

Endpoints (Keycloak bearer token) that register a testbed, provision its bucket, scoped Data Lake key
and piveau catalogue, and generate its connector bundle. They answer 503 until the variables below are set.

Access: members of the `testbed-admin` realm role manage every testbed and the registry (register,
provision, rotate keys, deregister). Each testbed also has its own Keycloak group, `/testbeds/<slug>`;
its members see only that testbed (detail, assets, audit, bundle) and can find assets, negotiate and start
transfers for it, but cannot use the registry-wide actions. The token needs a `groups` claim (a Group
Membership mapper on the `dataops-ui` client).

| Variable | Default | Description |
|---|---|---|
| `TESTBED_SECRET_KEY` | (required) | Master secret: encrypts stored data-lake secrets, derives bundle passwords. Keep it stable. |
| `KEYCLOAK_ISSUER` | (required) | e.g. `https://auth.dspace.sparkworks.net/auth/realms/dspace` |
| `TESTBED_ADMIN_ROLE` | `testbed-admin` | Realm role that manages every testbed and the registry |
| `KEYCLOAK_ADMIN_CLIENT_ID` / `_SECRET` | (unset) | Service-account client that creates a testbed's group when it is provisioned. Unset: that step is skipped and the group is made by hand |
| `TESTBED_GROUP_PREFIX` | `testbeds` | A testbed's owners are the members of the Keycloak group `/<prefix>/<slug>` (empty prefix: top-level group named by slug) |
| `TESTBED_DB_PATH` | `/srv/data/testbeds.db` | SQLite registry; must be on a persistent volume |
| `DATASPACE_S3_ADMIN_ACCESS_KEY` / `_SECRET_KEY` | `DATASPACE_S3_*` | MinIO admin key (create buckets, users, policies) |
| `TESTBED_BUCKET_PREFIX` | `6g-dali-` | Default bucket / catalogue / experiment prefix is `<prefix><slug>` |
| `TESTBED_DOMAIN_SUFFIX` | `6gdali.eu` | Default DSP URL is `https://edc.<slug>.<suffix>/protocol` |
| `CENTRAL_CONNECTOR_URL` | `https://edc.dataspace.6gdali.eu` | Written into generated connector properties |
| `DATALAKE_PUBLIC_ENDPOINT_URL` | `DATASPACE_S3_ENDPOINT_URL` | Data lake address written into a transfer's destination. Used by the *testbed's* data plane, so it must be reachable from the testbeds |

Tests: `pip install pytest && python -m pytest tests`.
