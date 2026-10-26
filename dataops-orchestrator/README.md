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

## Authentication of the DataOps endpoints

`/dags` (including the runs and their results), `/tasks`, `/datasets` and `/services` take a Keycloak bearer token
(the DataOps UI sends one on every call) of a member of the `dataops-operators` group, the group the UI shows its
operations pages to. The token needs the `groups` claim, like the testbed endpoints. `/stats`, `/health` and the
public `/stats/portal` stay open; `/testbeds` and `/monitor` have their own checks (below).

A call without a valid operator token is refused, and the refusal is easy to trace. It is logged as
`auth refused (<status>): <method> <path> by <user> (client <client>) from <host>: <reason>` (a warning; an error when
the cause is on our side, such as Keycloak being unreachable; never the token), and the response says the same reason:

| Status | Reason in the response |
|---|---|
| 401 | `Missing bearer token`; `Token expired`; `Token issuer '<iss>' is not the expected '<KEYCLOAK_ISSUER>'`; `The token's signature cannot be verified, no Keycloak key matches it`; `Invalid token: …`. Carries `WWW-Authenticate: Bearer`. |
| 403 | `'<user>' is not a member of the 'dataops-operators' group (the token, issued to client '<client>', has groups: […])`, or `The token … has no 'groups' claim`: a Group Membership mapper is missing on that client in Keycloak. |
| 503 | `KEYCLOAK_ISSUER is not configured` (also logged once at startup), or `Could not fetch Keycloak's signing keys from <url>`: the orchestrator cannot reach Keycloak. |

`POST /dags/dali_dataspace_validate_dataset/trigger` also accepts the s3-asset-monitor's `X-Monitor-Token` (the same
`MONITOR_API_TOKEN` it reads `/monitor/buckets` with, set on the monitor as `MONITOR_BUCKETS_TOKEN`), for that DAG
only. The token opens nothing else.

| Variable | Default | Description |
|---|---|---|
| `DATAOPS_OPERATOR_GROUP` | `dataops-operators` | The Keycloak group (path or bare name) whose members may use the DataOps endpoints |
| `MONITOR_TRIGGER_DAG_ID` | `dali_dataspace_validate_dataset` | The one DAG the monitor token may trigger |

### Open items

- **Audience.** The token's issuer and the operator group are checked, but not its audience or authorized party
  (`aud` / `azp`): a token the realm issued to another application, for a user in the group, is accepted. Check them
  against the clients that may call (`dataops-ui`, the monitor's).
- **Service account for the monitor.** The monitor authenticates its trigger with a shared token (`X-Monitor-Token`).
  Replace it with a Keycloak service account (client-credentials grant, as it already does for piveau) with a role of
  its own, and drop the header.
- **CORS** is still `*` (`main.py`). The UI reaches the API through its same-origin `/api` proxy, so it can be limited
  to the known origins.

## Testbed registry (`/testbeds`)

Endpoints (Keycloak bearer token) that register a testbed, provision its bucket, scoped Data Lake key
and piveau catalogue, and generate its connector bundle. They answer 503 until the variables below are set.

Access: members of the `testbed-admin` realm role manage every testbed and the registry (register,
provision, rotate keys, deregister). Each testbed also has its own Keycloak group, `/testbeds/<slug>`;
its members see only that testbed (detail, assets, audit, bundle) and can find assets, negotiate and start
transfers for it, but cannot use the registry-wide actions. The token needs a `groups` claim (a Group
Membership mapper on the `dataops-ui` client).

Requests: any signed-in user can ask for a testbed (`POST /testbeds/requests`: slug, name, organisation,
connector choice, catalogue visibility, a note on what it is). It is stored as a testbed in status
`requested` (up to 3 waiting per user), reserving its slug and identity; nothing is provisioned. The requester
sees it, and only it, until an administrator decides: `POST /testbeds/<slug>/approve` adds the requester (by their
Keycloak user id) to the testbed's Keycloak group, so they own it, and runs provisioning;
`POST /testbeds/<slug>/reject` keeps it as `rejected` with the reason for the requester to read.
`DELETE /testbeds/<slug>/request` withdraws a request that was not approved, freeing the slug.

Logo: a testbed's catalogue logo (`foaf:logo` in piveau) can be an uploaded file. `POST /testbeds/<slug>/logo`
(multipart, field `file`; the testbed's owner or an administrator) takes a PNG, JPEG, WebP or GIF of up to 512 KB,
judged by the file's own bytes (SVG is refused: opened directly it can run scripts). The orchestrator keeps the image
in its database and serves it, to anyone and without a token, at `GET /testbeds/<slug>/logo` (with `ETag`, `nosniff`
and a restrictive `Content-Security-Policy`). The address `<ORCHESTRATOR_PUBLIC_URL>/testbeds/<slug>/logo?v=<hash>`
becomes the testbed's `logo_url` and is written to its catalogue; the hash changes with each upload, so a cached image
never hides a new one. `DELETE /testbeds/<slug>/logo` removes it. Setting a logo URL by `PATCH` (administrators) replaces
an uploaded logo. If piveau refuses the update the logo stays saved, and re-running provisioning applies it.

| Variable | Default | Description |
|---|---|---|
| `ORCHESTRATOR_PUBLIC_URL` | (unset) | Where the API is reached from outside, e.g. `https://dataops.6gdali.eu/api`: the base of an uploaded logo's public address. Unset or not an absolute http(s) URL: uploading a logo answers 503 (424 in the registry's mapping). The compose file defaults it to the DataOps UI's address + `/api` |
| `TESTBED_SECRET_KEY` | (required) | Master secret: encrypts stored data-lake secrets, derives bundle passwords. Keep it stable. |
| `KEYCLOAK_ISSUER` | (required) | e.g. `https://auth.dspace.sparkworks.net/auth/realms/dspace` |
| `MONITOR_API_TOKEN` | (unset) | Token the s3-asset-monitor sends (`X-Monitor-Token`) to read `GET /monitor/buckets`, the Data Lake bucket of every provisioned testbed, so it watches them without `MONITOR_BUCKETS` being edited. Unset: the endpoint answers 503 |
| `TESTBED_ADMIN_ROLE` | `testbed-admin` | Realm role that manages every testbed and the registry |
| `KEYCLOAK_ADMIN_CLIENT_ID` / `_SECRET` | (unset) | Service-account client that creates a testbed's group when it is provisioned. Unset: that step is skipped and the group is made by hand |
| `FACILITY_DSP_URL` | `$CENTRAL_CONNECTOR_URL/protocol` | The facility connector's DSP address: the provider for testbeds hosted by it (registered with `hosted: true`: an upload bucket `<bucket>-upload` and key instead of their own connector) |
| `FACILITY_PARTICIPANT_ID` | `provider-facility` | The facility connector's `edc.participant.id` |
| `TESTBED_GROUP_PREFIX` | `testbeds` | A testbed's owners are the members of the Keycloak group `/<prefix>/<slug>` (empty prefix: top-level group named by slug) |
| `TESTBED_DB_PATH` | `/srv/data/testbeds.db` | SQLite registry; must be on a persistent volume |
| `DATASPACE_S3_ADMIN_ACCESS_KEY` / `_SECRET_KEY` | `DATASPACE_S3_*` | MinIO admin key (create buckets, users, policies) |
| `TESTBED_BUCKET_PREFIX` | `6g-dali-` | Default bucket / catalogue / experiment prefix is `<prefix><slug>` |
| `TESTBED_DOMAIN_SUFFIX` | `6gdali.eu` | Default DSP URL is `https://edc.<slug>.<suffix>/protocol` |
| `CENTRAL_CONNECTOR_URL` | `https://edc.dataspace.6gdali.eu` | Written into generated connector properties |
| `DATALAKE_PUBLIC_ENDPOINT_URL` | `DATASPACE_S3_ENDPOINT_URL` | Data lake address written into a transfer's destination. Used by the *testbed's* data plane, so it must be reachable from the testbeds |

Tests: `pip install pytest && python -m pytest tests`.
