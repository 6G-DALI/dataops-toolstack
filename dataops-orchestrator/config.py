import os
from dotenv import load_dotenv

load_dotenv()

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")]

MOCK = os.getenv("MOCK", "false").lower() == "true"

# Folder where generated DAG Python files will be written
AIRFLOW_DAGS_FOLDER = os.getenv("AIRFLOW_DAGS_FOLDER", "/srv/data/dags")

# Folder where user-defined task Python files will be stored
AIRFLOW_TASKS_FOLDER = os.getenv("AIRFLOW_TASKS_FOLDER", "/srv/data/tasks")

# --- Data Lake (Data Space MinIO/S3) — where newly submitted dataset files
# are uploaded. Distinct from any DataOps-internal S3; matches the endpoint
# Airflow's "dali-dataspace" connection points at (see dali_dataspace_validate_dataset).
DATASPACE_S3_ENDPOINT_URL = os.getenv("DATASPACE_S3_ENDPOINT_URL", "")
DATASPACE_S3_ACCESS_KEY   = os.getenv("DATASPACE_S3_ACCESS_KEY", "")
DATASPACE_S3_SECRET_KEY   = os.getenv("DATASPACE_S3_SECRET_KEY", "")
DATASPACE_S3_REGION       = os.getenv("DATASPACE_S3_REGION", "us-east-1")

# --- DataOps store: the bucket the validate DAG writes every run's outputs to (reports and
# CSVs, keys under "runs/"). It is read through the same Airflow connection ("dali-dataops")
# the DAG writes with. Endpoint and credentials default to the Data Lake's when unset, for a
# deployment where both buckets live on one S3 server.
DATAOPS_BUCKET            = os.getenv("DATAOPS_BUCKET", "6g-dali-dataops")
DATAOPS_S3_ENDPOINT_URL   = os.getenv("DATAOPS_S3_ENDPOINT_URL", "") or DATASPACE_S3_ENDPOINT_URL
DATAOPS_S3_ACCESS_KEY     = os.getenv("DATAOPS_S3_ACCESS_KEY", "") or DATASPACE_S3_ACCESS_KEY
DATAOPS_S3_SECRET_KEY     = os.getenv("DATAOPS_S3_SECRET_KEY", "") or DATASPACE_S3_SECRET_KEY
DATAOPS_S3_REGION         = os.getenv("DATAOPS_S3_REGION", "") or DATASPACE_S3_REGION

# DAG that validates a newly submitted dataset (SHACL-equivalent + Great
# Expectations checks), triggered automatically after a submission lands
# in the Staging Catalogue.
VALIDATION_DAG_ID = os.getenv("VALIDATION_DAG_ID", "dali_dataspace_validate_dataset")

# Fixed staging catalogue (and matching Data Lake bucket) for datasets
# contributed through POST /datasets (+ POST /datasets/{dataset_id}/distributions).
# Not user-selectable — every contribution lands here, keyed by a
# server-generated dataset_id.
CONTRIBUTED_DATASETS_CATALOGUE = os.getenv("CONTRIBUTED_DATASETS_CATALOGUE", "6g-external")

# --- RabbitMQ (optional) — lets an external system trigger a DAG by
# publishing a message instead of calling POST /dags/{dag_id}/trigger
# directly. The consumer is disabled unless RABBITMQ_URL is set.
RABBITMQ_URL   = os.getenv("RABBITMQ_URL", "")  # e.g. amqp://user:pass@host:5672/vhost
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "dataops.dag-triggers")

# --- EDC (Eclipse Dataspace Connector) provider — registers each newly
# submitted distribution as an EDC asset (see edc_client.py), so it becomes
# discoverable/negotiable the same way dali.datalake.download_dataset_edc
# already consumes assets from *other* providers. This is our own provider
# connector's Management API (distinct from EDC_PROVIDER_* in
# airflow/plugins/dali/utils.py, which points at whichever provider a
# consumer DAG run happens to be pulling from). Registration is skipped
# (with a log message, not an error) when left unset.
EDC_PROVIDER_MANAGEMENT_URL = os.getenv("EDC_PROVIDER_MANAGEMENT_URL", "")
# API key for that Management API, sent as X-Api-Key. Empty = no header.
EDC_API_KEY = os.getenv("EDC_API_KEY", "")

# --- Testbed registry (routers/testbeds.py) -----------------------------------
# Postgres connection string for the orchestrator's own database, e.g.
# postgresql://orchestrator:secret@orchestrator-db:5432/orchestrator. Tables are
# created on first use.
DATABASE_URL = os.getenv("DATABASE_URL", "")
# Fallback when DATABASE_URL is unset (local development, tests): a SQLite file.
# Not for production - it is lost with the container unless on a mounted volume.
TESTBED_DB_PATH = os.getenv("TESTBED_DB_PATH", "/srv/data/testbeds.db")
# Master secret. Per-testbed data-lake secrets are stored encrypted with a key
# derived from it, and bundle passwords are derived from it, so regenerating a
# bundle gives identical values. Required: the testbed endpoints refuse to run
# without it. Generate with: python -c "import secrets;print(secrets.token_urlsafe(48))"
TESTBED_SECRET_KEY = os.getenv("TESTBED_SECRET_KEY", "")
# Defaults used to derive a new testbed's identity from its slug.
TESTBED_BUCKET_PREFIX = os.getenv("TESTBED_BUCKET_PREFIX", "6g-dali-")
TESTBED_DOMAIN_SUFFIX = os.getenv("TESTBED_DOMAIN_SUFFIX", "6gdali.eu")
# Public URL of the central connector, written into each generated connector
# properties as edc.dali.connector.url.
CENTRAL_CONNECTOR_URL = os.getenv("CENTRAL_CONNECTOR_URL", "https://edc.dataspace.6gdali.eu")
# Data lake admin credentials: able to create buckets, users and policies.
# MinIO only (admin API, see minio_admin.py). Falls back to the
# regular DATASPACE_S3_* key, which works only if that key is an admin key.
DATASPACE_S3_ADMIN_ACCESS_KEY = os.getenv("DATASPACE_S3_ADMIN_ACCESS_KEY", "") or DATASPACE_S3_ACCESS_KEY
DATASPACE_S3_ADMIN_SECRET_KEY = os.getenv("DATASPACE_S3_ADMIN_SECRET_KEY", "") or DATASPACE_S3_SECRET_KEY

# --- Keycloak token validation for the testbed endpoints -------------------
# e.g. https://auth.dspace.sparkworks.net/auth/realms/dspace. Unset = the
# testbed endpoints answer 503 (they hand out credentials, so never open).
KEYCLOAK_ISSUER = os.getenv("KEYCLOAK_ISSUER", "")
TESTBED_ADMIN_ROLE = os.getenv("TESTBED_ADMIN_ROLE", "testbed-admin")
# Owners of a testbed are members of the Keycloak group <prefix>/<slug> (the token needs a "groups" claim:
# a Group Membership mapper on the dataops-ui client). Empty prefix = top-level groups named by slug.
TESTBED_GROUP_PREFIX = os.getenv("TESTBED_GROUP_PREFIX", "testbeds")
# Optional: a confidential client with a service account in the same realm, allowed to manage groups. With it,
# provisioning a testbed also creates its group; without it that step is skipped.
KEYCLOAK_ADMIN_CLIENT_ID = os.getenv("KEYCLOAK_ADMIN_CLIENT_ID", "")
KEYCLOAK_ADMIN_CLIENT_SECRET = os.getenv("KEYCLOAK_ADMIN_CLIENT_SECRET", "")

# --- Starting transfers from the registry (routers/testbeds.py) -----------------
# Data lake endpoint written into a transfer's destination. It is used by the *testbed connector's*
# data plane, so it must be reachable from the testbeds (public/tailnet address), which can differ
# from the address the orchestrator itself uses. Defaults to DATASPACE_S3_ENDPOINT_URL.
DATALAKE_PUBLIC_ENDPOINT_URL = os.getenv("DATALAKE_PUBLIC_ENDPOINT_URL", "") or DATASPACE_S3_ENDPOINT_URL
