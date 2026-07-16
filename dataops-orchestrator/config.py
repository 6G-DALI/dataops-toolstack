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
