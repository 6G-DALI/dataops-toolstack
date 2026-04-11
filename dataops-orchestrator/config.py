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
