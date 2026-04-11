"""
Mock Airflow client — returns static sample data.
Activated when MOCK=true in .env (or environment).
"""

from datetime import datetime, timezone

_NOW = "2026-03-30T10:00:00+00:00"
_DAGS = [
    {
        "dag_id": "csv_pipeline",
        "description": "Download, process and store a CSV dataset",
        "is_paused": False,
        "is_active": True,
        "owners": ["dataops"],
        "schedule_interval": None,
        "tags": [{"name": "dali"}, {"name": "dataops"}],
        "timetable_description": None,
    },
    {
        "dag_id": "data_quality_check",
        "description": "Run data quality validations on ingested datasets",
        "is_paused": False,
        "is_active": True,
        "owners": ["dataops"],
        "schedule_interval": None,
        "tags": [{"name": "quality"}],
        "timetable_description": None,
    },
    {
        "dag_id": "model_training_pipeline",
        "description": "Retrain ML model on updated data",
        "is_paused": True,
        "is_active": True,
        "owners": ["ml-team"],
        "schedule_interval": None,
        "tags": [{"name": "ml"}],
        "timetable_description": None,
    },
]

_RUNS = {
    "csv_pipeline": [
        {
            "dag_run_id": "scheduled__2026-03-30T00:00:00+00:00",
            "dag_id": "csv_pipeline",
            "run_type": "scheduled",
            "execution_date": "2026-03-30T00:00:00+00:00",
            "start_date": "2026-03-30T00:00:05+00:00",
            "end_date": "2026-03-30T00:01:12+00:00",
            "state": "success",
        },
        {
            "dag_run_id": "scheduled__2026-03-29T00:00:00+00:00",
            "dag_id": "csv_pipeline",
            "run_type": "scheduled",
            "execution_date": "2026-03-29T00:00:00+00:00",
            "start_date": "2026-03-29T00:00:04+00:00",
            "end_date": "2026-03-29T00:01:08+00:00",
            "state": "success",
        },
        {
            "dag_run_id": "manual__2026-03-28T15:30:00+00:00",
            "dag_id": "csv_pipeline",
            "run_type": "manual",
            "execution_date": "2026-03-28T15:30:00+00:00",
            "start_date": "2026-03-28T15:30:02+00:00",
            "end_date": "2026-03-28T15:30:45+00:00",
            "state": "failed",
        },
    ],
    "data_quality_check": [
        {
            "dag_run_id": "scheduled__2026-03-30T09:00:00+00:00",
            "dag_id": "data_quality_check",
            "run_type": "scheduled",
            "execution_date": "2026-03-30T09:00:00+00:00",
            "start_date": "2026-03-30T09:00:03+00:00",
            "end_date": None,
            "state": "running",
        },
        {
            "dag_run_id": "scheduled__2026-03-30T08:00:00+00:00",
            "dag_id": "data_quality_check",
            "run_type": "scheduled",
            "execution_date": "2026-03-30T08:00:00+00:00",
            "start_date": "2026-03-30T08:00:02+00:00",
            "end_date": "2026-03-30T08:00:58+00:00",
            "state": "success",
        },
    ],
    "model_training_pipeline": [
        {
            "dag_run_id": "scheduled__2026-03-24T00:00:00+00:00",
            "dag_id": "model_training_pipeline",
            "run_type": "scheduled",
            "execution_date": "2026-03-24T00:00:00+00:00",
            "start_date": "2026-03-24T00:00:10+00:00",
            "end_date": "2026-03-24T00:45:00+00:00",
            "state": "success",
        },
    ],
}

_TASK_INSTANCES = {
    "csv_pipeline": {
        "scheduled__2026-03-30T00:00:00+00:00": [
            {"task_id": "download_dataset",  "state": "success", "start_date": "2026-03-30T00:00:05+00:00", "end_date": "2026-03-30T00:00:22+00:00", "duration": 17.4,  "try_number": 1},
            {"task_id": "process_dataset",   "state": "success", "start_date": "2026-03-30T00:00:23+00:00", "end_date": "2026-03-30T00:00:55+00:00", "duration": 32.1,  "try_number": 1},
            {"task_id": "store_dataset",     "state": "success", "start_date": "2026-03-30T00:00:56+00:00", "end_date": "2026-03-30T00:01:12+00:00", "duration": 16.0,  "try_number": 1},
        ],
        "scheduled__2026-03-29T00:00:00+00:00": [
            {"task_id": "download_dataset",  "state": "success", "start_date": "2026-03-29T00:00:04+00:00", "end_date": "2026-03-29T00:00:19+00:00", "duration": 15.2,  "try_number": 1},
            {"task_id": "process_dataset",   "state": "success", "start_date": "2026-03-29T00:00:20+00:00", "end_date": "2026-03-29T00:00:51+00:00", "duration": 31.0,  "try_number": 1},
            {"task_id": "store_dataset",     "state": "success", "start_date": "2026-03-29T00:00:52+00:00", "end_date": "2026-03-29T00:01:08+00:00", "duration": 16.5,  "try_number": 1},
        ],
        "manual__2026-03-28T15:30:00+00:00": [
            {"task_id": "download_dataset",  "state": "success", "start_date": "2026-03-28T15:30:02+00:00", "end_date": "2026-03-28T15:30:19+00:00", "duration": 17.0,  "try_number": 1},
            {"task_id": "process_dataset",   "state": "failed",  "start_date": "2026-03-28T15:30:20+00:00", "end_date": "2026-03-28T15:30:45+00:00", "duration": 25.3,  "try_number": 2},
            {"task_id": "store_dataset",     "state": "skipped", "start_date": None,                        "end_date": None,                        "duration": None,  "try_number": 0},
        ],
    },
    "data_quality_check": {
        "scheduled__2026-03-30T09:00:00+00:00": [
            {"task_id": "run_null_checks",   "state": "success", "start_date": "2026-03-30T09:00:03+00:00", "end_date": "2026-03-30T09:00:20+00:00", "duration": 17.0,  "try_number": 1},
            {"task_id": "run_range_checks",  "state": "running", "start_date": "2026-03-30T09:00:21+00:00", "end_date": None,                        "duration": None,  "try_number": 1},
            {"task_id": "generate_report",   "state": "queued",  "start_date": None,                        "end_date": None,                        "duration": None,  "try_number": 0},
        ],
    },
}

_TASKS = {
    "csv_pipeline": [
        {"task_id": "download_dataset", "task_type": "PythonOperator", "owner": "dataops", "depends_on_past": False},
        {"task_id": "process_dataset",  "task_type": "PythonOperator", "owner": "dataops", "depends_on_past": False},
        {"task_id": "store_dataset",    "task_type": "PythonOperator", "owner": "dataops", "depends_on_past": False},
    ],
    "data_quality_check": [
        {"task_id": "run_null_checks",  "task_type": "PythonOperator", "owner": "dataops", "depends_on_past": False},
        {"task_id": "run_range_checks", "task_type": "PythonOperator", "owner": "dataops", "depends_on_past": False},
        {"task_id": "generate_report",  "task_type": "PythonOperator", "owner": "dataops", "depends_on_past": False},
    ],
    "model_training_pipeline": [
        {"task_id": "prepare_features", "task_type": "PythonOperator", "owner": "ml-team", "depends_on_past": False},
        {"task_id": "train_model",      "task_type": "PythonOperator", "owner": "ml-team", "depends_on_past": True},
        {"task_id": "evaluate_model",   "task_type": "PythonOperator", "owner": "ml-team", "depends_on_past": False},
        {"task_id": "publish_model",    "task_type": "PythonOperator", "owner": "ml-team", "depends_on_past": False},
    ],
}

_SAMPLE_LOG = """\
[2026-03-30 00:00:05,123] {{taskinstance.py:1035}} INFO - Dependencies all met for <TaskInstance: csv_pipeline.download_dataset scheduled__2026-03-30T00:00:00+00:00 [queued]>
[2026-03-30 00:00:05,201] {{taskinstance.py:1256}} INFO - Starting attempt 1 of 1
[2026-03-30 00:00:05,310] {{taskinstance.py:1276}} INFO - Executing <Task(PythonOperator): download_dataset>
[2026-03-30 00:00:05,400] {{python.py:151}} INFO - Done. Returned value was: None
[2026-03-30 00:00:22,001] {{logging_mixin.py:115}} INFO - Downloaded dataset to /tmp/dali_raw_dataset.csv (48231 bytes)
[2026-03-30 00:00:22,100] {{taskinstance.py:1490}} INFO - Marking task as SUCCESS.
"""

# ── State mutation (in-memory, resets on restart) ─────────────────────────────
_paused = {dag["dag_id"]: dag["is_paused"] for dag in _DAGS}
_triggered_runs: list = []


# ── Public API (mirrors airflow_client.py) ────────────────────────────────────

async def list_dags(limit: int = 100) -> dict:
    dags = [
        {**d, "is_paused": _paused.get(d["dag_id"], d["is_paused"])}
        for d in _DAGS[:limit]
    ]
    return {"dags": dags, "total_entries": len(dags)}


async def get_dag(dag_id: str) -> dict:
    dag = next((d for d in _DAGS if d["dag_id"] == dag_id), None)
    if not dag:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")
    return {**dag, "is_paused": _paused.get(dag_id, dag["is_paused"])}


async def set_dag_paused(dag_id: str, is_paused: bool) -> dict:
    _paused[dag_id] = is_paused
    return await get_dag(dag_id)


async def delete_dag(dag_id: str) -> dict:
    import os
    from config import AIRFLOW_DAGS_FOLDER
    global _DAGS
    _DAGS = [d for d in _DAGS if d["dag_id"] != dag_id]
    _paused.pop(dag_id, None)
    path = os.path.join(AIRFLOW_DAGS_FOLDER, f"{dag_id}.py")
    if os.path.exists(path):
        os.remove(path)
    return {}


async def trigger_dag(dag_id: str, conf: dict = None) -> dict:
    run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')}"
    run = {
        "dag_run_id": run_id,
        "dag_id": dag_id,
        "run_type": "manual",
        "execution_date": datetime.now(timezone.utc).isoformat(),
        "start_date": datetime.now(timezone.utc).isoformat(),
        "end_date": None,
        "state": "queued",
        "conf": conf or {},
    }
    _triggered_runs.append(run)
    return run


async def list_dag_runs(dag_id: str, limit: int = 25) -> dict:
    runs = _RUNS.get(dag_id, [])
    extra = [r for r in _triggered_runs if r["dag_id"] == dag_id]
    all_runs = (extra + runs)[:limit]
    return {"dag_runs": all_runs, "total_entries": len(all_runs)}


async def get_dag_run(dag_id: str, run_id: str) -> dict:
    runs = _RUNS.get(dag_id, []) + _triggered_runs
    run = next((r for r in runs if r["dag_run_id"] == run_id), None)
    if not run:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return run


async def list_task_instances(dag_id: str, run_id: str) -> dict:
    tasks = _TASK_INSTANCES.get(dag_id, {}).get(run_id, [])
    return {"task_instances": tasks, "total_entries": len(tasks)}


async def get_task_instance(dag_id: str, run_id: str, task_id: str) -> dict:
    tasks = _TASK_INSTANCES.get(dag_id, {}).get(run_id, [])
    task = next((t for t in tasks if t["task_id"] == task_id), None)
    if not task:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    return task


async def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    return _SAMPLE_LOG.replace("download_dataset", task_id).replace(
        "csv_pipeline", dag_id
    )


async def list_tasks(dag_id: str) -> dict:
    tasks = _TASKS.get(dag_id, [])
    return {"tasks": tasks, "total_entries": len(tasks)}


async def get_task(dag_id: str, task_id: str) -> dict:
    tasks = _TASKS.get(dag_id, [])
    task = next((t for t in tasks if t["task_id"] == task_id), None)
    if not task:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    return task


async def list_all_tasks() -> dict:
    all_tasks = [
        {**task, "dag_id": dag_id}
        for dag_id, tasks in _TASKS.items()
        for task in tasks
    ]
    # also include any tasks from dynamically created DAGs
    for dag in _created_dags:
        for task_id in dag["task_ids"]:
            all_tasks.append({
                "task_id": task_id,
                "task_type": "PythonOperator",
                "owner": "dataops",
                "depends_on_past": False,
                "dag_id": dag["dag_id"],
            })
    return {"tasks": all_tasks, "total_entries": len(all_tasks)}


_created_dags: list = []


async def list_datasets() -> dict:
    from piveau_client import fetch_datasets
    datasets = await fetch_datasets()
    return {"datasets": datasets, "total_entries": len(datasets)}


async def get_dataset(dataset_id) -> dict:
    from piveau_client import fetch_datasets
    datasets = await fetch_datasets()
    ds = next((d for d in datasets if str(d["id"]) == str(dataset_id)), None)
    if not ds:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return ds


_custom_tasks: list = []


async def create_task(task_id: str, description: str, code: str) -> dict:
    existing = next((t for t in _custom_tasks if t["task_id"] == task_id), None)
    if existing:
        existing["description"] = description
        existing["code"] = code
    else:
        _custom_tasks.append({"task_id": task_id, "description": description, "code": code, "file": f"(mock) {task_id}.py"})
    return {"task_id": task_id, "description": description, "file": f"(mock) {task_id}.py", "status": "created"}


async def get_custom_task(task_id: str) -> dict:
    task = next((t for t in _custom_tasks if t["task_id"] == task_id), None)
    if not task:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    return task


async def list_custom_tasks() -> dict:
    return {"tasks": _custom_tasks, "total_entries": len(_custom_tasks)}


async def create_dag(dag_id: str, description: str, schedule: str, task_ids: list, owner: str = "airflow") -> dict:
    entry = {
        "dag_id": dag_id,
        "description": description,
        "is_paused": False,
        "is_active": True,
        "owners": [owner],
        "schedule_interval": {"value": schedule},
        "tags": [],
        "timetable_description": schedule,
        "task_ids": task_ids,
    }
    _DAGS.append(entry)
    _paused[dag_id] = False
    _created_dags.append(entry)
    return {"dag_id": dag_id, "file": f"(mock) {dag_id}.py", "tasks": task_ids, "status": "created"}
