from config import MOCK

if MOCK:
    from mock_client import (  # noqa: F401
        list_dags, get_dag, set_dag_paused, trigger_dag,
        list_dag_runs, get_dag_run,
        list_task_instances, get_task_instance, get_task_logs,
        list_tasks, get_task,
        list_all_tasks, create_dag,
        list_datasets, get_dataset,
    )
else:
    import os
    import textwrap
    import httpx
    from fastapi import HTTPException
    from config import AIRFLOW_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD, AIRFLOW_DAGS_FOLDER

    _BASE = f"{AIRFLOW_URL}/api/v1"
    _AUTH = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    _HEADERS = {"Content-Type": "application/json"}

    def _client() -> httpx.AsyncClient:
        return httpx.AsyncClient(auth=_AUTH, headers=_HEADERS, timeout=30)

    async def _get(path: str, params: dict = None):
        async with _client() as client:
            r = await client.get(f"{_BASE}{path}", params=params)
        _raise(r)
        return r.json()

    async def _post(path: str, body: dict = None):
        async with _client() as client:
            r = await client.post(f"{_BASE}{path}", json=body or {})
        _raise(r)
        return r.json()

    async def _patch(path: str, body: dict):
        async with _client() as client:
            r = await client.patch(f"{_BASE}{path}", json=body)
        _raise(r)
        return r.json()

    async def _get_text(path: str):
        async with _client() as client:
            r = await client.get(f"{_BASE}{path}")
        _raise(r)
        return r.text

    def _raise(r: httpx.Response):
        if r.is_error:
            raise HTTPException(status_code=r.status_code, detail=r.text)

    # ── DAGs ──────────────────────────────────────────────────────────────────

    async def list_dags(limit: int = 100) -> dict:
        return await _get("/dags", params={"limit": limit, "order_by": "dag_id"})

    async def get_dag(dag_id: str) -> dict:
        return await _get(f"/dags/{dag_id}")

    async def set_dag_paused(dag_id: str, is_paused: bool) -> dict:
        return await _patch(f"/dags/{dag_id}", {"is_paused": is_paused})

    async def trigger_dag(dag_id: str, conf: dict = None) -> dict:
        return await _post(f"/dags/{dag_id}/dagRuns", {"conf": conf or {}})

    # ── DAG Runs ──────────────────────────────────────────────────────────────

    async def list_dag_runs(dag_id: str, limit: int = 25) -> dict:
        return await _get(
            f"/dags/{dag_id}/dagRuns",
            params={"order_by": "-execution_date", "limit": limit},
        )

    async def get_dag_run(dag_id: str, run_id: str) -> dict:
        return await _get(f"/dags/{dag_id}/dagRuns/{run_id}")

    # ── Task Instances ────────────────────────────────────────────────────────

    async def list_task_instances(dag_id: str, run_id: str) -> dict:
        return await _get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")

    async def get_task_instance(dag_id: str, run_id: str, task_id: str) -> dict:
        return await _get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}")

    async def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        return await _get_text(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        )

    # ── Tasks (structure) ─────────────────────────────────────────────────────

    async def list_tasks(dag_id: str) -> dict:
        return await _get(f"/dags/{dag_id}/tasks")

    async def get_task(dag_id: str, task_id: str) -> dict:
        return await _get(f"/dags/{dag_id}/tasks/{task_id}")

    # ── Datasets ──────────────────────────────────────────────────────────────

    async def list_datasets() -> dict:
        from piveau_client import fetch_datasets
        datasets = await fetch_datasets()
        return {"datasets": datasets, "total_entries": len(datasets)}

    async def get_dataset(dataset_id) -> dict:
        from piveau_client import fetch_datasets
        datasets = await fetch_datasets()
        ds = next((d for d in datasets if str(d.get("id")) == str(dataset_id)), None)
        if not ds:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
        return ds

    async def list_all_tasks() -> dict:
        dags_data = await list_dags(limit=100)
        all_tasks = []
        for dag in dags_data.get("dags", []):
            tasks_data = await list_tasks(dag["dag_id"])
            for task in tasks_data.get("tasks", []):
                all_tasks.append({**task, "dag_id": dag["dag_id"]})
        return {"tasks": all_tasks, "total_entries": len(all_tasks)}

    # ── DAG Creation ──────────────────────────────────────────────────────────

    async def create_dag(dag_id: str, description: str, schedule: str, task_ids: list) -> dict:
        os.makedirs(AIRFLOW_DAGS_FOLDER, exist_ok=True)
        path = os.path.join(AIRFLOW_DAGS_FOLDER, f"{dag_id}.py")
        code = _generate_dag_code(dag_id, description, schedule, task_ids)
        with open(path, "w") as f:
            f.write(code)
        return {"dag_id": dag_id, "file": path, "tasks": task_ids, "status": "created"}

    def _generate_dag_code(dag_id: str, description: str, schedule: str, task_ids: list) -> str:
        task_defs = "\n\n".join(
            f'    def {t}(**context):\n        pass  # TODO: implement {t}'
            for t in task_ids
        )
        task_ops = "\n".join(
            f'    t_{t} = PythonOperator(task_id="{t}", python_callable={t})'
            for t in task_ids
        )
        chain = " >> ".join(f"t_{t}" for t in task_ids)
        return textwrap.dedent(f'''\
            from airflow import DAG
            from airflow.operators.python import PythonOperator
            from datetime import datetime

            {task_defs.replace(chr(10), chr(10))}

            with DAG(
                dag_id="{dag_id}",
                description="""{description}""",
                schedule_interval=None,
                start_date=datetime(2024, 1, 1),
                catchup=False,
            ) as dag:

            {task_ops}

                {chain}
            ''')
