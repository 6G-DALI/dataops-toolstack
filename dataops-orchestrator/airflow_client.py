from config import MOCK

if MOCK:
    from mock_client import (  # noqa: F401
        list_dags, get_dag, set_dag_paused, delete_dag, trigger_dag,
        list_dag_runs, get_dag_run,
        list_task_instances, get_task_instance, get_task_logs,
        list_tasks, get_task,
        list_all_tasks, create_dag,
        list_datasets, get_dataset,
        create_task, list_custom_tasks, get_custom_task,
    )
else:
    import os
    import textwrap
    import httpx
    from fastapi import HTTPException
    from config import AIRFLOW_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD, AIRFLOW_DAGS_FOLDER, AIRFLOW_TASKS_FOLDER

    _BASE = f"{AIRFLOW_URL}/api/v2"
    _AUTH_URL = f"{AIRFLOW_URL}/auth/token"
    _token: str | None = None

    async def _ensure_token() -> str:
        global _token
        if _token:
            return _token
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                _AUTH_URL,
                json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
                headers={"Content-Type": "application/json"},
            )
            if r.is_error:
                raise HTTPException(status_code=r.status_code, detail=f"Airflow auth failed: {r.text}")
            _token = r.json()["access_token"]
        return _token

    def _client(token: str) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=30,
        )

    async def _get(path: str, params: dict = None):
        global _token
        token = await _ensure_token()
        async with _client(token) as client:
            r = await client.get(f"{_BASE}{path}", params=params)
        if r.status_code == 401:
            _token = None
            token = await _ensure_token()
            async with _client(token) as client:
                r = await client.get(f"{_BASE}{path}", params=params)
        _raise(r)
        return r.json()

    async def _post(path: str, body: dict = None):
        global _token
        token = await _ensure_token()
        async with _client(token) as client:
            r = await client.post(f"{_BASE}{path}", json=body or {})
        if r.status_code == 401:
            _token = None
            token = await _ensure_token()
            async with _client(token) as client:
                r = await client.post(f"{_BASE}{path}", json=body or {})
        _raise(r)
        return r.json()

    async def _patch(path: str, body: dict):
        global _token
        token = await _ensure_token()
        async with _client(token) as client:
            r = await client.patch(f"{_BASE}{path}", json=body)
        if r.status_code == 401:
            _token = None
            token = await _ensure_token()
            async with _client(token) as client:
                r = await client.patch(f"{_BASE}{path}", json=body)
        _raise(r)
        return r.json()

    async def _get_text(path: str):
        global _token
        token = await _ensure_token()
        async with _client(token) as client:
            r = await client.get(f"{_BASE}{path}")
        if r.status_code == 401:
            _token = None
            token = await _ensure_token()
            async with _client(token) as client:
                r = await client.get(f"{_BASE}{path}")
        _raise(r)
        return r.text

    def _raise(r: httpx.Response):
        if r.is_error:
            raise HTTPException(status_code=r.status_code, detail=r.text)

    async def _delete(path: str):
        global _token
        token = await _ensure_token()
        async with _client(token) as client:
            r = await client.delete(f"{_BASE}{path}")
        if r.status_code == 401:
            _token = None
            token = await _ensure_token()
            async with _client(token) as client:
                r = await client.delete(f"{_BASE}{path}")
        _raise(r)
        return r.json() if r.content else {}

    # ── DAGs ──────────────────────────────────────────────────────────────────

    async def list_dags(limit: int = 100) -> dict:
        return await _get("/dags", params={"limit": limit, "order_by": "dag_id", "tags": ["6gdali"]})

    async def get_dag(dag_id: str) -> dict:
        return await _get(f"/dags/{dag_id}")

    async def set_dag_paused(dag_id: str, is_paused: bool) -> dict:
        return await _patch(f"/dags/{dag_id}", {"is_paused": is_paused})

    async def delete_dag(dag_id: str) -> dict:
        result = await _delete(f"/dags/{dag_id}")
        path = os.path.join(AIRFLOW_DAGS_FOLDER, f"{dag_id}.py")
        if os.path.exists(path):
            os.remove(path)
        return result

    async def trigger_dag(dag_id: str, conf: dict = None) -> dict:
        from datetime import datetime, timezone
        return await _post(f"/dags/{dag_id}/dagRuns", {
            "conf": conf or {},
            "logical_date": datetime.now(timezone.utc).isoformat(),
        })

    # ── DAG Runs ──────────────────────────────────────────────────────────────

    async def list_dag_runs(dag_id: str, limit: int = 25, offset: int = 0) -> dict:
        return await _get(
            f"/dags/{dag_id}/dagRuns",
            params={"order_by": "-logical_date", "limit": limit, "offset": offset},
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

    # ── Custom Tasks ──────────────────────────────────────────────────────────

    async def create_task(task_id: str, description: str, code: str) -> dict:
        os.makedirs(AIRFLOW_TASKS_FOLDER, exist_ok=True)
        init_path = os.path.join(AIRFLOW_TASKS_FOLDER, "__init__.py")
        if not os.path.exists(init_path):
            open(init_path, "w").close()
        path = os.path.join(AIRFLOW_TASKS_FOLDER, f"{task_id}.py")
        with open(path, "w") as f:
            f.write(code)
        return {"task_id": task_id, "description": description, "file": path, "status": "created"}

    async def get_custom_task(task_id: str) -> dict:
        if not os.path.isdir(AIRFLOW_TASKS_FOLDER):
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
        fpath = os.path.join(AIRFLOW_TASKS_FOLDER, f"{task_id}.py")
        if not os.path.exists(fpath):
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
        with open(fpath) as f:
            code = f.read()
        return {"task_id": task_id, "file": fpath, "code": code}

    async def list_custom_tasks() -> dict:
        if not os.path.isdir(AIRFLOW_TASKS_FOLDER):
            return {"tasks": [], "total_entries": 0}
        tasks = []
        for fname in sorted(os.listdir(AIRFLOW_TASKS_FOLDER)):
            if fname.endswith(".py") and fname != "__init__.py":
                task_id = fname[:-3]
                fpath = os.path.join(AIRFLOW_TASKS_FOLDER, fname)
                with open(fpath) as f:
                    code = f.read()
                tasks.append({"task_id": task_id, "file": fpath, "code": code})
        return {"tasks": tasks, "total_entries": len(tasks)}

    # ── DAG Creation ──────────────────────────────────────────────────────────

    async def create_dag(dag_id: str, description: str, schedule: str, task_ids: list, owner: str = "airflow") -> dict:
        os.makedirs(AIRFLOW_DAGS_FOLDER, exist_ok=True)
        path = os.path.join(AIRFLOW_DAGS_FOLDER, f"{dag_id}.py")
        code = _generate_dag_code(dag_id, description, schedule, task_ids, owner)
        with open(path, "w") as f:
            f.write(code)
        return {"dag_id": dag_id, "file": path, "tasks": task_ids, "status": "created"}

    def _generate_dag_code(dag_id: str, description: str, schedule: str, task_ids: list, owner: str = "airflow") -> str:
        lines = [
            "from airflow import DAG",
            "from airflow.operators.python import PythonOperator",
            "from datetime import datetime",
            "",
            "",
        ]

        for t in task_ids:
            fpath = os.path.join(AIRFLOW_TASKS_FOLDER, f"{t}.py")
            if os.path.exists(fpath):
                with open(fpath) as f:
                    lines.append(f.read().strip())
            else:
                lines.append(f"def {t}(**context):")
                lines.append(f"    pass  # TODO: implement {t}")
            lines += ["", ""]

        lines += [
            f'default_args = {{"owner": "{owner}"}}',
            "",
            f'with DAG(',
            f'    dag_id="{dag_id}",',
            f'    description="""{description}""",',
            f'    default_args=default_args,',
            f'    schedule=None,',
            f'    start_date=datetime(2024, 1, 1),',
            f'    catchup=False,',
            f') as dag:',
            "",
        ]

        for t in task_ids:
            lines.append(f'    t_{t} = PythonOperator(task_id="{t}", python_callable={t})')

        chain = " >> ".join(f"t_{t}" for t in task_ids)
        lines += ["", f"    {chain}", ""]

        return "\n".join(lines)
