"""GET /datasets/{id}/runs: the runs whose conf names a dataset, found by scanning recent runs of every DAG."""
import asyncio

import test_testbeds  # noqa: F401  (sets the environment main needs before it is imported)
import airflow_client as af
from routers import datasets


def _run(i, dataset):
    return {"dag_id": "d", "dag_run_id": f"r{i}", "state": "success",
            "conf": {"dataset_id": dataset, "asset_id": f"a{i}", "triggered_by": "ann"}}


def _feed(monkeypatch, runs):
    calls = []

    async def list_dag_runs(dag_id, limit=25, offset=0):
        calls.append((dag_id, offset))
        return {"dag_runs": runs[offset:offset + limit], "total_entries": len(runs)}

    monkeypatch.setattr(af, "list_dag_runs", list_dag_runs)
    return calls


def test_only_the_runs_of_that_dataset_are_returned_with_who_and_which_distribution(monkeypatch):
    _feed(monkeypatch, [_run(1, "x"), _run(2, "y"), {"dag_run_id": "noconf", "conf": None}, _run(4, "x")])
    out = asyncio.run(datasets.list_dataset_runs("x", limit=20))
    assert [r["dag_run_id"] for r in out["runs"]] == ["r1", "r4"]
    assert out["runs"][0]["asset_id"] == "a1" and out["runs"][0]["triggered_by"] == "ann"
    assert out["complete"] is True


def test_it_reads_every_dag_and_pages_through_the_runs(monkeypatch):
    calls = _feed(monkeypatch, [_run(i, "y") for i in range(250)] + [_run(999, "x")])
    out = asyncio.run(datasets.list_dataset_runs("x", limit=20))
    assert [r["dag_run_id"] for r in out["runs"]] == ["r999"]
    assert {c[0] for c in calls} == {"~"} and [c[1] for c in calls] == [0, 100, 200]


def test_it_stops_at_the_scan_cap_and_says_the_answer_may_be_incomplete(monkeypatch):
    _feed(monkeypatch, [_run(i, "y") for i in range(2500)] + [_run(9999, "x")])
    out = asyncio.run(datasets.list_dataset_runs("x", limit=20))
    assert out["runs"] == [] and out["complete"] is False and out["scanned"] == 1000


def test_reaching_the_limit_is_complete(monkeypatch):
    _feed(monkeypatch, [_run(i, "x") for i in range(30)])
    out = asyncio.run(datasets.list_dataset_runs("x", limit=5))
    assert len(out["runs"]) == 5 and out["complete"] is True
