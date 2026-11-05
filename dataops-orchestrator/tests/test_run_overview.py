"""GET /dags/{dag}/runs/{run}/artifacts/{name}/overview: a whole CSV thinned to a plottable size."""
import asyncio

import pytest

import test_testbeds  # noqa: F401  (sets the environment main needs before it is imported)
import airflow_client as af
import datalake_client as dlc
from routers import runs


def _csv(n):
    return ("t,v\n" + "".join(f"{i},{i * 2}\n" for i in range(n))).encode()


def _chunks(data, size=7):
    return [data[i:i + size] for i in range(0, len(data), size)]


def test_a_short_file_is_returned_whole():
    out = runs.decimate_csv(_chunks(_csv(10)), 100)
    assert out["header"] == ["t", "v"] and len(out["rows"]) == 10
    assert out["stride"] == 1 and out["total_rows"] == 10 and out["complete"] is True


def test_a_long_file_is_thinned_evenly_and_still_reaches_its_end():
    out = runs.decimate_csv(_chunks(_csv(10_000), 4096), 100)
    assert 50 <= len(out["rows"]) <= 101 and out["total_rows"] == 10_000
    xs = [int(r[0]) for r in out["rows"]]
    assert xs[0] == 0 and xs[-1] == 9_999
    # every kept row but the appended last one sits a whole stride after the one before
    assert {b - a for a, b in zip(xs[:-2], xs[1:-1])} == {out["stride"]}


def test_a_quoted_field_with_a_newline_stays_one_row_and_a_missing_final_newline_is_read():
    data = b'a,b\n1,"x\ny"\n2,z'
    out = runs.decimate_csv(_chunks(data, 3), 100)
    assert out["rows"] == [["1", "x\ny"], ["2", "z"]]


def test_the_read_cap_stops_early_and_says_so():
    out = runs.decimate_csv(_chunks(_csv(10_000), 1024), 100, byte_cap=20_000)
    assert out["complete"] is False and 0 < out["total_rows"] < 10_000


class _Body:
    def __init__(self, data):
        self.data, self.closed = data, False

    def iter_chunks(self, chunk_size):
        return iter(_chunks(self.data, chunk_size))

    def close(self):
        self.closed = True


@pytest.fixture
def served(monkeypatch):
    body = _Body(_csv(5_000))
    opened = []

    async def get_dag_run(dag_id, run_id):
        return {"conf": {"catalogue_id": "cat"}}

    async def get_xcom(dag_id, run_id, task_id, key="return_value"):
        return {"output_csv": "runs/cat/d/a/r/remediated.csv", "report_json": "runs/cat/d/a/r/report.json"}

    def open_stream(bucket, key, store="dataspace"):
        opened.append((bucket, key, store))
        return body, len(body.data)

    monkeypatch.setattr(af, "get_dag_run", get_dag_run)
    monkeypatch.setattr(af, "get_xcom", get_xcom)
    monkeypatch.setattr(dlc, "open_stream", open_stream)
    runs._overview_cache.clear()
    return body, opened


def _call(name="output_csv", max_points=100):
    return asyncio.run(runs.get_run_artifact_overview("d", "r", name, max_points))


def test_the_endpoint_reads_the_run_artifact_once_closes_it_and_caches_the_answer(served):
    body, opened = served
    out = _call()
    assert out["total_rows"] == 5_000 and len(out["rows"]) <= 101 and out["key"].endswith("remediated.csv")
    assert body.closed is True
    assert len(opened) == 1 and opened[0][1] == "runs/cat/d/a/r/remediated.csv"
    assert _call() == out and len(opened) == 1


def test_an_unknown_or_non_csv_artifact_is_refused(served):
    with pytest.raises(runs.HTTPException) as e:
        _call("nope")
    assert e.value.status_code == 404
    with pytest.raises(runs.HTTPException) as e:
        _call("report_json")
    assert e.value.status_code == 400
