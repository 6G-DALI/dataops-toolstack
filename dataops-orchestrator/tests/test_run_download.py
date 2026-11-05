"""GET /dags/{dag}/runs/{run}/artifacts/{name}/download: a presigned link to a run's artifact, straight from the store."""
import asyncio
from urllib.parse import parse_qs, urlsplit

import pytest

import test_testbeds  # noqa: F401  (sets the environment main needs before it is imported)
import airflow_client as af
import datalake_client as dlc
from routers import runs

CAT = "6g-dali"
KEY = "runs/6g-dali/ds/asset/run1/imputed_darts_linear.csv"
PUBLIC = "https://datalake.example.org"
INTERNAL = "http://minio:9000"
LAKE = "https://lake.example.org"     # the Data Lake's own download address, for artifacts stored in a catalogue bucket


@pytest.fixture
def run(monkeypatch):
    """A run whose artifact map holds a current key and a legacy one; the store holds whatever `present` lists."""
    present = {(runs.DATAOPS_BUCKET, KEY, "dataops")}
    probed = []

    async def get_dag_run(dag_id, run_id):
        return {"conf": {"catalogue_id": CAT}}

    async def get_xcom(dag_id, run_id, task_id, key="return_value"):
        return {
            "imputed_darts_linear_final_csv": KEY,
            "report_json": "ds/asset/run1/report.json",          # stored before runs/ existed: in the catalogue's bucket
            "escape": "runs/../secret.csv",
        }

    def object_exists(bucket, key, store="dataspace"):
        probed.append((bucket, key, store))
        return (bucket, key, store) in present

    monkeypatch.setattr(af, "get_dag_run", get_dag_run)
    monkeypatch.setattr(af, "get_xcom", get_xcom)
    monkeypatch.setattr(dlc, "object_exists", object_exists)
    # real signing, against fixed credentials: the signature is computed locally, nothing is sent anywhere
    for name, value in (("DATAOPS_S3_ENDPOINT_URL", INTERNAL), ("DATAOPS_S3_ACCESS_KEY", "ak"), ("DATAOPS_S3_SECRET_KEY", "sk"),
                        ("DATAOPS_S3_REGION", "us-east-1"), ("DATASPACE_S3_ENDPOINT_URL", INTERNAL),
                        ("DATASPACE_S3_ACCESS_KEY", "ak2"), ("DATASPACE_S3_SECRET_KEY", "sk2"), ("DATASPACE_S3_REGION", "us-east-1")):
        monkeypatch.setattr(dlc, name, value)
    monkeypatch.setattr(runs, "DATAOPS_DOWNLOAD_ENDPOINT_URL", PUBLIC)
    monkeypatch.setattr(runs, "DATALAKE_DOWNLOAD_ENDPOINT_URL", LAKE)
    monkeypatch.setattr(runs, "DOWNLOAD_URL_TTL_SECONDS", 900)
    return present, probed


def _call(name="imputed_darts_linear_final_csv"):
    return asyncio.run(runs.get_run_artifact_download("d", "r", name))


def test_the_link_points_the_browser_at_the_store_not_at_the_orchestrator(run):
    out = _call()
    url = urlsplit(out["url"])
    assert f"{url.scheme}://{url.netloc}" == PUBLIC                       # the address a browser can reach, not minio:9000
    assert url.path == f"/{runs.DATAOPS_BUCKET}/{KEY}"
    q = parse_qs(url.query)
    assert q["X-Amz-Expires"] == ["900"] and q["X-Amz-Signature"][0] and q["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert q["response-content-disposition"] == ['attachment; filename="imputed_darts_linear.csv"']
    assert out["filename"] == "imputed_darts_linear.csv" and out["expires_in"] == 900


def test_the_signature_is_for_the_public_address(run, monkeypatch):
    """Signing for the internal address and handing out the public one would fail at the store: the host is signed."""
    a = parse_qs(urlsplit(_call()["url"]).query)["X-Amz-Signature"]
    monkeypatch.setattr(runs, "DATAOPS_DOWNLOAD_ENDPOINT_URL", "https://other.example.org")
    b = parse_qs(urlsplit(_call()["url"]).query)["X-Amz-Signature"]
    assert a != b


def test_a_name_the_run_did_not_produce_is_a_404(run):
    with pytest.raises(runs.HTTPException) as e:
        _call("nope")
    assert e.value.status_code == 404


def test_an_object_the_store_does_not_have_is_a_404_and_is_not_signed(run):
    present, _ = run
    present.clear()
    with pytest.raises(runs.HTTPException) as e:
        _call()
    assert e.value.status_code == 404


def test_an_artifact_stored_before_runs_prefix_is_found_in_the_catalogue_bucket(run):
    present, probed = run
    present.add((CAT, "ds/asset/run1/report.json", "dataspace"))
    out = _call("report_json")
    assert urlsplit(out["url"]).path == f"/{CAT}/ds/asset/run1/report.json"
    assert urlsplit(out["url"]).netloc == "lake.example.org"
    assert out["filename"] == "report.json"


def test_a_key_that_climbs_out_of_the_bucket_is_refused_before_the_store_is_asked(run):
    _, probed = run
    with pytest.raises(runs.HTTPException) as e:
        _call("escape")
    assert e.value.status_code == 400 and probed == []


def test_a_store_with_no_download_address_is_a_503_not_a_link_to_nowhere(run, monkeypatch):
    monkeypatch.setattr(runs, "DATAOPS_DOWNLOAD_ENDPOINT_URL", "")
    with pytest.raises(runs.HTTPException) as e:
        _call()
    assert e.value.status_code == 503


def test_a_run_with_no_catalogue_and_a_legacy_key_is_a_400(run, monkeypatch):
    async def get_dag_run(dag_id, run_id):
        return {"conf": {}}

    monkeypatch.setattr(af, "get_dag_run", get_dag_run)
    with pytest.raises(runs.HTTPException) as e:
        _call("report_json")
    assert e.value.status_code == 400
