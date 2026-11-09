# wavestitch-service

The WaveStitch+ imputation service: a container next to Airflow in the DataOps stack that runs the WaveStitch+
methods (`v1`, `v2`, `harpoon`) for the DAG `dali_dataspace_validate_dataset`.

WaveStitch+ needs torch and pykeops and takes minutes to hours, so it is not part of the Airflow image. The DAG
(`airflow/plugins/dali/wavestitch.py`) sends this service a job over HTTP; the regularized bundle and the filled
splits travel over a volume both containers mount (`wsp-work`).

```
Airflow worker                                   wavestitch (this service)
  pipeline writes <work>/dali-dataops-xyz/<name>_regularized/
  POST /jobs {prepared_dir, method, params} ───▶  runs the method's runner on the bundle
  GET  /jobs/{id}  (polls) ◀───────────────────   writes <work>/dali-dataops-xyz/<name>_generated/
  reads wavestitchplus_<method>_<split>_imputed.csv  from the volume
```

## Using it from a DAG run

```json
{"imputation": {"runs": [
  {"lib": "wavestitchplus", "method": "v1", "device": "cpu", "params": {"fast": true}},
  {"lib": "wavestitchplus", "method": "v2"},
  {"lib": "wavestitchplus", "method": "harpoon", "params": {"ddim_steps": 5, "repaint_rounds": 1}}
]}}
```

- `v1` trains the EM diffusion model on the bundle and imputes. `v2` anchors v1's output to an interpolation prior.
  `harpoon` samples from v1's checkpoint with manifold-bound guidance.
- v2 and harpoon need v1 on the same bundle. The DAG orders the runs so v1 comes first; when v1 is not listed the
  service runs it first, with `v1_params`.
- `device` is `cpu` (default), `gpu` or `auto`. `params` are the options of that method's runner (see `RUNNERS` in
  `app.py`); an option the runner does not have is refused with a 422.
- Without `fast`, v1 runs 3 EM rounds of 50 epochs: minutes to hours on CPU depending on the bundle. A `fast` run on
  a 600-row bundle took about 30 s.

## API

| | |
|---|---|
| `POST /jobs` | `{"prepared_dir", "method", "device", "inputs", "params", "v1_params"}` → `202` with the job |
| `GET /jobs/{id}` | `status` (`queued`, `running`, `done`, `error`), `steps`, `error`, and the end of the log |
| `GET /jobs` | the 50 most recent jobs |
| `GET /health` | no token needed |

Jobs run one at a time, in the order they arrive. They are kept in memory: after a restart a job id is unknown (404),
and the DAG records that run as failed. The logs stay next to the results, in `<name>_generated/wavestitch_job_<id>.log`.

`prepared_dir` must be inside `WSP_WORK_ROOT` (the volume) and hold a bundle (`meta.json`). Nothing outside it is read
or written.

## Configuration

| Variable | Default | |
|---|---|---|
| `WSP_WORK_ROOT` | `/opt/airflow/wsp-work` | the shared volume |
| `WSP_APP_DIR` | `/app/WaveStitchPlus_app` | the runners |
| `WSP_STEP_TIMEOUT_SECONDS` | `21600` | longest one runner may take |
| `WSP_TOKEN` | (none) | when set, every endpoint but `/health` needs `Authorization: Bearer <token>` |

The DAG side is configured with `WAVESTITCH_URL`, `WAVESTITCH_TOKEN`, `WAVESTITCH_TIMEOUT_SECONDS` and
`DALI_WORK_ROOT`; the compose files pass them (see `dataops-devops/docker/prd/dataops/docker-compose.yaml`).

## Build and publish

```sh
./build.sh            # builds wavestitchplus-cpu:latest from ../WaveStitchPlus, then sparkworks/wavestitch:dev on it
./build.sh --push     # and pushes it to Docker Hub (docker login as a member of sparkworks first)
SKIP_BASE=1 ./build.sh   # only the service layer, reusing the wavestitchplus-cpu:latest already built
```

The image is built from the working tree of the WaveStitchPlus checkout, uncommitted changes included. It is a CPU
image. The GPU image of the WaveStitchPlus repo (`Dockerfile.wavestitchplus-gpu`) can be the `BASE` build argument
for a GPU host, with the container given the GPU in compose.

The service runs as uid 50000, group 0, like the Airflow containers, so the files it writes on the volume are the
workers' to read. The `wavestitch-init` one-shot service in compose prepares the volume's ownership.

## Tests

```sh
pip install fastapi httpx pytest uvicorn
pytest tests
```

The tests use stand-in runners, so they check what the service decides (which runner, in what order, with which
arguments, where) without torch. The real runners were run through the service on a small bundle (v1 `--fast`, v2,
harpoon); `airflow/tests/test_wavestitch.py` covers the DAG's side.
