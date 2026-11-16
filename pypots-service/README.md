# PyPOTS imputation service

The sidecar the DataOps DAG calls for its `pypots` imputation runs: SAITS, Transformer, TimesNet, BRITS, M-RNN, GP-VAE,
CSDI and US-GAN from [PyPOTS](https://github.com/WenjieDu/PyPOTS). PyPOTS and torch are not in the Airflow image, so the
models train in this container, next to Airflow, on the CPU.

The DAG hands it a regularized bundle over the volume both containers mount (`DALI_WORK_ROOT`, `PYPOTS_WORK_ROOT` here)
and reads the filled splits back from the same volume. Only paths and small JSON cross HTTP. The service is a sibling of
`wavestitch-service/` and works the same way.

## API

| Request | Purpose |
|---|---|
| `POST /jobs` | `{"prepared_dir", "method", "inputs", "params"}`; answers 202 with the job |
| `GET /jobs/{id}` | status (`queued`, `running`, `done`, `error`) and the end of the log |
| `GET /jobs` | the 50 most recent jobs |
| `GET /health` | liveness; no token needed |

`method` is one of `saits`, `brits`, `transformer`, `gpvae`, `mrnn`, `csdi`, `usgan`, `timesnet`. `params` takes four whole
numbers, the runner's own options: `epochs` (200), `batch_size` (32), `window` (100) and `stride` (0). Any other key is
refused with 422, so a typo fails visibly.

A job fits the model on `train.csv` (standardized with the train statistics), imputes train and test in windows of
`window` steps, and writes `<bundle parent>/<base>_generated/pypots_<method>_<train|test>_imputed.csv`. Jobs run one at a
time, because training uses every core it gets.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `PYPOTS_WORK_ROOT` | `/opt/airflow/wsp-work` | the shared volume; every `prepared_dir` must be inside it |
| `PYPOTS_APP_DIR` | `/app/PyPOTS_app` | where the runner is |
| `PYPOTS_STEP_TIMEOUT_SECONDS` | `21600` | the longest one runner may take (6 h) |
| `PYPOTS_TOKEN` | empty | when set, every endpoint but `/health` needs `Authorization: Bearer <token>` |

The DAG side reads `PYPOTS_URL`, `PYPOTS_TOKEN`, `PYPOTS_POLL_SECONDS` and `PYPOTS_TIMEOUT_SECONDS`
(`airflow/plugins/dali/pypots.py`).

## Build and run

```bash
./build.sh            # builds sparkworks/pypots:dev from WaveStitchPlus/dockers/tools/PyPOTS_app
./build.sh --push     # and pushes it (needs `docker login` as a member of sparkworks)
```

The image is based on `python:3.11-slim` with the CPU build of torch 2.8.0. The DataOps stack's `docker-compose.yaml`
has the `pypots` service; size `PYPOTS_CPUS` and `PYPOTS_MEMORY` (4 and 8 GB by default) for the host. Training time
grows with `epochs` and with the series length: try `{"epochs": 20}` first.

## Run from a DAG trigger

```json
{"imputation": {"runs": [
  {"lib": "pypots", "method": "saits", "params": {"epochs": 50}},
  {"lib": "pypots", "method": "timesnet", "params": {"epochs": 30}},
  {"lib": "pypots", "method": "transformer", "params": {"epochs": 30}}
]}}
```

The `Imputation` field of the DataOps UI offers PyPOTS as a library with these methods and options. The runs are
added to the DAG's defaults only when they are listed.

## Tests

```bash
pytest tests -v       # a stand-in runner replaces the real one
```
