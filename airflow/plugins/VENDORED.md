# Vendored code in this plugins directory

Everything here except `dali/` is a **verbatim copy** of code that lives
elsewhere in this repository. It is copied rather than mounted so the Airflow
containers need no extra volume: Airflow puts `plugins_folder` on `sys.path`,
so these import like any other plugin module.

| Path | Copied from |
| --- | --- |
| `minimal_dataops.py` | `WaveStitchPlus/pipelines/minimal_dataops.py` |
| `dataops/` | `WaveStitchPlus/src/dataops/` |
| `data_process_modules/` | `WaveStitchPlus/src/data_process_modules/` |
| `Darts_app/run_imputation.py` | `WaveStitchPlus/dockers/tools/Darts_app/run_imputation.py` |

`dali.processing.run_dataops_pipeline` imports `run_pipeline` from
`minimal_dataops`; that module imports `data_process_modules`, which re-exports
`dataops`. All three are needed — `data_process_modules` is only a shim.

## Do not edit these files

Change them in `WaveStitchPlus/` and re-sync, or the two copies drift and the
next sync silently reverts the edit. `dali/` is the place for anything specific
to running the pipeline under Airflow.

## Re-syncing

From `airflow/`:

```sh
cp ../WaveStitchPlus/pipelines/minimal_dataops.py plugins/minimal_dataops.py
rm -rf plugins/dataops plugins/data_process_modules
cp -r ../WaveStitchPlus/src/dataops plugins/dataops
cp -r ../WaveStitchPlus/src/data_process_modules plugins/data_process_modules
mkdir -p plugins/Darts_app
cp ../WaveStitchPlus/dockers/tools/Darts_app/run_imputation.py plugins/Darts_app/run_imputation.py
find plugins/dataops plugins/data_process_modules -name __pycache__ -type d -exec rm -rf {} +
```

Verify the copies are still identical:

```sh
cmp ../WaveStitchPlus/pipelines/minimal_dataops.py plugins/minimal_dataops.py
diff -rq ../WaveStitchPlus/src/dataops plugins/dataops --exclude=__pycache__
diff -rq ../WaveStitchPlus/src/data_process_modules plugins/data_process_modules --exclude=__pycache__
```

`minimal_dataops.py` copies unmodified even though it computes
`PROJECT_ROOT/src` and inserts it on `sys.path` at import: here that resolves to
`airflow/src`, which does not exist, and the `if SRC_ROOT.exists()` guard makes
the block a no-op. Keeping it byte-identical is what lets the sync above be a
plain `cp`.

## Dependencies

The pipeline needs `pandas`, `numpy`, `PyYAML`, `great_expectations` and
`pandera`. Only `pandera` was missing from the Airflow image; it is in
`airflow/base/requirements.txt`, so **the image must be rebuilt** after a sync
that introduces new imports.

Imputation adds two more, both in the same file: `scipy` (the pandas engine needs
it for every method but linear) and `u8darts[notorch]` (the Darts engine and
`darts/kalman`). The Dockerfile installs under Airflow's constraints file; the
requirement set resolves against Airflow 3.1.8's Python 3.11 constraints without
conflict (checked with `pip install --dry-run`), but **no image has been built
from it**. `u8darts` is the heavy one: about fifty extra packages.

**Great Expectations must stay on 1.x for both sides.** `dali/validation.py` was
written against the 1.x API, and `dataops/gx.py`, `ts_checks.py` and
`tabular_checks.py` were ported from 0.18 to match (upstream, in
`WaveStitchPlus/src/dataops/`, whose `requirements.txt` now asks for
`great-expectations>=1.0,<2.0`). Re-vendoring an older WaveStitchPlus checkout
would reintroduce the 0.18 calls — `ctx.sources.add_pandas`, `get_validator`,
`validator.expect_*` — which fail at runtime under 1.x with
`Could not find a datasource named 'pipeline_ts_source'`, reported as
`gx_passed: false` with an empty summary rather than as an error.

The heavy extras in `WaveStitchPlus/requirements.txt` (`torch`, `darts`,
`pypots`, `imputegap`) are **not** needed: they belong to the imputation apps,
which this pipeline never imports — `dataops.imputation_runner` invokes them
through `subprocess`, so importing it costs nothing.

## Behaviour that changed with the 2026-11 re-sync

Two upstream defaults differ from what this DAG did before.

- `build_final_dataset(restore_eval_holdout=True)`: the artificial evaluation
  holdout (about 15% of the test rows, hidden to score the imputer) is put back
  to its observed values in `imputed_final.csv`. Before, those cells held the
  model's guess. Cells that were missing in the raw file stay imputed. The
  imputed train and test splits are unchanged. `dali/processing.py` passes
  nothing, so the DAG takes the new behaviour. Intended: keep it on.
- `timeline_config`: upstream now **drops** rows that go backwards in time
  (`disorder_policy: drop`) instead of sorting them. This DAG overrides that:
  `dali.processing.DEFAULT_TIMELINE_CONFIG` sets `disorder_policy: sort`, and
  the DAG's `timeline` param replaces it per run (for example
  `{"sweep_aware": true}` for parameter-sweep files). Duplicate timestamps keep
  the last row, as before. Regularization now runs per campaign, with a grid per
  collection pause of a day or more; a file with one continuous campaign is
  unaffected.

`WaveStitchPlus/config/dataops.yaml` also sets `disorder_policy: sort`, so the
standalone CLI and this DAG agree. The `timeline` defaults live in two places on
purpose: the YAML for the CLI, `DEFAULT_TIMELINE_CONFIG` for Airflow.
