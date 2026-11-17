"""
Tests for how dali.processing scores imputation runs: on the hidden cells, and on the same cells for every run.

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_scoring.py -v
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dali.processing import _column_spread, _rank_runs, _score_on_common_cells, _score_run

GT = pd.DataFrame({"t": range(6), "a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "b": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]})


def _bundle(tmp_path: Path) -> str:
    """Rows 1, 2 of `a` and row 4 of `b` are hidden in the test input."""
    test_input = GT.copy()
    test_input.loc[[1, 2], "a"] = np.nan
    test_input.loc[4, "b"] = np.nan
    test_input.to_csv(tmp_path / "test_input.csv", index=False)
    GT.to_csv(tmp_path / "test_gt.csv", index=False)
    GT.to_csv(tmp_path / "train.csv", index=False)
    (tmp_path / "meta.json").write_text(json.dumps({"target_cols": ["a", "b"]}))
    return str(tmp_path)


def _run(tmp_path: Path, name: str, filled: dict, *, off_by: float = 0.0) -> dict:
    """A run whose test split holds `filled` ({(row, col): value}) and the truth elsewhere."""
    frame = GT.copy()
    for (row, col), value in filled.items():
        frame.loc[row, col] = value + off_by
    for (row, col) in {(1, "a"), (2, "a"), (4, "b")} - set(filled):
        frame.loc[row, col] = np.nan
    path = tmp_path / f"{name}_test.csv"
    frame.to_csv(path, index=False)
    return {"lib": "x", "method": name, "status": "imputed", "files": {"test": {"path": str(path)}}}


def test_a_run_is_scored_on_its_own_cells_with_its_coverage(tmp_path):
    bundle = _bundle(tmp_path)
    run = _run(tmp_path, "full", {(1, "a"): 2.0, (2, "a"): 3.0, (4, "b"): 50.0}, off_by=1.0)
    acc = _score_run(bundle, run)
    assert acc["eval_cells"] == 3 and acc["hidden_cells"] == 3 and acc["coverage"] == 1.0
    assert acc["per_column"]["a"]["RMSE"] == 1.0 and acc["per_column"]["b"]["RMSE"] == 1.0
    assert acc["nrmse"] is not None and acc["nrmse"] > 0


def test_runs_that_fill_different_cells_are_compared_on_the_cells_all_filled(tmp_path):
    bundle = _bundle(tmp_path)
    full = _run(tmp_path, "full", {(1, "a"): 2.0, (2, "a"): 3.0, (4, "b"): 50.0}, off_by=1.0)
    partial = _run(tmp_path, "partial", {(1, "a"): 2.0})                      # perfect, but fills one cell of three
    runs = [full, partial]
    for r in runs:
        r["accuracy"] = _score_run(bundle, r)
    assert runs[1]["accuracy"]["nrmse"] == 0.0 and runs[1]["accuracy"]["coverage"] < 0.5

    _score_on_common_cells(bundle, runs)

    a_full, a_partial = runs[0]["accuracy"], runs[1]["accuracy"]
    assert a_full["scored_on"] == a_partial["scored_on"] == "common"
    assert a_full["eval_cells"] == a_partial["eval_cells"] == 1             # the one cell both filled
    assert a_partial["nrmse"] == 0.0 and a_full["nrmse"] > 0
    assert a_full["own_cells"] == 3 and a_full["coverage"] == 1.0
    assert a_partial["own_cells"] == 1
    assert [r["method"] for r in _rank_runs(runs) and runs if r["rank"] == 1] == ["partial"]


def test_no_cell_filled_by_every_run_says_the_scores_are_not_comparable(tmp_path):
    bundle = _bundle(tmp_path)
    one = _run(tmp_path, "one", {(1, "a"): 2.0})
    two = _run(tmp_path, "two", {(4, "b"): 50.0})
    runs = [one, two]
    for r in runs:
        r["accuracy"] = _score_run(bundle, r)
    _score_on_common_cells(bundle, runs)
    assert all(r["accuracy"]["scored_on"] == "own" and "not comparable" in r["accuracy"]["note"] for r in runs)


def test_a_single_run_keeps_its_own_score(tmp_path):
    bundle = _bundle(tmp_path)
    run = _run(tmp_path, "only", {(1, "a"): 2.0, (2, "a"): 3.0, (4, "b"): 50.0})
    run["accuracy"] = _score_run(bundle, run)
    before = dict(run["accuracy"])
    _score_on_common_cells(bundle, [run])
    assert run["accuracy"] == before


def test_a_column_that_varies_only_by_rounding_noise_is_left_out_of_the_average(tmp_path):
    """A stationary sensor's latitude: 37.0329350000000x. Dividing any error by its 1e-14 spread gives 1e12."""
    gt = GT.copy()
    gt["lat"] = 37.032935 + np.array([0.0, 1e-14, 0.0, 2e-14, 0.0, 1e-14])
    test_input = gt.copy()
    test_input.loc[[1, 2], "a"] = np.nan
    test_input.loc[[1, 4], "lat"] = np.nan
    test_input.to_csv(tmp_path / "test_input.csv", index=False)
    gt.to_csv(tmp_path / "test_gt.csv", index=False)
    gt.to_csv(tmp_path / "train.csv", index=False)
    (tmp_path / "meta.json").write_text(json.dumps({"target_cols": ["a", "lat"]}))
    filled = gt.copy()
    filled.loc[[1, 2], "a"] += 1.0                      # a is off by 1, the real signal
    filled.loc[[1, 4], "lat"] += 0.02                   # lat is off by 0.02 degrees (about 2 km), on a column that never moves
    filled.to_csv(tmp_path / "run_test.csv", index=False)
    run = {"lib": "x", "method": "m", "status": "imputed", "files": {"test": {"path": str(tmp_path / "run_test.csv")}}}

    accuracy = _score_run(str(tmp_path), run)

    assert np.std(gt["lat"]) > 0                                         # not exactly constant, which is the point
    assert accuracy["per_column"]["lat"]["nrmse"] is None and accuracy["per_column"]["lat"]["constant"] is True
    assert accuracy["per_column"]["lat"]["RMSE"] == pytest.approx(0.02)  # the error is still reported, in degrees
    assert accuracy["nrmse"] == pytest.approx(accuracy["per_column"]["a"]["nrmse"])   # only `a` is in the average
    assert accuracy["nrmse"] < 10


def test_the_spread_of_a_real_column_is_unchanged_and_a_constant_one_has_none():
    truth = pd.DataFrame({"v": [1.0, 2.0, 3.0, 4.0], "k": [5.0, 5.0, 5.0, 5.0], "tiny": [1e-12, 2e-12, 3e-12, 4e-12]})
    assert _column_spread(truth, None, "v") == pytest.approx(np.std([1, 2, 3, 4]))
    assert _column_spread(truth, None, "k") == 0.0
    assert _column_spread(truth, None, "tiny") == pytest.approx(np.std([1e-12, 2e-12, 3e-12, 4e-12]))  # small, not constant
