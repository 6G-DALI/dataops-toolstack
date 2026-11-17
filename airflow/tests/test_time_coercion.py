"""
A time column the pipeline wrote can mix fractional and whole seconds, and both readers of it must cope.
dali.processing wraps the vendored dataops._preprocess_impl.coerce_time_column to read it (_tolerate_mixed_time_formats),
because the vendored copy is not edited here.

DataFrame.to_csv formats a datetime column in chunks of 100000 // (number of columns) rows, each on its own: a chunk that
holds a fractional second is written "2026-07-19 10:03:50.000", a chunk that holds none "2026-09-19 01:02:54". Reading the
column back, pandas infers one format from the first value and drops every value in the other, so a run on a file with a
few sub-second timestamps failed with "Could not parse time column: only 0/6488 parse as numeric and 826/6488 parse as
datetime" (826 = 100000 // 121 columns).

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_time_coercion.py -v
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

import dataops._preprocess_impl as impl
from dali.processing import _tolerate_mixed_time_formats

MIXED = ["2026-07-19 10:03:50.000"] * 3 + ["2026-09-19 01:02:54"] * 3
_ORIGINAL = impl.coerce_time_column


@pytest.fixture
def coerce_time_column():
    """The pipeline's coercion with the wrapper applied, and the original back afterwards."""
    _tolerate_mixed_time_formats()
    yield impl.coerce_time_column
    impl.coerce_time_column = _ORIGINAL


def test_a_column_that_mixes_fractional_and_whole_seconds_is_read_whole(coerce_time_column):
    seconds, kind = coerce_time_column(pd.Series(MIXED))
    assert kind == "datetime" and seconds.notna().all()
    assert seconds.iloc[3] - seconds.iloc[2] == pytest.approx(
        (pd.Timestamp("2026-09-19 01:02:54") - pd.Timestamp("2026-07-19 10:03:50")).total_seconds())


def test_a_column_to_csv_wrote_in_chunks_is_read_whole(coerce_time_column):
    frame = pd.DataFrame({f"c{i}": 0.0 for i in range(129)} | {"t": pd.date_range("2026-01-01", periods=2000, freq="s")})
    frame.loc[3, "t"] = frame.loc[3, "t"] + pd.Timedelta(milliseconds=500)       # one fractional second, in the first chunk
    text = frame[["t", *[c for c in frame.columns if c != "t"]]].to_csv(index=False)
    stamps = pd.read_csv(io.StringIO(text), usecols=["t"], dtype=str)["t"]
    if stamps.str.len().nunique() < 2:
        pytest.skip("this pandas writes the column uniformly, so there is nothing to mix")
    seconds, kind = coerce_time_column(pd.read_csv(io.StringIO(text))["t"])
    assert kind == "datetime" and seconds.notna().all() and len(seconds) == 2000


def test_a_column_that_is_not_a_time_still_raises(coerce_time_column):
    with pytest.raises(ValueError, match="Could not parse time column"):
        coerce_time_column(pd.Series(["a", "b", "c", "d"]))


def test_the_wrapper_is_applied_once_and_to_every_module_that_imported_the_function():
    import dataops.transform as transform

    try:
        _tolerate_mixed_time_formats()
        first = impl.coerce_time_column
        _tolerate_mixed_time_formats()
        assert impl.coerce_time_column is first and transform.coerce_time_column is first
    finally:
        impl.coerce_time_column = _ORIGINAL
        transform.coerce_time_column = _ORIGINAL


def test_a_frame_whose_time_column_mixes_the_two_is_wrapped_back(tmp_path: Path):
    from dali.processing import _wrap_artifacts
    from dali.unwrap import unwrap

    narrow = pd.DataFrame({
        "cell_id": ["1"] * 6, "metric_id": ["m"] * 6,
        "timestamp": [f"2026-07-19T10:03:5{i}+00:00" for i in range(6)], "value": [str(i) for i in range(6)]})
    wide, state = unwrap(narrow, "cell_id,metric_id", timestamp_col="timestamp")
    soft = wide.copy()
    soft["timestamp"] = MIXED                                                    # as the pipeline's to_csv left it
    soft.to_csv(tmp_path / "soft.csv", index=False)
    raw = tmp_path / "raw.csv"
    narrow.to_csv(raw, index=False)
    out, rows, left_out = _wrap_artifacts(
        {"input_csv": str(raw), "soft_cleaned_csv": str(tmp_path / "soft.csv")}, state, list(wide.columns), {}, None,
        tmp_path)
    assert left_out == {} and rows == {"soft_cleaned_csv": 6}
