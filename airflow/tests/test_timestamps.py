"""A timezone-aware time column is made naive UTC before the vendored pipeline reads it (dali.timestamps)."""

import pandas as pd

from dali.timestamps import normalise_timestamps


def _write(tmp_path, stamps, name="in.csv", column="timestamp"):
    path = tmp_path / name
    pd.DataFrame({column: stamps, "value": [str(i) for i in range(len(stamps))]}).to_csv(path, index=False)
    return path


def test_a_z_suffix_becomes_naive_utc(tmp_path):
    src = _write(tmp_path, ["2026-10-22T07:52:29Z", "2026-10-22T07:52:30Z"])
    out = tmp_path / "out.csv"

    info = normalise_timestamps(src, "timestamp", out)

    assert info == {"column": "timestamp", "offset": "UTC", "rows": 2, "unparsed": 0, "timezone": "UTC"}
    assert pd.read_csv(out, dtype=str)["timestamp"].tolist() == ["2026-10-22 07:52:29", "2026-10-22 07:52:30"]


def test_an_offset_is_converted_to_utc(tmp_path):
    src = _write(tmp_path, ["2026-09-10 20:07:46.000 +0300", "2026-09-10 20:07:47.000 +0300"])
    out = tmp_path / "out.csv"

    info = normalise_timestamps(src, "timestamp", out)

    assert info["offset"] == "+0300"
    # the instant is the same: 20:07:46 at +03:00 is 17:07:46 UTC; whole seconds stay whole
    assert pd.read_csv(out, dtype=str)["timestamp"].tolist() == ["2026-09-10 17:07:46", "2026-09-10 17:07:47"]


def test_one_fractional_second_gives_every_row_the_same_format(tmp_path):
    src = _write(tmp_path, ["2026-10-22T07:52:29Z", "2026-10-22T07:52:30.250Z"])
    out = tmp_path / "out.csv"

    normalise_timestamps(src, "timestamp", out)

    assert pd.read_csv(out, dtype=str)["timestamp"].tolist() == ["2026-10-22 07:52:29.000000", "2026-10-22 07:52:30.250000"]


def test_other_columns_are_passed_on_as_text(tmp_path):
    src = _write(tmp_path, ["2026-10-22T07:52:29Z"])
    out = tmp_path / "out.csv"

    normalise_timestamps(src, "timestamp", out)

    assert pd.read_csv(out, dtype=str)["value"].tolist() == ["0"]


def test_a_column_without_an_offset_is_left_alone(tmp_path):
    src = _write(tmp_path, ["2026-10-22 07:52:29", "2026-10-22 07:52:30"])
    out = tmp_path / "out.csv"

    assert normalise_timestamps(src, "timestamp", out) is None
    assert not out.exists()


def test_epoch_seconds_are_left_alone(tmp_path):
    src = _write(tmp_path, ["1792655549", "1792655550"])

    assert normalise_timestamps(src, "timestamp", tmp_path / "out.csv") is None


def test_no_timestamp_column_or_an_unknown_one_changes_nothing(tmp_path):
    src = _write(tmp_path, ["2026-10-22T07:52:29Z"])

    assert normalise_timestamps(src, None, tmp_path / "a.csv") is None
    assert normalise_timestamps(src, "no_such_column", tmp_path / "b.csv") is None


def test_the_header_is_matched_the_way_the_pipeline_names_it(tmp_path):
    src = _write(tmp_path, ["2026-10-22T07:52:29Z"], column="Measured At")

    assert normalise_timestamps(src, "measured_at", tmp_path / "out.csv")["column"] == "Measured At"


def test_a_value_that_does_not_parse_is_passed_on_and_counted(tmp_path):
    src = _write(tmp_path, ["2026-10-22T07:52:29Z", "not a time Z"])
    out = tmp_path / "out.csv"

    info = normalise_timestamps(src, "timestamp", out)

    assert info["rows"] == 1 and info["unparsed"] == 1
    assert pd.read_csv(out, dtype=str)["timestamp"].tolist() == ["2026-10-22 07:52:29", "not a time Z"]
