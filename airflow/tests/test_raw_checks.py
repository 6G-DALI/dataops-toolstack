"""
Tests for the quality checks of the file as received, and the unwrap task that runs just before the pipeline.

The quality a run reports must name the columns of the submitter's file, not the series an unwrapping generated:
dali.rawchecks builds the Great Expectations suite and the pandera check over the raw frame, run_expectations runs
them, and merge_quality_report joins them to the pipeline's report. dali.processing.unwrap_dataset unwraps a narrow
file for the pipeline only.

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_raw_checks.py -v
"""

from __future__ import annotations

import json
import sys
import types

import numpy as np
import pandas as pd
import pytest

from dali import rawchecks
from dali.unwrap import UnwrapState, unwrap, wrap

NARROW_CSV = """source,metric_id,timestamp,unit,value,cell_id,gNB_id,type,ue_id
dt,DRB.UEThpDl,2026-05-28T11:36:54+02:00,N/A,9.28,1,1193046,Cell,N/A
dt,DRB.UEBlerDl,2026-05-28T11:36:54+02:00,N/A,0.25,3,1193046,UE,4
dt,DRB.UEThpDl,2026-05-28T11:36:55+02:00,N/A,9.30,1,1193046,Cell,N/A
dt,DRB.UEBlerDl,2026-05-28T11:36:55+02:00,N/A,0.26,3,1193046,UE,4
dt,DRB.UEThpDl,2026-05-28T11:36:56+02:00,N/A,9.41,1,1193046,Cell,N/A
dt,DRB.UEBlerDl,2026-05-28T11:36:56+02:00,N/A,0.27,3,1193046,UE,4
"""

LAYOUT = {"hierarchy": ["cell_id,ue_id,metric_id"], "separator": "_", "ignore_columns": ["source", "gNB_id"],
          "value_col": "value", "source": "piveau"}


def narrow_frame() -> pd.DataFrame:
    """The file as run_expectations reads it (pandas defaults: "N/A" is a missing value)."""
    import io

    return pd.read_csv(io.StringIO(NARROW_CSV))


def narrow_strings() -> pd.DataFrame:
    """The file as the unwrapping reads it: every cell a string."""
    import io

    return pd.read_csv(io.StringIO(NARROW_CSV), dtype=str, keep_default_na=False)


def wide_frame(n: int = 40) -> pd.DataFrame:
    rng = np.random.default_rng(1)
    return pd.DataFrame({"time": pd.date_range("2026-01-01", periods=n, freq="s").astype(str),
                         "temp": rng.random(n), "load": rng.random(n)})


def kinds(expectations) -> list[tuple[str, object]]:
    return [(e.expectation_type, getattr(e, "column", None)) for e in expectations]


class TestPlan:
    def test_a_timestamp_column_is_found_by_its_name(self):
        p = rawchecks.plan(wide_frame(), {})
        assert p.mode == "time_series" and p.timestamp_col == "time"

    def test_the_runs_timestamp_col_wins(self):
        df = wide_frame()
        df["other"] = df["time"]
        assert rawchecks.plan(df, {"timestamp_col": "other"}).timestamp_col == "other"

    def test_no_timestamp_is_tabular(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        p = rawchecks.plan(df, {})
        assert p.mode == "tabular" and p.timestamp_col is None

    def test_the_configured_mode_is_kept(self):
        assert rawchecks.plan(wide_frame(), {"validation": {"mode": "none"}}).mode == "none"
        assert rawchecks.plan(wide_frame(), {"validation": {"mode": "tabular"}}).mode == "tabular"

    def test_a_declared_layout_gives_the_series_key_of_the_file(self):
        p = rawchecks.plan(narrow_frame(), {}, LAYOUT)
        assert p.timestamp_col == "timestamp" and p.key_columns == ["cell_id", "ue_id", "metric_id"]


class TestAutoExpectations:
    def test_a_time_series_gets_the_pipelines_suite_over_its_own_columns(self):
        df = wide_frame()
        found = kinds(rawchecks.auto_expectations(df, rawchecks.plan(df, {})))
        assert ("expect_column_values_to_be_unique", "time") in found
        assert ("expect_column_values_to_be_increasing", "time") in found
        assert ("expect_column_values_to_not_be_null", "temp") in found
        assert ("expect_column_values_to_be_between", "load") in found

    def test_a_narrow_file_checks_its_series_not_its_timestamps(self):
        df = narrow_frame()
        p = rawchecks.plan(df, {}, LAYOUT)
        exps = rawchecks.auto_expectations(df, p)
        found = kinds(exps)
        # the timestamp repeats on purpose: one row per series
        assert ("expect_column_values_to_be_unique", "timestamp") not in found
        assert ("expect_column_values_to_be_increasing", "timestamp") not in found
        compound = [e for e in exps if e.expectation_type == "expect_compound_columns_to_be_unique"]
        assert compound and compound[0].column_list == ["timestamp", "cell_id", "ue_id", "metric_id"]
        # a level that does not apply (ue_id N/A on cell rows) is absent on purpose
        for col in ("cell_id", "ue_id", "metric_id"):
            assert ("expect_column_values_to_not_be_null", col) not in found
        assert ("expect_column_values_to_not_be_null", "value") in found

    def test_no_generated_series_name_appears(self):
        df = narrow_frame()
        exps = rawchecks.auto_expectations(df, rawchecks.plan(df, {}, LAYOUT))
        columns = {getattr(e, "column", None) for e in exps} - {None}
        assert columns <= set(df.columns)

    def test_an_entirely_empty_column_has_no_band_and_fails_its_not_null_check(self):
        df = narrow_frame()                                  # unit is "N/A" on every row: all missing
        assert df["unit"].isna().all()
        found = kinds(rawchecks.auto_expectations(df, rawchecks.plan(df, {}, LAYOUT)))
        assert ("expect_column_values_to_be_between", "unit") not in found
        assert ("expect_column_values_to_not_be_null", "unit") in found

    def test_none_mode_builds_nothing(self):
        df = wide_frame()
        assert rawchecks.auto_expectations(df, rawchecks.plan(df, {"validation": {"mode": "none"}})) == []


class TestGxFrame:
    def test_the_timestamp_becomes_epoch_seconds_under_the_same_name(self):
        df = wide_frame(5)
        out = rawchecks.gx_frame(df, rawchecks.plan(df, {}))
        assert list(out.columns) == list(df.columns)
        assert out["time"].diff().dropna().eq(1.0).all()
        assert not pd.api.types.is_numeric_dtype(df["time"])   # the input frame is untouched

    def test_a_numeric_timestamp_is_left_alone(self):
        df = pd.DataFrame({"timestamp": [1700000000, 1700000001, 1700000002], "v": [1.0, 2.0, 3.0]})
        out = rawchecks.gx_frame(df, rawchecks.plan(df, {}))
        pd.testing.assert_frame_equal(out, df)

    def test_a_suite_runs_on_it(self):
        import great_expectations as gx

        df = narrow_frame()
        p = rawchecks.plan(df, {}, LAYOUT)
        ctx = gx.get_context(mode="ephemeral")
        asset = ctx.data_sources.add_pandas("s").add_dataframe_asset("a")
        batch = asset.add_batch_definition_whole_dataframe("b")
        suite = ctx.suites.add(gx.ExpectationSuite(name="x"))
        for e in rawchecks.auto_expectations(df, p):
            suite.add_expectation(e)
        res = ctx.validation_definitions.add(gx.ValidationDefinition(name="v", data=batch, suite=suite)).run(
            batch_parameters={"dataframe": rawchecks.gx_frame(df, p)})
        by_check = {(r.expectation_config.type, r.expectation_config.kwargs.get("column")): r.success
                    for r in res.results}
        # a series has one value per timestamp; the unit column is empty on every row, which the suite says
        assert by_check[("expect_compound_columns_to_be_unique", None)] is True
        assert by_check[("expect_column_values_to_not_be_null", "unit")] is False
        assert by_check[("expect_column_values_to_not_be_null", "value")] is True


class TestPanderaCheck:
    @pytest.fixture
    def stub(self, monkeypatch):
        """A stand-in for data_process_modules.validation, which needs pandera."""
        calls = []
        mod = types.ModuleType("data_process_modules.validation")

        def series(df, **kw):
            calls.append(("time_series", df, kw))
            if kw.get("boom"):
                raise ValueError("boom")

        def tabular(df, **kw):
            calls.append(("tabular", df, kw))

        mod.validate_numeric_timeseries = series
        mod.validate_tabular_dataframe = tabular
        pkg = types.ModuleType("data_process_modules")
        pkg.validation = mod
        monkeypatch.setitem(sys.modules, "data_process_modules", pkg)
        monkeypatch.setitem(sys.modules, "data_process_modules.validation", mod)
        return calls

    def test_a_passing_file(self, stub):
        df = wide_frame()
        out = rawchecks.pandera_check(df, rawchecks.plan(df, {}))
        assert out["pandera_passed"] is True and out["errors"] == [] and out["timestamp_column"] == "time"
        kind, frame, kw = stub[0]
        assert kind == "time_series" and list(frame.columns) == list(df.columns)
        assert pd.api.types.is_datetime64_any_dtype(frame["time"])
        assert kw["require_timestamp_unique"] is True and kw["require_timestamp_monotonic"] is True

    def test_a_failure_is_reported_not_raised(self, monkeypatch, stub):
        import data_process_modules.validation as v

        def fail(df, **kw):
            raise ValueError("column 'temp' missing ratio 0.2 exceeds 0.0")

        monkeypatch.setattr(v, "validate_numeric_timeseries", fail)
        df = wide_frame()
        out = rawchecks.pandera_check(df, rawchecks.plan(df, {}))
        assert out["pandera_passed"] is False and "temp" in out["errors"][0]

    def test_a_narrow_file_need_not_be_unique_or_sorted_and_its_absent_levels_are_filled(self, stub):
        df = narrow_frame()
        out = rawchecks.pandera_check(df, rawchecks.plan(df, {}, LAYOUT))
        assert out["pandera_passed"] is True
        _, frame, kw = stub[0]
        assert kw["require_timestamp_unique"] is False and kw["require_timestamp_monotonic"] is False
        assert frame["ue_id"].isna().sum() == 0

    def test_tabular(self, stub):
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert rawchecks.pandera_check(df, rawchecks.plan(df, {}))["pandera_passed"] is True
        assert stub[0][0] == "tabular"

    def test_mode_none_says_nothing(self, stub):
        df = wide_frame()
        out = rawchecks.pandera_check(df, rawchecks.plan(df, {"validation": {"mode": "none"}}))
        assert out["pandera_passed"] is None and stub == []

    def test_without_pandera_it_says_nothing(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "data_process_modules.validation", None)
        df = wide_frame()
        out = rawchecks.pandera_check(df, rawchecks.plan(df, {}))
        assert out["pandera_passed"] is None and "not available" in out["errors"][0]


class TestUnwrapState:
    def test_it_survives_json_and_wraps_back_the_same(self):
        narrow = narrow_strings()
        wide, state = unwrap(narrow, "cell_id,ue_id,metric_id", ["source", "gNB_id"])
        again = UnwrapState.from_dict(json.loads(json.dumps(state.to_dict())))
        assert again == state
        pd.testing.assert_frame_equal(wrap(wide, again), wrap(wide, state))


class TestUnwrapTask:
    @pytest.fixture
    def run(self, monkeypatch):
        from dali import processing

        def go(layout, content=NARROW_CSV, format_check=None, params=None):
            monkeypatch.setattr(processing, "get_current_context",
                                lambda: {"params": {"dataset_id": "d", "catalogue_id": "c", "asset_id": "a",
                                                    **(params or {})}})
            monkeypatch.setattr(processing, "fetch_series_layout", lambda *a: layout)
            return processing.unwrap_dataset.function(content, "readings.csv", format_check)

        return go

    def test_it_unwraps_into_one_column_per_series(self, run):
        out = run({k: v for k, v in LAYOUT.items() if k != "source"})
        assert out["unwrapped"] is True
        wide = pd.read_csv(__import__("io").StringIO(out["file_content"]))
        assert {"1_DRB.UEThpDl", "3_4_DRB.UEBlerDl"} <= set(wide.columns)
        assert len(wide) == 3 and out["layout"]["source"] == "piveau"
        assert out["wide_columns"] == list(wide.columns)
        json.dumps(out)                                     # it must cross an XCom

    def test_no_layout_nothing_to_do(self, run):
        assert run(None) == {"unwrapped": False, "reason": "no layout declared"}

    def test_a_failed_format_check_nothing_to_do(self, run):
        assert run(LAYOUT, format_check={"success": False})["unwrapped"] is False

    def test_a_hierarchy_that_does_not_fit_is_an_error_not_a_crash(self, run):
        out = run({**LAYOUT, "hierarchy": ["nope,metric_id"]})
        assert out["unwrapped"] is False and out["error"]


class TestPipelineWithAnUnwrappedInput:
    """run_dataops_pipeline takes the unwrapped file from the task before it, and runs no quality check of its own."""

    @pytest.fixture
    def pipeline(self, monkeypatch, tmp_path):
        from dali import processing

        seen = {}

        def fake_run_pipeline(input_csv, output_csv, report_json, **kw):
            seen["input_csv"], seen["kw"] = input_csv, kw
            for path in (output_csv, kw["soft_cleaned_csv"]):
                open(path, "w").write("x\n")
            open(report_json, "w").write(json.dumps({"mode": "time_series"}))

        monkeypatch.setattr(processing, "_load_run_pipeline", lambda: fake_run_pipeline)
        monkeypatch.setattr(processing, "_scratch_dir", lambda: tmp_path)
        monkeypatch.setattr(processing, "impute_prepared_bundle", lambda report, cfg: {})
        monkeypatch.setattr(processing, "get_current_context", lambda: {"params": {}})
        return processing, seen

    def test_the_pipeline_reads_the_wide_file_and_switches_its_own_checks_off(self, pipeline):
        processing, seen = pipeline
        narrow = narrow_strings()
        wide, state = unwrap(narrow, "cell_id,ue_id,metric_id", ["source", "gNB_id"])
        handed = {"unwrapped": True, "file_content": wide.to_csv(index=False), "timestamp_col": "timestamp",
                  "wide_columns": list(wide.columns), "layout": LAYOUT, "state": state.to_dict()}
        out = processing.run_dataops_pipeline.function(NARROW_CSV, "readings.csv", {"success": True}, handed)
        assert seen["input_csv"].endswith("readings_unwrapped.csv")
        assert open(seen["input_csv"], newline="").read().replace("\r", "") == handed["file_content"].replace("\r", "")
        assert seen["kw"]["validation_config"]["run_gx"] is False
        assert seen["kw"]["validation_config"]["run_pandera"] is False
        assert out["report"]["unwrap"]["hierarchy"] == LAYOUT["hierarchy"]
        # the results are stored narrow: the wide frame the pipeline read is not an artifact, and every frame the
        # pipeline wrote comes back in the layout of the file that came in
        assert "unwrapped" not in out["artifacts"] and "input_csv" in out["artifacts"]
        assert out["report"]["unwrap"]["not_wrapped"] == {} or "output_csv" in out["report"]["unwrap"]["not_wrapped"]

    def test_an_unwrap_that_failed_skips_the_run(self, pipeline):
        processing, seen = pipeline
        out = processing.run_dataops_pipeline.function(
            NARROW_CSV, "readings.csv", {"success": True}, {"unwrapped": False, "error": "KeyError: 'nope'"})
        assert out["report"] is None and "hierarchy not applied" in out["error"] and "input_csv" not in seen

    def test_nothing_to_unwrap_runs_the_file_as_it_is(self, pipeline):
        processing, seen = pipeline
        out = processing.run_dataops_pipeline.function(
            NARROW_CSV, "readings.csv", {"success": True}, {"unwrapped": False, "reason": "no layout declared"})
        assert seen["input_csv"].endswith("readings.csv") and out["error"] is None


@pytest.fixture
def pandera_stub(monkeypatch):
    """data_process_modules.validation without pandera: every schema check passes."""
    mod = types.ModuleType("data_process_modules.validation")
    mod.validate_numeric_timeseries = lambda df, **kw: df
    mod.validate_tabular_dataframe = lambda df, **kw: df
    pkg = types.ModuleType("data_process_modules")
    pkg.validation = mod
    monkeypatch.setitem(sys.modules, "data_process_modules", pkg)
    monkeypatch.setitem(sys.modules, "data_process_modules.validation", mod)


class TestRunExpectations:
    """The task itself, over a narrow file: what it reports names the file's columns."""

    @pytest.fixture
    def run(self, monkeypatch, pandera_stub):
        from dali import processing, validation

        def go(expectations=None, layout=LAYOUT):
            params = {"dataset_id": "d", "catalogue_id": "c", "asset_id": "a",
                      "expectations": expectations or [], "timestamp_col": None, "validation": {}}
            monkeypatch.setattr(validation, "get_current_context", lambda: {"params": params})
            monkeypatch.setattr(validation, "fetch_columns_from_piveau", lambda d, a: ["timestamp", "value", "cell_id"])
            monkeypatch.setattr(processing, "fetch_series_layout", lambda *a: layout)
            return validation.run_expectations.function(NARROW_CSV, "readings.csv", {
                "expectation_type": "expect_file_to_be_valid_csv", "kwargs": {}, "success": True, "result": {}})

        return go

    def test_every_check_names_a_column_of_the_file(self, run):
        report = run()
        columns = set(narrow_frame().columns)
        named = {(e.get("kwargs") or {}).get("column") for e in report["results"]} - {None}
        assert named and named <= columns
        assert "DRB" not in json.dumps(report["results"])       # no generated series name anywhere

    def test_the_generated_suite_and_the_catalogues_are_both_there_without_duplicates(self, run):
        results = run()["results"]
        keys = [(e["expectation_type"], (e.get("kwargs") or {}).get("column")) for e in results]
        assert len(keys) == len(set(keys))
        assert ("expect_column_values_to_be_between", "value") in keys          # generated
        assert ("expect_column_to_exist", "value") in keys                       # from the catalogue's columns
        assert ("expect_compound_columns_to_be_unique", None) in keys

    def test_pandera_is_one_more_entry_of_the_same_report(self, run):
        report = run()
        entries = [e for e in report["results"] if e["expectation_type"] == "expect_pandera_schema_to_validate"]
        assert len(entries) == 1 and entries[0]["success"] is True
        assert report["validation"]["pandera_passed"] is True and report["validation"]["timestamp_column"] == "timestamp"
        stats = report["statistics"]
        assert stats["evaluated_expectations"] == len(report["results"])

    def test_explicit_expectations_are_not_topped_up(self, run):
        report = run(expectations=[{"type": "expect_column_to_exist", "column": "value"}])
        types_ = [e["expectation_type"] for e in report["results"]]
        assert "expect_column_values_to_be_between" not in types_
        assert "expect_pandera_schema_to_validate" in types_       # the schema check does not depend on them


class TestMergeWithTheFileChecks:
    def _gx(self, with_pandera=True):
        results = [
            {"expectation_type": "expect_file_to_be_valid_csv", "kwargs": {}, "success": True, "result": {}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"},
             "success": True, "result": {}},
        ]
        if with_pandera:
            results.append({"expectation_type": "expect_pandera_schema_to_validate",
                            "kwargs": {"source": "pandera", "mode": "time_series"}, "success": False,
                            "result": {"errors": ["column 'unit' missing ratio 1.0 exceeds 0.0"]}})
        return {"input_key": "d/readings.csv", "run_time": "t", "success": False, "results": results,
                "statistics": {"evaluated_expectations": len(results)}}

    # the pipeline as it now reports: no Great Expectations, no pandera, only its own findings
    PIPELINE = {"workdir": "/w", "artifacts": {}, "error": None, "report": {
        "validation": {"mode": "time_series", "pandera_passed": None, "errors": []},
        "quality": {"gx_passed": None, "report": {"gx": {}, "issues": {"missing": {"1_DRB.UEThpDl": {}}}}},
        "quality_after": {"gx_passed": None, "report": {"gx": {}}},
    }}

    def test_the_pandera_entry_of_the_file_is_kept_once_and_the_pipeline_adds_none(self):
        from dali.validation import merge_quality_report

        merged = merge_quality_report.function(gx_report=self._gx(), pipeline=self.PIPELINE)
        types_ = [e["expectation_type"] for e in merged["results"]]
        assert types_.count("expect_pandera_schema_to_validate") == 1
        assert merged["sources"]["pandera"] == {"success": False, "mode": "time_series", "errors": 1}
        # the Great Expectations half counts its own checks, not the schema check
        assert merged["sources"]["great_expectations"]["evaluated"] == 2
        assert "great_expectations_after_remediation" not in merged["sources"]
        assert "1_DRB" not in json.dumps(merged["results"])

    def test_without_a_pandera_entry_and_without_a_pipeline_one_there_is_none(self):
        from dali.validation import merge_quality_report

        merged = merge_quality_report.function(gx_report=self._gx(with_pandera=False), pipeline=self.PIPELINE)
        assert "pandera" not in merged["sources"]
        assert all(e["expectation_type"] != "expect_pandera_schema_to_validate" for e in merged["results"])

    def test_a_pipeline_that_still_ran_pandera_is_still_reported(self):
        from dali.validation import merge_quality_report

        pipeline = json.loads(json.dumps(self.PIPELINE))
        pipeline["report"]["validation"] = {"mode": "time_series", "pandera_passed": True, "errors": []}
        merged = merge_quality_report.function(gx_report=self._gx(with_pandera=False), pipeline=pipeline)
        assert merged["sources"]["pandera"]["success"] is True
