"""
Tests for dali.unwrap (narrow <-> one column per series), the layout a distribution declares in piveau, and the way
dali.processing wraps the pipeline's output back.

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_unwrap.py -v
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from dali.unwrap import UnwrapError, parse_hierarchies, parse_hierarchy, series_name, unwrap, wrap

FIELDS = "cell_id,ue_id,metric_id"                  # the parts in order, as the catalogue holds them
HIERARCHY = "{cell_id}_{ue_id}_{metric_id}"         # the older template form, still read
IGNORE = ["source", "gNB_id"]

# Two cells, one UE under cell 3; the cell rows have no UE (ue_id N/A). source and gNB_id hold one value.
NARROW = """source,metric_id,timestamp,unit,value,cell_id,gNB_id,type,ue_id
Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:54.462892+02:00,N/A,9.5,1,1193046,Cell,N/A
Digital_Twin,Viavi.Geo.x,2026-05-28T11:36:54.462892+02:00,N/A,100.5,1,1193046,Cell,N/A
Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:55.462892+02:00,N/A,10.5,1,1193046,Cell,N/A
Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:54.562892+02:00,N/A,3.5,3,1193046,Cell,N/A
Digital_Twin,DRB.UEBlerDl,2026-05-28T11:36:54.562892+02:00,N/A,0.25,3,1193046,UE,4
Digital_Twin,DRB.UEBlerDl,2026-05-28T11:36:55.562892+02:00,N/A,0.5,3,1193046,UE,4
Digital_Twin,Viavi.Geo.x,2026-05-28T11:36:54.562892+02:00,N/A,7.0,3,1193046,UE,4
"""


@pytest.fixture
def narrow() -> pd.DataFrame:
    return pd.read_csv(io.StringIO(NARROW), dtype=str, keep_default_na=False)


def _canon(frame: pd.DataFrame) -> pd.DataFrame:
    """Order-independent form for comparing two narrow frames."""
    frame = frame.assign(value=pd.to_numeric(frame["value"])).astype(str)
    return frame.sort_values(list(frame.columns)).reset_index(drop=True)


class TestHierarchy:
    def test_fields_and_separator(self):
        assert parse_hierarchy(HIERARCHY) == (["cell_id", "ue_id", "metric_id"], "_")

    @pytest.mark.parametrize("bad", ["", "{a}", "a_{b}_{c}", "{a}_{b}.{c}", "{a}_{a}", "{a}__{b}x"])
    def test_rejects(self, bad):
        with pytest.raises(UnwrapError):
            parse_hierarchy(bad)

    def test_absent_levels_leave_the_name(self):
        assert series_name(["1", "N/A", "DRB.UEThpDl"], "_") == "1_DRB.UEThpDl"
        assert series_name(["3", "4", "DRB.UEBlerDl"], "_") == "3_4_DRB.UEBlerDl"


class TestUnwrap:
    def test_one_column_per_series(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        assert list(wide.columns) == [
            "timestamp",
            "1_DRB.UEThpDl", "1_Viavi.Geo.x",
            "3_DRB.UEThpDl",
            "3_4_DRB.UEBlerDl", "3_4_Viavi.Geo.x",
        ]
        assert state.stats["series"] == 5 and state.stats["ignored_columns"] == IGNORE
        assert state.tz_offset == "+02:00"

    def test_values_land_on_their_timestamps_in_naive_utc(self, narrow):
        wide, _ = unwrap(narrow, HIERARCHY, IGNORE)
        assert wide["timestamp"].tolist() == [
            "2026-05-28T09:36:54.462892", "2026-05-28T09:36:54.562892",
            "2026-05-28T09:36:55.462892", "2026-05-28T09:36:55.562892",
        ]
        assert wide["1_DRB.UEThpDl"].tolist()[0] == 9.5
        assert pd.isna(wide["1_DRB.UEThpDl"].tolist()[1])
        assert wide["3_4_DRB.UEBlerDl"].tolist()[1:4:2] == [0.25, 0.5]

    def test_ignored_columns_are_not_part_of_the_series(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        assert state.constants == {"source": "Digital_Twin", "gNB_id": "1193046"}
        assert not any("1193046" in c or "Digital" in c for c in wide.columns)

    def test_a_column_that_varies_cannot_be_ignored(self, narrow):
        narrow.loc[0, "gNB_id"] = "2"
        with pytest.raises(UnwrapError, match="only a column with one value"):
            unwrap(narrow, HIERARCHY, IGNORE)

    def test_ignoring_an_unknown_or_structural_column(self, narrow):
        with pytest.raises(UnwrapError, match="not in the dataset"):
            unwrap(narrow, HIERARCHY, ["nope"])
        for structural in ("cell_id", "timestamp", "value"):
            with pytest.raises(UnwrapError, match="cannot be ignored"):
                unwrap(narrow, HIERARCHY, [structural])

    def test_a_constant_gnb_left_in_the_hierarchy_would_prefix_every_name(self, narrow):
        wide, _ = unwrap(narrow, "{gNB_id}_{cell_id}_{ue_id}_{metric_id}", ["source"])
        assert "1193046_1_DRB.UEThpDl" in wide.columns

    def test_missing_column(self, narrow):
        with pytest.raises(UnwrapError, match="no column"):
            unwrap(narrow, "{cell_id}_{nope}_{metric_id}")
        with pytest.raises(UnwrapError, match="no column"):
            unwrap(narrow, HIERARCHY, value_col="reading")

    def test_a_column_that_varies_within_a_series_is_refused(self, narrow):
        narrow.loc[0, "unit"] = "Mbps"
        with pytest.raises(UnwrapError, match="not constant per series"):
            unwrap(narrow, HIERARCHY, IGNORE)

    def test_bad_timestamp(self, narrow):
        narrow.loc[0, "timestamp"] = "yesterday"
        with pytest.raises(UnwrapError, match="not a timestamp"):
            unwrap(narrow, HIERARCHY, IGNORE)

    def test_duplicate_samples_keep_the_last(self, narrow):
        narrow = pd.concat([narrow, narrow.iloc[[0]].assign(value="99")], ignore_index=True)
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        assert state.stats["duplicate_samples_dropped"] == 1
        assert wide["1_DRB.UEThpDl"].tolist()[0] == 99.0


class TestWrap:
    def test_round_trip_is_lossless(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        back = wrap(wide, state)
        assert list(back.columns) == list(narrow.columns)
        pd.testing.assert_frame_equal(_canon(back), _canon(narrow))

    def test_ignored_columns_are_restored_with_their_value(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        back = wrap(wide, state)
        assert set(back["source"]) == {"Digital_Twin"} and set(back["gNB_id"]) == {"1193046"}

    def test_filled_values_become_rows(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        wide["1_DRB.UEThpDl"] = wide["1_DRB.UEThpDl"].fillna(0.0)          # an "imputation"
        back = wrap(wide, state)
        cell1 = back[(back["cell_id"] == "1") & (back["metric_id"] == "DRB.UEThpDl")]
        assert len(cell1) == 4
        assert set(cell1["type"]) == {"Cell"} and set(cell1["ue_id"]) == {"N/A"}

    def test_epoch_time_and_renamed_columns_as_the_pipeline_leaves_them(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        times = pd.to_datetime(wide["timestamp"])
        clean = lambda c: c.lower().replace(".", "_")                       # what the pipeline's snake_case does
        processed = pd.DataFrame({
            "timestamp": (times - pd.Timestamp("1970-01-01")).dt.total_seconds(),
            "split": ["train", "train", "test", "test"],                    # a column the pipeline adds
            **{clean(c): wide[c] for c in wide.columns[1:]},
        })
        back = wrap(processed, state, rename={clean(c): c for c in wide.columns[1:]}, time_unit="epoch")
        assert back["timestamp"].iloc[0] == "2026-05-28T11:36:54.462892+02:00"
        assert len(back) == len(narrow)

    def test_nothing_recognised(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        with pytest.raises(UnwrapError, match="none of the frame's columns"):
            wrap(wide.rename(columns=lambda c: c if c == "timestamp" else "x" + c), state)


class TestWrapArtifacts:
    """dali.processing._wrap_artifacts: every frame the pipeline made is stored narrow, under its own name."""

    def _frames(self, narrow, tmp_path: Path):
        """The wide frames of a run, as the pipeline writes them: snake_cased names, a datetime or epoch time."""
        from dataops.cleaning import build_column_mapping

        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        mapping = build_column_mapping(list(wide.columns))
        times = pd.to_datetime(wide["timestamp"])
        series = {mapping[c]: wide[c] for c in wide.columns[1:]}

        soft = pd.DataFrame({"timestamp": times, **series})
        soft_csv = tmp_path / "ds_soft_cleaned.csv"
        soft.to_csv(soft_csv, index=False)
        remediated_csv = tmp_path / "ds_remediated.csv"
        soft.to_csv(remediated_csv, index=False)

        final = pd.DataFrame({
            "timestamp": (times - pd.Timestamp("1970-01-01")).dt.total_seconds(),
            "split": "train",
            **{mapping[c]: wide[c].fillna(1.0) for c in wide.columns[1:]},
        })
        final_csv = tmp_path / "final.csv"
        final.to_csv(final_csv, index=False)
        prepared = tmp_path / "bundle"
        prepared.mkdir()
        (prepared / "meta.json").write_text(json.dumps({"time_col": "timestamp", "time_unit_seconds": 1.0}))
        wide_input = tmp_path / "ds_unwrapped.csv"
        wide.to_csv(wide_input, index=False)
        produced = {
            "input_csv": str(tmp_path / "ds.csv"), "soft_cleaned_csv": str(soft_csv), "output_csv": str(remediated_csv),
            "imputed_darts_linear_final_csv": str(final_csv), "report_json": str(tmp_path / "ds_report.json"),
            "unwrapped": str(wide_input),
        }
        return wide, state, produced, str(prepared)

    def test_every_frame_is_wrapped_and_the_wide_input_is_not_kept(self, narrow, tmp_path: Path):
        from dali.processing import _wrap_artifacts

        wide, state, produced, prepared = self._frames(narrow, tmp_path)
        out, rows, left_out = _wrap_artifacts(produced, state, list(wide.columns), {}, prepared, tmp_path)

        assert "unwrapped" not in out and left_out == {}
        assert set(out) == {"input_csv", "soft_cleaned_csv", "output_csv", "imputed_darts_linear_final_csv",
                            "report_json"}
        assert out["input_csv"] == produced["input_csv"] and out["report_json"] == produced["report_json"]
        for name in ("soft_cleaned_csv", "output_csv", "imputed_darts_linear_final_csv"):
            result = pd.read_csv(out[name], dtype=str, keep_default_na=False)
            assert list(result.columns) == list(narrow.columns), name       # the layout of the file that came in
            assert set(result["gNB_id"]) == {"1193046"} and set(result["source"]) == {"Digital_Twin"}
            assert result["timestamp"].iloc[0] == "2026-05-28T11:36:54.462892+02:00"
            assert rows[name] == len(result)
        # only the observed cells of the soft-cleaned frame; the imputed one is filled throughout
        assert rows["soft_cleaned_csv"] == len(narrow) and rows["imputed_darts_linear_final_csv"] == 5 * 4

    def test_no_generated_series_name_is_left_in_any_stored_frame(self, narrow, tmp_path: Path):
        from dali.processing import _wrap_artifacts

        wide, state, produced, prepared = self._frames(narrow, tmp_path)
        out, _, _ = _wrap_artifacts(produced, state, list(wide.columns), {}, prepared, tmp_path)
        generated = [c for c in wide.columns if c != "timestamp"]
        for name in ("soft_cleaned_csv", "output_csv", "imputed_darts_linear_final_csv"):
            header = pd.read_csv(out[name], nrows=0).columns
            assert not set(header) & set(generated), name

    def test_a_frame_that_cannot_be_wrapped_is_left_out_not_stored_wide(self, narrow, tmp_path: Path):
        from dali.processing import _wrap_artifacts

        wide, state, produced, prepared = self._frames(narrow, tmp_path)
        bad = tmp_path / "bad.csv"
        bad.write_text("a,b\n1,2\n")
        produced["output_csv"] = str(bad)
        out, rows, left_out = _wrap_artifacts(produced, state, list(wide.columns), {}, prepared, tmp_path)
        assert "output_csv" not in out and "output_csv" in left_out
        assert "soft_cleaned_csv" in out                                    # the others still are

    def test_the_reports_time_column_name_is_used(self, narrow, tmp_path: Path):
        from dali.processing import _wrap_artifacts

        wide, state, produced, prepared = self._frames(narrow, tmp_path)
        out, _, left_out = _wrap_artifacts(
            produced, state, list(wide.columns), {"validation": {"timestamp_column": "timestamp"}}, prepared, tmp_path)
        assert left_out == {} and "soft_cleaned_csv" in out


class TestSeriesLayoutFromCatalogue:
    """The layout a distribution declares in piveau, and the precedence against the run's own params."""

    NODE = {
        "@id": "https://dataspace.6gdali.eu/distributions/64d5",
        "@type": "dcat:Distribution",
        "https://dali-project.eu/ns#assetId": "5953562c",
        "https://dali-project.eu/ns#seriesHierarchy": FIELDS,
        "https://dali-project.eu/ns#ignoredColumn": ["source", {"@value": "gNB_id"}],
        "https://dali-project.eu/ns#valueColumn": "value",
    }

    def test_read_from_full_iris(self):
        from dali.utils import series_layout_of

        assert series_layout_of(self.NODE) == {
            "hierarchy": [FIELDS], "separator": "_", "ignore_columns": ["source", "gNB_id"], "value_col": "value", "measure_cols": [], "timestamp_col": None, "carry_columns": []}

    def test_read_from_prefixed_keys_and_a_single_ignored_value(self):
        from dali.utils import series_layout_of

        node = {"@type": "dcat:Distribution", "dali:seriesHierarchy": {"@value": "{a}_{b}"},
                "dali:ignoredColumn": "x", "dali:valueColumn": "reading"}
        assert series_layout_of(node) == {
            "hierarchy": ["{a}_{b}"], "separator": "_", "ignore_columns": ["x"], "value_col": "reading", "measure_cols": [], "timestamp_col": None, "carry_columns": []}

    def test_no_hierarchy_means_no_layout(self):
        from dali.utils import series_layout_of

        assert series_layout_of({"@type": "dcat:Distribution", "dali:ignoredColumn": "x"}) is None

    def test_value_column_defaults(self):
        from dali.utils import series_layout_of

        assert series_layout_of({"dali:seriesHierarchy": "{a}_{b}"})["value_col"] == "value"

    def test_fetch_targets_the_distribution_by_asset_id(self, monkeypatch):
        from dali import utils

        other = {"@id": "x/other", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "other",
                 "https://dali-project.eu/ns#seriesHierarchy": "{a}_{b}"}
        dataset_node = {"@id": "d", "@type": ["dcat:Dataset"], "dali:seriesHierarchy": "{z}_{y}"}   # never read
        body = json.dumps({"@graph": [dataset_node, other, self.NODE]}).encode()
        seen = {}

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return body

        def fake_urlopen(request, timeout=0):
            seen["url"], seen["key"] = request.full_url, request.get_header("X-api-key")
            return Resp()

        monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)
        monkeypatch.setenv("PIVEAU_API_KEY", "k")
        layout = utils.fetch_series_layout("ds1", "cat1", "5953562c")
        assert layout["hierarchy"] == [FIELDS]
        assert seen["url"].endswith("/ds1?catalogue=cat1") and seen["key"] == "k"
        assert utils.fetch_series_layout("ds1", "cat1", "nope") is None
        assert utils.fetch_series_layout("ds1", "cat1", "") is None        # two distributions, none named

    def test_an_unreadable_catalogue_means_no_layout(self, monkeypatch):
        from dali import utils

        def boom(*a, **k):
            raise OSError("down")

        monkeypatch.setattr(utils.urllib.request, "urlopen", boom)
        assert utils.fetch_series_layout("ds1") is None

    def test_only_the_catalogue_names_the_layout(self, monkeypatch):
        from dali import processing

        monkeypatch.setattr(processing, "fetch_series_layout",
                            lambda *a: {"hierarchy": "{x}_{y}", "ignore_columns": [], "value_col": "value"})
        declared = processing.resolve_series_layout({"dataset_id": "d", "catalogue_id": "c", "asset_id": "a"})
        assert declared["hierarchy"] == "{x}_{y}" and declared["source"] == "piveau"
        # a hierarchy in the run's conf is not read
        stray = processing.resolve_series_layout({"dataset_id": "d", "hierarchy": HIERARCHY, "ignore_columns": ["a"]})
        assert stray["hierarchy"] == "{x}_{y}" and stray["ignore_columns"] == []

    def test_nothing_declared_nothing_unwrapped(self, monkeypatch):
        from dali import processing

        monkeypatch.setattr(processing, "fetch_series_layout", lambda *a: None)
        assert processing.resolve_series_layout({"dataset_id": "d"}) is None
        assert processing.resolve_series_layout({}) is None


class TestSetSeriesLayoutScript:
    SCRIPTS = str(Path(__file__).resolve().parents[1] / "scripts")

    def _graph(self):
        return {"@graph": [
            {"@id": "d", "@type": ["dcat:Dataset"]},
            {"@id": "x1", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "a1"},
            {"@id": "x2", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "a2",
             "https://dali-project.eu/ns#ignoredColumn": ["old"]},
        ]}

    def test_sets_only_the_named_distribution_never_the_dataset(self):
        sys.path.insert(0, self.SCRIPTS)
        from set_series_layout import apply_layout
        from dali.utils import series_layout_of

        graph = self._graph()
        change = apply_layout(graph, "a1", FIELDS, IGNORE, "value")
        assert change["before"] == {}
        assert series_layout_of(graph["@graph"][1]) == {
            "hierarchy": [FIELDS], "separator": "_", "ignore_columns": IGNORE, "value_col": "value", "measure_cols": [], "timestamp_col": None, "carry_columns": []}
        assert series_layout_of(graph["@graph"][2]) is None                 # the other distribution is untouched
        assert graph["@graph"][2]["https://dali-project.eu/ns#ignoredColumn"] == ["old"]
        assert not any(k.startswith("https://dali-project.eu/ns#") for k in graph["@graph"][0])   # nor the dataset

    def test_replaces_rather_than_merges_and_clears(self):
        sys.path.insert(0, self.SCRIPTS)
        from set_series_layout import apply_layout

        graph = self._graph()
        apply_layout(graph, "a2", HIERARCHY, ["new"], "value")
        assert graph["@graph"][2]["https://dali-project.eu/ns#ignoredColumn"] == ["new"]
        apply_layout(graph, "a2", None, [], None)
        assert not any(k.startswith("https://dali-project.eu/ns#") and not k.endswith("assetId")
                       for k in graph["@graph"][2])

    def test_reads_the_layout_from_a_metadata_file(self, tmp_path):
        sys.path.insert(0, self.SCRIPTS)
        import json
        from set_series_layout import apply_layout, layout_from_metadata
        from dali.utils import series_layout_of

        meta = tmp_path / "metadata.json"
        meta.write_text(json.dumps({"title": "t", "series_layout": {"throughput.csv": {
            "hierarchy": ["phase", "cell_id", "ue_id", "metric_id"], "separator": "_",
            "measure_columns": ["dl", "ul", "cqi"], "timestamp_column": "timestamp",
            "carry_columns": [], "ignored_columns": []}}}))
        layout = layout_from_metadata(str(meta))                            # one entry: no --file needed
        assert layout["hierarchy"] == "phase,cell_id,ue_id,metric_id" and layout["measure"] == ["dl", "ul", "cqi"]
        graph = self._graph()
        apply_layout(graph, "a1", layout["hierarchy"], layout["ignore"], layout["value_col"] or "value",
                     layout["separator"], layout["measure"], layout["timestamp_col"], layout["carry"])
        got = series_layout_of(graph["@graph"][1])
        assert got["hierarchy"] == ["phase,cell_id,ue_id,metric_id"] and got["measure_cols"] == ["dl", "ul", "cqi"]
        assert got["timestamp_col"] == "timestamp" and got["carry_columns"] == []

    def test_a_metadata_file_without_exactly_one_layout_is_refused(self, tmp_path):
        sys.path.insert(0, self.SCRIPTS)
        import json
        from set_series_layout import layout_from_metadata

        block = {"hierarchy": "a,metric_id", "measure_columns": ["x"]}
        meta = tmp_path / "metadata.json"
        meta.write_text(json.dumps({"series_layout": {"one.csv": block, "two.csv": block}}))
        with pytest.raises(SystemExit):
            layout_from_metadata(str(meta))                                  # two entries: the file must be named
        assert layout_from_metadata(str(meta), "two.csv")["hierarchy"] == "a,metric_id"
        with pytest.raises(SystemExit):
            layout_from_metadata(str(meta), "missing.csv")
        empty = tmp_path / "none.json"
        empty.write_text(json.dumps({"title": "t"}))
        with pytest.raises(SystemExit):
            layout_from_metadata(str(empty))                                 # no layout at all

    def test_unknown_asset(self):
        sys.path.insert(0, self.SCRIPTS)
        from set_series_layout import apply_layout

        with pytest.raises(SystemExit):
            apply_layout(self._graph(), "nope", HIERARCHY, [], "value")


class TestSeveralHierarchies:
    LIST = "{cell_id}_{metric_id},{cell_id}_{ue_id}_{metric_id}"

    def test_parse(self):
        assert parse_hierarchies(self.LIST) == [
            (["cell_id", "metric_id"], "_"), (["cell_id", "ue_id", "metric_id"], "_")]
        assert parse_hierarchies(f" {self.LIST} , ") == parse_hierarchies(self.LIST)
        assert parse_hierarchies([HIERARCHY]) == [(["cell_id", "ue_id", "metric_id"], "_")]

    def test_templates_must_share_the_metric_field(self):
        with pytest.raises(UnwrapError, match="same metric field"):
            parse_hierarchies("{a}_{b}_{metric_id},{a}_{b}_{type}")
        with pytest.raises(UnwrapError, match="empty"):
            parse_hierarchies(" , ")

    def test_each_row_takes_the_template_that_fits_it(self, narrow):
        wide, state = unwrap(narrow, self.LIST, IGNORE)
        assert "3_4_DRB.UEBlerDl" in wide.columns and "3_DRB.UEThpDl" in wide.columns
        assert state.stats["hierarchies"] == ["{cell_id}_{metric_id}", "{cell_id}_{ue_id}_{metric_id}"]

    def test_same_result_as_one_template_with_absent_levels_left_out(self, narrow):
        listed, _ = unwrap(narrow, self.LIST, IGNORE)
        single, _ = unwrap(narrow, HIERARCHY, IGNORE)
        pd.testing.assert_frame_equal(listed, single)

    def test_round_trip(self, narrow):
        wide, state = unwrap(narrow, self.LIST, IGNORE)
        pd.testing.assert_frame_equal(_canon(wrap(wide, state)), _canon(narrow))

    def test_a_ue_row_is_not_swallowed_by_the_shorter_template(self, narrow):
        # listed shortest first: the UE row must still get the three-field name, or UEs of one cell would collide
        wide, _ = unwrap(narrow, self.LIST, IGNORE)
        assert "3_4_Viavi.Geo.x" in wide.columns

    def test_catalogue_value_as_list_or_commas(self):
        from dali.utils import series_layout_of

        a = series_layout_of({"dali:seriesHierarchy": self.LIST})
        b = series_layout_of({"dali:seriesHierarchy": ["{cell_id}_{metric_id}",
                                                       {"@value": "{cell_id}_{ue_id}_{metric_id}"}]})
        assert parse_hierarchies(a["hierarchy"]) == parse_hierarchies(b["hierarchy"]) == parse_hierarchies(self.LIST)


class TestOrderedFieldList:
    """dali:seriesHierarchy as the parts in order, comma-separated, with an optional dali:seriesSeparator."""

    def test_parse_the_parts_and_the_separator(self):
        assert parse_hierarchies(FIELDS) == [(["cell_id", "ue_id", "metric_id"], "_")]
        assert parse_hierarchies(" cell_id , ue_id,metric_id ") == parse_hierarchies(FIELDS)
        assert parse_hierarchies(FIELDS, ".") == [(["cell_id", "ue_id", "metric_id"], ".")]

    def test_several_values_are_several_hierarchies(self):
        assert parse_hierarchies(["cell_id,metric_id", "cell_id,ue_id,metric_id"]) == [
            (["cell_id", "metric_id"], "_"), (["cell_id", "ue_id", "metric_id"], "_")]

    @pytest.mark.parametrize("bad", ["cell_id", "cell_id,cell_id,metric_id"])
    def test_rejects(self, bad):
        with pytest.raises(UnwrapError):
            parse_hierarchies(bad)

    @pytest.mark.parametrize("sep", ["", "{", "a,b"])
    def test_rejects_a_bad_separator(self, sep):
        with pytest.raises(UnwrapError, match="separator"):
            parse_hierarchies(FIELDS, sep)

    def test_same_series_as_the_template_form(self, narrow):
        fields, _ = unwrap(narrow, FIELDS, IGNORE)
        template, _ = unwrap(narrow, HIERARCHY, IGNORE)
        pd.testing.assert_frame_equal(fields, template)

    def test_the_separator_joins_the_names(self, narrow):
        wide, state = unwrap(narrow, FIELDS, IGNORE, separator=".")
        assert "3.4.DRB.UEBlerDl" in wide.columns and "1.DRB.UEThpDl" in wide.columns
        pd.testing.assert_frame_equal(_canon(wrap(wide, state)), _canon(narrow))

    def test_the_separator_from_the_catalogue_reaches_the_run(self, monkeypatch):
        from dali import processing

        monkeypatch.setattr(processing, "fetch_series_layout",
                            lambda *a: {"hierarchy": FIELDS, "separator": ".", "ignore_columns": [], "value_col": "value"})
        layout = processing.resolve_series_layout({"dataset_id": "d"})
        assert layout["separator"] == "." and layout["source"] == "piveau"

    def test_the_script_writes_the_separator_only_when_given(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        from set_series_layout import apply_layout

        graph = {"@graph": [{"@id": "x", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "a"}]}
        assert "seriesSeparator" not in apply_layout(graph, "a", FIELDS, IGNORE, "value")["after"]
        assert apply_layout(graph, "a", FIELDS, IGNORE, "value", ".")["after"]["seriesSeparator"] == "."


def test_levels_list_the_choices_for_picking_one_series(narrow):
    _, state = unwrap(narrow, FIELDS, ignore=IGNORE)
    levels = state.levels()
    assert levels["fields"] == ["cell_id", "ue_id", "metric_id"]
    assert levels["values"][0] == ["1", "3"]
    assert levels["values"][1] == ["N/A", "4"]                      # absent first, then numbers
    assert ["3", "4", "DRB.UEBlerDl"] in levels["series"]
    assert ["1", "N/A", "DRB.UEThpDl"] in levels["series"]
    assert len(levels["series"]) == state.stats["series"]
    json.dumps(levels)                                              # plain JSON, it goes in the report


# ── Panels: several entities on their own clocks, one column per measure ────────────────────────────────────────

PANEL = """id,entity_id,name,temp,hum,lat,loc,measured_at,inserted_at
1,st:1,Alpha,20.5,50.0,37.0,POINT (22.0 37.0),2026-09-10 20:00:00.000 +0300,2026-09-10 22:00:00.000 +0300
2,st:2,Beta,21.5,55.0,38.0,POINT (23.0 38.0),2026-09-10 20:00:30.000 +0300,2026-09-10 22:00:00.000 +0300
3,st:1,Alpha,21.5,,37.1,POINT (22.1 37.1),2026-09-10 21:00:00.000 +0300,2026-09-10 22:00:00.000 +0300
4,st:2,Beta 2,22.5,56.0,38.1,POINT (23.1 38.1),2026-09-10 22:00:30.000 +0300,2026-09-10 23:00:00.000 +0300
"""
MEASURES = ["temp", "hum", "lat"]


@pytest.fixture
def panel() -> pd.DataFrame:
    return pd.read_csv(io.StringIO(PANEL), dtype=str, keep_default_na=False)


def _unwrap_panel(panel):
    from dali.unwrap import unwrap_panel

    return unwrap_panel(panel, "entity_id,metric_id", MEASURES, timestamp_col="measured_at", carry=["name"])


class TestPanel:
    def test_one_series_per_entity_and_measure_on_the_union_of_the_clocks(self, panel):
        wide, state = _unwrap_panel(panel)
        assert list(wide.columns) == ["measured_at"] + [f"st:{e}_{m}" for e in (1, 2) for m in sorted(MEASURES)]
        assert len(wide) == 4                                              # four distinct times
        assert state.stats["series"] == 6 and state.stats["layout"] == "panel"
        assert state.stats["narrow_rows"] == 4 and state.stats["melted_rows"] == 11    # one empty `hum` is no observation
        assert wide.loc[0, "st:1_temp"] == 20.5 and pd.isna(wide.loc[0, "st:2_temp"])

    def test_the_choices_are_the_entities_and_the_measures(self, panel):
        _, state = _unwrap_panel(panel)
        levels = state.levels()
        assert levels["fields"] == ["entity_id", "metric_id"]
        assert levels["values"] == [["st:1", "st:2"], ["hum", "lat", "temp"]]

    def test_a_round_trip_returns_the_file_as_it_came(self, panel):
        from dali.unwrap import wrap_panel

        wide, state = _unwrap_panel(panel)
        back = wrap_panel(wide, state, panel)
        assert list(back.columns) == list(panel.columns)
        assert len(back) == len(panel)
        key = ["entity_id", "measured_at"]
        merged = panel.merge(back, on=key, suffixes=("_o", "_b"))
        assert len(merged) == 4
        for column in ("id", "name", "loc", "inserted_at"):
            assert (merged[f"{column}_o"] == merged[f"{column}_b"]).all(), column
        for column in MEASURES:                                            # numbers come back as numbers; empty stays empty
            original = pd.to_numeric(merged[f"{column}_o"].replace("", None))
            pd.testing.assert_series_equal(original, pd.to_numeric(merged[f"{column}_b"]), check_names=False)

    def test_an_imputed_row_has_its_clock_style_its_name_and_empty_row_columns(self, panel):
        from dali.unwrap import wrap_panel

        wide, state = _unwrap_panel(panel)
        filled = wide.copy()
        series = [c for c in filled.columns if c != "measured_at"]
        filled[series] = filled[series].ffill().bfill()
        full = wrap_panel(filled, state, panel)
        assert len(full) == 8                                              # 2 entities x 4 times
        added = full[~full.set_index(["entity_id", "measured_at"]).index.isin(
            panel.set_index(["entity_id", "measured_at"]).index)]
        assert len(added) == 4
        assert added["measured_at"].str.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+0300$").all()
        assert (added["id"] == "").all() and (added["inserted_at"] == "").all() and (added["loc"] == "").all()
        assert set(added["name"][added["entity_id"] == "st:1"]) == {"Alpha"}
        beta = added[added["entity_id"] == "st:2"]
        assert list(beta["name"]) == ["Beta", "Beta"]                      # the entity's previous name, else the next one
        assert (added[["temp", "hum", "lat"]].astype(float).notna()).all().all()

    def test_the_state_travels_as_json(self, panel):
        from dali.unwrap import UnwrapState, wrap_panel

        wide, state = _unwrap_panel(panel)
        again = UnwrapState.from_dict(json.loads(json.dumps(state.to_dict())))
        assert again.panel == state.panel
        assert len(wrap_panel(wide, again, panel)) == 4

    @pytest.mark.parametrize("hierarchy, measures, message", [
        ("entity_id,name", MEASURES, "is a column of the file"),
        ("entity_id,metric_id", ["temp", "nope"], "no column"),
        ("entity_id,metric_id", [], "measure columns"),
        ("entity_id,metric_id", ["temp", "entity_id"], "both a measure"),
    ])
    def test_rejects(self, panel, hierarchy, measures, message):
        from dali.unwrap import unwrap_panel

        with pytest.raises(UnwrapError, match=message):
            unwrap_panel(panel, hierarchy, measures, timestamp_col="measured_at")

    def test_a_carried_column_must_be_an_extra_column(self, panel):
        from dali.unwrap import unwrap_panel

        with pytest.raises(UnwrapError, match="carried"):
            unwrap_panel(panel, "entity_id,metric_id", MEASURES, timestamp_col="measured_at", carry=["temp"])

    def test_the_layout_properties_are_read_from_the_catalogue(self):
        from dali.utils import series_layout_of

        node = {"dali:seriesHierarchy": "entity_id,metric_id", "dali:measureColumn": ["temp", {"@value": "hum"}],
                "dali:timestampColumn": "measured_at", "dali:carryColumn": "name"}
        layout = series_layout_of(node)
        assert layout["measure_cols"] == ["temp", "hum"] and layout["timestamp_col"] == "measured_at"
        assert layout["carry_columns"] == ["name"]

    def test_every_frame_is_stored_in_the_panels_layout(self, panel, tmp_path: Path):
        from dali.processing import _wrap_artifacts

        wide, state = _unwrap_panel(panel)
        raw = tmp_path / "raw.csv"
        raw.write_text(PANEL, encoding="utf-8")
        remediated = tmp_path / "output.csv"
        wide.to_csv(remediated, index=False)
        produced = {"input_csv": str(raw), "output_csv": str(remediated)}
        out, rows, left_out = _wrap_artifacts(produced, state, list(wide.columns), {}, None, tmp_path)
        assert left_out == {} and rows == {"output_csv": 4}
        stored = pd.read_csv(out["output_csv"], dtype=str, keep_default_na=False)
        assert list(stored.columns) == list(panel.columns)
        assert out["input_csv"] == str(raw)                                # what arrived is not rewritten
